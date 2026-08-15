// license:GPLv3+

#include "plugins/VPXPlugin.h"
#include "plugins/B2SPluginEventStream.h"
#include "plugins/ControllerPlugin.h"
#include "pinmame/PinMAMEPlugin.h"
#include "pinmame/libpinmame.h"
#include "nlohmann/json.hpp"

#include <thread>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <fstream>
#include <filesystem>
#include <map>
#include <cmath>
#include <algorithm>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#endif

using namespace std;

///////////////////////////////////////////////////////////////////////////////
//
// Cabinet Bridge plugin -- forwards live PinMAME switch/lamp/solenoid/GI/mech
// state to the pinball_cab dashboard's cabinet event bus (POST /api/emit,
// localhost only), so the dashboard's "Cabinet event bus" panel and the
// dedicated Event Bus View overlay window show real gameplay events instead
// of only the demo Simulate buttons. See CLAUDE.md "Live gameplay event
// capture" for the full design writeup.
//

namespace CabinetBridgePlugin {

static const MsgPluginAPI* msgApi = nullptr;
static uint32_t endpointId;

static std::unique_ptr<B2SPluginEventStream> b2sPluginEventStream;

// Maps B2SPluginEventStream's single-letter event type (see B2SPluginEventStream.h) to what
// the dashboard's event bus already knows how to color/display (.tag.switch/.lamp/.gi/.mech/
// .solenoid/.score in cabinet-dashboard/public/index.html + eventbus.html). D (DMD/segment
// display frame ids) and the B2S-controller-specific E/B are deliberately NOT forwarded: DMD
// frame updates fire far too often (every rendered frame) to be useful as discrete log lines,
// E is a generic backglass input (not game state), and B is the PER-DIGIT score-reel update
// that drives C -- same "too noisy to be a useful discrete line" problem as D, one event per
// digit per change instead of one per actual score change.
// C (the actual running score, event->index = player number, event->value = score) IS
// forwarded as "score" -- broadcast by the table's own .directb2s B2S controller whenever a
// player's score changes (OnB2SStateChange in B2SPluginEventStream.cpp), so this only fires
// for tables with a real backglass loaded, same as the on-screen score reels it drives.
static const char* TypeName(char c)
{
   switch (c) {
      case 'W': return "switch";
      case 'N': return "mech";
      case 'L': return "lamp";
      case 'S': return "solenoid";
      case 'G': return "gi";
      case 'C': return "score";
      default: return nullptr; // not forwarded
   }
}

struct QueuedEvent { char type; int id; int value; };

// Batches events over a short window before sending one HTTP POST per batch, rather than one
// POST per individual event: GI fades and multiplexed solenoid drive can fire many state
// changes within a single frame, and POSTing synchronously per-event on this thread would
// either back up unboundedly (async) or visibly stall the queue (sync, waiting on the
// dashboard's response each time). A dedicated thread owns the queue + a plain timed-wait
// batching loop -- same "get real work off the event-stream thread" shape as DOFPlugin's
// DOFEventConsumer, adapted for batched HTTP instead of one synchronous hardware call per
// event. server.js's /api/emit accepts either a single event object or an array -- see its
// comment for why (this plugin is the reason it accepts arrays at all).
class HttpSender
{
public:
   HttpSender() { m_thread = std::thread(&HttpSender::Run, this); }
   ~HttpSender()
   {
      {
         std::lock_guard<std::mutex> lock(m_mutex);
         m_stopRequested = true;
      }
      m_cv.notify_one();
      if (m_thread.joinable())
         m_thread.join();
   }
   HttpSender(const HttpSender&) = delete;
   HttpSender& operator=(const HttpSender&) = delete;

   void PostEvent(char type, int id, int value)
   {
      if (!TypeName(type))
         return; // not a forwarded type -- skip queuing it at all
      {
         std::lock_guard<std::mutex> lock(m_mutex);
         m_queue.push_back({ type, id, value });
      }
      m_cv.notify_one();
   }

private:
   void Run()
   {
      while (true)
      {
         std::vector<QueuedEvent> batch;
         {
            std::unique_lock<std::mutex> lock(m_mutex);
            m_cv.wait_for(lock, std::chrono::milliseconds(30), [this] { return !m_queue.empty() || m_stopRequested; });
            if (m_queue.empty()) {
               if (m_stopRequested)
                  return;
               continue;
            }
            batch.swap(m_queue);
         }
         SendBatch(batch);
      }
   }

   static void AppendEscaped(std::string& out, const std::string& s)
   {
      for (char c : s) {
         if (c == '"' || c == '\\')
            out += '\\';
         out += c;
      }
   }

   static void SendBatch(const std::vector<QueuedEvent>& batch)
   {
      std::string json = "[";
      bool first = true;
      for (const QueuedEvent& ev : batch) {
         const char* typeName = TypeName(ev.type);
         if (!typeName)
            continue;
         if (!first)
            json += ",";
         first = false;
         // Score events get a readable "P<n>" tag (event->index is the player number) and a
         // "score=" detail label instead of the generic "<type><id>"/"value=" every other
         // forwarded type uses -- "C0 value=14500" reads a lot less clearly on the event bus
         // than "P1 score=14500" for the one type that's actually meant to be read as a number
         // going up, not just a discrete state change.
         char tag[16];
         if (ev.type == 'C')
            snprintf(tag, sizeof(tag), "P%d", ev.id + 1);
         else
            snprintf(tag, sizeof(tag), "%c%d", ev.type, ev.id);
         json += "{\"type\":\"";
         json += typeName;
         json += "\",\"tag\":\"";
         AppendEscaped(json, tag);
         json += "\",\"label\":\"\",\"detail\":\"";
         json += (ev.type == 'C') ? "score=" : "value=";
         json += std::to_string(ev.value);
         json += "\"}";
      }
      json += "]";
      if (!first) // at least one event actually got written
         PostJson("/api/emit", json);
   }

   // A minimal, blocking HTTP/1.1 POST over a raw TCP socket to 127.0.0.1:7333 -- no TLS, no
   // libcurl dependency (nothing else in this plugin tree links it), since the destination is
   // always localhost. Short connect/read timeouts + silent drop-on-failure so a dashboard
   // that isn't running (or briefly restarting) can never stall this thread.
public:
   // Public + path-parameterised because the audio meter below posts to a DIFFERENT endpoint
   // (/api/audio-levels, not the event bus) over the same tiny localhost socket helper.
   // Minimal HTTP/1.1 GET returning just the body, same raw-socket approach and 200ms timeouts
   // as PostJson below -- the gain poller needs to READ from the dashboard, which every other
   // path in this plugin never had to do.
   static std::string GetBody(const char* path)
   {
      std::string resp;
#ifdef _WIN32
      SOCKET sock = socket(AF_INET, SOCK_STREAM, 0);
      if (sock == INVALID_SOCKET) return resp;
      DWORD toMs = 200;
      setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&toMs, sizeof(toMs));
      setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, (const char*)&toMs, sizeof(toMs));
#else
      int sock = socket(AF_INET, SOCK_STREAM, 0);
      if (sock < 0) return resp;
      struct timeval to { 0, 200000 };
      setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &to, sizeof(to));
      setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &to, sizeof(to));
#endif
      sockaddr_in addr {};
      addr.sin_family = AF_INET;
      addr.sin_port = htons(7333);
      inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
      if (connect(sock, (sockaddr*)&addr, sizeof(addr)) == 0) {
         const std::string req = std::string("GET ") + path + " HTTP/1.1\r\n"
            "Host: localhost\r\nConnection: close\r\n\r\n";
         send(sock, req.c_str(), (int)req.size(), 0);
         char buf[2048];
         int n;
         while ((n = (int)recv(sock, buf, sizeof(buf), 0)) > 0)
            resp.append(buf, n);
      }
#ifdef _WIN32
      closesocket(sock);
#else
      close(sock);
#endif
      const size_t sep = resp.find("\r\n\r\n");
      if (sep == std::string::npos)
         return std::string();
      const std::string body = resp.substr(sep + 4);
      // Node sends these small responses CHUNKED, so the raw body is "3b\r\n{...}\r\n0\r\n\r\n"
      // and feeding it straight to a JSON parser throws on the chunk-size line. Rather than
      // implement de-chunking for one tiny endpoint, take the outermost JSON object. Verified
      // against the real server: without this the poller silently caught, continued, and the
      // whole live-control path did nothing while looking perfectly healthy.
      const size_t open = body.find('{'), close = body.rfind('}');
      if (open == std::string::npos || close == std::string::npos || close < open)
         return std::string();
      return body.substr(open, close - open + 1);
   }

   static void PostJson(const char* path, const std::string& json)
   {
#ifdef _WIN32
      static bool wsaInit = false;
      if (!wsaInit) {
         WSADATA wsaData;
         WSAStartup(MAKEWORD(2, 2), &wsaData);
         wsaInit = true;
      }
      SOCKET sock = socket(AF_INET, SOCK_STREAM, 0);
      if (sock == INVALID_SOCKET)
         return;
      DWORD timeoutMs = 200;
      setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeoutMs, sizeof(timeoutMs));
      setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, (const char*)&timeoutMs, sizeof(timeoutMs));
#else
      int sock = socket(AF_INET, SOCK_STREAM, 0);
      if (sock < 0)
         return;
      struct timeval timeout { 0, 200000 }; // 200ms
      setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
      setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
#endif

      sockaddr_in addr {};
      addr.sin_family = AF_INET;
      addr.sin_port = htons(7333);
      inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

      if (connect(sock, (sockaddr*)&addr, sizeof(addr)) == 0) {
         const std::string req = std::string("POST ") + path + " HTTP/1.1\r\n"
            "Host: localhost\r\n"
            "Content-Type: application/json\r\n"
            "Content-Length: " + std::to_string(json.size()) + "\r\n"
            "Connection: close\r\n\r\n" + json;
#ifdef _WIN32
         send(sock, req.c_str(), (int)req.size(), 0);
         char buf[256];
         recv(sock, buf, sizeof(buf), 0); // drain the response so the connection closes cleanly
#else
         send(sock, req.c_str(), req.size(), 0);
         char buf[256];
         recv(sock, buf, sizeof(buf), 0);
#endif
      }

#ifdef _WIN32
      closesocket(sock);
#else
      close(sock);
#endif
   }

private:
   std::vector<QueuedEvent> m_queue;
   std::mutex m_mutex;
   std::condition_variable m_cv;
   bool m_stopRequested = false;
   std::thread m_thread;
};

static std::unique_ptr<HttpSender> sender;

///////////////////////////////////////////////////////////////////////////////
//
// Live audio lane metering.
//
// PinMAME, PUP and AltSound each BROADCAST CTLPI_AUDIO_ON_UPDATE_MSG carrying their raw sample
// buffer; player.cpp subscribes to that same broadcast to actually play it. We subscribe purely
// to MEASURE it. Nothing here touches the engine core and nothing here affects what is heard --
// the meter is a passive tap on a message that was already being sent.
//
// SCOPE: this is PLUGIN audio only, i.e. the backglass bus. The table's own SFX and music are
// played through VPX's internal SoundPlayer/MusicPlayer and never appear as an AudioUpdate, so
// they have no live meter. See docs/decisions/audio-streams.md.
//
// Levels are PRE-lane-gain: we scale by msg.volume (the stream's own volume, which is where
// per-clip PuP volumes above 100% show up) but NOT by AudioSource.<id>.Gain or MusicVolume,
// because those live host-side. The dashboard applies them, so the panel can show pre- and
// post-fader from one number without this plugin having to know the mixer state.
class AudioMeter
{
public:
   AudioMeter() { m_thread = std::thread(&AudioMeter::Run, this); }
   ~AudioMeter()
   {
      {
         std::lock_guard<std::mutex> lock(m_mutex);
         m_stopRequested = true;
      }
      m_cv.notify_one();
      if (m_thread.joinable())
         m_thread.join();
   }
   AudioMeter(const AudioMeter&) = delete;
   AudioMeter& operator=(const AudioMeter&) = delete;

   // Called on the plugin API thread, once per enqueued buffer. Kept to a tight arithmetic loop
   // plus one short lock: this runs in the audio path's critical section and must never block.
   void Accumulate(const AudioUpdateMsg& msg)
   {
      if (msg.buffer == nullptr || msg.bufferSize == 0)
         return;
      double sumSq = 0.0, peak = 0.0;
      size_t n = 0;
      // bufferSize is BYTES -- it is handed straight to SDL_PutAudioStreamData downstream.
      if (msg.sampleFormat == CTLPI_AUDIO_FORMAT_SAMPLE_INT16) {
         n = msg.bufferSize / sizeof(int16_t);
         const int16_t* p = reinterpret_cast<const int16_t*>(msg.buffer);
         for (size_t i = 0; i < n; ++i) {
            const double v = p[i] / 32768.0;
            sumSq += v * v;
            if (std::abs(v) > peak) peak = std::abs(v);
         }
      }
      else if (msg.sampleFormat == CTLPI_AUDIO_FORMAT_SAMPLE_FLOAT) {
         n = msg.bufferSize / sizeof(float);
         const float* p = reinterpret_cast<const float*>(msg.buffer);
         for (size_t i = 0; i < n; ++i) {
            const double v = p[i];
            sumSq += v * v;
            if (std::abs(v) > peak) peak = std::abs(v);
         }
      }
      else
         return; // unknown sample format -- measuring it would be worse than not measuring it
      if (n == 0)
         return;
      const double vol = msg.volume;
      std::lock_guard<std::mutex> lock(m_mutex);
      Lane& lane = m_lanes[msg.sourceId.id];
      lane.sumSq += sumSq * vol * vol;
      lane.samples += n;
      lane.peak = std::max(lane.peak, peak * vol);
   }

   // Called on the plugin API thread when the source list changes (and once at load), mirroring
   // player.cpp's own OnAudioSrcChanged. Resolving names here rather than in the flush thread
   // keeps every msgApi call on the API thread, which the host asserts on.
   void RefreshNames()
   {
      std::vector<AudioSrcId> srcs;
      GetCtrlItems<AudioSrcId>(msgApi, endpointId, m_getSrcId, srcs);
      std::lock_guard<std::mutex> lock(m_mutex);
      m_names.clear();
      for (const auto& s : srcs)
         m_names[s.id.id] = (s.name && *s.name) ? s.name : "audio";
   }

   void SetGetSrcId(unsigned int id) { m_getSrcId = id; }

private:
   struct Lane { double sumSq = 0.0; double peak = 0.0; uint64_t samples = 0; };

   void Run()
   {
      // ~20Hz. Fast enough that a callout visibly spikes its lane, slow enough that an idle
      // cabinet isn't posting constantly -- and a tick with no audio at all posts nothing.
      while (true) {
         std::map<uint64_t, Lane> lanes;
         std::map<uint64_t, std::string> names;
         {
            std::unique_lock<std::mutex> lock(m_mutex);
            m_cv.wait_for(lock, std::chrono::milliseconds(50), [this] { return m_stopRequested; });
            if (m_stopRequested)
               return;
            lanes.swap(m_lanes);
            names = m_names;
         }
         if (lanes.empty())
            continue; // silence: say nothing rather than post a wall of zeroes
         std::string json = "{\"lanes\":[";
         bool first = true;
         for (const auto& [id, lane] : lanes) {
            if (lane.samples == 0)
               continue;
            const double rms = std::sqrt(lane.sumSq / static_cast<double>(lane.samples));
            if (!first) json += ",";
            first = false;
            const auto it = names.find(id);
            char buf[192];
            snprintf(buf, sizeof(buf), "{\"id\":%llu,\"name\":\"%s\",\"rms\":%.5f,\"peak\":%.5f}",
                     static_cast<unsigned long long>(id),
                     it == names.end() ? "audio" : it->second.c_str(), rms, std::min(lane.peak, 4.0));
            json += buf;
         }
         json += "]}";
         if (!first)
            HttpSender::PostJson("/api/audio-levels", json);
      }
   }

   std::map<uint64_t, Lane> m_lanes;
   std::map<uint64_t, std::string> m_names;
   unsigned int m_getSrcId = 0;
   std::mutex m_mutex;
   std::condition_variable m_cv;
   bool m_stopRequested = false;
   std::thread m_thread;
};

static std::unique_ptr<AudioMeter> audioMeter;
static std::mutex gainPollerMutex;
static std::unique_ptr<std::map<std::string, float>> gainPollerPending;
static unsigned int onAudioUpdateId = 0;
static unsigned int onAudioSrcChangedId = 0;
static unsigned int getAudioSrcId = 0;

///////////////////////////////////////////////////////////////////////////////
//
// Live mixer control -- the inbound direction.
//
// Everything else in this plugin pushes data OUT. This pulls: it polls the dashboard for the
// gain each audio lane should have and applies it to the RUNNING table, so moving a fader
// changes the sound immediately instead of at the next launch. VPX's mixer gain was previously
// reachable only from its own in-game audio page.
//
// THREADING. MsgPlugin.h is explicit: "The plugin API is not thread safe", and RunOnMainThread
// is the ONLY method callable from any thread. So the poll (a blocking socket read) happens on
// our own thread, and the actual BroadcastMsg is marshalled onto the main thread. Calling
// BroadcastMsg straight from the poller would be a race against the host's own audio bookkeeping.
//
// Declarative, not command-based: we poll the DESIRED state and push whatever differs from what
// we last applied. A dropped poll self-heals on the next tick, and nothing has to queue,
// sequence or retry individual commands.
class GainPoller
{
public:
   GainPoller() { m_thread = std::thread(&GainPoller::Run, this); }
   ~GainPoller()
   {
      {
         std::lock_guard<std::mutex> lock(m_mutex);
         m_stopRequested = true;
      }
      m_cv.notify_one();
      if (m_thread.joinable())
         m_thread.join();
   }
   GainPoller(const GainPoller&) = delete;
   GainPoller& operator=(const GainPoller&) = delete;

   // Runs on the MAIN thread, via RunOnMainThread.
   static void ApplyPending(void* /*userData*/)
   {
      if (!msgApi || !gainPollerPending)
         return;
      std::map<std::string, float> pending;
      {
         std::lock_guard<std::mutex> lock(gainPollerMutex);
         pending.swap(*gainPollerPending);
      }
      if (pending.empty())
         return;
      // Resolve names to source ids here rather than in the poller: GetCtrlItems talks to the
      // msg API, which is main-thread-only like everything else.
      std::vector<AudioSrcId> srcs;
      GetCtrlItems<AudioSrcId>(msgApi, endpointId, getAudioSrcId, srcs);

      const unsigned int setVolId = msgApi->GetMsgID(CTLPI_NAMESPACE, CTLPI_AUDIO_SET_SRC_VOL_MSG);
      for (const auto& src : srcs) {
         const std::string name = (src.name && *src.name) ? src.name : "";
         const auto it = pending.find(name);
         if (it == pending.end())
            continue;
         SetAudioSrcVolumeMsg msg { src.id, it->second };
         msgApi->BroadcastMsg(endpointId, setVolId, &msg);
      }
      msgApi->ReleaseMsgID(setVolId);
   }

private:
   void Run()
   {
      while (true) {
         {
            std::unique_lock<std::mutex> lock(m_mutex);
            m_cv.wait_for(lock, std::chrono::milliseconds(200), [this] { return m_stopRequested; });
            if (m_stopRequested)
               return;
         }
         const std::string body = HttpSender::GetBody("/api/audio-lane-gains");
         if (body.empty())
            continue;
         std::map<std::string, float> changed;
         try {
            const auto j = nlohmann::json::parse(body);
            if (!j.is_object())
               continue;
            for (auto it = j.begin(); it != j.end(); ++it) {
               if (!it.value().is_number())
                  continue;
               const float v = it.value().get<float>();
               const auto prev = m_applied.find(it.key());
               // Only push real changes: re-broadcasting an unchanged gain 5x a second would
               // fight VPX's own in-game audio page every time someone used it.
               if (prev == m_applied.end() || std::abs(prev->second - v) > 0.0005f) {
                  changed[it.key()] = v;
                  m_applied[it.key()] = v;
               }
            }
         }
         catch (...) {
            continue; // dashboard restarting, or a partial read -- next tick retries
         }
         if (changed.empty())
            continue;
         {
            std::lock_guard<std::mutex> lock(gainPollerMutex);
            if (!gainPollerPending)
               gainPollerPending = std::make_unique<std::map<std::string, float>>();
            for (const auto& [k, v] : changed)
               (*gainPollerPending)[k] = v;
         }
         if (msgApi)
            msgApi->RunOnMainThread(endpointId, 0.0, GainPoller::ApplyPending, nullptr);
      }
   }

   std::map<std::string, float> m_applied;
   std::mutex m_mutex;
   std::condition_variable m_cv;
   bool m_stopRequested = false;
   std::thread m_thread;
};

static std::unique_ptr<GainPoller> gainPoller;

static void OnAudioUpdate(const unsigned int eventId, void* userData, void* msgData)
{
   if (audioMeter && msgData)
      audioMeter->Accumulate(*static_cast<AudioUpdateMsg*>(msgData));
}

static void OnAudioSrcChanged(const unsigned int eventId, void* userData, void* msgData)
{
   if (audioMeter)
      audioMeter->RefreshNames();
}

///////////////////////////////////////////////////////////////////////////////
//
// Live score forwarding via PinMAME's own NVRAM, using the community
// "pinball-memory-maps" per-ROM byte layout format (https://github.com/tomlogic/
// pinball-memory-maps -- formerly pinmame-nvram-maps). See CLAUDE.md "Live gameplay event
// capture" for the full writeup of why this exists: the table's own .directb2s backglass
// data does NOT reliably carry a numeric score at all for DMD/alphanumeric-era tables --
// confirmed live for both tables in this repo (Medieval Madness has no <Scores> block at
// all; Back to the Future's is a 16-char alphanumeric line mirror, not per-player numeric
// reels) -- so B2SPluginEventStream's 'C' event (still forwarded above, for the minority of
// tables whose own script calls B2SSetScorePlayer) never fires for either of ours. NVRAM is
// the actual ROM-authoritative source real tools (PINemHi, cabinet frontends, etc) use.
//

struct ScoreField
{
   int playerNo;
   uint32_t offset; // byte offset into PinmameGetNVRAM's buffer
   int length;       // number of BCD-encoded bytes (2 decimal digits each)
};

// ~/.pinmame/memmaps, matching this project's existing convention (CLAUDE.md: everything
// PinMAME-related lives under ~/.pinmame/) and, deliberately, the SAME directory
// plugins/pinmame/Controller.cpp already scans for a memmap (see its own PinmameSetMemMap
// call) -- one place to drop pinball-memory-maps files, not two.
static std::filesystem::path MemmapsDir()
{
#ifdef _WIN32
   const char* home = std::getenv("USERPROFILE");
#else
   const char* home = std::getenv("HOME");
#endif
   return std::filesystem::path(home ? home : ".") / ".pinmame" / "memmaps";
}

// pinball-memory-maps addresses can be a plain decimal number or a hex string ("0x0010") --
// see its README's "Numbers can appear as decimal values... or hexadecimal values inside of
// strings" note. strtoul with base 0 handles both.
static uint32_t ParseAddr(const nlohmann::json& v)
{
   if (v.is_number_integer())
      return static_cast<uint32_t>(v.get<int64_t>());
   if (v.is_string())
      return static_cast<uint32_t>(std::strtoul(v.get<std::string>().c_str(), nullptr, 0));
   return 0;
}

// Loads romName's score-field byte layout from ~/.pinmame/memmaps. Returns an empty vector
// (score forwarding silently does nothing, same as every other optional piece of this
// plugin) if no memmap is set up for this ROM, or on any parse error -- a malformed/missing
// file must never crash the plugin, just degrade to "no score events."
// Only "scores" array entries labeled "Player <n>" with "bcd" encoding are used -- other
// game_state fields (credits, ball, player_count, ...) and other encodings are deliberately
// left unparsed; this is scoped to exactly what CabinetBridge forwards as "score", not a
// general-purpose memmap reader.
static std::vector<ScoreField> LoadScoreFields(const std::string& romName)
{
   std::vector<ScoreField> fields;
   try {
      const std::filesystem::path dir = MemmapsDir();
      std::ifstream indexFile(dir / "index.json");
      if (!indexFile.is_open())
         return fields;
      nlohmann::json index;
      indexFile >> index;
      if (!index.is_object() || !index.contains(romName) || !index[romName].is_string())
         return fields;

      std::ifstream mapFile(dir / index[romName].get<std::string>());
      if (!mapFile.is_open())
         return fields;
      nlohmann::json map;
      mapFile >> map;

      // fileformat >= 0.7 stores "start" as a CPU address (referencing the platform file's
      // memory_layout), not a raw .nv-file offset -- they only coincide when the NVRAM
      // region's own base address is 0, which both platforms we have maps for (williams-wpc,
      // dataeast) happen to have, but this computes it properly rather than assuming that.
      uint32_t nvramBase = 0;
      if (map.contains("_metadata") && map["_metadata"].contains("platform") && map["_metadata"]["platform"].is_string()) {
         std::ifstream platformFile(dir / "platforms" / (map["_metadata"]["platform"].get<std::string>() + ".json"));
         if (platformFile.is_open()) {
            nlohmann::json platform;
            platformFile >> platform;
            if (platform.contains("memory_layout") && platform["memory_layout"].is_array()) {
               for (const auto& region : platform["memory_layout"]) {
                  if (region.value("type", std::string()) == "nvram") {
                     nvramBase = region.contains("address") ? ParseAddr(region["address"]) : 0;
                     break;
                  }
               }
            }
         }
      }

      if (!map.contains("game_state") || !map["game_state"].contains("scores") || !map["game_state"]["scores"].is_array())
         return fields;

      for (const auto& entry : map["game_state"]["scores"]) {
         if (!entry.contains("label") || !entry["label"].is_string())
            continue;
         if (entry.value("encoding", std::string()) != "bcd")
            continue; // only bcd is decoded -- see comment above
         const std::string label = entry["label"].get<std::string>();
         if (label.rfind("Player ", 0) != 0)
            continue;
         int playerNo = 0;
         try { playerNo = std::stoi(label.substr(7)); } catch (...) { continue; }
         if (playerNo <= 0 || !entry.contains("start"))
            continue;
         const uint32_t addr = ParseAddr(entry["start"]);
         const int length = entry.value("length", 1);
         if (addr < nvramBase || length <= 0)
            continue;
         fields.push_back({ playerNo, addr - nvramBase, length });
      }
   } catch (...) {
      fields.clear(); // malformed/missing memmap files degrade to "no score forwarding", never crash
   }
   return fields;
}

// Big-endian BCD decode per the pinball-memory-maps spec: each byte holds two decimal digits
// (high nibble = tens, low nibble = ones); nibbles A-F count as 0 ("treat the nibbles 0xA to
// 0xF as 0 numerically"). E.g. bytes {0x12,0x34,0x50} -> 123450. Both platforms we currently
// have maps for are big-endian; little-endian isn't implemented since nothing here needs it.
static int DecodeBcd(const uint8_t* nvram, size_t nvramLen, const ScoreField& field)
{
   if (field.offset + static_cast<uint32_t>(field.length) > nvramLen)
      return -1;
   long value = 0;
   for (int i = 0; i < field.length; i++) {
      const uint8_t b = nvram[field.offset + i];
      const int hi = (b >> 4) & 0xF, lo = b & 0xF;
      value = value * 100 + (hi <= 9 ? hi : 0) * 10 + (lo <= 9 ? lo : 0);
   }
   return static_cast<int>(value);
}

// Polls PinmameGetNVRAM -- a full snapshot, not PinmameGetChangedNVRAM's per-byte diff, since
// a multi-byte score field changing across several poll ticks would otherwise risk decoding a
// torn read -- every 200ms, and diffs each configured field's DECODED value (not raw bytes)
// against its last known value, broadcasting a "score" event only on an actual numeric
// change. Links directly against libpinmame (see CMakeLists_plugin_CabinetBridge.txt), same
// as plugins/pinmame/Controller.cpp's own GetNVRAM -- there's no MsgPluginAPI message for
// this, PinmameGetNVRAM/PinmameGetMaxNVRAM are libpinmame's only exposed surface for it.
class ScorePoller
{
public:
   explicit ScorePoller(HttpSender* sender) : m_sender(sender) { m_thread = std::thread(&ScorePoller::Run, this); }
   ~ScorePoller()
   {
      { std::lock_guard<std::mutex> lock(m_mutex); m_stopRequested = true; }
      m_cv.notify_one();
      if (m_thread.joinable())
         m_thread.join();
   }
   ScorePoller(const ScorePoller&) = delete;
   ScorePoller& operator=(const ScorePoller&) = delete;

   void SetFields(std::vector<ScoreField> fields)
   {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_fields = std::move(fields);
      m_lastValues.assign(m_fields.size(), -1);
   }

private:
   void Run()
   {
      while (true) {
         std::vector<ScoreField> fields;
         {
            std::unique_lock<std::mutex> lock(m_mutex);
            m_cv.wait_for(lock, std::chrono::milliseconds(200), [this] { return m_stopRequested; });
            if (m_stopRequested)
               return;
            fields = m_fields;
         }
         if (fields.empty())
            continue;

         const int maxLen = PinmameGetMaxNVRAM();
         if (maxLen <= 0)
            continue;
         std::vector<PinmameNVRAMState> raw(maxLen);
         const int realCount = PinmameGetNVRAM(raw.data());
         if (realCount <= 0)
            continue;
         std::vector<uint8_t> bytes(realCount);
         for (int i = 0; i < realCount; i++)
            bytes[i] = raw[i].currStat;

         std::lock_guard<std::mutex> lock(m_mutex);
         if (fields.size() != m_fields.size())
            continue; // fields changed mid-poll (ROM switch) -- skip this tick, next one is consistent
         for (size_t i = 0; i < fields.size(); i++) {
            const int value = DecodeBcd(bytes.data(), bytes.size(), fields[i]);
            if (value < 0 || value == m_lastValues[i])
               continue;
            m_lastValues[i] = value;
            if (m_sender)
               // Matches B2S's own 'C' event shape (index = playerNo - 1, see TypeName/SendBatch
               // above) so both sources render identically on the dashboard regardless of which
               // one a given table happens to feed.
               m_sender->PostEvent('C', fields[i].playerNo - 1, value);
         }
      }
   }

   HttpSender* m_sender;
   std::mutex m_mutex;
   std::condition_variable m_cv;
   bool m_stopRequested = false;
   std::vector<ScoreField> m_fields;
   std::vector<int> m_lastValues;
   std::thread m_thread;
};

static std::unique_ptr<ScorePoller> scorePoller;
static unsigned int onControllersChangedId = 0;
static unsigned int getControllersId = 0;
static std::string currentRomName;

// Re-resolves the current ROM whenever the controller list changes (same GetControllers /
// "pinmame::<rom>" gameId convention every other plugin here uses to find PinMAME, e.g.
// AltSoundPlugin.cpp's OnControllersChanged) and reloads that ROM's score fields -- so
// switching tables (or a fresh launch with no ROM loaded yet) doesn't keep forwarding a
// stale, wrong previous table's score layout.
static void OnControllersChanged(const unsigned int eventId, void* userData, void* msgData)
{
   std::string romName;
   GetControllersMsg getMsg = { 0, 0, nullptr };
   msgApi->BroadcastMsg(endpointId, getControllersId, &getMsg);
   if (getMsg.count > 0) {
      const std::string pinmamePrefix(PMPI_GAMEID_PREFIX);
      std::vector<ControllerDef> controllers(getMsg.count);
      getMsg = { getMsg.count, 0, controllers.data() };
      msgApi->BroadcastMsg(endpointId, getControllersId, &getMsg);
      for (const auto& controller : controllers) {
         const std::string gameId = controller.gameId;
         if (gameId.starts_with(pinmamePrefix)) {
            romName = gameId.substr(pinmamePrefix.length());
            break;
         }
      }
   }
   if (romName == currentRomName)
      return;
   currentRomName = romName;
   if (scorePoller)
      scorePoller->SetFields(romName.empty() ? std::vector<ScoreField>() : LoadScoreFields(romName));
}

}

using namespace CabinetBridgePlugin;

MSGPI_EXPORT void MSGPIAPI CabinetBridgePluginLoad(const uint32_t sessionId, const MsgPluginAPI* api)
{
   msgApi = api;
   endpointId = sessionId;

   sender = std::make_unique<HttpSender>();
   scorePoller = std::make_unique<ScorePoller>(sender.get());
   b2sPluginEventStream = std::make_unique<B2SPluginEventStream>(msgApi, endpointId, [](char type, int id, int value) {
      if (HttpSender* s = sender.get())
         s->PostEvent(type, id, value);
   });

   onControllersChangedId = msgApi->GetMsgID(CTLPI_NAMESPACE, CTLPI_CONTROLLERS_ON_CHG_MSG);
   getControllersId = msgApi->GetMsgID(CTLPI_NAMESPACE, CTLPI_CONTROLLERS_GET_MSG);
   msgApi->SubscribeMsg(endpointId, onControllersChangedId, OnControllersChanged, nullptr);

   // Passive tap on the audio broadcast PinMAME/PUP/AltSound already send -- see AudioMeter.
   audioMeter = std::make_unique<AudioMeter>();
   onAudioUpdateId = msgApi->GetMsgID(CTLPI_NAMESPACE, CTLPI_AUDIO_ON_UPDATE_MSG);
   onAudioSrcChangedId = msgApi->GetMsgID(CTLPI_NAMESPACE, CTLPI_AUDIO_ON_SRC_CHG_MSG);
   getAudioSrcId = msgApi->GetMsgID(CTLPI_NAMESPACE, CTLPI_AUDIO_GET_SRC_MSG);
   audioMeter->SetGetSrcId(getAudioSrcId);
   msgApi->SubscribeMsg(endpointId, onAudioUpdateId, OnAudioUpdate, nullptr);
   msgApi->SubscribeMsg(endpointId, onAudioSrcChangedId, OnAudioSrcChanged, nullptr);
   audioMeter->RefreshNames(); // sources may already exist if we loaded late

   // Inbound: dashboard fader -> running table's mixer. Started after the ids above exist,
   // because ApplyPending needs getAudioSrcId to resolve names to sources.
   gainPoller = std::make_unique<GainPoller>();
}

MSGPI_EXPORT void MSGPIAPI CabinetBridgePluginUnload()
{
   if (msgApi && onControllersChangedId) {
      msgApi->UnsubscribeMsg(onControllersChangedId, OnControllersChanged, nullptr);
      msgApi->ReleaseMsgID(onControllersChangedId);
      msgApi->ReleaseMsgID(getControllersId);
   }
   if (msgApi && onAudioUpdateId) {
      msgApi->UnsubscribeMsg(onAudioUpdateId, OnAudioUpdate, nullptr);
      msgApi->UnsubscribeMsg(onAudioSrcChangedId, OnAudioSrcChanged, nullptr);
      msgApi->ReleaseMsgID(onAudioUpdateId);
      msgApi->ReleaseMsgID(onAudioSrcChangedId);
      msgApi->ReleaseMsgID(getAudioSrcId);
   }
   // Stop submitting runnables, THEN flush the ones already queued -- MsgPlugin.h requires that
   // order on unload, or a marshalled ApplyPending could run after its plugin state is gone.
   gainPoller = nullptr;
   if (msgApi)
      msgApi->FlushPendingCallbacks(endpointId);
   {
      std::lock_guard<std::mutex> lock(gainPollerMutex);
      gainPollerPending = nullptr;
   }
   audioMeter = nullptr;           // unsubscribe BEFORE this, so no callback lands on a dead meter
   b2sPluginEventStream = nullptr; // stop the event stream first, so nothing posts to a dying sender
   scorePoller = nullptr;          // ~ScorePoller() joins its thread
   sender = nullptr;               // ~HttpSender() joins its thread, flushing/dropping cleanly
   msgApi = nullptr;
}
