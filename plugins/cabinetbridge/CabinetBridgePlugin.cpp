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

///////////////////////////////////////////////////////////////////////////////
//
// The Start lock -- the cabinet's turn queue, enforced.
//
// WHAT IT IS. When somebody is waiting for a turn and the current game ends, the dashboard
// locks the cabinet and offers the turn to whoever is next. Until they answer, pressing the
// physical Start button must do nothing. That is enforced HERE, because it cannot be enforced
// anywhere else: an overlay window can't stop a key press, and quitting the table is
// destructive. VPX broadcasts every input action to plugins with a mutable event
// (VPXPI_EVT_ON_ACTION_CHANGED / InputManager::OnInputActionStateChanged) and honours a plugin
// zeroing enableVPXProcessing -- upstream API, not a patch to our fork.
//
// HOW IT GETS HERE. It rides home on the reply to the /api/emit POST this plugin already makes
// constantly during play. No new socket, no new thread, no polling.
//
// IT FAILS OPEN, THREE WAYS, AND THAT IS THE WHOLE DESIGN. A stuck lock turns a pinball machine
// into furniture in a house where most of the players are under ten and can't debug it. So:
//   1. the dashboard goes quiet for LOCK_STALE_MS  -> unlocked (server died, wifi died, restart)
//   2. a lock stands longer than the server's own ceiling -> unlocked
//   3. somebody holds Start for OVERRIDE_HOLD_MS -> unlocked here AND at the dashboard
// Any one of them is enough on its own. None of them depends on the dashboard being reachable
// except the one that only matters when it is.
//
// See docs/decisions/table-sessions-and-queue.md in the pinball_cab repo.

/** No word from the dashboard in this long and the lock is abandoned. */
static const uint64_t LOCK_STALE_MS = 5000;
/** Hold Start this long, then release, to force the cabinet open. */
static const uint64_t OVERRIDE_HOLD_MS = 3000;

static std::atomic<bool> lockFlag { false };
static std::atomic<uint64_t> lockSeenAtMs { 0 };   // last time the dashboard told us anything
static std::atomic<uint64_t> lockSinceMs { 0 };    // when this lock first went up
static std::atomic<uint64_t> lockCeilingMs { 180000 }; // the server's own MAX_LOCK_MS
static std::atomic<uint64_t> startPressedAtMs { 0 };
static std::atomic<bool> overrideRequested { false };
// A Start press that the veto swallowed, waiting to be reported from the poller thread.
static std::atomic<bool> startSwallowed { false };
// True only while we are synthesizing a press ourselves. MUST exist: SetInputState goes through
// InputAction::SetDirectState -> OnInputChanged -> OnInputActionStateChanged, which is the very
// callback the veto lives in -- without this the plugin would swallow its own remote start and
// the phone button would do nothing, in a way that looks exactly like the lock working.
static std::atomic<bool> synthesizing { false };
// The VPX API. Needed only to press buttons; every other path in this plugin talks to the
// message bus instead.
static const VPXPluginAPI* vpxApi = nullptr;
// What the plugin last TOLD the bus about itself. Announcements are driven by the EFFECTIVE
// veto (StartIsVetoed), sampled on the poller thread -- never by what the server said. Those
// two differ in exactly the cases that matter: a dashboard that went quiet, or a lock that
// outlived its ceiling, both leave the server saying "locked" while the Start button works
// perfectly. Only the effective state explains a Start press that did nothing, which is the
// entire question anyone will be asking when they read this line back.
static bool announcedVeto = false;

static uint64_t NowMs()
{
   return (uint64_t)std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
}

/** Called from the HTTP thread with whatever the dashboard just said. */
static void SetLockFromServer(bool locked, uint64_t ceilingMs)
{
   const uint64_t t = NowMs();
   lockSeenAtMs.store(t);
   if (ceilingMs > 0)
      lockCeilingMs.store(ceilingMs);
   const bool was = lockFlag.exchange(locked);
   if (locked && !was)
      lockSinceMs.store(t);
   else if (!locked)
      lockSinceMs.store(0);
}

/**
 * Press and release one action, as if the cabinet's own button had been pushed.
 *
 * THIS IS HOW THE PHONE STARTS A GAME. The queued player taps "Start my game" and the cabinet
 * stays locked the whole time -- the physical button never opens, so the kid standing at the
 * machine cannot beat them to it. The only way in is the app.
 *
 * Runs on the poller thread, never the input thread. The 80ms hold is a real button press's
 * worth: PinMAME samples switches on its own clock, and a press released within a single frame
 * can be missed entirely.
 */
static void PressAction(VPXAction action, int holdMs)
{
   if (!vpxApi || !vpxApi->SetInputState)
      return;
   const uint64_t bit = 1ULL << static_cast<int>(action);
   synthesizing.store(true);
   VPXInputState down {}; down.actionMask = bit; down.actionState = bit;
   vpxApi->SetInputState(&down);
   std::this_thread::sleep_for(std::chrono::milliseconds(holdMs));
   VPXInputState up {}; up.actionMask = bit; up.actionState = 0;
   vpxApi->SetInputState(&up);
   synthesizing.store(false);
}

/**
 * Should a Start press be swallowed right now?
 *
 * RUNS ON THE INPUT THREAD. Atomics only -- no allocation, no lock, and above all no network.
 * A blocking call here would stall input for every player, including the one whose turn it is.
 */
static bool StartIsVetoed()
{
   if (synthesizing.load())
      return false; // our own press -- see `synthesizing`
   if (!lockFlag.load())
      return false;
   const uint64_t t = NowMs();
   const uint64_t seen = lockSeenAtMs.load();
   if (seen == 0 || t - seen > LOCK_STALE_MS)
      return false; // fail open #1: nobody is home at the dashboard
   const uint64_t since = lockSinceMs.load();
   if (since != 0 && t - since > lockCeilingMs.load())
      return false; // fail open #2: this lock has outstayed the server's own ceiling
   return true;
}

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
      if (first)
         return; // nothing actually got written
      // The reply carries {locked, lockMaxMs}. Parsed leniently: a dashboard too old to send
      // them, or any malformed reply, must leave the lock untouched rather than latch it on --
      // the failure that matters here is a cabinet stuck locked, never one stuck unlocked.
      const std::string reply = PostJson("/api/emit", json);
      if (reply.empty())
         return;
      try {
         const nlohmann::json parsed = nlohmann::json::parse(reply);
         if (parsed.contains("locked") && parsed["locked"].is_boolean())
            SetLockFromServer(parsed["locked"].get<bool>(), parsed.value("lockMaxMs", (uint64_t)0));
      } catch (...) { /* a bad reply is not a reason to change the lock */ }
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
      return ExtractJsonBody(resp);
   }

   // Node sends these small responses CHUNKED, so the raw body is "3b\r\n{...}\r\n0\r\n\r\n" and
   // feeding it straight to a JSON parser throws on the chunk-size line. Rather than implement
   // de-chunking, take the outermost JSON object. Verified against the real server: without
   // this the gain poller silently caught, continued, and the whole live-control path did
   // nothing while looking perfectly healthy.
   //
   // Shared by GetBody and PostJson since the lock state started riding home on the POST
   // response -- one de-chunker, not two that can drift apart.
   static std::string ExtractJsonBody(const std::string& resp)
   {
      const size_t sep = resp.find("\r\n\r\n");
      if (sep == std::string::npos)
         return std::string();
      const std::string body = resp.substr(sep + 4);
      const size_t open = body.find('{'), close = body.rfind('}');
      if (open == std::string::npos || close == std::string::npos || close < open)
         return std::string();
      return body.substr(open, close - open + 1);
   }

   // Returns the response body (see ExtractJsonBody), or "" on any failure. Callers that don't
   // care simply ignore it -- but /api/emit's reply carries the Start lock, which is how the
   // veto reaches this plugin without a second socket or a poll of its own.
   static std::string PostJson(const char* path, const std::string& json)
   {
      std::string resp;
#ifdef _WIN32
      static bool wsaInit = false;
      if (!wsaInit) {
         WSADATA wsaData;
         WSAStartup(MAKEWORD(2, 2), &wsaData);
         wsaInit = true;
      }
      SOCKET sock = socket(AF_INET, SOCK_STREAM, 0);
      if (sock == INVALID_SOCKET)
         return resp;
      DWORD timeoutMs = 200;
      setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeoutMs, sizeof(timeoutMs));
      setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, (const char*)&timeoutMs, sizeof(timeoutMs));
#else
      int sock = socket(AF_INET, SOCK_STREAM, 0);
      if (sock < 0)
         return resp;
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
         // Read the WHOLE response rather than one 256-byte recv. It used to be drained and
         // discarded purely so the connection closed cleanly; now the body is the transport for
         // the Start lock, and a single recv can return a partial read on any TCP boundary.
#ifdef _WIN32
         send(sock, req.c_str(), (int)req.size(), 0);
#else
         send(sock, req.c_str(), req.size(), 0);
#endif
         char buf[1024];
         int n;
         while ((n = (int)recv(sock, buf, sizeof(buf), 0)) > 0)
            resp.append(buf, n);
      }

#ifdef _WIN32
      closesocket(sock);
#else
      close(sock);
#endif
      return ExtractJsonBody(resp);
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

   // Bus levels come from the HOST, not from a source: the table's own SFX and music are mixed
   // straight into the playfield device and never appear as an AudioUpdate, so this is the only
   // way the playfield lane can have a real meter instead of a permanent, lying zero.
   void SetBusLevel(unsigned int bus, float rms, float peak)
   {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_buses[bus] = { rms, peak };
   }

private:
   struct Lane { double sumSq = 0.0; double peak = 0.0; uint64_t samples = 0; };
   struct Bus { float rms = 0.f; float peak = 0.f; };

   void Run()
   {
      // ~20Hz. Fast enough that a callout visibly spikes its lane, slow enough that an idle
      // cabinet isn't posting constantly -- and a tick with no audio at all posts nothing.
      while (true) {
         std::map<uint64_t, Lane> lanes;
         std::map<uint64_t, std::string> names;
         std::map<unsigned int, Bus> buses;
         {
            std::unique_lock<std::mutex> lock(m_mutex);
            m_cv.wait_for(lock, std::chrono::milliseconds(50), [this] { return m_stopRequested; });
            if (m_stopRequested)
               return;
            lanes.swap(m_lanes);
            names = m_names;
            buses = m_buses;
         }
         if (lanes.empty() && buses.empty())
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
            // Skip a lane we cannot name yet rather than labelling it "audio". The dashboard
            // gives every reported lane a row, and a placeholder name produced a phantom lane
            // with a fader wired to nothing. Names resolve on OnAudioSrcChanged, so an
            // unresolved id is transient and the lane appears properly a moment later.
            if (it == names.end())
               continue;
            char buf[192];
            snprintf(buf, sizeof(buf), "{\"id\":%llu,\"name\":\"%s\",\"rms\":%.5f,\"peak\":%.5f}",
                     static_cast<unsigned long long>(id), it->second.c_str(), rms, std::min(lane.peak, 4.0));
            json += buf;
         }
         json += "],\"buses\":[";
         bool firstBus = true;
         for (const auto& [id, bus] : buses)
         {
            if (!firstBus) json += ",";
            firstBus = false;
            char buf[128];
            snprintf(buf, sizeof(buf), "{\"bus\":%u,\"rms\":%.5f,\"peak\":%.5f}", id, bus.rms, bus.peak);
            json += buf;
         }
         json += "]}";
         if (!first || !firstBus)
            HttpSender::PostJson("/api/audio-levels", json);
      }
   }

   std::map<uint64_t, Lane> m_lanes;
   std::map<uint64_t, std::string> m_names;
   std::map<unsigned int, Bus> m_buses;
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

      // Bus masters are keyed "bus:0"/"bus:1" in the same map, because they arrive through the
      // same poll and there is no sane source name they could collide with.
      const unsigned int setBusVolId = msgApi->GetMsgID(CTLPI_NAMESPACE, CTLPI_AUDIO_SET_BUS_VOL_MSG);
      for (const auto& [key, value] : pending) {
         if (key.rfind("bus:", 0) != 0)
            continue;
         SetAudioBusVolumeMsg busMsg { static_cast<unsigned int>(std::stoul(key.substr(4))), value };
         msgApi->BroadcastMsg(endpointId, setBusVolId, &busMsg);
      }
      msgApi->ReleaseMsgID(setBusVolId);

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

// ── Live table view ──────────────────────────────────────────────────────────────────────────
// The same declarative shape as GainPoller above, for the same reasons (see its header): poll
// the DESIRED view from the dashboard, push only what differs, self-heal on a dropped tick.
//
// WHY IT EXISTS. A table's camera is baked into the .vpx by its author for the screen THEY had,
// and on this cabinet's portrait playfield window some tables need overriding. Doing that
// through table.ini means a relaunch per attempt -- roughly a minute to judge a value you can
// only judge by looking. This makes the same four numbers live.
//
// STATIC PREPASS. Changing the view invalidates the prepass's cached lighting, so it has to be
// off while tuning -- but leaving it off costs frame rate for the rest of the session, which on
// a fanless machine is exactly the budget we spent elsewhere getting the ball smooth. So it is
// disabled on the FIRST change and restored when the dashboard reports tuning has stopped,
// rather than being disabled for good the moment this plugin loads.
static std::mutex viewPollerMutex;
static std::unique_ptr<VPXViewSetupDef> viewPollerPending;
static bool viewPollerPrepassOff = false;

class ViewPoller
{
public:
   ViewPoller() { m_thread = std::thread(&ViewPoller::Run, this); }
   ~ViewPoller()
   {
      { std::lock_guard<std::mutex> lock(m_mutex); m_stopRequested = true; }
      m_cv.notify_one();
      if (m_thread.joinable())
         m_thread.join();
   }
   ViewPoller(const ViewPoller&) = delete;
   ViewPoller& operator=(const ViewPoller&) = delete;

   // Runs on the MAIN thread, via RunOnMainThread -- the plugin API is not thread safe, and
   // GetActiveViewSetup/SetActiveViewSetup both assert they are in game.
   static void ApplyPending(void* /*userData*/)
   {
      if (!vpxApi || !vpxApi->GetActiveViewSetup || !vpxApi->SetActiveViewSetup)
         return;
      std::unique_ptr<VPXViewSetupDef> want;
      {
         std::lock_guard<std::mutex> lock(viewPollerMutex);
         want.swap(viewPollerPending);
      }
      if (!want)
         return;
      // Read-modify-write, never write the struct wholesale: the dashboard only knows about the
      // four framing fields, and everything else in here (scene scale, window Z offsets, the
      // screen geometry) belongs to the table and the cabinet. Building a fresh struct would
      // quietly reset all of it to whatever this plugin happened to leave zeroed.   // TEMP
      VPXViewSetupDef view;
      vpxApi->GetActiveViewSetup(&view);
      view.FOV = want->FOV;
      view.layback = want->layback;
      view.lookAt = want->lookAt;
      view.viewVOfs = want->viewVOfs;
      view.viewHOfs = want->viewHOfs;
      view.viewX = want->viewX;
      view.viewY = want->viewY;
      view.viewZ = want->viewZ;
      view.viewportRotation = want->viewportRotation;
      view.sceneScaleX = want->sceneScaleX;
      view.sceneScaleY = want->sceneScaleY;
      view.sceneScaleZ = want->sceneScaleZ;
      vpxApi->SetActiveViewSetup(&view);
   }

   static void SetPrepass(void* userData)
   {   // TEMP
      if (vpxApi && vpxApi->DisableStaticPrerendering)
         vpxApi->DisableStaticPrerendering(userData != nullptr);
   }

   // Read the table's ACTUAL current view and hand it to the dashboard, so the tuner opens
   // showing where the table already is instead of snapping it to some default the moment it is
   // switched on. Without this, turning tuning on visibly lurched the view -- the dashboard had
   // no way to know a camera it has never been able to read.
   //
   // Runs on the main thread (GetActiveViewSetup asserts in-game and the API is not thread
   // safe), and does its own POST from here: this is one call at the start of a tuning session,
   // not a per-tick cost.
   static void SendSeed(void* /*userData*/)
   {
      if (!vpxApi || !vpxApi->GetActiveViewSetup)
         return;
      VPXViewSetupDef view;
      vpxApi->GetActiveViewSetup(&view);
      nlohmann::json j;
      j["seeded"] = true;
      // The active view MODE, reported because it decides which controls are even meaningful:
      // layback is applied only in Legacy (ViewSetup.cpp gates it on isLegacy in both the
      // projection and the camera fit), and the H/V frustum offsets only in Camera/Window. A
      // tuner that showed all of them regardless would offer sliders that silently do nothing.
      // 0=Legacy, 1=Camera, 2=Window.
      j["mode"] = view.viewMode;
      j["FOV"] = view.FOV;
      j["layback"] = view.layback;
      // RAW, no conversion. An earlier x100 here was guesswork from ViewSetup.h's `mLookAt =
      // 0.25f` initialiser, and it was wrong: getFloat() applies no scaling on load and Camera
      // mode uses `mLookAt / 100.0f`, so the field is already the same 0..100 the ini and every
      // UI use. The conversion made the tuner show a number 100x off and save it that way.
      j["lookAt"] = view.lookAt;
      j["vOfs"] = view.viewVOfs;
      j["hOfs"] = view.viewHOfs;
      j["playerX"] = view.viewX;
      j["playerY"] = view.viewY;
      j["playerZ"] = view.viewZ;
      j["rotation"] = view.viewportRotation;
      j["scaleX"] = view.sceneScaleX;
      j["scaleY"] = view.sceneScaleY;
      j["scaleZ"] = view.sceneScaleZ;
      HttpSender::PostJson("/api/table-view-live", j.dump());
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
         const std::string body = HttpSender::GetBody("/api/table-view-live");
         if (body.empty())
            continue;
         try {
            const auto j = nlohmann::json::parse(body);
            if (!j.is_object())
               continue;
            const bool tuning = j.value("tuning", false);
            if (tuning != m_tuning) {
               m_tuning = tuning;
               if (msgApi)
                  msgApi->RunOnMainThread(endpointId, 0.0, ViewPoller::SetPrepass, tuning ? this : nullptr);
               // Leaving tuning means the dashboard has stopped driving the view. Forget what we
               // applied so that re-entering re-pushes it: the table may have been relaunched,
               // or VPX's own POV page may have moved the view underneath us in between.
               if (!tuning) { m_have = false; continue; }
            }
            if (!tuning)
               continue;
            // Until the dashboard holds the table's real values there is nothing safe to apply:
            // applying its placeholder defaults is exactly the lurch this handshake exists to
            // prevent. Ask once, then wait for a poll that comes back seeded.
            if (!j.value("seeded", false)) {
               if (msgApi)
                  msgApi->RunOnMainThread(endpointId, 0.0, ViewPoller::SendSeed, nullptr);
               continue;
            }
            VPXViewSetupDef want {};
            want.FOV = j.value("FOV", 0.f);
            want.layback = j.value("layback", 0.f);
            want.lookAt = j.value("lookAt", 25.f);   // 0..100, same as the ini -- see SendSeed
            want.viewVOfs = j.value("vOfs", 0.f);
            want.viewHOfs = j.value("hOfs", 0.f);
            want.viewX = j.value("playerX", 0.f);
            want.viewY = j.value("playerY", 371.f);
            want.viewZ = j.value("playerZ", 1297.f);
            want.viewportRotation = j.value("rotation", 0.f);
            // Scene scale has NO safe zero: `want` is zero-initialised, so a field parsed here
            // but not assigned reaches the engine as 0 and collapses the whole scene to nothing.
            // That is precisely what "the table goes black the moment I press Start tuning" was:
            // an edit that silently failed to apply left these eight unparsed, and viewZ=0 plus
            // sceneScale=0 went straight into the live view.
            want.sceneScaleX = j.value("scaleX", 1.f);
            want.sceneScaleY = j.value("scaleY", 1.f);
            want.sceneScaleZ = j.value("scaleZ", 1.f);
            // Every driven field, not a subset: a field left out of this comparison is one whose
            // changes are silently swallowed once anything else has been applied.
            const float d[] = { m_applied.FOV - want.FOV, m_applied.layback - want.layback,
                                m_applied.lookAt - want.lookAt, m_applied.viewVOfs - want.viewVOfs,
                                m_applied.viewHOfs - want.viewHOfs,
                                m_applied.viewX - want.viewX, m_applied.viewY - want.viewY,
                                m_applied.viewZ - want.viewZ,
                                m_applied.viewportRotation - want.viewportRotation,
                                m_applied.sceneScaleX - want.sceneScaleX,
                                m_applied.sceneScaleY - want.sceneScaleY,
                                m_applied.sceneScaleZ - want.sceneScaleZ };
            bool same = true;
            for (const float v : d) if (std::abs(v) >= 0.0001f) { same = false; break; }
            if (m_have && same)
               continue;   // unchanged -- never fight VPX's own POV page 5x a second
            m_applied = want;
            m_have = true;
            {
               std::lock_guard<std::mutex> lock(viewPollerMutex);
               viewPollerPending = std::make_unique<VPXViewSetupDef>(want);
            }
            if (msgApi)
               msgApi->RunOnMainThread(endpointId, 0.0, ViewPoller::ApplyPending, nullptr);
         }
         catch (...) {
            continue; // dashboard restarting, or a partial read -- next tick retries
         }
      }
   }

   VPXViewSetupDef m_applied {};
   bool m_have = false;
   bool m_tuning = false;
   std::mutex m_mutex;
   std::condition_variable m_cv;
   bool m_stopRequested = false;
   std::thread m_thread;
};

static std::unique_ptr<ViewPoller> viewPoller;

static void OnAudioUpdate(const unsigned int eventId, void* userData, void* msgData)
{
   if (audioMeter && msgData)
      audioMeter->Accumulate(*static_cast<AudioUpdateMsg*>(msgData));
}

static unsigned int onAudioBusLevelId = 0;

static void OnAudioBusLevel(const unsigned int eventId, void* userData, void* msgData)
{
   if (audioMeter && msgData)
   {
      const AudioBusLevelMsg& msg = *static_cast<AudioBusLevelMsg*>(msgData);
      audioMeter->SetBusLevel(msg.bus, msg.rms, msg.peak);
   }
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

// One game_state field that isn't a score -- game_over, current_ball, and friends. Separate
// from ScoreField because these are the SESSION's shape (did a game start, is it still going,
// did it reach the last ball) rather than its result, they aren't all BCD, and only these can
// be false/zero as a meaningful value rather than as "not read".
//
// WHY THESE ARE WORTH READING AT ALL: the dashboard has to record only games that were played
// end to end, and inferring that from drain events is guesswork -- the ROM already knows. See
// docs/decisions/table-sessions-and-queue.md in the pinball_cab repo.
//
// WARNING, and it is not a small one: on both platforms we have maps for, these fields live in
// LOW working RAM (addresses 135-950 on WPC), not in the battery-backed region the scores are
// in. They are only meaningful while the CPU is running. A saved .nv file's copy of them is
// whatever happened to be resident at dump time -- verified 2026-08-16 with the repo's
// scripts/nvram-state.py, which reads mm_109c.nv as game_over=false AND current_ball=0, a flat
// contradiction, on perfect checksums. That is exactly why this is polled live here and cannot
// be recovered from disk afterwards.
struct StateField
{
   std::string key;   // the name the dashboard sees, e.g. "gameOver"
   uint32_t offset;
   int length;
   char encoding;     // 'i' int (big-endian), 'b' bcd, 'o' bool
   bool invert;       // bool only: bttf's game_over is "current player is 0", inverted
};

// Everything one ROM's memmap gives us, parsed in one pass. Both halves come from the same
// file and the same base-address computation, so they resolve together rather than through two
// near-identical loaders that could disagree about the offset.
struct RomMap
{
   std::vector<ScoreField> scores;
   std::vector<StateField> state;
   bool empty() const { return scores.empty() && state.empty(); }
};

// The game_state keys worth forwarding, mapped to the camelCase name the dashboard uses.
// Deliberately a fixed list rather than "forward every field in the map": the maps also carry
// audits, volume, replay levels and per-mode champions, none of which a session state machine
// wants and all of which would be noise on the event bus.
static const std::pair<const char*, const char*> STATE_KEYS[] = {
   { "game_over", "gameOver" },
   { "current_player", "player" },
   { "current_ball", "ball" },
   { "player_count", "players" },
   { "ball_count", "ballCount" },
   { "credits", "credits" },
   { "free_play", "freePlay" },
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

// Loads romName's byte layout from ~/.pinmame/memmaps: the per-player score fields, and the
// game_state fields named in STATE_KEYS above. Returns an empty map (forwarding silently does
// nothing, same as every other optional piece of this plugin) if no memmap is set up for this
// ROM, or on any parse error -- a malformed/missing file must never crash the plugin, just
// degrade to "no events."
// Still deliberately NOT a general-purpose memmap reader: only "scores" entries labeled
// "Player <n>" with "bcd" encoding, and only the fixed STATE_KEYS list, are parsed. Everything
// else in a map file (audits, champions, volume) stays unread.
static RomMap LoadRomMap(const std::string& romName)
{
   RomMap romMap;
   try {
      const std::filesystem::path dir = MemmapsDir();
      std::ifstream indexFile(dir / "index.json");
      if (!indexFile.is_open())
         return romMap;
      nlohmann::json index;
      indexFile >> index;
      if (!index.is_object() || !index.contains(romName) || !index[romName].is_string())
         return romMap;

      std::ifstream mapFile(dir / index[romName].get<std::string>());
      if (!mapFile.is_open())
         return romMap;
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

      if (!map.contains("game_state") || !map["game_state"].is_object())
         return romMap;
      const auto& gameState = map["game_state"];

      // The session fields. Each is optional: a map missing current_ball still yields useful
      // scores, and the dashboard is told which keys it actually got rather than being handed
      // a zero it would have to treat as real.
      for (const auto& [mapKey, outKey] : STATE_KEYS) {
         if (!gameState.contains(mapKey) || !gameState[mapKey].is_object())
            continue;
         const auto& entry = gameState[mapKey];
         if (!entry.contains("start"))
            continue;
         const std::string enc = entry.value("encoding", std::string());
         char encoding = 0;
         if (enc == "int") encoding = 'i';
         else if (enc == "bcd") encoding = 'b';
         else if (enc == "bool") encoding = 'o';
         else continue; // an encoding we don't decode is skipped, never guessed at
         const uint32_t addr = ParseAddr(entry["start"]);
         const int length = entry.value("length", 1);
         if (addr < nvramBase || length <= 0)
            continue;
         romMap.state.push_back({ outKey, addr - nvramBase, length, encoding, entry.value("invert", false) });
      }

      if (!gameState.contains("scores") || !gameState["scores"].is_array())
         return romMap;

      for (const auto& entry : gameState["scores"]) {
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
         romMap.scores.push_back({ playerNo, addr - nvramBase, length });
      }
   } catch (...) {
      // malformed/missing memmap files degrade to "no forwarding", never crash. Both halves are
      // cleared together: a partially-parsed map is the one state that could produce confident,
      // wrong session boundaries.
      romMap.scores.clear();
      romMap.state.clear();
   }
   return romMap;
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

// Decode one game_state field. Returns -1 for out-of-range, same "not read" convention
// DecodeBcd uses -- which is why bools come back as 0/1 rather than as a C++ bool: the caller
// has to be able to tell "false" from "never read it".
static int DecodeState(const uint8_t* nvram, size_t nvramLen, const StateField& field)
{
   if (field.offset + static_cast<uint32_t>(field.length) > nvramLen)
      return -1;
   if (field.encoding == 'b')
      return DecodeBcd(nvram, nvramLen, { 0, field.offset, field.length });
   if (field.encoding == 'o') {
      const bool set = nvram[field.offset] != 0;
      return (field.invert ? !set : set) ? 1 : 0;
   }
   long value = 0; // 'i': big-endian, both platforms we have maps for
   for (int i = 0; i < field.length; i++)
      value = value * 256 + nvram[field.offset + i];
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

   void SetMap(RomMap romMap)
   {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_map = std::move(romMap);
      m_lastValues.assign(m_map.scores.size(), -1);
      m_lastState.clear(); // a ROM switch must re-announce state, not diff against the old table's
   }

private:
   // The lock's safety net, and the override's outbound leg. Both live on this thread because
   // it is the one thing in the plugin that ticks regardless of anything else -- it does not
   // depend on events flowing, which matters: VPX pauses itself when the playfield loses focus
   // and the whole event stream stops with it. Without this, a lock would go stale during a
   // pause and fail open just as the player refocused and reached for Start.
   void PollLock(int tick)
   {
      if (overrideRequested.exchange(false)) {
         HttpSender::PostJson("/api/table/override", "{}");
         HttpSender::PostJson("/api/emit",
            "{\"type\":\"system\",\"tag\":\"CABINET\",\"label\":\"Start held -- cabinet forced open\","
            "\"detail\":\"override from the machine itself\"}");
      }
      if (startSwallowed.exchange(false))
         HttpSender::PostJson("/api/emit",
            "{\"type\":\"system\",\"tag\":\"CABINET\",\"label\":\"Start ignored -- not your turn\","
            "\"detail\":\"the button was pressed while the cabinet was locked\"}");
      // Sampled every tick (~200ms), not just when the server speaks -- a fail-open is a
      // transition nobody sent us, and it is the one most worth seeing on the bus.
      const bool veto = StartIsVetoed();
      if (veto != announcedVeto) {
         announcedVeto = veto;
         HttpSender::PostJson("/api/emit", veto
            ? "{\"type\":\"system\",\"tag\":\"CABINET\",\"label\":\"Start locked\",\"detail\":\"the plugin is now vetoing the Start button\"}"
            : "{\"type\":\"system\",\"tag\":\"CABINET\",\"label\":\"Start unlocked\",\"detail\":\"the Start button works again\"}");
      }
      if (tick % 5 != 0) // ~1s; the batch replies already carry it whenever play is happening
         return;
      const std::string body = HttpSender::GetBody("/api/table/lock");
      if (body.empty())
         return; // dashboard unreachable: leave it to LOCK_STALE_MS, which fails open
      try {
         const nlohmann::json parsed = nlohmann::json::parse(body);
         if (parsed.contains("locked") && parsed["locked"].is_boolean())
            SetLockFromServer(parsed["locked"].get<bool>(), parsed.value("lockMaxMs", (uint64_t)0));
         // The remote start. One-shot: the server clears it the moment it hands it over, so a
         // poll that arrives twice cannot start two games.
         if (parsed.value("startNow", false)) {
            // A credit first, when the ROM needs one -- the server knows from NVRAM whether
            // this table is on free play and how many credits are banked, so we press it only
            // when a real player would have had to. Without this, "Start my game" silently
            // does nothing on a credit-mode table, which is precisely what a Start press does
            // when it is vetoed: two very different faults that look identical.
            if (parsed.value("addCredit", false)) {
               PressAction(VPXACTION_AddCredit, 80);
               std::this_thread::sleep_for(std::chrono::milliseconds(250));
            }
            PressAction(VPXACTION_StartGame, 80);
            HttpSender::PostJson("/api/emit",
               "{\"type\":\"system\",\"tag\":\"CABINET\",\"label\":\"Start pressed from the app\","
               "\"detail\":\"the queued player took their turn from their phone\"}");
         }
      } catch (...) { }
   }

   void Run()
   {
      int tick = 0;
      while (true) {
         RomMap romMap;
         {
            std::unique_lock<std::mutex> lock(m_mutex);
            m_cv.wait_for(lock, std::chrono::milliseconds(200), [this] { return m_stopRequested; });
            if (m_stopRequested)
               return;
            romMap = m_map;
         }
         // BEFORE the romMap check, deliberately: the lock has nothing to do with whether this
         // ROM has a memory map, and a table we can't read scores for must still honour a turn.
         PollLock(tick++);
         if (romMap.empty())
            continue;
         const std::vector<ScoreField>& fields = romMap.scores;

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
         if (fields.size() != m_map.scores.size())
            continue; // fields changed mid-poll (ROM switch) -- skip this tick, next one is consistent
         PollState(romMap, bytes);
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

   // Emits a "gamestate" event whenever the SESSION's shape changes -- a game started or ended,
   // the ball or player advanced, a credit was added. Caller holds m_mutex.
   //
   // WHY SCORES RIDE ALONG BUT DON'T TRIGGER IT: the change test deliberately ignores scores.
   // Including them would make this fire as often as a score event does (constantly, mid-ball),
   // and the whole reason a named/structured event can be persisted at all is that it's rare --
   // see table-events.json's persistence-budget rule. But the snapshot still CARRIES the scores,
   // because the one moment that matters most is the game_over edge, and a final score read from
   // a separate event arriving separately is a final score that can be torn by a poll boundary.
   // Riding along makes "the game ended, and here is what everyone finished on" one atomic fact.
   void PollState(const RomMap& romMap, const std::vector<uint8_t>& bytes)
   {
      if (romMap.state.empty() || !m_sender)
         return;

      std::map<std::string, int> now;
      for (const StateField& field : romMap.state) {
         const int value = DecodeState(bytes.data(), bytes.size(), field);
         if (value >= 0)
            now[field.key] = value;
      }
      if (now.empty() || now == m_lastState)
         return;
      m_lastState = now;

      nlohmann::json state(now);
      nlohmann::json scores = nlohmann::json::array();
      for (const ScoreField& field : romMap.scores) {
         const int value = DecodeBcd(bytes.data(), bytes.size(), field);
         if (value >= 0)
            scores.push_back({ { "player", field.playerNo }, { "score", value } });
      }
      state["scores"] = scores;

      // type/tag/label/detail so this reads on the event bus like every other forwarded event,
      // plus `state` -- real keys, never packed into the display strings, per the structured-
      // fields rule in docs/decisions/table-events.md. The dashboard's session state machine
      // reads `state`; the bus just shows the label.
      const auto at = [&now](const char* key) { const auto it = now.find(key); return it == now.end() ? -1 : it->second; };
      char label[96];
      if (at("gameOver") == 1)
         snprintf(label, sizeof(label), "attract");
      else
         snprintf(label, sizeof(label), "player %d/%d · ball %d/%d",
            at("player"), at("players"), at("ball"), at("ballCount"));

      nlohmann::json event = {
         { "type", "gamestate" }, { "tag", "GAME" },
         { "label", label }, { "detail", "" }, { "state", state },
      };
      HttpSender::PostJson("/api/emit", event.dump());
   }

   HttpSender* m_sender;
   std::mutex m_mutex;
   std::condition_variable m_cv;
   bool m_stopRequested = false;
   RomMap m_map;
   std::vector<int> m_lastValues;
   std::map<std::string, int> m_lastState;
   std::thread m_thread;
};

static std::unique_ptr<ScorePoller> scorePoller;
static unsigned int onControllersChangedId = 0;
static unsigned int onActionChangedId = 0;
static unsigned int getControllersId = 0;
static std::string currentRomName;

// Re-resolves the current ROM whenever the controller list changes (same GetControllers /
// "pinmame::<rom>" gameId convention every other plugin here uses to find PinMAME, e.g.
// AltSoundPlugin.cpp's OnControllersChanged) and reloads that ROM's byte layout -- so
// switching tables (or a fresh launch with no ROM loaded yet) doesn't keep forwarding a
// stale, wrong previous table's score layout, or worse, decide a game started because the
// previous ROM's game_over byte happens to sit somewhere meaningful in this one.
// The veto itself. VPX broadcasts this for every action state change and uses what we leave in
// the struct (InputManager::OnInputActionStateChanged returns event.enableVPXProcessing != 0).
//
// ONLY VPXACTION_StartGame IS VETOED. Blocking AddCredit too was considered and dropped: adding
// a credit doesn't begin a game, so blocking it buys no fairness and makes a coin-up during
// someone else's handoff silently fail, which reads as a broken machine rather than a queue.
//
// KEEP THIS FUNCTION CHEAP. It is on the input path for every flipper press.
static void OnActionChanged(const unsigned int eventId, void* userData, void* msgData)
{
   VPXActionEvent* const ev = static_cast<VPXActionEvent*>(msgData);
   if (!ev || ev->action != VPXACTION_StartGame)
      return;

   // Hold-to-override, measured on the RELEASE: there is no "still held" event to watch, only
   // state changes, so the press timestamp is stashed and the duration checked when it comes
   // back up. Tracked before the veto returns, deliberately -- the escape hatch has to work
   // while the button is doing nothing, which is exactly when somebody reaches for it.
   if (ev->isPressed) {
      startPressedAtMs.store(NowMs());
   }
   else {
      const uint64_t pressedAt = startPressedAtMs.exchange(0);
      if (pressedAt != 0 && NowMs() - pressedAt >= OVERRIDE_HOLD_MS) {
         lockFlag.store(false); // locally, instantly -- do not wait for a round trip
         lockSinceMs.store(0);
         overrideRequested.store(true); // and tell the dashboard, off this thread
      }
   }

   if (!StartIsVetoed())
      return;
   const bool wasPress = ev->isPressed != 0; // read BEFORE we clear it
   ev->isPressed = 0;
   ev->enableVPXProcessing = 0;
   // Say so, on the press edge only. Two reasons, and the second is why this is permanent
   // rather than debug instrumentation: a player who pressed Start and got nothing deserves an
   // answer, and this is the ONLY way the veto can be observed at all -- no test can press a
   // physical button, so the machine reporting its own refusal is the evidence.
   if (wasPress)
      startSwallowed.store(true);
}

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
      scorePoller->SetMap(romName.empty() ? RomMap() : LoadRomMap(romName));
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

   // The Start veto. VPX broadcasts this for every action state change and honours what a
   // subscriber leaves in the event -- see OnActionChanged and the Start lock section above.
   onActionChangedId = msgApi->GetMsgID(VPXPI_NAMESPACE, VPXPI_EVT_ON_ACTION_CHANGED);
   msgApi->SubscribeMsg(endpointId, onActionChangedId, OnActionChanged, nullptr);

   // The VPX API, used only to PRESS buttons -- the phone's "Start my game" is delivered as a
   // real Start action so the cabinet never has to unlock for it. Same GetAPI handshake every
   // other plugin here uses (see DOFPlugin).
   unsigned int getVpxApiId = msgApi->GetMsgID(VPXPI_NAMESPACE, VPXPI_MSG_GET_API);
   msgApi->BroadcastMsg(endpointId, getVpxApiId, &vpxApi);
   msgApi->ReleaseMsgID(getVpxApiId);

   // Passive tap on the audio broadcast PinMAME/PUP/AltSound already send -- see AudioMeter.
   audioMeter = std::make_unique<AudioMeter>();
   onAudioUpdateId = msgApi->GetMsgID(CTLPI_NAMESPACE, CTLPI_AUDIO_ON_UPDATE_MSG);
   onAudioSrcChangedId = msgApi->GetMsgID(CTLPI_NAMESPACE, CTLPI_AUDIO_ON_SRC_CHG_MSG);
   getAudioSrcId = msgApi->GetMsgID(CTLPI_NAMESPACE, CTLPI_AUDIO_GET_SRC_MSG);
   audioMeter->SetGetSrcId(getAudioSrcId);
   msgApi->SubscribeMsg(endpointId, onAudioUpdateId, OnAudioUpdate, nullptr);
   msgApi->SubscribeMsg(endpointId, onAudioSrcChangedId, OnAudioSrcChanged, nullptr);
   onAudioBusLevelId = msgApi->GetMsgID(CTLPI_NAMESPACE, CTLPI_AUDIO_ON_BUS_LEVEL_MSG);
   msgApi->SubscribeMsg(endpointId, onAudioBusLevelId, OnAudioBusLevel, nullptr);
   audioMeter->RefreshNames(); // sources may already exist if we loaded late

   // Inbound: dashboard fader -> running table's mixer. Started after the ids above exist,
   // because ApplyPending needs getAudioSrcId to resolve names to sources.
   gainPoller = std::make_unique<GainPoller>();
   viewPoller = std::make_unique<ViewPoller>();
}

MSGPI_EXPORT void MSGPIAPI CabinetBridgePluginUnload()
{
   if (msgApi && onControllersChangedId) {
      msgApi->UnsubscribeMsg(onControllersChangedId, OnControllersChanged, nullptr);
      msgApi->ReleaseMsgID(onControllersChangedId);
      msgApi->UnsubscribeMsg(onActionChangedId, OnActionChanged, nullptr);
      msgApi->ReleaseMsgID(onActionChangedId);
      msgApi->ReleaseMsgID(getControllersId);
   }
   if (msgApi && onAudioUpdateId) {
      msgApi->UnsubscribeMsg(onAudioUpdateId, OnAudioUpdate, nullptr);
      msgApi->UnsubscribeMsg(onAudioSrcChangedId, OnAudioSrcChanged, nullptr);
      msgApi->UnsubscribeMsg(onAudioBusLevelId, OnAudioBusLevel, nullptr);
      msgApi->ReleaseMsgID(onAudioBusLevelId);
      msgApi->ReleaseMsgID(onAudioUpdateId);
      msgApi->ReleaseMsgID(onAudioSrcChangedId);
      msgApi->ReleaseMsgID(getAudioSrcId);
   }
   // Stop submitting runnables, THEN flush the ones already queued -- MsgPlugin.h requires that
   // order on unload, or a marshalled ApplyPending could run after its plugin state is gone.
   gainPoller = nullptr;
   viewPoller = nullptr;
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
