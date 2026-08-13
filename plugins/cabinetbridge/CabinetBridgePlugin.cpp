// license:GPLv3+

#include "plugins/VPXPlugin.h"
#include "plugins/B2SPluginEventStream.h"

#include <thread>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <chrono>
#include <cstdio>
#include <string>

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
         PostJson(json);
   }

   // A minimal, blocking HTTP/1.1 POST over a raw TCP socket to 127.0.0.1:7333 -- no TLS, no
   // libcurl dependency (nothing else in this plugin tree links it), since the destination is
   // always localhost. Short connect/read timeouts + silent drop-on-failure so a dashboard
   // that isn't running (or briefly restarting) can never stall this thread.
   static void PostJson(const std::string& json)
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
         const std::string req = "POST /api/emit HTTP/1.1\r\n"
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

   std::vector<QueuedEvent> m_queue;
   std::mutex m_mutex;
   std::condition_variable m_cv;
   bool m_stopRequested = false;
   std::thread m_thread;
};

static std::unique_ptr<HttpSender> sender;

}

using namespace CabinetBridgePlugin;

MSGPI_EXPORT void MSGPIAPI CabinetBridgePluginLoad(const uint32_t sessionId, const MsgPluginAPI* api)
{
   msgApi = api;
   endpointId = sessionId;

   sender = std::make_unique<HttpSender>();
   b2sPluginEventStream = std::make_unique<B2SPluginEventStream>(msgApi, endpointId, [](char type, int id, int value) {
      if (HttpSender* s = sender.get())
         s->PostEvent(type, id, value);
   });
}

MSGPI_EXPORT void MSGPIAPI CabinetBridgePluginUnload()
{
   b2sPluginEventStream = nullptr; // stop the event stream first, so nothing posts to a dying sender
   sender = nullptr;               // ~HttpSender() joins its thread, flushing/dropping cleanly
   msgApi = nullptr;
}
