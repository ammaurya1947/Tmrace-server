// ============================================================================
//  TYPING MAESTRO RACE  -  online multiplayer typing car racing game (ONE FILE)
//  SDL3 client + dedicated server in the same source.
//
//  Home hub UI, 32 vehicles, 16 nitro FX. Server = fixed Render link (see TM_DEFAULT_HOST below).
//  Client build (Linux/macOS):
//    g++ -std=c++17 -O2 tmrace.cpp -o tmrace $(pkg-config --cflags --libs sdl3 sdl3-ttf) -pthread
//    (no SDL3_ttf installed?  add -DTM_NO_TTF and drop sdl3-ttf: falls back to the built-in bitmap font)
//  Server only (no SDL needed, e.g. Render/Docker):
//    g++ -std=c++17 -O2 -DTM_SERVER_ONLY tmrace.cpp -o tmserver -pthread
//  Run:   ./tmserver --server            (reads $PORT)      |   ./tmrace --client --host wss://NAME.onrender.com
//  Bake your server into the build:  -DTM_DEFAULT_HOST='"wss://NAME.onrender.com"'
//  Test:  ./tmrace --selftest            (headless protocol test)
// ============================================================================
// ============================================================================
// >>> PUT YOUR RENDER LINK HERE (the game always uses this server; players cannot change it):
#ifndef TM_DEFAULT_HOST
#define TM_DEFAULT_HOST "wss://YOUR-SERVICE.onrender.com"
#endif
#define _CRT_SECURE_NO_WARNINGS
#ifdef _WIN32
#define NOMINMAX
#define WIN32_LEAN_AND_MEAN
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#ifndef TM_SERVER_ONLY
#include <SDL3/SDL.h>
#include <SDL3/SDL_main.h>
#endif

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

// ============================================================================
//  1. BASIC UTILITIES
// ============================================================================
static double nowSec() {
    using namespace std::chrono;
    static const auto t0 = steady_clock::now();
    return duration<double>(steady_clock::now() - t0).count();
}
static uint64_t nowMs() { return (uint64_t)(nowSec() * 1000.0); }
template <class T> static T clampv(T v, T lo, T hi) { return v < lo ? lo : (v > hi ? hi : v); }

static constexpr uint16_t DEFAULT_PORT = 7777;
static constexpr uint32_t PROTO_MAGIC = 0x544D5231;  // "TMR1"
static constexpr size_t MAX_PACKET = 2048;           // type byte + payload
static constexpr double COUNTDOWN_SEC = 4.0;         // 3, 2, 1, GO

// ---------------- sockets (Winsock / POSIX) ----------------
#ifdef _WIN32
using sock_t = SOCKET;
static const sock_t SOCK_BAD = INVALID_SOCKET;
static int sockErr() { return WSAGetLastError(); }
static bool errWouldBlock(int e) { return e == WSAEWOULDBLOCK || e == WSAEINPROGRESS; }
static void sockClose(sock_t s) { closesocket(s); }
static void sockNonBlock(sock_t s) { u_long m = 1; ioctlsocket(s, FIONBIO, &m); }
#else
using sock_t = int;
static const sock_t SOCK_BAD = -1;
static int sockErr() { return errno; }
static bool errWouldBlock(int e) { return e == EWOULDBLOCK || e == EAGAIN || e == EINPROGRESS || e == EINTR; }
static void sockClose(sock_t s) { close(s); }
static void sockNonBlock(sock_t s) { fcntl(s, F_SETFL, fcntl(s, F_GETFL, 0) | O_NONBLOCK); }
#endif
static void netInit() {
#ifdef _WIN32
    WSADATA d; WSAStartup(MAKEWORD(2, 2), &d);
#else
    signal(SIGPIPE, SIG_IGN);
#endif
}
static void netShutdown() {
#ifdef _WIN32
    WSACleanup();
#endif
}
static void sockNoDelay(sock_t s) { int one = 1; setsockopt(s, IPPROTO_TCP, TCP_NODELAY, (const char*)&one, sizeof(one)); }

// ============================================================================
//  2. PACKET PROTOCOL  (frame = u16 len | u8 type | payload, big endian)
// ============================================================================
enum MsgType : uint8_t {
    C_HELLO = 1, C_CREATE, C_JOIN, C_QUICK, C_LEAVE, C_READY, C_CAR, C_START, C_TYPE, C_NITRO, C_PING, C_TIME, C_NAME,
    S_WELCOME = 64, S_ROOM, S_ERROR, S_RACE_START, S_RACE_STATE, S_RACE_END, S_PONG, S_KICK, S_STATS
};
enum { EV_CONNECTED = 250, EV_DISCONNECTED = 251, EV_FAILED = 252 };

struct Writer {
    std::vector<uint8_t> b;
    void u8(uint8_t v) { b.push_back(v); }
    void u16(uint16_t v) { b.push_back(v >> 8); b.push_back(v & 255); }
    void u32(uint32_t v) { for (int i = 3; i >= 0; i--) b.push_back((v >> (i * 8)) & 255); }
    void u64(uint64_t v) { for (int i = 7; i >= 0; i--) b.push_back((v >> (i * 8)) & 255); }
    void str(const std::string& s) { size_t n = std::min<size_t>(s.size(), 255); u8((uint8_t)n); b.insert(b.end(), s.begin(), s.begin() + n); }
    void lstr(const std::string& s) { size_t n = std::min<size_t>(s.size(), 1400); u16((uint16_t)n); b.insert(b.end(), s.begin(), s.begin() + n); }
};
struct Reader {
    const uint8_t* p; size_t n, i = 0; bool ok = true;
    Reader(const std::vector<uint8_t>& v) : p(v.data()), n(v.size()) {}
    uint8_t u8() { if (i + 1 > n) { ok = false; return 0; } return p[i++]; }
    uint16_t u16() { if (i + 2 > n) { ok = false; return 0; } uint16_t v = (p[i] << 8) | p[i + 1]; i += 2; return v; }
    uint32_t u32() { if (i + 4 > n) { ok = false; return 0; } uint32_t v = 0; for (int k = 0; k < 4; k++) v = (v << 8) | p[i + k]; i += 4; return v; }
    uint64_t u64() { if (i + 8 > n) { ok = false; return 0; } uint64_t v = 0; for (int k = 0; k < 8; k++) v = (v << 8) | p[i + k]; i += 8; return v; }
    std::string str(size_t maxLen = 255) { size_t l = u8(); if (l > maxLen || i + l > n) { ok = false; return ""; } std::string s((const char*)p + i, l); i += l; return s; }
    std::string lstr(size_t maxLen = 1400) { size_t l = u16(); if (l > maxLen || i + l > n) { ok = false; return ""; } std::string s((const char*)p + i, l); i += l; return s; }
};
static void appendFrame(std::vector<uint8_t>& out, uint8_t type, const Writer& w) {
    size_t len = 1 + w.b.size();
    if (len > MAX_PACKET) return;
    out.push_back((len >> 8) & 255); out.push_back(len & 255); out.push_back(type);
    out.insert(out.end(), w.b.begin(), w.b.end());
}
struct FrameBuffer {
    std::vector<uint8_t> buf; size_t rd = 0; bool bad = false;
    void feed(const uint8_t* d, size_t n) {
        if (rd > 0 && rd >= buf.size() / 2) { buf.erase(buf.begin(), buf.begin() + rd); rd = 0; }
        buf.insert(buf.end(), d, d + n);
        if (buf.size() - rd > MAX_PACKET * 16) bad = true;  // flooding / garbage
    }
    bool next(uint8_t& type, std::vector<uint8_t>& payload) {
        if (bad || buf.size() - rd < 2) return false;
        size_t len = ((size_t)buf[rd] << 8) | buf[rd + 1];
        if (len < 1 || len > MAX_PACKET) { bad = true; return false; }
        if (buf.size() - rd < 2 + len) return false;
        type = buf[rd + 2];
        payload.assign(buf.begin() + rd + 3, buf.begin() + rd + 2 + len);
        rd += 2 + len;
        return true;
    }
};

// ============================================================================
//  3. TEXT POOL
// ============================================================================
static const char* kSentences[3][12] = {
    {"the sun is warm and the sea is calm today.", "a red car drives fast along the sandy road.", "we run to the beach and play in the water.",
     "my friend has a small boat and a big hat.", "the wind is soft and the sky is very blue.", "birds fly over the palm trees near the shore.",
     "i like to type fast and keep my hands light.", "the road is long but the car is quick.", "we sit under an umbrella and drink cold juice.",
     "a little crab walks slowly across the sand.", "good typists look at the screen and not the keys.", "the race is about to start so get ready."},
    {"Practice makes progress, so keep your eyes on the text and your fingers relaxed.", "The checkered flag waves as the engines roar along the sunny coastline.",
     "Accuracy matters more than speed, because every mistake costs precious time.", "A steady rhythm will carry you farther than a sudden burst of frantic keystrokes.",
     "Waves crash against the shore while palm trees sway in the warm evening breeze.", "Champions are made in the quiet hours when nobody is watching the clock.",
     "Take a deep breath, trust your training, and let your hands do the talking.", "The finish line is closer than it looks, so do not slow down now.",
     "Every great driver starts as a beginner who refused to give up.", "Keep your posture straight, your wrists level, and your focus sharp.",
     "Sand, sun, and speed: nothing beats a race along the golden beach.", "Fast fingers win trophies, but calm minds win championships."},
    {"At 7:45 a.m., the pit crew checked 4 tires, 2 spoilers & a turbo; all were \"perfect\".", "Zoom! Lap #3 begins (finally), and Maya's engine hums at 9,500 RPM.",
     "\"Never lift,\" he whispered; the throttle stayed pinned as the sand blurred.", "Rival drivers - Quinn, Zoe, and Xavier - jostled for position at the 2nd chicane.",
     "Wow! Can you really type 120 words per minute without a single typo?", "Mr. Jenkins's stopwatch read 01:58.42; a new record for the coastal circuit!",
     "Boost levels: 25%, 50%, 75%, 100% - choose wisely, because cooldowns are brutal.", "The quick brown fox jumps over the lazy dog; meanwhile, 3 jaguars zigzag by.",
     "Check the dashboard: fuel [OK], oil [OK], brakes [WARN], nitro [READY].", "She said, 'Keep calm & type on,' and then crossed the line at 142 mph.",
     "Quartz jukebox vexes: pack my box with five dozen liquor jugs, quickly.", "E-mail the results to team@race.example by 5:30 p.m. (sharp), please."}};

static std::string genParagraph(int difficulty, int length, std::mt19937_64& rng) {
    difficulty = clampv(difficulty, 0, 2); length = clampv(length, 0, 2);
    int count = length == 0 ? 3 : (length == 1 ? 5 : 8);
    int idx[12]; for (int i = 0; i < 12; i++) idx[i] = i;
    std::shuffle(idx, idx + 12, rng);
    std::string s;
    for (int i = 0; i < count; i++) { if (i) s += ' '; s += kSentences[difficulty][idx[i]]; }
    return s;
}

// ============================================================================
//  4. RACE CORE  (used by the server AND by offline practice - one rule set)
// ============================================================================
struct RacerSnap {
    uint8_t slot = 0, nitro = 0, flags = 0, place = 0;
    uint16_t progress = 0, correct = 0, wrong = 0, wpm10 = 0, acc10 = 0;
    uint32_t lastSeq = 0, score100 = 0, finishMs = 0;
};
enum { SF_NITRO = 1, SF_FINISHED = 2, SF_DISC = 4, SF_ACTIVE = 8 };

struct RacerCore {
    bool active = false, connected = true, finished = false, nitroActive = false;
    std::string name; uint8_t car = 0, color = 0;
    uint32_t progress = 0, correct = 0, wrong = 0, lastSeq = 0;
    float nitroCharge = 0, nitroBonus = 0;
    double nitroEnd = 0, nitroReadyAt = 0, finishTime = 0;
    int place = 0;
    double tokens = 12, lastTok = 0, lastCharT = 0;
    float emaInt = 0.25f, emaDev = 0.05f;
};

struct RaceCore {
    static constexpr double kGrace = 6.0, kNitroDur = 2.5, kNitroCooldown = 5.0;
    static constexpr float kNitroCost = 50.f;
    std::string text; double startAt = 0, timeLimit = 180, firstFinishAt = -1; bool over = false;
    std::array<RacerCore, 4> r;

    void reset(const std::string& t, double start, double limit) {
        text = t; startAt = start; timeLimit = limit; over = false; firstFinishAt = -1;
        for (auto& x : r) x = RacerCore();
    }
    float accuracy(int s) const { const RacerCore& p = r[s]; uint32_t t = p.correct + p.wrong; return t ? 100.f * p.correct / t : 100.f; }
    float consistency(int s) const { const RacerCore& p = r[s]; return clampv(1.f - p.emaDev / std::max(p.emaInt, 0.05f), 0.f, 1.f); }
    float wpm(int s, double now) const {
        const RacerCore& p = r[s];
        double el = p.finished ? p.finishTime : std::max(0.0, now - startAt);
        if (el <= 0) return 0;
        return (float)(p.correct / 5.0 / (std::max(el, 3.0) / 60.0));
    }
    // movement model: validated progress + nitro bonus + small accuracy*consistency style bonus
    float score(int s) const {
        const RacerCore& p = r[s];
        float len = (float)text.size();
        float style = 0.02f * p.progress * (accuracy(s) / 100.f) * consistency(s);
        return p.progress + std::min(p.nitroBonus, 0.05f * len) + style;
    }
    // 0 = rejected, 1 = correct, 2 = wrong
    int onChar(int s, uint32_t seq, char c, double now) {
        if (s < 0 || s >= 4) return 0;
        RacerCore& p = r[s];
        if (!p.active || over || p.finished || !p.connected) return 0;
        if (seq <= p.lastSeq) return 0;  // duplicate / replayed input
        p.lastSeq = seq;
        if (now < startAt) return 0;     // nobody types before GO
        if (c < 32 || c > 126) return 0;
        p.tokens = std::min(12.0, p.tokens + (now - p.lastTok) * 22.0); p.lastTok = now;
        if (p.tokens < 1.0) return 0;    // rate limit (~22 chars/s sustained)
        p.tokens -= 1.0;
        if (p.progress < text.size() && text[p.progress] == c) {
            p.progress++; p.correct++;
            p.nitroCharge = std::min(100.f, p.nitroCharge + 1.6f);
            if (p.nitroActive) p.nitroBonus += 0.8f;
            double dt = p.lastCharT > 0 ? now - p.lastCharT : 0.25; p.lastCharT = now;
            if (dt < 2.0) { p.emaInt = 0.9f * p.emaInt + 0.1f * (float)dt; p.emaDev = 0.9f * p.emaDev + 0.1f * std::fabs((float)dt - p.emaInt); }
            if (p.progress >= text.size()) {
                p.finished = true; p.finishTime = now - startAt; p.nitroActive = false;
                if (firstFinishAt < 0) firstFinishAt = now;
            }
            return 1;
        }
        p.wrong++; p.nitroCharge = std::max(0.f, p.nitroCharge - 3.f);
        return 2;
    }
    bool onNitro(int s, double now) {
        if (s < 0 || s >= 4) return false;
        RacerCore& p = r[s];
        if (!p.active || over || p.finished || !p.connected || now < startAt) return false;
        if (p.nitroActive || now < p.nitroReadyAt || p.nitroCharge < kNitroCost) return false;
        p.nitroCharge -= kNitroCost; p.nitroActive = true; p.nitroEnd = now + kNitroDur; p.nitroReadyAt = now + kNitroDur + kNitroCooldown;
        return true;
    }
    void computePlaces() {
        int idx[4], n = 0;
        for (int i = 0; i < 4; i++) if (r[i].active) idx[n++] = i;
        std::sort(idx, idx + n, [&](int a, int b) {
            if (r[a].finished != r[b].finished) return r[a].finished;
            if (r[a].finished) return r[a].finishTime < r[b].finishTime;
            float sa = score(a), sb = score(b);
            if (sa != sb) return sa > sb;
            return a < b;
        });
        for (int i = 0; i < n; i++) r[idx[i]].place = i + 1;
    }
    void tick(double now) {
        if (over) return;
        int act = 0, live = 0;
        for (auto& p : r) {
            if (!p.active) continue;
            act++;
            if (p.nitroActive && now >= p.nitroEnd) p.nitroActive = false;
            if (!p.finished && p.connected) live++;
        }
        computePlaces();
        if (act == 0 || live == 0 || now >= startAt + timeLimit || (firstFinishAt >= 0 && now >= firstFinishAt + kGrace)) over = true;
    }
    uint64_t forcedEndMs() const { return firstFinishAt >= 0 ? (uint64_t)((firstFinishAt + kGrace) * 1000.0) : 0; }
    RacerSnap snap(int s, double now) const {
        const RacerCore& p = r[s]; RacerSnap o;
        o.slot = (uint8_t)s; o.progress = (uint16_t)p.progress; o.correct = (uint16_t)std::min<uint32_t>(p.correct, 65535); o.wrong = (uint16_t)std::min<uint32_t>(p.wrong, 65535);
        o.lastSeq = p.lastSeq; o.nitro = (uint8_t)clampv(p.nitroCharge, 0.f, 100.f);
        o.flags = (p.nitroActive ? SF_NITRO : 0) | (p.finished ? SF_FINISHED : 0) | (p.connected ? 0 : SF_DISC) | (p.active ? SF_ACTIVE : 0);
        o.place = (uint8_t)p.place; o.wpm10 = (uint16_t)clampv(wpm(s, now) * 10.f, 0.f, 6000.f); o.acc10 = (uint16_t)(accuracy(s) * 10.f);
        o.score100 = (uint32_t)(score(s) * 100.f); o.finishMs = p.finished ? (uint32_t)(p.finishTime * 1000.0) : 0;
        return o;
    }
};
static void putSnap(Writer& w, const RacerSnap& s) {
    w.u8(s.slot); w.u16(s.progress); w.u16(s.correct); w.u16(s.wrong); w.u32(s.lastSeq); w.u8(s.nitro); w.u8(s.flags); w.u8(s.place);
    w.u16(s.wpm10); w.u16(s.acc10); w.u32(s.score100); w.u32(s.finishMs);
}
static RacerSnap getSnap(Reader& r) {
    RacerSnap s; s.slot = r.u8(); s.progress = r.u16(); s.correct = r.u16(); s.wrong = r.u16(); s.lastSeq = r.u32(); s.nitro = r.u8(); s.flags = r.u8(); s.place = r.u8();
    s.wpm10 = r.u16(); s.acc10 = r.u16(); s.score100 = r.u32(); s.finishMs = r.u32(); return s;
}

// ============================================================================
//  5. MESSAGE DECODERS (shared by client + tests)
// ============================================================================
struct RoomSlotInfo { bool occupied = false, ready = false, connected = false; std::string name; uint8_t car = 0, color = 0; float lastWpm = 0; };
struct RoomInfo { std::string code; uint8_t state = 0, host = 0, difficulty = 1, length = 1, mySlot = 0; bool isPublic = false; uint16_t limit = 90; RoomSlotInfo slot[4]; };
static bool decodeRoom(const std::vector<uint8_t>& d, RoomInfo& o) {
    Reader r(d); o.code = r.str(8); o.state = r.u8(); o.host = r.u8(); o.difficulty = r.u8(); o.length = r.u8(); o.isPublic = r.u8() != 0; o.limit = r.u16(); o.mySlot = r.u8();
    for (int i = 0; i < 4; i++) {
        RoomSlotInfo& s = o.slot[i]; s = RoomSlotInfo(); s.occupied = r.u8() != 0;
        if (!s.occupied) continue;
        s.name = r.str(32); s.car = r.u8(); s.color = r.u8(); s.ready = r.u8() != 0; s.connected = r.u8() != 0; s.lastWpm = r.u16() / 10.f;
    }
    return r.ok;
}
struct RaceStartInfo { uint64_t startAtMs = 0; uint16_t limitSec = 0; uint8_t mySlot = 255, mask = 0, car[4] = {}, color[4] = {}; std::string name[4], text; };
static bool decodeRaceStart(const std::vector<uint8_t>& d, RaceStartInfo& o) {
    Reader r(d); o.startAtMs = r.u64(); o.limitSec = r.u16(); o.mySlot = r.u8(); o.mask = r.u8();
    for (int i = 0; i < 4; i++) if (o.mask & (1 << i)) { o.name[i] = r.str(32); o.car[i] = r.u8(); o.color[i] = r.u8(); }
    o.text = r.lstr(1400);
    return r.ok && !o.text.empty();
}
struct RaceStateInfo { uint64_t serverMs = 0, forcedEndMs = 0; int n = 0; RacerSnap s[4]; };
static bool decodeRaceState(const std::vector<uint8_t>& d, RaceStateInfo& o) {
    Reader r(d); o.serverMs = r.u64(); o.forcedEndMs = r.u64(); o.n = r.u8();
    if (o.n > 4) return false;
    for (int i = 0; i < o.n; i++) o.s[i] = getSnap(r);
    return r.ok;
}
struct RaceEndInfo { int n = 0; RacerSnap s[4]; uint8_t winner = 255, best = 255; uint16_t textLen = 0; };
static bool decodeRaceEnd(const std::vector<uint8_t>& d, RaceEndInfo& o) {
    Reader r(d); o.winner = r.u8(); o.best = r.u8(); o.textLen = r.u16(); o.n = r.u8();
    if (o.n > 4) return false;
    for (int i = 0; i < o.n; i++) o.s[i] = getSnap(r);
    return r.ok;
}

// ============================================================================
//  6. DEDICATED SERVER  (server-authoritative rooms, validation, rate limits)
// ============================================================================
struct Conn {
    sock_t fd = SOCK_BAD; FrameBuffer in; std::vector<uint8_t> out; double lastRecv = 0, created = 0, tokens = 60, lastTok = 0;
    uint32_t session = 0; int strikes = 0; bool dead = false, closeAfter = false; std::string ip;
    std::vector<uint8_t> raw; int mode = 0;  // mode: 0 sniffing, 1 raw TCP frames, 2 WebSocket
};
struct Session { uint32_t id = 0; uint64_t token = 0; std::string name; Conn* conn = nullptr; std::string room; int slot = -1; double disconnectedAt = 0; };
struct Room {
    std::string code; uint32_t sid[4] = {0, 0, 0, 0}; bool ready[4] = {false, false, false, false}; uint8_t car[4] = {0, 1, 2, 3}, color[4] = {0, 1, 2, 3};
    float lastWpm[4] = {0, 0, 0, 0}; int host = 0; uint8_t state = 0;  // 0 lobby, 1 racing, 2 results
    uint8_t difficulty = 1, length = 1; bool isPublic = false; uint16_t limit = 90; RaceCore core; double resultsAt = 0, lastBroadcast = 0, lastChange = 0, fullAt = 0;
    int count() const { int c = 0; for (int i = 0; i < 4; i++) if (sid[i]) c++; return c; }
};


// ---------------- SHA-1 / Base64 / WebSocket helpers (for Render & other HTTP-only hosts) ----------------
static void sha1(const uint8_t* d, size_t n, uint8_t out[20]) {
    uint32_t h[5] = {0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0};
    std::vector<uint8_t> m(d, d + n); m.push_back(0x80); while (m.size() % 64 != 56) m.push_back(0);
    uint64_t bits = (uint64_t)n * 8; for (int i = 7; i >= 0; i--) m.push_back((bits >> (i * 8)) & 255);
    auto rol = [](uint32_t v, int s) { return (v << s) | (v >> (32 - s)); };
    for (size_t o = 0; o < m.size(); o += 64) {
        uint32_t w[80]; for (int i = 0; i < 16; i++) w[i] = ((uint32_t)m[o + i * 4] << 24) | (m[o + i * 4 + 1] << 16) | (m[o + i * 4 + 2] << 8) | m[o + i * 4 + 3];
        for (int i = 16; i < 80; i++) w[i] = rol(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);
        uint32_t a = h[0], b = h[1], c = h[2], dd = h[3], e = h[4];
        for (int i = 0; i < 80; i++) {
            uint32_t f, k; if (i < 20) { f = (b & c) | (~b & dd); k = 0x5A827999; } else if (i < 40) { f = b ^ c ^ dd; k = 0x6ED9EBA1; } else if (i < 60) { f = (b & c) | (b & dd) | (c & dd); k = 0x8F1BBCDC; } else { f = b ^ c ^ dd; k = 0xCA62C1D6; }
            uint32_t t = rol(a, 5) + f + e + k + w[i]; e = dd; dd = c; c = rol(b, 30); b = a; a = t;
        }
        h[0] += a; h[1] += b; h[2] += c; h[3] += dd; h[4] += e;
    }
    for (int i = 0; i < 5; i++) for (int k = 0; k < 4; k++) out[i * 4 + k] = (h[i] >> (24 - k * 8)) & 255;
}
static std::string base64(const uint8_t* d, size_t n) {
    static const char* t = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"; std::string o;
    for (size_t i = 0; i < n; i += 3) {
        uint32_t v = d[i] << 16 | (i + 1 < n ? d[i + 1] << 8 : 0) | (i + 2 < n ? d[i + 2] : 0);
        o += t[v >> 18 & 63]; o += t[v >> 12 & 63]; o += i + 1 < n ? t[v >> 6 & 63] : '='; o += i + 2 < n ? t[v & 63] : '=';
    }
    return o;
}
static std::string wsAccept(const std::string& key) {
    std::string s = key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"; uint8_t h[20]; sha1((const uint8_t*)s.data(), s.size(), h); return base64(h, 20);
}
// wrap payload into one WebSocket binary frame (mask = client->server)
static void wsFrame(std::vector<uint8_t>& out, const uint8_t* d, size_t n, uint8_t opcode, bool mask, std::mt19937& rng) {
    out.push_back(0x80 | opcode);
    uint8_t mb = mask ? 0x80 : 0;
    if (n < 126) out.push_back(mb | (uint8_t)n); else { out.push_back(mb | 126); out.push_back((n >> 8) & 255); out.push_back(n & 255); }
    uint8_t key[4] = {0, 0, 0, 0}; if (mask) { uint32_t r = rng(); memcpy(key, &r, 4); out.insert(out.end(), key, key + 4); }
    for (size_t i = 0; i < n; i++) out.push_back(mask ? (d[i] ^ key[i & 3]) : d[i]);
}
// parse as many complete frames from buf as possible; returns false on protocol error/close
static bool wsParse(std::vector<uint8_t>& buf, FrameBuffer& in, std::vector<uint8_t>& replies, bool replyMask, std::mt19937& rng) {
    size_t pos = 0;
    for (;;) {
        if (buf.size() - pos < 2) break;
        uint8_t b0 = buf[pos], b1 = buf[pos + 1]; size_t len = b1 & 127, hdr = 2; bool masked = (b1 & 0x80) != 0;
        if (len == 126) { if (buf.size() - pos < 4) break; len = (buf[pos + 2] << 8) | buf[pos + 3]; hdr = 4; }
        else if (len == 127) { if (buf.size() - pos < 10) break; len = 0; for (int i = 0; i < 8; i++) len = (len << 8) | buf[pos + 2 + i]; hdr = 10; }
        if (len > 65536) return false;
        size_t need = hdr + (masked ? 4 : 0) + len; if (buf.size() - pos < need) break;
        const uint8_t* key = &buf[pos + hdr]; uint8_t* pl = &buf[pos + hdr + (masked ? 4 : 0)];
        if (masked) for (size_t i = 0; i < len; i++) pl[i] ^= key[i & 3];
        uint8_t op = b0 & 15;
        if (op == 0 || op == 1 || op == 2) in.feed(pl, len);
        else if (op == 8) return false;
        else if (op == 9) wsFrame(replies, pl, len, 10, replyMask, rng);
        pos += need;
    }
    buf.erase(buf.begin(), buf.begin() + pos);
    return true;
}

class NetworkServer {
public:
    bool start(uint16_t port) {
        listen_ = socket(AF_INET, SOCK_STREAM, 0);
        if (listen_ == SOCK_BAD) return false;
        int one = 1; setsockopt(listen_, SOL_SOCKET, SO_REUSEADDR, (const char*)&one, sizeof(one));
        sockaddr_in a; memset(&a, 0, sizeof(a)); a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_ANY); a.sin_port = htons(port);
        if (bind(listen_, (sockaddr*)&a, sizeof(a)) != 0 || listen(listen_, 64) != 0) { sockClose(listen_); listen_ = SOCK_BAD; return false; }
        sockNonBlock(listen_); port_ = port;
        std::random_device rd; rng_.seed(((uint64_t)rd() << 32) ^ rd() ^ (uint64_t)std::chrono::system_clock::now().time_since_epoch().count());
        log("Server listening on TCP port %u", (unsigned)port);
        return true;
    }
    void run(std::atomic<bool>& stop) {
        double lastTick = 0, lastStats = 0;
        while (!stop) {
            fd_set rd, wr; FD_ZERO(&rd); FD_ZERO(&wr); int maxfd = (int)listen_; FD_SET(listen_, &rd);
            for (auto& c : conns_) { FD_SET(c->fd, &rd); if (!c->out.empty()) FD_SET(c->fd, &wr); maxfd = std::max(maxfd, (int)c->fd); }
            timeval tv; tv.tv_sec = 0; tv.tv_usec = 15000;
            select(maxfd + 1, &rd, &wr, nullptr, &tv);
            double t = nowSec();
            if (FD_ISSET(listen_, &rd)) acceptAll(t);
            for (auto& c : conns_) {
                if (c->dead) continue;
                if (FD_ISSET(c->fd, &rd)) readConn(*c, t);
                if (!c->dead && !c->out.empty()) writeConn(*c);
                if (c->closeAfter && c->out.empty()) c->dead = true;
            }
            if (t - lastTick >= 0.05) { lastTick = t; updateRooms(t); }
            if (t - lastStats >= 5.0) { lastStats = t; int online = 0, racing = 0; for (auto& kv : sessions_) if (kv.second.conn) online++; for (auto& kv : rooms_) if (kv.second.state == 1) racing += kv.second.count();
                Writer w; w.u16((uint16_t)online); w.u16((uint16_t)racing); for (auto& c : conns_) if (!c->dead && c->session) sendTo(c.get(), S_STATS, w); }
            for (auto& c : conns_) {
                if (c->dead) continue;
                if (t - c->lastRecv > 10.0) { log("timeout %s", c->ip.c_str()); c->dead = true; }
                else if (!c->session && t - c->created > 5.0) c->dead = true;  // no HELLO
            }
            for (auto& c : conns_) if (!c->dead && !c->out.empty()) writeConn(*c);
            reap(t);
        }
        shutdown();
    }
    void shutdown() {
        for (auto& c : conns_) if (c->fd != SOCK_BAD) { ::shutdown(c->fd, 2); sockClose(c->fd); c->fd = SOCK_BAD; }
        conns_.clear();
        if (listen_ != SOCK_BAD) { sockClose(listen_); listen_ = SOCK_BAD; }
    }

private:
    sock_t listen_ = SOCK_BAD; uint16_t port_ = 0; std::vector<std::unique_ptr<Conn>> conns_;
    std::unordered_map<uint32_t, Session> sessions_; std::unordered_map<std::string, Room> rooms_; uint32_t nextSid_ = 1; std::mt19937_64 rng_; std::mt19937 wsRng_{12345};

    static void log(const char* f, ...) {
        char b[256]; va_list ap; va_start(ap, f); vsnprintf(b, sizeof(b), f, ap); va_end(ap);
        printf("[%8.1f] %s\n", nowSec(), b); fflush(stdout);
    }
    void acceptAll(double t) {
        for (int i = 0; i < 16; i++) {
            sockaddr_in a; socklen_t l = sizeof(a);
            sock_t s = accept(listen_, (sockaddr*)&a, &l);
            if (s == SOCK_BAD) return;
            if (conns_.size() >= 500) { sockClose(s); continue; }
            sockNonBlock(s); sockNoDelay(s);
            auto c = std::make_unique<Conn>(); c->fd = s; c->lastRecv = c->created = c->lastTok = t; c->ip = inet_ntoa(a.sin_addr);
            conns_.push_back(std::move(c));
        }
    }
    void ingest(Conn& c, const uint8_t* d, size_t n) {
        if (c.mode == 1) { c.in.feed(d, n); return; }
        c.raw.insert(c.raw.end(), d, d + n);
        if (c.mode == 0) {
            if (c.raw.size() < 4) return;
            if (memcmp(c.raw.data(), "GET ", 4) != 0 && memcmp(c.raw.data(), "HEAD", 4) != 0) { c.mode = 1; c.in.feed(c.raw.data(), c.raw.size()); c.raw.clear(); return; }
            std::string s((const char*)c.raw.data(), c.raw.size()); size_t he = s.find("\r\n\r\n");
            if (he == std::string::npos) { if (s.size() > 8192) c.dead = true; return; }
            std::string low = s.substr(0, he); for (auto& ch : low) ch = (char)tolower((unsigned char)ch);
            size_t kp = low.find("sec-websocket-key:");
            if (low.find("upgrade: websocket") != std::string::npos && kp != std::string::npos) {
                size_t a = kp + 18, b = s.find("\r\n", a); std::string key = s.substr(a, (b == std::string::npos ? he : b) - a);
                while (!key.empty() && key.front() == ' ') key.erase(key.begin()); while (!key.empty() && (key.back() == ' ' || key.back() == '\r')) key.pop_back();
                std::string r = "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + wsAccept(key) + "\r\n\r\n";
                c.out.insert(c.out.end(), r.begin(), r.end()); c.mode = 2; c.raw.erase(c.raw.begin(), c.raw.begin() + he + 4);
            } else {  // plain HTTP: health check / browser visit
                std::string body = "Typing Maestro Race server is running.\n"; bool head = memcmp(c.raw.data(), "HEAD", 4) == 0;
                std::string r = "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: " + std::to_string(body.size()) + "\r\nConnection: close\r\n\r\n" + (head ? "" : body);
                c.out.insert(c.out.end(), r.begin(), r.end()); c.closeAfter = true; c.raw.clear(); return;
            }
        }
        std::vector<uint8_t> replies;
        if (!wsParse(c.raw, c.in, replies, false, wsRng_)) c.dead = true;
        c.out.insert(c.out.end(), replies.begin(), replies.end());
    }
    void readConn(Conn& c, double t) {
        uint8_t buf[4096];
        for (int i = 0; i < 8; i++) {
            int n = (int)recv(c.fd, (char*)buf, sizeof(buf), 0);
            if (n > 0) { c.lastRecv = t; ingest(c, buf, (size_t)n); }
            else if (n == 0) { c.dead = true; return; }
            else { if (!errWouldBlock(sockErr())) c.dead = true; break; }
        }
        uint8_t type; std::vector<uint8_t> pl;
        while (!c.dead && c.in.next(type, pl)) {
            c.tokens = std::min(60.0, c.tokens + (t - c.lastTok) * 60.0); c.lastTok = t;
            if (c.tokens < 1.0) { if (c.tokens < -120) c.dead = true; c.tokens -= 1.0; continue; }  // basic rate limit
            c.tokens -= 1.0;
            handle(c, type, pl, t);
        }
        if (c.in.bad) { log("bad frame from %s", c.ip.c_str()); c.dead = true; }
    }
    void writeConn(Conn& c) {
        while (!c.out.empty()) {
            int n = (int)send(c.fd, (const char*)c.out.data(), (int)std::min<size_t>(c.out.size(), 8192), MSG_NOSIGNAL);
            if (n > 0) c.out.erase(c.out.begin(), c.out.begin() + n);
            else { if (!errWouldBlock(sockErr())) c.dead = true; break; }
        }
        if (c.out.size() > 256 * 1024) c.dead = true;  // client not reading
    }
    void reap(double t) {
        for (auto& c : conns_) {
            if (!c->dead || c->fd == SOCK_BAD) continue;
            writeConn(*c);  // best-effort flush (kick messages)
            sockClose(c->fd); c->fd = SOCK_BAD;
            if (c->session) {
                auto it = sessions_.find(c->session);
                if (it != sessions_.end() && it->second.conn == c.get()) {
                    Session& s = it->second; s.conn = nullptr; s.disconnectedAt = t;
                    log("%s disconnected", s.name.c_str());
                    onDisconnected(s);
                }
            }
        }
        conns_.erase(std::remove_if(conns_.begin(), conns_.end(), [](const std::unique_ptr<Conn>& c) { return c->fd == SOCK_BAD; }), conns_.end());
    }

    // ---------- send helpers ----------
    void sendTo(Conn* c, uint8_t type, const Writer& w) {
        if (!c || c->dead) return;
        if (c->mode == 2) { std::vector<uint8_t> f; appendFrame(f, type, w); wsFrame(c->out, f.data(), f.size(), 2, false, wsRng_); } else appendFrame(c->out, type, w);
    }
    void sendErr(Conn& c, const std::string& m) { Writer w; w.str(m); sendTo(&c, S_ERROR, w); }
    void kick(Conn& c, const std::string& m) { Writer w; w.str(m); sendTo(&c, S_KICK, w); c.dead = true; }
    void broadcast(Room& r, uint8_t type, const Writer& w) { for (int i = 0; i < 4; i++) if (r.sid[i]) sendTo(sessions_[r.sid[i]].conn, type, w); }
    Room* roomOf(Session& s) { if (s.room.empty()) return nullptr; auto it = rooms_.find(s.room); return it == rooms_.end() ? nullptr : &it->second; }

    void sendRoom(Room& r) {
        for (int i = 0; i < 4; i++) {
            if (!r.sid[i]) continue;
            Session& me = sessions_[r.sid[i]]; if (!me.conn) continue;
            Writer w; w.str(r.code); w.u8(r.state); w.u8((uint8_t)r.host); w.u8(r.difficulty); w.u8(r.length); w.u8(r.isPublic); w.u16(r.limit); w.u8((uint8_t)i);
            for (int k = 0; k < 4; k++) {
                if (!r.sid[k]) { w.u8(0); continue; }
                Session& o = sessions_[r.sid[k]];
                w.u8(1); w.str(o.name); w.u8(r.car[k]); w.u8(r.color[k]); w.u8(r.ready[k]); w.u8(o.conn ? 1 : 0); w.u16((uint16_t)(r.lastWpm[k] * 10));
            }
            sendTo(me.conn, S_ROOM, w);
        }
    }
    void sendRaceStart(Room& r, int slot) {
        if (!r.sid[slot]) return;
        Session& me = sessions_[r.sid[slot]]; if (!me.conn) return;
        uint8_t mask = 0; for (int i = 0; i < 4; i++) if (r.core.r[i].active) mask |= 1 << i;
        Writer w; w.u64((uint64_t)(r.core.startAt * 1000.0)); w.u16((uint16_t)r.core.timeLimit); w.u8(r.core.r[slot].active ? (uint8_t)slot : 255); w.u8(mask);
        for (int i = 0; i < 4; i++) if (mask & (1 << i)) { w.str(r.core.r[i].name); w.u8(r.core.r[i].car); w.u8(r.core.r[i].color); }
        w.lstr(r.core.text);
        sendTo(me.conn, S_RACE_START, w);
    }
    static std::string cleanName(const std::string& n, uint32_t id) {
        std::string o; for (char c : n) if (c >= 32 && c <= 126) o += c;
        while (!o.empty() && o.front() == ' ') o.erase(o.begin());
        while (!o.empty() && o.back() == ' ') o.pop_back();
        if (o.size() > 14) o.resize(14);
        return o.empty() ? "Racer" + std::to_string(id) : o;
    }
    std::string newCode() {
        for (;;) { std::string c; for (int i = 0; i < 4; i++) c += (char)('0' + rng_() % 10); if (c[0] == '0') c[0] = '1'; if (!rooms_.count(c)) return c; }
    }
    Room& createRoom(Session& s, uint8_t diff, uint8_t len, bool pub, int tmIdx = 1) {
        static const uint16_t kTimes[4] = {60, 90, 120, 180}; Room r; r.code = newCode(); r.limit = kTimes[clampv(tmIdx, 0, 3)]; r.lastChange = nowSec(); r.difficulty = clampv<uint8_t>(diff, 0, 2); r.length = clampv<uint8_t>(len, 0, 2); r.isPublic = pub;
        r.sid[0] = s.id; r.host = 0; s.room = r.code; s.slot = 0;
        Room& ref = rooms_[r.code] = r; log("room %s created by %s", r.code.c_str(), s.name.c_str()); return ref;
    }
    bool joinRoom(Session& s, Room& r) {
        for (int i = 0; i < 4; i++) if (!r.sid[i]) {
            r.sid[i] = s.id; r.ready[i] = false; r.car[i] = (uint8_t)i; r.color[i] = (uint8_t)i; s.room = r.code; s.slot = i; r.lastChange = nowSec();
            log("%s joined %s (slot %d)", s.name.c_str(), r.code.c_str(), i); return true;
        }
        return false;
    }
    void removeFromRoom(Session& s) {
        Room* r = roomOf(s); int slot = s.slot; s.room.clear(); s.slot = -1;
        if (!r || slot < 0) return;
        r->sid[slot] = 0; r->ready[slot] = false; r->lastChange = nowSec();
        if (r->state == 1 && r->core.r[slot].active) r->core.r[slot].connected = false;
        if (r->host == slot) { for (int i = 0; i < 4; i++) if (r->sid[i]) { r->host = i; break; } }
        if (r->count() == 0) { log("room %s closed (empty)", r->code.c_str()); rooms_.erase(r->code); return; }
        sendRoom(*r);
    }
    void onDisconnected(Session& s) {
        Room* r = roomOf(s); if (!r) return;
        if (r->state == 1 && s.slot >= 0 && r->core.r[s.slot].active) { r->core.r[s.slot].connected = false; sendRoom(*r); }  // grace period for reconnect
        else removeFromRoom(s);
    }

    // ---------- message handling ----------
    void handle(Conn& c, uint8_t type, const std::vector<uint8_t>& pl, double t) {
        Reader rd(pl);
        if (type == C_HELLO) {
            uint32_t magic = rd.u32(); uint64_t token = rd.u64(); std::string name = rd.str(32);
            if (!rd.ok || magic != PROTO_MAGIC) { kick(c, "Protocol mismatch"); return; }
            if (c.session) return;
            Session* s = nullptr; bool resumed = false;
            if (token) for (auto& kv : sessions_) if (kv.second.token == token) { s = &kv.second; break; }
            if (s) {
                if (s->conn && s->conn != &c) { s->conn->session = 0; s->conn->dead = true; }
                s->conn = &c; s->disconnectedAt = 0; resumed = true;
            } else {
                Session ns; ns.id = nextSid_++; ns.token = rng_() | 1; ns.name = cleanName(name, ns.id); ns.conn = &c;
                s = &(sessions_[ns.id] = ns);
            }
            c.session = s->id;
            Writer w; w.u32(s->id); w.u64(s->token); w.u64(nowMs()); w.u8(resumed);
            sendTo(&c, S_WELCOME, w); { int online = 0, racing = 0; for (auto& kv : sessions_) if (kv.second.conn) online++; for (auto& kv : rooms_) if (kv.second.state == 1) racing += kv.second.count(); Writer sw; sw.u16((uint16_t)online); sw.u16((uint16_t)racing); sendTo(&c, S_STATS, sw); }
            log("%s %s (%s)", s->name.c_str(), resumed ? "reconnected" : "connected", c.ip.c_str());
            if (resumed) if (Room* r = roomOf(*s)) {
                if (r->state == 1) r->core.r[s->slot].connected = r->core.r[s->slot].active ? true : r->core.r[s->slot].connected;
                sendRoom(*r); if (r->state == 1) sendRaceStart(*r, s->slot);
            }
            return;
        }
        if (!c.session) { kick(c, "HELLO required"); return; }
        Session& s = sessions_[c.session];
        Room* r = roomOf(s);
        auto bad = [&]() { if (++c.strikes > 8) kick(c, "Too many invalid messages"); };
        switch (type) {
        case C_PING: { uint64_t ct = rd.u64(); if (!rd.ok) return bad(); Writer w; w.u64(ct); w.u64(nowMs()); sendTo(&c, S_PONG, w); break; }
        case C_CREATE: {
            uint8_t d = rd.u8(), l = rd.u8(), pub = rd.u8(), tm = rd.u8(); if (!rd.ok) return bad();
            if (r) return sendErr(c, "Already in a room");
            Room& nr = createRoom(s, d, l, pub != 0, tm); sendRoom(nr); break;
        }
        case C_JOIN: {
            std::string code = rd.str(8); if (!rd.ok) return bad();
            for (auto& ch : code) ch = (char)toupper((unsigned char)ch);
            if (r) return sendErr(c, "Already in a room");
            auto it = rooms_.find(code); if (it == rooms_.end()) return sendErr(c, "Room not found");
            if (it->second.state != 0) return sendErr(c, "Race already in progress");
            if (!joinRoom(s, it->second)) return sendErr(c, "Room is full (4/4)");
            sendRoom(it->second); break;
        }
        case C_QUICK: {
            uint8_t d = rd.u8(), l = rd.u8(); if (!rd.ok) return bad();
            if (r) return sendErr(c, "Already in a room");
            Room* best = nullptr;
            for (auto& kv : rooms_) if (kv.second.isPublic && kv.second.state == 0 && kv.second.count() < 4 && (!best || kv.second.count() > best->count())) best = &kv.second;
            if (best) joinRoom(s, *best); else best = &createRoom(s, d, l, true, 1);
            sendRoom(*best); break;
        }
        case C_LEAVE: removeFromRoom(s); break;
        case C_READY: {
            uint8_t v = rd.u8(); if (!rd.ok) return bad();
            if (!r || r->state != 0) { return; } r->ready[s.slot] = v != 0; sendRoom(*r); break;
        }
        case C_CAR: {
            uint8_t car = rd.u8(), col = rd.u8(); if (!rd.ok || car > 63) return bad();
            if (!r || r->state != 0) { return; } r->car[s.slot] = car; r->color[s.slot] = col; sendRoom(*r); break;
        }
        case C_START: {
            if (!r || r->state != 0) return;
            if (s.slot != r->host) return sendErr(c, "Only the host can start");
            int nready = 0; for (int i = 0; i < 4; i++) if (r->sid[i] && r->ready[i] && sessions_[r->sid[i]].conn) nready++;
            if (nready == 0) return sendErr(c, "At least one player must be ready");
            startRace(*r); break;
        }
        case C_TIME: {
            uint8_t idx = rd.u8(); if (!rd.ok || idx > 3) return bad();
            if (!r || r->state != 0 || s.slot != r->host) return; static const uint16_t kTimes[4] = {60, 90, 120, 180}; r->limit = kTimes[idx]; sendRoom(*r); break;
        }
        case C_NAME: { std::string nm = rd.str(32); if (!rd.ok) return bad(); s.name = cleanName(nm, s.id); if (r) sendRoom(*r); break; }
        case C_TYPE: {
            uint32_t seq = rd.u32(); uint8_t ch = rd.u8(); if (!rd.ok) return bad();
            if (!r || r->state != 1) { return; } r->core.onChar(s.slot, seq, (char)ch, nowSec()); break;
        }
        case C_NITRO: if (r && r->state == 1) r->core.onNitro(s.slot, nowSec()); break;
        default: bad(); break;
        }
        (void)t;
    }

    void startRace(Room& r) {
        int nready = 0; for (int i = 0; i < 4; i++) if (r.sid[i] && r.ready[i] && sessions_[r.sid[i]].conn) nready++;
        int lenIdx = r.limit <= 90 ? 1 : 2; std::string text = genParagraph(r.difficulty, lenIdx, rng_);
        r.core.reset(text, nowSec() + COUNTDOWN_SEC, (double)r.limit);
        for (int i = 0; i < 4; i++) if (r.sid[i] && r.ready[i] && sessions_[r.sid[i]].conn) {
            RacerCore& p = r.core.r[i]; p.active = true; p.connected = true; p.name = sessions_[r.sid[i]].name; p.car = r.car[i]; p.color = r.color[i];
        }
        r.state = 1; r.lastBroadcast = 0; log("room %s race start: %d racers, %zu chars, %us", r.code.c_str(), nready, text.size(), (unsigned)r.limit);
        for (int i = 0; i < 4; i++) sendRaceStart(r, i);
        sendRoom(r);
    }
    void finishRace(Room& r, double now) {
        r.core.computePlaces();
        int winner = -1, best = -1; float bestScore = -1;
        for (int i = 0; i < 4; i++) {
            if (!r.core.r[i].active) continue;
            if (r.core.r[i].place == 1) winner = i;
            float sc = r.core.wpm(i, now) * (r.core.accuracy(i) / 100.f) * (r.core.r[i].finished ? 1.f : 0.8f);
            if (sc > bestScore) { bestScore = sc; best = i; }
            r.lastWpm[i] = r.core.wpm(i, now);
        }
        Writer w; w.u8(winner < 0 ? 255 : (uint8_t)winner); w.u8(best < 0 ? 255 : (uint8_t)best); w.u16((uint16_t)r.core.text.size());
        int n = 0; for (int i = 0; i < 4; i++) if (r.core.r[i].active) n++;
        w.u8((uint8_t)n); for (int i = 0; i < 4; i++) if (r.core.r[i].active) putSnap(w, r.core.snap(i, now));
        broadcast(r, S_RACE_END, w);
        r.state = 2; r.resultsAt = now; log("room %s race finished", r.code.c_str());
        sendRoom(r);
    }
    void updateRooms(double t) {
        std::vector<std::string> dead;
        for (auto& kv : rooms_) {
            Room& r = kv.second;
            // drop players whose reconnect grace expired
            for (int i = 0; i < 4; i++) if (r.sid[i]) {
                Session& s = sessions_[r.sid[i]];
                if (!s.conn && t - s.disconnectedAt > 15.0) { log("%s timed out of room %s", s.name.c_str(), r.code.c_str()); removeFromRoom(s); break; }
            }
            if (rooms_.find(kv.first) == rooms_.end()) break;
            if (r.state == 0 && r.isPublic) {  // matchmaking rooms start by themselves
                int n = 0; for (int i = 0; i < 4; i++) if (r.sid[i] && sessions_[r.sid[i]].conn) n++;
                if (n == 4) { if (r.fullAt == 0) r.fullAt = t; } else r.fullAt = 0;
                if ((r.fullAt > 0 && t - r.fullAt > 2.0) || (n >= 2 && t - r.lastChange > 18.0)) { for (int i = 0; i < 4; i++) r.ready[i] = r.sid[i] != 0; r.fullAt = 0; startRace(r); }
            }
            if (r.state == 1) {
                r.core.tick(t);
                if (t - r.lastBroadcast >= 0.05) {
                    r.lastBroadcast = t;
                    Writer w; w.u64(nowMs()); w.u64(r.core.forcedEndMs()); int n = 0; for (auto& p : r.core.r) if (p.active) n++;
                    w.u8((uint8_t)n); for (int i = 0; i < 4; i++) if (r.core.r[i].active) putSnap(w, r.core.snap(i, t));
                    broadcast(r, S_RACE_STATE, w);
                }
                if (r.core.over) finishRace(r, t);
            } else if (r.state == 2 && t - r.resultsAt > 6.0) {
                r.state = 0; r.lastChange = t; for (int i = 0; i < 4; i++) r.ready[i] = false; sendRoom(r);
            }
        }
        for (auto it = sessions_.begin(); it != sessions_.end();) {  // forget stale sessions
            if (!it->second.conn && it->second.room.empty() && t - it->second.disconnectedAt > 120.0) it = sessions_.erase(it); else ++it;
        }
    }
};


// ============================================================================
//  7. NETWORK CLIENT  (own thread; raw TCP, WebSocket "ws://" and TLS WebSocket "wss://")
//     wss:// uses OpenSSL loaded at runtime (libssl) - no build-time dependency.
// ============================================================================
#ifndef TM_SERVER_ONLY
enum NetState { NS_IDLE = 0, NS_CONNECTING, NS_CONNECTED, NS_RECONNECTING, NS_FAILED };
struct NetEvent { uint8_t type = 0; std::vector<uint8_t> data; };

struct Endpoint { int scheme = 0; std::string host, path = "/"; int port = 0; };  // scheme: 0 raw TCP, 1 ws, 2 wss
static Endpoint parseEndpoint(std::string h, int defPort) {
    Endpoint e; auto starts = [&](const char* p) { return h.compare(0, strlen(p), p) == 0; };
    if (starts("wss://")) { e.scheme = 2; h = h.substr(6); } else if (starts("https://")) { e.scheme = 2; h = h.substr(8); }
    else if (starts("ws://")) { e.scheme = 1; h = h.substr(5); } else if (starts("http://")) { e.scheme = 1; h = h.substr(7); }
    size_t sl = h.find('/'); if (sl != std::string::npos) { e.path = h.substr(sl); h = h.substr(0, sl); }
    size_t c = h.rfind(':'); bool hasPort = false;
    if (c != std::string::npos && h.find(']') == std::string::npos) { e.port = atoi(h.substr(c + 1).c_str()); h = h.substr(0, c); hasPort = true; }
    e.host = h; if (!hasPort || e.port <= 0) e.port = e.scheme == 2 ? 443 : e.scheme == 1 ? 80 : defPort;
    return e;
}

struct TlsApi {
    SDL_SharedObject* lib = nullptr; bool ok = false, tried = false; void* ctx = nullptr;
    const void* (*method)() = nullptr; void* (*ctxNew)(const void*) = nullptr; long (*ctxCtrl)(void*, int, long, void*) = nullptr; void (*setVerify)(void*, int, void*) = nullptr;
    void* (*sslNew)(void*) = nullptr; void (*sslFree)(void*) = nullptr; int (*setFd)(void*, int) = nullptr; long (*ctrl)(void*, int, long, void*) = nullptr; int (*connect)(void*) = nullptr;
    int (*read)(void*, void*, int) = nullptr; int (*write)(void*, const void*, int) = nullptr; int (*getErr)(const void*, int) = nullptr; int (*shut)(void*) = nullptr;
    bool init() {
        if (tried) return ok; tried = true;
        static const char* names[] = {"libssl.so.3", "libssl.so.1.1", "libssl.so", "libssl-3-x64.dll", "libssl-3.dll", "libssl-1_1-x64.dll", "libssl-1_1.dll", "libssl.3.dylib", "libssl.dylib", "/usr/lib/libssl.dylib", nullptr};
        for (int i = 0; names[i] && !lib; i++) lib = SDL_LoadObject(names[i]);
        if (!lib) return false;
#define TM_SYM(var, name) var = reinterpret_cast<decltype(var)>(SDL_LoadFunction(lib, name)); if (!var) return false;
        TM_SYM(method, "TLS_client_method") TM_SYM(ctxNew, "SSL_CTX_new") TM_SYM(ctxCtrl, "SSL_CTX_ctrl") TM_SYM(setVerify, "SSL_CTX_set_verify") TM_SYM(sslNew, "SSL_new") TM_SYM(sslFree, "SSL_free")
        TM_SYM(setFd, "SSL_set_fd") TM_SYM(ctrl, "SSL_ctrl") TM_SYM(connect, "SSL_connect") TM_SYM(read, "SSL_read") TM_SYM(write, "SSL_write") TM_SYM(getErr, "SSL_get_error") TM_SYM(shut, "SSL_shutdown")
#undef TM_SYM
        ctx = ctxNew(method()); if (!ctx) return false;
        ctxCtrl(ctx, 33, 3, nullptr);  // SSL_CTX_set_mode(PARTIAL_WRITE | ACCEPT_MOVING_WRITE_BUFFER)
        setVerify(ctx, 0, nullptr);    // encrypted transport; certificate chain is NOT verified (see README)
        ok = true; return true;
    }
};

class NetworkClient {
public:
    ~NetworkClient() { stop(); }
    void start() { if (running_) return; running_ = true; th_ = std::thread(&NetworkClient::loop, this); }
    void stop() { if (!running_) return; running_ = false; want_ = false; cv_.notify_all(); if (th_.joinable()) th_.join(); }
    void connectTo(const std::string& host, int port, const std::string& name) {
        { std::lock_guard<std::mutex> l(m_); host_ = host; port_ = port; name_ = name; evq_.clear(); outq_.clear(); status_ = "Connecting..."; }
        want_ = true; state_ = NS_CONNECTING; cv_.notify_all();
    }
    void disconnect() { want_ = false; cv_.notify_all(); }
    void send(uint8_t type, const Writer& w) {
        if (state_ != NS_CONNECTED) return;
        std::vector<uint8_t> f; appendFrame(f, type, w);
        std::lock_guard<std::mutex> l(m_); if (outq_.size() < 65536) outq_.insert(outq_.end(), f.begin(), f.end());
    }
    void send(uint8_t type) { send(type, Writer()); }
    bool poll(NetEvent& e) { std::lock_guard<std::mutex> l(m_); if (evq_.empty()) return false; e = std::move(evq_.front()); evq_.pop_front(); return true; }
    int state() const { return state_; }
    std::string status() { std::lock_guard<std::mutex> l(m_); return status_; }
    void setToken(uint64_t t) { token_ = t; }

private:
    struct Link { sock_t fd = SOCK_BAD; void* ssl = nullptr; bool ws = false; std::vector<uint8_t> wsIn, wire; };
    std::thread th_; std::atomic<bool> running_{false}, want_{false}; std::atomic<int> state_{NS_IDLE}; std::atomic<uint64_t> token_{0};
    std::mutex m_; std::condition_variable cv_; std::deque<NetEvent> evq_; std::vector<uint8_t> outq_; std::string host_, name_, status_; int port_ = DEFAULT_PORT;
    TlsApi tls_; std::mt19937 rng_{std::random_device{}()};

    void push(uint8_t type, const std::string& s = "") { std::lock_guard<std::mutex> l(m_); NetEvent e; e.type = type; e.data.assign(s.begin(), s.end()); evq_.push_back(std::move(e)); }
    void setStatus(const std::string& s) { std::lock_guard<std::mutex> l(m_); status_ = s; }
    static void waitFd(sock_t fd, bool wr, int ms) { fd_set s; FD_ZERO(&s); FD_SET(fd, &s); timeval tv{0, ms * 1000}; if (wr) select((int)fd + 1, nullptr, &s, nullptr, &tv); else select((int)fd + 1, &s, nullptr, nullptr, &tv); }

    int lowRecv(Link& l, uint8_t* b, int n) {
        if (l.ssl) { int r = tls_.read(l.ssl, b, n); if (r > 0) return r; int e = tls_.getErr(l.ssl, r); return (e == 2 || e == 3) ? 0 : -1; }
        int r = (int)recv(l.fd, (char*)b, n, 0); if (r > 0) return r; if (r == 0) return -1; return errWouldBlock(sockErr()) ? 0 : -1;
    }
    int lowSend(Link& l, const uint8_t* b, int n) {
        if (l.ssl) { int r = tls_.write(l.ssl, b, n); if (r > 0) return r; int e = tls_.getErr(l.ssl, r); return (e == 2 || e == 3) ? 0 : -1; }
        int r = (int)::send(l.fd, (const char*)b, n, MSG_NOSIGNAL); if (r > 0) return r; return errWouldBlock(sockErr()) ? 0 : -1;
    }
    void closeLink(Link& l) {
        if (l.ssl) { tls_.shut(l.ssl); tls_.sslFree(l.ssl); l.ssl = nullptr; }
        if (l.fd != SOCK_BAD) { ::shutdown(l.fd, 2); sockClose(l.fd); l.fd = SOCK_BAD; }
        l.ws = false; l.wsIn.clear(); l.wire.clear();
    }
    sock_t tcpConnect(const std::string& host, int port, double timeout, std::string& err) {
        addrinfo hints; memset(&hints, 0, sizeof(hints)); hints.ai_family = AF_UNSPEC; hints.ai_socktype = SOCK_STREAM; addrinfo* res = nullptr;
        if (getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &res) != 0 || !res) { err = "Cannot resolve " + host; return SOCK_BAD; }
        sock_t s = SOCK_BAD;
        for (addrinfo* a = res; a && s == SOCK_BAD; a = a->ai_next) {
            sock_t t = socket(a->ai_family, a->ai_socktype, a->ai_protocol); if (t == SOCK_BAD) continue;
            sockNonBlock(t); int rc = connect(t, a->ai_addr, (int)a->ai_addrlen); bool ok = rc == 0;
            if (!ok && errWouldBlock(sockErr())) {
                double end = nowSec() + timeout;
                while (nowSec() < end && want_) {
                    fd_set wr, ex; FD_ZERO(&wr); FD_ZERO(&ex); FD_SET(t, &wr); FD_SET(t, &ex); timeval tv{0, 100000};
                    if (select((int)t + 1, nullptr, &wr, &ex, &tv) > 0) { int e = 0; socklen_t el = sizeof(e); getsockopt(t, SOL_SOCKET, SO_ERROR, (char*)&e, &el); ok = (e == 0 && FD_ISSET(t, &wr)); break; }
                }
            }
            if (ok) s = t; else sockClose(t);
        }
        freeaddrinfo(res);
        if (s == SOCK_BAD) err = "Could not reach " + host + ":" + std::to_string(port); else sockNoDelay(s);
        return s;
    }
    // TCP (+TLS) (+WebSocket upgrade). Blocks only this worker thread.
    bool openLink(Link& l, const Endpoint& ep, std::string& err) {
        double tmo = ep.scheme ? 75.0 : 5.0;  // free hosts need up to ~1 minute to wake up
        if (ep.scheme == 2) setStatus("Connecting securely (server may be waking up, ~1 min)...");
        else if (ep.scheme == 1) setStatus("Connecting (WebSocket)...");
        l.fd = tcpConnect(ep.host, ep.port, 15.0, err); if (l.fd == SOCK_BAD) return false;
        if (ep.scheme == 2) {
            if (!tls_.init()) { err = "wss:// needs OpenSSL (libssl) installed/bundled"; closeLink(l); return false; }
            l.ssl = tls_.sslNew(tls_.ctx); tls_.setFd(l.ssl, (int)l.fd); tls_.ctrl(l.ssl, 55, 0, (void*)ep.host.c_str());  // SNI
            double end = nowSec() + 20;
            for (;;) {
                int r = tls_.connect(l.ssl); if (r == 1) break;
                int e = tls_.getErr(l.ssl, r); if ((e != 2 && e != 3) || nowSec() > end || !want_) { err = "TLS handshake failed"; closeLink(l); return false; }
                waitFd(l.fd, e == 3, 100);
            }
        }
        if (ep.scheme >= 1) {
            uint8_t k[16]; for (auto& b : k) b = (uint8_t)(rng_() & 255);
            std::string req = "GET " + ep.path + " HTTP/1.1\r\nHost: " + ep.host + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: " + base64(k, 16) + "\r\nSec-WebSocket-Version: 13\r\nUser-Agent: TypingMaestro\r\n\r\n";
            size_t off = 0; double end = nowSec() + tmo;
            while (off < req.size()) { int r = lowSend(l, (const uint8_t*)req.data() + off, (int)(req.size() - off)); if (r < 0 || nowSec() > end || !want_) { err = "Handshake send failed"; closeLink(l); return false; } off += r; if (r == 0) waitFd(l.fd, true, 50); }
            std::string resp;
            while (resp.find("\r\n\r\n") == std::string::npos) {
                uint8_t b[1024]; int r = lowRecv(l, b, sizeof(b));
                if (r < 0 || nowSec() > end || !want_ || resp.size() > 16384) { err = "Server did not answer (is it awake?)"; closeLink(l); return false; }
                if (r == 0) { if (l.ssl) std::this_thread::sleep_for(std::chrono::milliseconds(20)); else waitFd(l.fd, false, 100); } else resp.append((char*)b, r);
            }
            if (resp.compare(0, 12, "HTTP/1.1 101") != 0) { err = "Server not ready: " + resp.substr(0, std::min<size_t>(resp.find("\r\n"), 40)); closeLink(l); return false; }
            size_t he = resp.find("\r\n\r\n") + 4; l.wsIn.assign(resp.begin() + he, resp.end()); l.ws = true;
        }
        return true;
    }
    void loop() {
        Link l; FrameBuffer in; std::vector<uint8_t> out; double lastRecv = 0, lastPing = 0, nextTry = 0; int attempts = 0; bool ever = false;
        while (running_) {
            if (!want_) {
                if (l.fd != SOCK_BAD) {  // flush final messages (e.g. LEAVE), then close
                    { std::lock_guard<std::mutex> g(m_); out.insert(out.end(), outq_.begin(), outq_.end()); outq_.clear(); }
                    if (l.ws && !out.empty()) { wsFrame(l.wire, out.data(), out.size(), 2, true, rng_); out.clear(); } else if (!out.empty()) { l.wire.insert(l.wire.end(), out.begin(), out.end()); out.clear(); }
                    for (int i = 0; i < 20 && !l.wire.empty(); i++) { int n = lowSend(l, l.wire.data(), (int)l.wire.size()); if (n > 0) l.wire.erase(l.wire.begin(), l.wire.begin() + n); else std::this_thread::sleep_for(std::chrono::milliseconds(5)); }
                    closeLink(l);
                }
                if (state_ != NS_FAILED) state_ = NS_IDLE;
                ever = false; attempts = 0; out.clear();
                std::this_thread::sleep_for(std::chrono::milliseconds(50)); continue;  // plain sleep: condition_variable::wait_for needs API 30+ on Android
            }
            if (l.fd == SOCK_BAD) {
                double t = nowSec(); if (t < nextTry) { std::this_thread::sleep_for(std::chrono::milliseconds(20)); continue; }
                state_ = ever ? NS_RECONNECTING : NS_CONNECTING;
                Endpoint ep; { std::lock_guard<std::mutex> g(m_); ep = parseEndpoint(host_, port_); }
                if (ever) setStatus("Connection lost - reconnecting (" + std::to_string(attempts + 1) + ")..."); else setStatus("Connecting...");
                std::string err;
                if (!openLink(l, ep, err)) {
                    attempts++; if (!want_) continue;
                    int maxTries = ever ? 12 : (ep.scheme ? 4 : 1);
                    if (attempts >= maxTries) { want_ = false; state_ = NS_FAILED; std::string m = ever ? "Lost connection to server" : err; setStatus(m); push(EV_FAILED, m); }
                    else { setStatus(err + " - retrying..."); nextTry = nowSec() + 3.0; }
                    continue;
                }
                in = FrameBuffer(); out.clear(); { std::lock_guard<std::mutex> g(m_); outq_.clear(); }
                Writer h; h.u32(PROTO_MAGIC); h.u64(token_); { std::lock_guard<std::mutex> g(m_); h.str(name_); } appendFrame(out, C_HELLO, h);
                lastRecv = nowSec(); lastPing = 0; attempts = 0; ever = true; state_ = NS_CONNECTED; setStatus("Connected"); push(EV_CONNECTED);
            }
            { std::lock_guard<std::mutex> g(m_); if (!outq_.empty()) { out.insert(out.end(), outq_.begin(), outq_.end()); outq_.clear(); } }
            double t = nowSec();
            if (t - lastPing > 1.0) { Writer p; p.u64(nowMs()); appendFrame(out, C_PING, p); lastPing = t; }
            if (!out.empty()) { if (l.ws) wsFrame(l.wire, out.data(), out.size(), 2, true, rng_); else l.wire.insert(l.wire.end(), out.begin(), out.end()); out.clear(); }
            if (l.ssl) std::this_thread::sleep_for(std::chrono::milliseconds(6));
            else { fd_set rd, wr; FD_ZERO(&rd); FD_ZERO(&wr); FD_SET(l.fd, &rd); if (!l.wire.empty()) FD_SET(l.fd, &wr); timeval tv{0, 12000}; select((int)l.fd + 1, &rd, &wr, nullptr, &tv); }
            bool lost = false; uint8_t buf[4096];
            for (int i = 0; i < 8 && !lost; i++) {
                int r = lowRecv(l, buf, sizeof(buf)); if (r < 0) { lost = true; break; } if (r == 0) break;
                lastRecv = nowSec();
                if (!l.ws) in.feed(buf, r); else { l.wsIn.insert(l.wsIn.end(), buf, buf + r); if (!wsParse(l.wsIn, in, l.wire, true, rng_)) lost = true; }
            }
            if (l.ws && !l.wsIn.empty() && !lost) { if (!wsParse(l.wsIn, in, l.wire, true, rng_)) lost = true; }  // leftover bytes from the handshake read
            uint8_t type; std::vector<uint8_t> pl;
            while (in.next(type, pl)) { std::lock_guard<std::mutex> g(m_); NetEvent e; e.type = type; e.data = std::move(pl); evq_.push_back(std::move(e)); }
            if (in.bad) lost = true;
            if (!lost && !l.wire.empty()) { int n = lowSend(l, l.wire.data(), (int)std::min<size_t>(l.wire.size(), 8192)); if (n > 0) l.wire.erase(l.wire.begin(), l.wire.begin() + n); else if (n < 0) lost = true; }
            if (!lost && nowSec() - lastRecv > 8.0) lost = true;
            if (lost) { closeLink(l); push(EV_DISCONNECTED); nextTry = nowSec() + 1.0; attempts = 0; if (want_) { state_ = NS_RECONNECTING; setStatus("Connection lost - reconnecting..."); } }
        }
        closeLink(l);
    }
};
#endif  // TM_SERVER_ONLY

#ifndef TM_SERVER_ONLY
// ============================================================================
//  9. COLORS, FONT DATA, CPU CANVAS, RENDERER
// ============================================================================
struct Col { uint8_t r, g, b, a; };
static Col rgba(int r, int g, int b, int a = 255) { return Col{(uint8_t)r, (uint8_t)g, (uint8_t)b, (uint8_t)a}; }
static Col withA(Col c, int a) { c.a = (uint8_t)clampv(a, 0, 255); return c; }
static Col mixc(Col a, Col b, float t) {
    t = clampv(t, 0.f, 1.f);
    return Col{(uint8_t)(a.r + (b.r - a.r) * t), (uint8_t)(a.g + (b.g - a.g) * t), (uint8_t)(a.b + (b.b - a.b) * t), (uint8_t)(a.a + (b.a - a.a) * t)};
}
static SDL_FColor fcol(Col c) { return SDL_FColor{c.r / 255.f, c.g / 255.f, c.b / 255.f, c.a / 255.f}; }
static const Col C_BG0 = {7, 12, 28, 255}, C_BG1 = {14, 30, 66, 255}, C_CYAN = {40, 215, 255, 255}, C_BLUE = {50, 120, 255, 255}, C_WHITE = {236, 245, 255, 255},
                 C_GREY = {150, 165, 196, 255}, C_GREEN = {70, 230, 130, 255}, C_RED = {255, 84, 94, 255}, C_GOLD = {255, 205, 70, 255};
static uint32_t hash32(uint32_t x) { x ^= x >> 16; x *= 0x7feb352dU; x ^= x >> 15; x *= 0x846ca68bU; x ^= x >> 16; return x; }
static float frand01(uint32_t& s) { s = hash32(s + 0x9e3779b9U); return (s & 0xFFFFFF) / 16777216.f; }

// classic 5x7 font, 95 glyphs (ASCII 32..126), column-major, bit0 = top row
static const uint8_t kFont[95][5] = {
    {0,0,0,0,0},{0,0,0x5F,0,0},{0,7,0,7,0},{0x14,0x7F,0x14,0x7F,0x14},{0x24,0x2A,0x7F,0x2A,0x12},{0x23,0x13,8,0x64,0x62},{0x36,0x49,0x56,0x20,0x50},{0,5,3,0,0},
    {0,0x1C,0x22,0x41,0},{0,0x41,0x22,0x1C,0},{0x2A,0x1C,0x7F,0x1C,0x2A},{8,8,0x3E,8,8},{0,0x80,0x70,0x30,0},{8,8,8,8,8},{0,0,0x60,0x60,0},{0x20,0x10,8,4,2},
    {0x3E,0x51,0x49,0x45,0x3E},{0,0x42,0x7F,0x40,0},{0x72,0x49,0x49,0x49,0x46},{0x21,0x41,0x49,0x4D,0x33},{0x18,0x14,0x12,0x7F,0x10},{0x27,0x45,0x45,0x45,0x39},{0x3C,0x4A,0x49,0x49,0x31},{0x41,0x21,0x11,9,7},
    {0x36,0x49,0x49,0x49,0x36},{0x46,0x49,0x49,0x29,0x1E},{0,0,0x14,0,0},{0,0x40,0x34,0,0},{0,8,0x14,0x22,0x41},{0x14,0x14,0x14,0x14,0x14},{0,0x41,0x22,0x14,8},{2,1,0x59,9,6},
    {0x3E,0x41,0x5D,0x59,0x4E},{0x7C,0x12,0x11,0x12,0x7C},{0x7F,0x49,0x49,0x49,0x36},{0x3E,0x41,0x41,0x41,0x22},{0x7F,0x41,0x41,0x41,0x3E},{0x7F,0x49,0x49,0x49,0x41},{0x7F,9,9,9,1},{0x3E,0x41,0x41,0x51,0x73},
    {0x7F,8,8,8,0x7F},{0,0x41,0x7F,0x41,0},{0x20,0x40,0x41,0x3F,1},{0x7F,8,0x14,0x22,0x41},{0x7F,0x40,0x40,0x40,0x40},{0x7F,2,0x1C,2,0x7F},{0x7F,4,8,0x10,0x7F},{0x3E,0x41,0x41,0x41,0x3E},
    {0x7F,9,9,9,6},{0x3E,0x41,0x51,0x21,0x5E},{0x7F,9,0x19,0x29,0x46},{0x26,0x49,0x49,0x49,0x32},{3,1,0x7F,1,3},{0x3F,0x40,0x40,0x40,0x3F},{0x1F,0x20,0x40,0x20,0x1F},{0x3F,0x40,0x38,0x40,0x3F},
    {0x63,0x14,8,0x14,0x63},{3,4,0x78,4,3},{0x61,0x59,0x49,0x4D,0x43},{0,0x7F,0x41,0x41,0x41},{2,4,8,0x10,0x20},{0,0x41,0x41,0x41,0x7F},{4,2,1,2,4},{0x40,0x40,0x40,0x40,0x40},
    {0,3,7,8,0},{0x20,0x54,0x54,0x78,0x40},{0x7F,0x28,0x44,0x44,0x38},{0x38,0x44,0x44,0x44,0x28},{0x38,0x44,0x44,0x28,0x7F},{0x38,0x54,0x54,0x54,0x18},{0,8,0x7E,9,2},{0x18,0xA4,0xA4,0x9C,0x78},
    {0x7F,8,4,4,0x78},{0,0x44,0x7D,0x40,0},{0x20,0x40,0x40,0x3D,0},{0x7F,0x10,0x28,0x44,0},{0,0x41,0x7F,0x40,0},{0x7C,4,0x78,4,0x78},{0x7C,8,4,4,0x78},{0x38,0x44,0x44,0x44,0x38},
    {0xFC,0x18,0x24,0x24,0x18},{0x18,0x24,0x24,0x18,0xFC},{0x7C,8,4,4,8},{0x48,0x54,0x54,0x54,0x24},{4,4,0x3F,0x44,0x24},{0x3C,0x40,0x40,0x20,0x7C},{0x1C,0x20,0x40,0x20,0x1C},{0x3C,0x40,0x30,0x40,0x3C},
    {0x44,0x28,0x10,0x28,0x44},{0x4C,0x90,0x90,0x90,0x7C},{0x44,0x64,0x54,0x4C,0x44},{0,8,0x36,0x41,0},{0,0,0x77,0,0},{0,0x41,0x36,8,0},{2,1,2,4,2}};



#if defined(__has_include) && !defined(TM_NO_TTF)
#if __has_include(<SDL3_ttf/SDL_ttf.h>)
#include <SDL3_ttf/SDL_ttf.h>
#define TM_HAVE_TTF 1
#endif
#endif

static float smooth01(float a, float b, float x) { float t = clampv((x - a) / (b - a), 0.f, 1.f); return t * t * (3 - 2 * t); }
static float covOf(float d) { return clampv(d + 0.5f, 0.f, 1.f); }  // signed distance (inside positive) -> AA coverage
static float sdRRect(float px, float py, float cx, float cy, float hx, float hy, float r) {
    float qx = std::fabs(px - cx) - hx + r, qy = std::fabs(py - cy) - hy + r;
    return std::min(std::max(qx, qy), 0.f) + std::sqrt(std::max(qx, 0.f) * std::max(qx, 0.f) + std::max(qy, 0.f) * std::max(qy, 0.f)) - r;  // negative inside
}

// ---------------------------------------------------------------------------
//  CPU canvas: float RGBA (premultiplied), analytic anti-aliasing via coverage functions.
//  Used once at startup / on demand to bake sprites (cars, glows, panels) into GPU textures.
// ---------------------------------------------------------------------------
struct PxF { float r = 0, g = 0, b = 0, a = 0; };
class Canvas {
public:
    int w, h; std::vector<PxF> d;
    Canvas(int W, int H) : w(W), h(H), d((size_t)W * H) {}
    template <class F> void paint(float fx0, float fy0, float fx1, float fy1, F f) {  // f(x, y, PxF& straightColorWithCoverageAlpha) -> bool
        int x0 = std::max(0, (int)std::floor(fx0)), y0 = std::max(0, (int)std::floor(fy0)), x1 = std::min(w, (int)std::ceil(fx1)), y1 = std::min(h, (int)std::ceil(fy1));
        for (int y = y0; y < y1; y++) for (int x = x0; x < x1; x++) {
            PxF s; if (!f(x + 0.5f, y + 0.5f, s) || s.a <= 0.002f) continue;
            PxF& o = d[(size_t)y * w + x]; float a = clampv(s.a, 0.f, 1.f);
            o.r = s.r * a + o.r * (1 - a); o.g = s.g * a + o.g * (1 - a); o.b = s.b * a + o.b * (1 - a); o.a = a + o.a * (1 - a);
        }
    }
    void blur(int r, int passes = 2) {
        if (r < 1) return; std::vector<PxF> t(d.size());
        for (int p = 0; p < passes; p++) {
            for (int y = 0; y < h; y++) {  // horizontal
                float sr = 0, sg = 0, sb = 0, sa = 0; int n = 2 * r + 1;
                for (int x = -r; x <= r; x++) { const PxF& q = d[(size_t)y * w + clampv(x, 0, w - 1)]; sr += q.r; sg += q.g; sb += q.b; sa += q.a; }
                for (int x = 0; x < w; x++) {
                    t[(size_t)y * w + x] = PxF{sr / n, sg / n, sb / n, sa / n};
                    const PxF& a = d[(size_t)y * w + clampv(x + r + 1, 0, w - 1)]; const PxF& b = d[(size_t)y * w + clampv(x - r, 0, w - 1)];
                    sr += a.r - b.r; sg += a.g - b.g; sb += a.b - b.b; sa += a.a - b.a;
                }
            }
            for (int x = 0; x < w; x++) {  // vertical
                float sr = 0, sg = 0, sb = 0, sa = 0; int n = 2 * r + 1;
                for (int y = -r; y <= r; y++) { const PxF& q = t[(size_t)clampv(y, 0, h - 1) * w + x]; sr += q.r; sg += q.g; sb += q.b; sa += q.a; }
                for (int y = 0; y < h; y++) {
                    d[(size_t)y * w + x] = PxF{sr / n, sg / n, sb / n, sa / n};
                    const PxF& a = t[(size_t)clampv(y + r + 1, 0, h - 1) * w + x]; const PxF& b = t[(size_t)clampv(y - r, 0, h - 1) * w + x];
                    sr += a.r - b.r; sg += a.g - b.g; sb += a.b - b.b; sa += a.a - b.a;
                }
            }
        }
    }
    SDL_Texture* toTexture(SDL_Renderer* rd, SDL_BlendMode bm = SDL_BLENDMODE_BLEND) const {
        std::vector<uint8_t> px((size_t)w * h * 4);
        for (size_t i = 0; i < d.size(); i++) {
            const PxF& p = d[i]; float a = p.a; float inv = a > 1e-4f ? 1.f / a : 0.f;
            px[i * 4] = (uint8_t)(clampv(p.r * inv, 0.f, 1.f) * 255 + 0.5f); px[i * 4 + 1] = (uint8_t)(clampv(p.g * inv, 0.f, 1.f) * 255 + 0.5f);
            px[i * 4 + 2] = (uint8_t)(clampv(p.b * inv, 0.f, 1.f) * 255 + 0.5f); px[i * 4 + 3] = (uint8_t)(clampv(a, 0.f, 1.f) * 255 + 0.5f);
        }
        SDL_Texture* t = SDL_CreateTexture(rd, SDL_PIXELFORMAT_RGBA32, SDL_TEXTUREACCESS_STATIC, w, h); if (!t) return nullptr;
        SDL_UpdateTexture(t, nullptr, px.data(), w * 4); SDL_SetTextureBlendMode(t, bm); SDL_SetTextureScaleMode(t, SDL_SCALEMODE_LINEAR); return t;
    }
};

// ---------------------------------------------------------------------------
//  Gfx: anti-aliased rounded rects (cached textures), glow sprites, TTF text (bitmap fallback)
// ---------------------------------------------------------------------------
class Gfx {
public:
    SDL_Renderer* rd = nullptr; SDL_Texture *font = nullptr, *disc = nullptr, *softAdd = nullptr, *softBlend = nullptr; int S = 1; bool hasTTF = false;
    std::vector<SDL_Vertex> vb; std::vector<int> ib; std::unordered_map<uint64_t, SDL_Texture*> masks; double clock = 0;
#ifdef TM_HAVE_TTF
    std::unordered_map<int, TTF_Font*> fonts; std::string fontPath;
    struct TEnt { SDL_Texture* t = nullptr; float w = 0, h = 0; double last = 0; }; std::unordered_map<uint64_t, TEnt> tcache;
#endif
    std::string fontName = "bitmap";

    bool init(SDL_Renderer* r, const std::string& fontOverride) {
        rd = r; vb.reserve(512); ib.reserve(1536); SDL_SetRenderDrawBlendMode(rd, SDL_BLENDMODE_BLEND);
        // bitmap fallback atlas (always built; tiny)
        SDL_Surface* s = SDL_CreateSurface(96, 48, SDL_PIXELFORMAT_RGBA32);
        if (s) {
            memset(s->pixels, 0, (size_t)s->pitch * s->h);
            for (int g = 0; g < 95; g++) for (int c = 0; c < 5; c++) for (int row = 0; row < 8; row++) if ((kFont[g][c] >> row) & 1) { uint8_t* p = (uint8_t*)s->pixels + (size_t)((g / 16) * 8 + row) * s->pitch + ((g % 16) * 6 + c) * 4; p[0] = p[1] = p[2] = p[3] = 255; }
            font = SDL_CreateTextureFromSurface(rd, s); SDL_DestroySurface(s); if (font) SDL_SetTextureScaleMode(font, SDL_SCALEMODE_NEAREST);
        }
        Canvas cd(128, 128), cs(128, 128);
        cd.paint(0, 0, 128, 128, [](float x, float y, PxF& o) { float d = std::sqrt((x - 64) * (x - 64) + (y - 64) * (y - 64)); o = PxF{1, 1, 1, covOf(62.5f - d)}; return true; });
        cs.paint(0, 0, 128, 128, [](float x, float y, PxF& o) { float d = std::sqrt((x - 64) * (x - 64) + (y - 64) * (y - 64)) / 64.f; float a = std::pow(std::max(0.f, 1 - d), 2.3f); o = PxF{1, 1, 1, a}; return true; });
        disc = cd.toTexture(rd); softBlend = cs.toTexture(rd); softAdd = cs.toTexture(rd, SDL_BLENDMODE_ADD);
#ifdef TM_HAVE_TTF
        if (TTF_Init()) {
            std::vector<std::string> c; if (!fontOverride.empty()) c.push_back(fontOverride);
            const char* base = SDL_GetBasePath(); std::string b = base ? base : "";
            for (const char* n : {"font.ttf", "Orbitron-Bold.ttf", "Orbitron.ttf", "Rajdhani-Bold.ttf", "Exo2-Bold.ttf", "assets/font.ttf", "fonts/font.ttf"}) { c.push_back(b + n); c.push_back(n); }
            for (const char* n : {"/system/fonts/Roboto-Bold.ttf", "/system/fonts/Roboto-Medium.ttf", "/system/fonts/Roboto-Regular.ttf", "/system/fonts/NotoSans-Bold.ttf", "/system/fonts/DroidSans-Bold.ttf",
                                  "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf", "/usr/share/fonts/TTF/DejaVuSans-Bold.ttf", "/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf",
                                  "/usr/share/fonts/noto/NotoSans-Bold.ttf", "C:/Windows/Fonts/bahnschrift.ttf", "C:/Windows/Fonts/segoeuib.ttf", "C:/Windows/Fonts/arialbd.ttf",
                                  "/System/Library/Fonts/Supplemental/Arial Bold.ttf", "/Library/Fonts/Arial Bold.ttf", "/System/Library/Fonts/Helvetica.ttc"}) c.push_back(n);
            for (auto& p : c) { TTF_Font* f = TTF_OpenFont(p.c_str(), 20.f); if (f) { TTF_CloseFont(f); fontPath = p; hasTTF = true; fontName = p; break; } }
        }
#endif
        return disc && softAdd;
    }
    void setScale(int s) { if (s == S) return; S = s; purge(); }
    void purge() {
        for (auto& kv : masks) SDL_DestroyTexture(kv.second); masks.clear();
#ifdef TM_HAVE_TTF
        for (auto& kv : tcache) if (kv.second.t) SDL_DestroyTexture(kv.second.t); tcache.clear();
#endif
    }
    void endFrame(double now) {
        clock = now;
        if (masks.size() > 450) { for (auto& kv : masks) SDL_DestroyTexture(kv.second); masks.clear(); }
#ifdef TM_HAVE_TTF
        if (tcache.size() > 900) for (auto it = tcache.begin(); it != tcache.end();) { if (it->second.last < now - 4) { if (it->second.t) SDL_DestroyTexture(it->second.t); it = tcache.erase(it); } else ++it; }
#endif
    }
    void shutdown() {
        purge();
#ifdef TM_HAVE_TTF
        for (auto& kv : fonts) TTF_CloseFont(kv.second); fonts.clear(); if (hasTTF) TTF_Quit();
#endif
        for (SDL_Texture* t : {font, disc, softAdd, softBlend}) if (t) SDL_DestroyTexture(t);
    }
    // ---------------- primitives ----------------
    void setCol(Col c) { SDL_SetRenderDrawColor(rd, c.r, c.g, c.b, c.a); }
    void rect(float x, float y, float w, float h, Col c) { setCol(c); SDL_FRect f{x, y, w, h}; SDL_RenderFillRect(rd, &f); }
    void line(float x1, float y1, float x2, float y2, Col c) { setCol(c); SDL_RenderLine(rd, x1, y1, x2, y2); }
    void flush() { if (!vb.empty()) SDL_RenderGeometry(rd, nullptr, vb.data(), (int)vb.size(), ib.data(), (int)ib.size()); vb.clear(); ib.clear(); }
    void vtx(float x, float y, Col c) { SDL_Vertex v; v.position = SDL_FPoint{x, y}; v.color = fcol(c); v.tex_coord = SDL_FPoint{0, 0}; vb.push_back(v); }
    void quad(float x0, float y0, float x1, float y1, float x2, float y2, float x3, float y3, Col c0, Col c1, Col c2, Col c3) {
        vb.clear(); ib.clear(); vtx(x0, y0, c0); vtx(x1, y1, c1); vtx(x2, y2, c2); vtx(x3, y3, c3); ib = {0, 1, 2, 0, 2, 3}; flush();
    }
    void quad(float x0, float y0, float x1, float y1, float x2, float y2, float x3, float y3, Col c) { quad(x0, y0, x1, y1, x2, y2, x3, y3, c, c, c, c); }
    void tri(float x0, float y0, float x1, float y1, float x2, float y2, Col c0, Col c1, Col c2) { vb.clear(); ib.clear(); vtx(x0, y0, c0); vtx(x1, y1, c1); vtx(x2, y2, c2); ib = {0, 1, 2}; flush(); }
    void thickLine(float x0, float y0, float x1, float y1, float th, Col c) {
        float dx = x1 - x0, dy = y1 - y0, l = std::sqrt(dx * dx + dy * dy); if (l < 0.001f) return; float nx = -dy / l * th * 0.5f, ny = dx / l * th * 0.5f;
        quad(x0 + nx, y0 + ny, x1 + nx, y1 + ny, x1 - nx, y1 - ny, x0 - nx, y0 - ny, c);
    }
    void hgrad(float x, float y, float w, float h, Col l, Col r) { quad(x, y, x + w, y, x + w, y + h, x, y + h, l, r, r, l); }
    void vgrad(float x, float y, float w, float h, Col t, Col b) { quad(x, y, x + w, y, x + w, y + h, x, y + h, t, t, b, b); }
    void tex(SDL_Texture* t, float x, float y, float w, float h, Col c, double rot = 0, bool flipX = false) {
        SDL_SetTextureColorMod(t, c.r, c.g, c.b); SDL_SetTextureAlphaMod(t, c.a); SDL_FRect d{x, y, w, h};
        if (rot == 0 && !flipX) SDL_RenderTexture(rd, t, nullptr, &d); else SDL_RenderTextureRotated(rd, t, nullptr, &d, rot, nullptr, flipX ? SDL_FLIP_HORIZONTAL : SDL_FLIP_NONE);
    }
    void circle(float cx, float cy, float r, Col c, int = 0) { tex(disc, cx - r, cy - r, r * 2, r * 2, c); }
    void ellipse(float cx, float cy, float rx, float ry, Col c, int = 0) { tex(disc, cx - rx, cy - ry, rx * 2, ry * 2, c); }
    void glow(float cx, float cy, float r, Col c, int = 0) { tex(softAdd, cx - r, cy - r, r * 2, r * 2, withA(c, std::min(255, (int)(c.a * 1.6f)))); }
    void glowE(float cx, float cy, float rx, float ry, Col c, bool add = true) { tex(add ? softAdd : softBlend, cx - rx, cy - ry, rx * 2, ry * 2, c); }
    void arc(float cx, float cy, float r, float th, float a0, float a1, Col c0, Col c1) {
        int seg = std::max(3, (int)(std::fabs(a1 - a0) * 14)); float ri = r - th;
        for (int i = 0; i < seg; i++) {
            float t0 = a0 + (a1 - a0) * i / seg, t1 = a0 + (a1 - a0) * (i + 1) / seg; Col ca = mixc(c0, c1, (float)i / seg), cb = mixc(c0, c1, (float)(i + 1) / seg);
            quad(cx + std::cos(t0) * ri, cy + std::sin(t0) * ri, cx + std::cos(t0) * r, cy + std::sin(t0) * r, cx + std::cos(t1) * r, cy + std::sin(t1) * r, cx + std::cos(t1) * ri, cy + std::sin(t1) * ri, ca, ca, cb, cb);
        }
    }
    // ---------------- cached AA rounded-rect masks (3-slice horizontally) ----------------
    SDL_Texture* mask(int kind, int wn, int hi, int r, int m) {
        uint64_t key = ((uint64_t)kind << 56) ^ ((uint64_t)r << 44) ^ ((uint64_t)m << 38) ^ ((uint64_t)wn << 20) ^ (uint64_t)hi;
        auto it = masks.find(key); if (it != masks.end()) return it->second;
        int W = (wn + 2 * m) * S, H = (hi + 2 * m) * S; Canvas c(W, H); float rr = r * S, hx = wn * S * 0.5f, hy = hi * S * 0.5f, cx = W * 0.5f, cy = H * 0.5f, lw = 1.5f * S, hh = hi * S;
        c.paint(0, 0, (float)W, (float)H, [&](float x, float y, PxF& o) {
            float sd = -sdRRect(x, y, cx, cy, hx, hy, rr); float a = 0;
            if (kind == 0) a = covOf(sd); else if (kind == 1) a = covOf(sd) * clampv(1.f - (y - m * S) / hh, 0.f, 1.f);
            else if (kind == 2) a = std::max(0.f, covOf(sd) - covOf(sd - lw)); else if (kind == 3) a = covOf(sd) * clampv(1.f - (y - m * S) / (hh * 0.55f), 0.f, 1.f) * 0.6f;
            else { a = clampv(0.5f + sd / (m * S * 1.2f), 0.f, 1.f); a = a * a * (3 - 2 * a); }  // 4 = soft shadow
            o = PxF{1, 1, 1, a}; return a > 0.001f;
        });
        SDL_Texture* t = c.toTexture(rd); masks[key] = t; return t;
    }
    void drawMask(int kind, float x, float y, float w, float h, float r, Col c, int m = 0) {
        if (w < 1 || h < 1 || c.a == 0) return;
        int wi = std::max(1, (int)std::lround(w)), hi = std::max(1, (int)std::lround(h)), ri = clampv((int)std::lround(r), 0, std::min(wi, hi) / 2), cap = ri + 3 + m, wn = std::min(wi, 2 * (ri + 3) + 2);
        SDL_Texture* t = mask(kind, wn, hi, ri, m); if (!t) return; SDL_SetTextureColorMod(t, c.r, c.g, c.b); SDL_SetTextureAlphaMod(t, c.a);
        float tw = (float)((wn + 2 * m) * S), th = (float)((hi + 2 * m) * S), capPx = (float)(cap * S), yy = y - m, hh = h + 2 * m;
        if (wi <= wn) { SDL_FRect d{x - m, yy, w + 2 * m, hh}; SDL_RenderTexture(rd, t, nullptr, &d); return; }
        SDL_FRect sl{0, 0, capPx, th}, dl{x - m, yy, (float)cap, hh}, sm{capPx, 0, tw - 2 * capPx, th}, dm{x - m + cap, yy, w + 2 * m - 2 * cap, hh}, sr{tw - capPx, 0, capPx, th}, dr{x + w + m - cap, yy, (float)cap, hh};
        dm.x -= 0.75f; dm.w += 1.5f; SDL_RenderTexture(rd, t, &sm, &dm); SDL_RenderTexture(rd, t, &sl, &dl); SDL_RenderTexture(rd, t, &sr, &dr);
    }
    void rrect(float x, float y, float w, float h, float r, Col c) { drawMask(0, x, y, w, h, r, c); }
    void rrectG(float x, float y, float w, float h, float r, Col top, Col bot) { drawMask(0, x, y, w, h, r, bot); drawMask(1, x, y, w, h, r, top); }
    void rrectLine(float x, float y, float w, float h, float r, Col c) { drawMask(2, x, y, w, h, r, c); }
    void gloss(float x, float y, float w, float h, float r, int a = 255) { drawMask(3, x, y, w, h, r, rgba(255, 255, 255, a)); }
    void shadow(float x, float y, float w, float h, float r, float blur, int a = 120) { drawMask(4, x, y, w, h, r, rgba(0, 0, 0, a), (int)blur); }

    // ---------------- text ----------------
#ifdef TM_HAVE_TTF
    TTF_Font* fontFor(int px) {
        auto it = fonts.find(px); if (it != fonts.end()) return it->second;
        TTF_Font* f = TTF_OpenFont(fontPath.c_str(), (float)px); if (f) TTF_SetFontHinting(f, TTF_HINTING_NORMAL); fonts[px] = f; return f;
    }
    TEnt* ent(const char* s, int n, int px) {
        uint64_t h = 1469598103934665603ULL ^ ((uint64_t)px << 40); for (int i = 0; i < n; i++) { h ^= (uint8_t)s[i]; h *= 1099511628211ULL; }
        auto it = tcache.find(h); if (it != tcache.end()) { it->second.last = clock; return &it->second; }
        TEnt e; e.last = clock; TTF_Font* f = fontFor(px);
        if (f) { SDL_Surface* sf = TTF_RenderText_Blended(f, s, (size_t)n, SDL_Color{255, 255, 255, 255}); if (sf) { e.t = SDL_CreateTextureFromSurface(rd, sf); e.w = (float)sf->w; e.h = (float)sf->h; SDL_DestroySurface(sf); if (e.t) SDL_SetTextureScaleMode(e.t, SDL_SCALEMODE_LINEAR); } }
        return &(tcache[h] = e);
    }
#endif
    int pxFor(float sc) const { return std::max(7, (int)std::lround(sc * 10.2f * S)); }
    float measure(const char* s, float sc) {
        int n = (int)strlen(s); if (!n) return 0;
#ifdef TM_HAVE_TTF
        if (hasTTF) { TEnt* e = ent(s, n, pxFor(sc)); return e->w / S; }
#endif
        return n * 6 * sc;
    }
    float prefixW(const char* s, int n, float sc) {  // width of the first n chars (for per-character layout)
        if (n <= 0) return 0;
#ifdef TM_HAVE_TTF
        if (hasTTF) { TTF_Font* f = fontFor(pxFor(sc)); int w = 0, hh = 0; if (f && TTF_GetStringSize(f, s, (size_t)n, &w, &hh)) return (float)w / S; }
#endif
        return n * 6 * sc;
    }
    void textn(float x, float y, const char* s, int n, float sc, Col c) {
        if (n <= 0 || c.a == 0) return;
#ifdef TM_HAVE_TTF
        if (hasTTF) { TEnt* e = ent(s, n, pxFor(sc)); if (!e->t) return; float w = e->w / S, h = e->h / S; SDL_SetTextureColorMod(e->t, c.r, c.g, c.b); SDL_SetTextureAlphaMod(e->t, c.a); SDL_FRect d{x, y + (8 * sc - h) * 0.5f, w, h}; SDL_RenderTexture(rd, e->t, nullptr, &d); return; }
#endif
        if (!font) return; SDL_SetTextureColorMod(font, c.r, c.g, c.b); SDL_SetTextureAlphaMod(font, c.a);
        for (int i = 0; i < n; i++, x += 6 * sc) { unsigned char ch = (unsigned char)s[i]; if (ch <= 32 || ch > 126) continue; int g = ch - 32; SDL_FRect src{(float)((g % 16) * 6), (float)((g / 16) * 8), 6, 8}, dst{x, y, 6 * sc, 8 * sc}; SDL_RenderTexture(rd, font, &src, &dst); }
    }
    // align: 0 left, 1 center, 2 right
    void text(float x, float y, const char* s, float sc, Col c, int align = 0, bool shadowOn = false) {
        int n = (int)strlen(s); if (!n) return; float w = measure(s, sc);
        if (align == 1) x -= w * 0.5f; else if (align == 2) x -= w;
        if (shadowOn) textn(x + std::max(1.f, sc * 0.7f), y + std::max(1.f, sc * 0.9f), s, n, sc, rgba(0, 0, 0, c.a * 0.6f));
        textn(x, y, s, n, sc, c);
    }
};


// ---------------------------------------------------------------------------
//  Procedural audio v2: layered synth voices, reverb, engine model, 4-on-the-floor music
// ---------------------------------------------------------------------------
enum Sfx { SFX_CLICK, SFX_TYPE, SFX_ERROR, SFX_BEEP, SFX_GO, SFX_NITRO, SFX_FINISH, SFX_JOIN, SFX_WHOOSH, SFX_POS, SFX_HOVER, SFX_HORN };
class AudioEngine {
public:
    std::atomic<float> engine{0.f}, musicVol{0.5f}, sfxVol{0.8f}; std::atomic<bool> musicOn{true}, sfxOn{true}, nitroOn{false}; std::atomic<int> musicMode{0};  // 0 menu, 1 race
    bool init() {
        SDL_AudioSpec spec; SDL_zero(spec); spec.format = SDL_AUDIO_F32; spec.channels = 1; spec.freq = 44100;
        stream_ = SDL_OpenAudioDeviceStream(SDL_AUDIO_DEVICE_DEFAULT_PLAYBACK, &spec, &AudioEngine::cb, this);
        if (!stream_) return false;
        SDL_ResumeAudioStreamDevice(stream_); return true;
    }
    void shutdown() { if (stream_) { SDL_DestroyAudioStream(stream_); stream_ = nullptr; } }
    void play(int s) {
        if (!stream_ || !sfxOn) return; std::lock_guard<std::mutex> l(m_);
        float r = rnd();
        switch (s) {
        case SFX_CLICK: trig(sfx_, 4, 880, 1500, 0.07f, 0.28f, 0.002f, 6000, 3000); trig(sfx_, 0, 1760, 1760, 0.05f, 0.08f); break;
        case SFX_HOVER: trig(sfx_, 0, 1200, 1300, 0.03f, 0.05f); break;
        case SFX_HORN: for (int i = 0; i < 2; i++) { float dl = i * 0.24f; trig(sfx_, 3, 392, 392, 0.2f, 0.2f, 0.006f, 1800, 1500, dl); trig(sfx_, 3, 494, 494, 0.2f, 0.18f, 0.006f, 1800, 1500, dl); trig(sfx_, 1, 196, 196, 0.2f, 0.08f, 0.006f, 900, 900, dl); } break;
        case SFX_TYPE: trig(sfx_, 2, 0, 0, 0.022f, 0.20f, 0.001f, 7000, 1800); trig(sfx_, 0, 330 + r * 140, 200, 0.04f, 0.10f); trig(sfx_, 4, 90, 60, 0.03f, 0.12f); break;
        case SFX_ERROR: trig(sfx_, 3, 150, 80, 0.2f, 0.3f, 0.002f, 1100, 300); trig(sfx_, 1, 110, 70, 0.18f, 0.12f, 0.002f, 800, 300); break;
        case SFX_BEEP: trig(sfx_, 0, 523, 523, 0.26f, 0.5f, 0.004f); trig(sfx_, 0, 1046, 1046, 0.18f, 0.16f); trig(sfx_, 4, 261, 261, 0.3f, 0.2f); break;
        case SFX_GO:
            trig(sfx_, 0, 1046, 1046, 0.6f, 0.45f, 0.004f); trig(sfx_, 0, 1568, 1568, 0.5f, 0.2f);
            for (float f : {523.f, 659.f, 784.f}) trig(sfx_, 3, f * 0.5f, f, 0.55f, 0.13f, 0.02f, 500, 7000);
            trig(sfx_, 2, 0, 0, 0.6f, 0.18f, 0.05f, 400, 9000, 0, 1); trig(sfx_, 0, 80, 40, 0.4f, 0.4f); break;
        case SFX_NITRO:
            trig(sfx_, 2, 0, 0, 1.0f, 0.34f, 0.15f, 300, 7000, 0, 1); trig(sfx_, 3, 80, 540, 1.0f, 0.24f, 0.1f, 350, 3500, 0, 1);
            trig(sfx_, 0, 700, 2400, 0.8f, 0.1f, 0.12f, 0, 0, 0, 1); trig(sfx_, 0, 60, 40, 0.5f, 0.45f); break;
        case SFX_FINISH: {
            const float n[6] = {523.25f, 659.25f, 783.99f, 1046.5f, 1318.5f, 1568.f};
            for (int i = 0; i < 6; i++) { trig(sfx_, 4, n[i], n[i], 0.7f, 0.26f, 0.005f, 0, 0, i * 0.09f); trig(sfx_, 0, n[i] * 2, n[i] * 2, 0.5f, 0.08f, 0.005f, 0, 0, i * 0.09f); }
            trig(sfx_, 2, 0, 0, 0.9f, 0.14f, 0.02f, 6000, 1500, 0.5f); break; }
        case SFX_JOIN: trig(sfx_, 0, 660, 660, 0.18f, 0.3f); trig(sfx_, 0, 990, 990, 0.25f, 0.3f, 0.003f, 0, 0, 0.1f); break;
        case SFX_WHOOSH: trig(sfx_, 2, 0, 0, 0.45f, 0.22f, 0.08f, 400, 5000, 0, 1); break;
        case SFX_POS: trig(sfx_, 0, 784, 1175, 0.14f, 0.3f); trig(sfx_, 0, 1175, 1568, 0.16f, 0.2f, 0.003f, 0, 0, 0.07f); break;
        }
    }
private:
    struct V { bool on = false; int wave = 0, shape = 0; float f = 0, mul = 1, vol = 0, t = 0, dur = 1, atk = 0.003f, phase = 0, a0 = 1, a1 = 1, lps = 0, delay = 0; };
    SDL_AudioStream* stream_ = nullptr; std::mutex m_; V sfx_[40], mus_[28]; uint32_t rng_ = 22222; double msamp_ = 0; int lastStep_ = -1;
    float ep_[3] = {0, 0, 0}, elp_ = 0, enoise_ = 0, turboPh_ = 0, duck_ = 1;
    float c1_[1637] = {0}, c2_[2251] = {0}, c3_[3079] = {0}; int p1_ = 0, p2_ = 0, p3_ = 0;
    float rnd() { rng_ = rng_ * 1664525u + 1013904223u; return ((rng_ >> 8) & 0xFFFF) / 65535.f; }
    static float midi(int n) { return 440.f * std::pow(2.f, (n - 69) / 12.f); }
    void trigN(V* pool, int n, int wave, float f0, float f1, float dur, float vol, float atk = 0.003f, float lp0 = 0, float lp1 = 0, float delay = 0, int shape = 0) {
        for (int i = 0; i < n; i++) if (!pool[i].on) {
            V& v = pool[i]; v = V(); v.on = true; v.wave = wave; v.f = f0; v.dur = dur; v.vol = vol; v.atk = atk; v.delay = delay; v.shape = shape;
            v.mul = (f0 > 0 && f1 > 0 && dur > 0) ? std::pow(f1 / f0, 1.f / (dur * 44100.f)) : 1.f;
            v.a0 = lp0 > 0 ? 1 - std::exp(-6.2832f * lp0 / 44100.f) : 1.f; v.a1 = lp1 > 0 ? 1 - std::exp(-6.2832f * lp1 / 44100.f) : v.a0; return;
        }
    }
    void trig(V* pool, int wave, float f0, float f1, float dur, float vol, float atk = 0.003f, float lp0 = 0, float lp1 = 0, float delay = 0, int shape = 0) { trigN(pool, pool == sfx_ ? 40 : 28, wave, f0, f1, dur, vol, atk, lp0, lp1, delay, shape); }
    float voice(V& v) {
        if (v.delay > 0) { v.delay -= 1.f / 44100.f; return 0; }
        float p = v.t / v.dur; if (p >= 1) { v.on = false; return 0; }
        v.phase += v.f / 44100.f; v.phase -= std::floor(v.phase); v.f *= v.mul; float x;
        switch (v.wave) {
        case 0: x = std::sin(6.2831853f * v.phase); break; case 1: x = v.phase < 0.5f ? 0.55f : -0.55f; break;
        case 2: x = rnd() * 2 - 1; break; case 3: x = (v.phase * 2 - 1) * 0.7f; break;
        case 4: x = 1.f - 4.f * std::fabs(v.phase - 0.5f); break; default: x = v.phase < 0.25f ? 0.55f : -0.55f; break;
        }
        if (v.a0 < 1.f || v.a1 < 1.f) { v.lps += (v.a0 + (v.a1 - v.a0) * p) * (x - v.lps); x = v.lps; }
        float env = std::min(1.f, v.t / v.atk); env *= v.shape == 1 ? (p < 0.7f ? 1.f : (1 - p) / 0.3f) : (1 - p) * (1 - p);
        v.t += 1.f / 44100.f; return x * env * v.vol;
    }
    void musicStep(int step) {
        static const int chords[4][3] = {{57, 60, 64}, {53, 57, 60}, {60, 64, 67}, {55, 59, 62}}; static const int roots[4] = {45, 41, 48, 43};
        static const int arp[16] = {0, 1, 2, 1, 0, 1, 2, 1, 2, 1, 0, 1, 2, 1, 0, 2};
        int bar = (step / 16) % 4, st = step % 16, mode = musicMode; bool race = mode == 1;
        if (st == 0) for (int k = 0; k < 3; k++) { float f = midi(chords[bar][k]); trig(mus_, 3, f * 0.997f, f * 0.997f, 16 * 60.f / (race ? 126.f : 100.f) / 4.f, 0.045f, 0.25f, 700, 1500, 0, 1); trig(mus_, 3, f * 1.003f, f * 1.003f, 16 * 60.f / (race ? 126.f : 100.f) / 4.f, 0.04f, 0.25f, 700, 1500, 0, 1); }
        if (race) {
            if (st % 4 == 0) { trig(mus_, 0, 130, 42, 0.2f, 0.55f, 0.001f); duck_ = 0.5f; }
            if (st % 4 == 2) trig(mus_, 2, 0, 0, 0.05f, 0.09f, 0.001f, 9000, 6000);
            if (st == 4 || st == 12) { trig(mus_, 2, 0, 0, 0.14f, 0.2f, 0.001f, 6500, 2500); trig(mus_, 4, 230, 170, 0.1f, 0.16f); }
            if (st == 0 || st == 3 || st == 6 || st == 8 || st == 11 || st == 14) trig(mus_, 3, midi(roots[bar] + (st == 6 || st == 14 ? 12 : 0)), midi(roots[bar]), 0.2f, 0.2f, 0.004f, 700, 250);
            trig(mus_, 5, midi(chords[bar][arp[st]] + 12), midi(chords[bar][arp[st]] + 12), 0.16f, 0.07f, 0.002f, 4500, 1500, 0);
            if (st == 8) trig(mus_, 4, midi(chords[bar][2] + 24), midi(chords[bar][2] + 24), 0.5f, 0.05f);
        } else {
            if (st % 8 == 0) trig(mus_, 3, midi(roots[bar]), midi(roots[bar]), 0.9f, 0.14f, 0.01f, 500, 200);
            if (st % 2 == 0) trig(mus_, 4, midi(chords[bar][arp[st]] + 12), midi(chords[bar][arp[st]] + 12), 0.4f, 0.09f, 0.003f, 0, 0);
            if (st == 0) trig(mus_, 2, 0, 0, 0.15f, 0.06f, 0.001f, 4000, 1200);
        }
    }
    static void SDLCALL cb(void* ud, SDL_AudioStream* s, int additional, int) {
        AudioEngine* a = (AudioEngine*)ud; float buf[1024]; int need = additional / (int)sizeof(float);
        while (need > 0) { int n = std::min(need, 1024); a->fill(buf, n); SDL_PutAudioStreamData(s, buf, n * (int)sizeof(float)); need -= n; }
    }
    void fill(float* out, int n) {
        const float sv = sfxVol, mv = musicVol, eng = engine; const bool mo = musicOn, so = sfxOn, nit = nitroOn;
        std::lock_guard<std::mutex> l(m_);
        const bool race = musicMode == 1; const double stepLen = 44100.0 * 60.0 / (race ? 126.0 : 100.0) / 4.0;
        for (int i = 0; i < n; i++) {
            float sfx = 0, mus = 0;
            for (auto& v : sfx_) if (v.on) sfx += voice(v);
            if (mo && mv > 0.001f) {
                msamp_ += 1; int step = (int)(msamp_ / stepLen); if (step != lastStep_) { lastStep_ = step; musicStep(step); }
                for (auto& v : mus_) if (v.on) mus += voice(v);
                duck_ += (1.f - duck_) * 0.0006f; mus *= duck_ * mv * 0.9f;
            }
            float e = 0;
            if (so && eng > 0.01f) {  // engine: harmonic stack + intake rumble + turbo whistle on nitro
                float f = 42.f + 170.f * std::min(1.f, eng); for (int k = 0; k < 3; k++) { ep_[k] += f * (k + 1) / 44100.f; ep_[k] -= std::floor(ep_[k]); }
                float raw = (ep_[0] * 2 - 1) * 0.55f + (ep_[1] * 2 - 1) * 0.25f + std::sin(6.2831853f * ep_[2]) * 0.2f;
                float cut = 0.03f + 0.22f * eng; elp_ += cut * (raw - elp_); enoise_ += 0.05f * ((rnd() * 2 - 1) - enoise_);
                e = (elp_ * 0.9f + enoise_ * 0.5f) * (0.05f + 0.10f * std::min(1.f, eng));
                if (nit) { turboPh_ += (1100.f + 1500.f * eng) / 44100.f; turboPh_ -= std::floor(turboPh_); e += std::sin(6.2831853f * turboPh_) * 0.03f + enoise_ * 0.12f; }
                e *= sv;
            }
            float dry = sfx * (so ? sv : 0.f) + mus + e;
            float y1 = c1_[p1_], y2 = c2_[p2_], y3 = c3_[p3_]; c1_[p1_] = dry + y1 * 0.46f; c2_[p2_] = dry + y2 * 0.40f; c3_[p3_] = dry + y3 * 0.34f;
            p1_ = (p1_ + 1) % 1637; p2_ = (p2_ + 1) % 2251; p3_ = (p3_ + 1) % 3079;
            out[i] = std::tanh((dry + 0.16f * (y1 + y2 + y3)) * 1.1f);
        }
    }
};


// ============================================================================
//  10. VEHICLES (32: cars, bikes, tractor, truck, rickshaw ...), NITRO FX (16), PARTICLES, BEACH TRACK
// ============================================================================
static constexpr int NCARS = 32, NCOLORS = 12, NNITRO = 16;
struct CarSpec {
    const char* name = ""; int cat = 0;  // cat: 0 cars, 1 bikes, 2 heavy, 3 fun
    float L = 100, W = 44; int np = 0; float p[8][2] = {}; float c0 = 0.3f, c1 = 0.6f, cabW = 0.6f; int spoil = 0, stripe = 0, vents = 0, neon = 0; float whOut = 0; bool hover = false;
    int kind = 0; int nAx = 2; float axU[4] = {0.22f, 0.76f, 0, 0}, axL[4] = {0, 0, 0, 0}, axH[4] = {0, 0, 0, 0}, axOut[4] = {0, 0, 0, 0}; int axMode[4] = {0, 0, 0, 0};  // axMode 1 = centered single wheel
    int cargo = 0; float cg0 = 0, cg1 = 0; int extra = 0, rider = 0;
};
static CarSpec kCars[NCARS];
static void initSpecs() {
    int n = 0;
    auto add = [&](const char* nm, int cat, float L, float W, std::initializer_list<std::array<float, 2>> pr, float c0, float c1, float cw) -> CarSpec& {
        CarSpec& s = kCars[n++]; s = CarSpec(); s.name = nm; s.cat = cat; s.L = L; s.W = W; for (auto& q : pr) { s.p[s.np][0] = q[0]; s.p[s.np][1] = q[1]; s.np++; } s.c0 = c0; s.c1 = c1; s.cabW = cw; return s; };
    CarSpec* s;
    s = &add("Sprint", 0, 104, 44, {{0, .62f}, {.06f, .88f}, {.25f, 1}, {.5f, .97f}, {.72f, .8f}, {.9f, .55f}, {1, .3f}}, .30f, .66f, .62f); s->spoil = 1; s->stripe = 1;
    s = &add("Falcon", 0, 112, 40, {{0, .66f}, {.1f, .9f}, {.35f, .95f}, {.6f, .72f}, {.82f, .42f}, {1, .14f}}, .28f, .6f, .56f); s->spoil = 1; s->stripe = 2;
    s = &add("Viper", 0, 116, 42, {{0, .8f}, {.12f, 1}, {.3f, .9f}, {.5f, .8f}, {.72f, .72f}, {.9f, .5f}, {1, .3f}}, .24f, .52f, .6f); s->spoil = 2; s->stripe = 1; s->vents = 1;
    s = &add("Comet", 0, 96, 46, {{0, .88f}, {.1f, 1}, {.5f, 1}, {.85f, .88f}, {1, .62f}}, .22f, .7f, .7f);
    s = &add("Titan", 0, 106, 50, {{0, .92f}, {.15f, 1}, {.5f, 1}, {.8f, .92f}, {1, .72f}}, .3f, .68f, .66f); s->spoil = 3; s->stripe = 2; s->vents = 1;
    s = &add("Phoenix", 3, 118, 34, {{0, .5f}, {.15f, .55f}, {.4f, .7f}, {.55f, .42f}, {.8f, .2f}, {1, .1f}}, .38f, .5f, .5f); s->spoil = 3; s->stripe = 1; s->whOut = 9;
    s = &add("Bolt", 0, 88, 38, {{0, .8f}, {.1f, .98f}, {.5f, 1}, {.85f, .82f}, {1, .5f}}, .3f, .66f, .64f); s->spoil = 2; s->stripe = 2;
    s = &add("Apex", 0, 122, 44, {{0, .7f}, {.12f, 1}, {.3f, .82f}, {.45f, .96f}, {.6f, .8f}, {.85f, .46f}, {1, .18f}}, .34f, .6f, .56f); s->spoil = 3; s->stripe = 1; s->vents = 1;
    s = &add("Nova X", 0, 120, 42, {{0, .6f}, {.08f, .9f}, {.3f, .96f}, {.55f, .84f}, {.8f, .55f}, {1, .2f}}, .3f, .58f, .55f); s->spoil = 3; s->vents = 1; s->neon = 1;
    s = &add("Phantom", 0, 126, 44, {{0, .9f}, {.2f, 1}, {.5f, .8f}, {.8f, .4f}, {1, .06f}}, .36f, .62f, .5f); s->stripe = 3; s->neon = 2;
    s = &add("Quantum", 0, 110, 48, {{0, .85f}, {.15f, 1}, {.5f, 1}, {.8f, .85f}, {1, .55f}}, .25f, .6f, .6f); s->spoil = 3; s->vents = 1; s->neon = 3; s->hover = true;
    s = &add("Aurora", 0, 124, 40, {{0, .55f}, {.1f, .85f}, {.35f, .98f}, {.6f, .78f}, {.85f, .46f}, {1, .16f}}, .3f, .58f, .54f); s->spoil = 1; s->stripe = 2; s->neon = 4; s->hover = true;
    // ---- bikes
    s = &add("Racer Bike", 1, 78, 20, {{0, .55f}, {.15f, .8f}, {.4f, 1}, {.7f, .75f}, {1, .35f}}, 0, 0, 0); s->kind = 1; s->stripe = 1; s->rider = 1; s->axU[0] = .14f; s->axU[1] = .86f; s->axL[0] = s->axL[1] = 24; s->axH[0] = s->axH[1] = 6; s->axMode[0] = s->axMode[1] = 1;
    s = &add("Chopper", 1, 88, 22, {{0, .4f}, {.2f, .7f}, {.5f, 1}, {.75f, .6f}, {1, .3f}}, 0, 0, 0); s->kind = 1; s->stripe = 2; s->rider = 1; s->axU[0] = .12f; s->axU[1] = .9f; s->axL[0] = s->axL[1] = 27; s->axH[0] = s->axH[1] = 6.5f; s->axMode[0] = s->axMode[1] = 1;
    s = &add("Scooter", 1, 66, 18, {{0, .7f}, {.3f, 1}, {.6f, .6f}, {1, .4f}}, 0, 0, 0); s->kind = 1; s->rider = 1; s->axU[0] = .15f; s->axU[1] = .85f; s->axL[0] = s->axL[1] = 17; s->axH[0] = s->axH[1] = 5; s->axMode[0] = s->axMode[1] = 1;
    // ---- fun / heavy
    s = &add("Auto Rickshaw", 3, 76, 40, {{0, .85f}, {.1f, 1}, {.7f, 1}, {.9f, .8f}, {1, .5f}}, .1f, .62f, .8f); s->kind = 4; s->axU[0] = .2f; s->axU[1] = .88f; s->axMode[1] = 1; s->axL[0] = 20; s->axH[0] = 10; s->axL[1] = 17; s->axH[1] = 8; s->stripe = 2;
    s = &add("Tractor", 2, 104, 46, {{0, .7f}, {.2f, .85f}, {.35f, .7f}, {.5f, .6f}, {.6f, .42f}, {.8f, .36f}, {1, .3f}}, .1f, .46f, .8f); s->kind = 2; s->extra = 4; s->axU[0] = .2f; s->axU[1] = .84f; s->axL[0] = 36; s->axH[0] = 18; s->axOut[0] = 5; s->axL[1] = 18; s->axH[1] = 9; s->axOut[1] = 1;
    s = &add("Big Truck", 2, 150, 46, {{0, .92f}, {.1f, .96f}, {.64f, .96f}, {.7f, .82f}, {.8f, .9f}, {.95f, .8f}, {1, .6f}}, .72f, .93f, .75f); s->kind = 3; s->cargo = 1; s->cg0 = .02f; s->cg1 = .66f; s->nAx = 3; s->axU[0] = .16f; s->axU[1] = .3f; s->axU[2] = .84f; s->axL[0] = s->axL[1] = s->axL[2] = 17; s->axH[0] = s->axH[1] = s->axH[2] = 9;
    s = &add("Pickup", 0, 118, 44, {{0, .85f}, {.08f, .98f}, {.4f, 1}, {.65f, .9f}, {.85f, .85f}, {1, .6f}}, .45f, .72f, .7f); s->kind = 7; s->cargo = 2; s->cg0 = .04f; s->cg1 = .4f; s->axU[0] = .2f; s->axU[1] = .8f;
    s = &add("Jeep", 0, 96, 44, {{0, .9f}, {.1f, 1}, {.8f, 1}, {1, .8f}}, .3f, .62f, .8f); s->extra = 3; s->axL[0] = s->axL[1] = 21; s->axH[0] = s->axH[1] = 11; s->axOut[0] = s->axOut[1] = 1.5f; s->stripe = 1;
    s = &add("City Bus", 2, 160, 46, {{0, .9f}, {.04f, 1}, {.95f, 1}, {1, .8f}}, 0, 0, 0); s->kind = 3; s->cargo = 4; s->cg0 = .03f; s->cg1 = .94f; s->axU[0] = .2f; s->axU[1] = .8f; s->axL[0] = s->axL[1] = 19; s->axH[0] = s->axH[1] = 9; s->stripe = 2;
    s = &add("Monster", 3, 104, 50, {{0, .7f}, {.15f, .85f}, {.4f, .9f}, {.7f, .8f}, {1, .55f}}, .3f, .6f, .6f); s->kind = 6; s->axU[0] = .22f; s->axU[1] = .78f; s->axL[0] = s->axL[1] = 36; s->axH[0] = s->axH[1] = 21; s->axOut[0] = s->axOut[1] = 12; s->stripe = 1;
    s = &add("Go-Kart", 3, 70, 38, {{0, .55f}, {.2f, .8f}, {.5f, .7f}, {.8f, .55f}, {1, .4f}}, 0, 0, 0); s->kind = 5; s->rider = 2; s->spoil = 2; s->axU[0] = .2f; s->axU[1] = .8f; s->axL[0] = s->axL[1] = 16; s->axH[0] = s->axH[1] = 11; s->axOut[0] = s->axOut[1] = 2;
    s = &add("Dragster", 3, 132, 32, {{0, .3f}, {.3f, .35f}, {.5f, .28f}, {.8f, .22f}, {1, .12f}}, .42f, .52f, .6f); s->kind = 8; s->spoil = 3; s->axU[0] = .16f; s->axU[1] = .88f; s->axL[0] = 38; s->axH[0] = 18; s->axOut[0] = 8; s->axL[1] = 15; s->axH[1] = 5.5f; s->axOut[1] = 6; s->stripe = 1;
    s = &add("Police", 0, 106, 44, {{0, .62f}, {.06f, .88f}, {.25f, 1}, {.5f, .97f}, {.72f, .8f}, {.9f, .55f}, {1, .3f}}, .28f, .66f, .64f); s->stripe = 2; s->extra = 1;
    s = &add("Taxi", 0, 104, 44, {{0, .88f}, {.1f, 1}, {.5f, 1}, {.85f, .88f}, {1, .62f}}, .22f, .7f, .7f); s->extra = 2; s->stripe = 2;
    s = &add("Ambulance", 2, 118, 46, {{0, .9f}, {.05f, 1}, {.7f, 1}, {.88f, .85f}, {1, .6f}}, .7f, .9f, .75f); s->kind = 3; s->cargo = 3; s->cg0 = .03f; s->cg1 = .68f; s->extra = 1;
    s = &add("Hot Rod", 0, 100, 42, {{0, .7f}, {.15f, .85f}, {.4f, .8f}, {.6f, .9f}, {.8f, .55f}, {1, .35f}}, .24f, .46f, .6f); s->stripe = 2; s->vents = 1; s->extra = 4; s->axL[0] = 24; s->axH[0] = 14; s->axL[1] = 17; s->axH[1] = 9;
    s = &add("Rally", 0, 98, 44, {{0, .88f}, {.1f, 1}, {.5f, 1}, {.85f, .9f}, {1, .65f}}, .22f, .72f, .66f); s->spoil = 2; s->stripe = 1; s->vents = 1;
    s = &add("Volt EV", 0, 116, 42, {{0, .6f}, {.08f, .9f}, {.3f, .98f}, {.6f, .85f}, {.85f, .5f}, {1, .2f}}, .3f, .62f, .6f); s->spoil = 1; s->neon = 1; s->stripe = 2;
    s = &add("Cyber Truck", 2, 118, 48, {{0, .9f}, {.2f, 1}, {.6f, .95f}, {.85f, .5f}, {1, .2f}}, .25f, .66f, .7f); s->vents = 1; s->stripe = 3; s->neon = 2;
    s = &add("Formula Ace", 3, 124, 36, {{0, .45f}, {.2f, .5f}, {.45f, .7f}, {.6f, .4f}, {.85f, .18f}, {1, .1f}}, .4f, .5f, .5f); s->spoil = 3; s->stripe = 1; s->whOut = 10;
}
static const Col kBody[NCOLORS] = {{225, 48, 56, 255}, {52, 112, 238, 255}, {246, 198, 40, 255}, {48, 200, 92, 255}, {152, 84, 222, 255}, {250, 128, 38, 255}, {232, 238, 248, 255}, {30, 190, 190, 255}, {240, 80, 160, 255}, {52, 56, 70, 255}, {160, 235, 40, 255}, {212, 175, 80, 255}};
static const char* kBodyName[NCOLORS] = {"Red", "Blue", "Yellow", "Green", "Purple", "Orange", "White", "Teal", "Pink", "Carbon", "Lime", "Gold"};
struct NitroStyle { const char* name; Col a, b; int mode; };  // mode: 0 flame, 1 rainbow, 2 electric, 3 sparkle, 4 smoke, 5 embers, 6 aurora
static const NitroStyle kNitro[NNITRO] = {
    {"Blue Flame", {120, 190, 255, 255}, {40, 100, 255, 255}, 0}, {"Cyan Ice", {200, 255, 255, 255}, {40, 210, 255, 255}, 0}, {"Violet", {225, 160, 255, 255}, {120, 50, 230, 255}, 0},
    {"Toxic Green", {190, 255, 160, 255}, {50, 210, 90, 255}, 0}, {"Orange Fire", {255, 230, 140, 255}, {255, 110, 30, 255}, 0}, {"Magenta", {255, 170, 220, 255}, {240, 40, 150, 255}, 0},
    {"Plasma Arc", {240, 215, 255, 255}, {130, 90, 255, 255}, 2}, {"Rainbow", {255, 255, 255, 255}, {255, 90, 90, 255}, 1}, {"Gold Rush", {255, 245, 180, 255}, {255, 190, 40, 255}, 3},
    {"Ghost", {255, 255, 255, 255}, {150, 175, 215, 255}, 4}, {"Lava", {255, 210, 90, 255}, {235, 45, 20, 255}, 5}, {"Galaxy", {200, 180, 255, 255}, {70, 50, 180, 255}, 3},
    {"Toxic Cloud", {190, 255, 110, 255}, {70, 170, 30, 255}, 4}, {"Thunder", {255, 252, 170, 255}, {255, 205, 40, 255}, 2}, {"Aurora", {150, 255, 200, 255}, {60, 170, 255, 255}, 6}, {"Blackfire", {215, 130, 255, 255}, {40, 14, 70, 255}, 4}};
static const Col kNeon[4] = {{60, 235, 255, 255}, {255, 70, 210, 255}, {110, 255, 120, 255}, {255, 190, 60, 255}};
static const char* kPlace[5] = {"-", "1st", "2nd", "3rd", "4th"};
static void hsv2rgb(float h, float s, float v, Col& c) { h = h - std::floor(h); float r = std::fabs(h * 6 - 3) - 1, g = 2 - std::fabs(h * 6 - 2), b = 2 - std::fabs(h * 6 - 4); r = clampv(r, 0.f, 1.f); g = clampv(g, 0.f, 1.f); b = clampv(b, 0.f, 1.f); c = rgba((int)((1 + (r - 1) * s) * v * 255), (int)((1 + (g - 1) * s) * v * 255), (int)((1 + (b - 1) * s) * v * 255)); }
static void nitroCols(int st, float t, Col& core, Col& outer) {
    const NitroStyle& n = kNitro[clampv(st, 0, NNITRO - 1)]; core = n.a; outer = n.b;
    if (n.mode == 1) { hsv2rgb(t * 0.7f, 0.85f, 1.f, outer); core = mixc(outer, C_WHITE, 0.7f); }
    else if (n.mode == 6) { float k = t * 0.6f; Col a = rgba(80, 255, 170), b = rgba(70, 150, 255), c = rgba(255, 110, 220); float f = k - std::floor(k); outer = f < 0.333f ? mixc(a, b, f * 3) : f < 0.666f ? mixc(b, c, (f - 0.333f) * 3) : mixc(c, a, (f - 0.666f) * 3); core = mixc(outer, C_WHITE, 0.6f); }
}

struct CarArt { SDL_Texture *body = nullptr, *det = nullptr; int w = 0, h = 0; float cx = 0, cy = 0; std::vector<float> prof; };
static constexpr float CAR_SS = 3.f;
static float sdSeg(float px, float py, float x0, float y0, float x1, float y1) { float dx = x1 - x0, dy = y1 - y0, t = clampv(((px - x0) * dx + (py - y0) * dy) / std::max(1e-4f, dx * dx + dy * dy), 0.f, 1.f); float qx = x0 + dx * t - px, qy = y0 + dy * t - py; return std::sqrt(qx * qx + qy * qy); }

static void genCar(const CarSpec& d, SDL_Renderer* rd, CarArt& art) {
    const int N = 512; art.prof.assign(N, 0);
    for (int i = 0; i < N; i++) {
        float u = i / (N - 1.f), v = d.p[0][1];
        if (u >= d.p[d.np - 1][0]) v = d.p[d.np - 1][1];
        else for (int k = 0; k + 1 < d.np; k++) if (u >= d.p[k][0] && u <= d.p[k + 1][0]) { float t = (u - d.p[k][0]) / std::max(0.0001f, d.p[k + 1][0] - d.p[k][0]); v = d.p[k][1] + (d.p[k + 1][1] - d.p[k][1]) * t; break; }
        art.prof[i] = v;
    }
    for (int pass = 0; pass < 10; pass++) { std::vector<float> t = art.prof; for (int i = 1; i < N - 1; i++) t[i] = 0.25f * art.prof[i - 1] + 0.5f * art.prof[i] + 0.25f * art.prof[i + 1]; art.prof = t; }
    const float C = CAR_SS, L = d.L * C, Wd = d.W * C; int cw = (int)((d.L + 40) * C), ch = (int)((d.W + 36) * C);
    Canvas B(cw, ch), D(cw, ch); float cx = cw * 0.5f, cy = ch * 0.5f, xr = cx - L * 0.5f; art.w = cw; art.h = ch; art.cx = cx; art.cy = cy;
    auto hwAt = [&](float u) { u = clampv(u, 0.f, 1.f); float h = art.prof[(int)(u * (N - 1))] * Wd * 0.5f; float cap = std::min(std::sqrt(clampv(u / 0.04f, 0.f, 1.f)), std::sqrt(clampv((1 - u) / 0.085f, 0.f, 1.f))); return h * cap; };
    B.paint(xr - 2, cy - Wd * 0.62f, xr + L + 2, cy + Wd * 0.62f, [&](float px, float py, PxF& o) {
        float u = (px - xr) / L; if (u < 0 || u > 1) return false; float hw = hwAt(u), dy = std::fabs(py - cy), cov = covOf(hw - dy); if (cov <= 0) return false;
        float n = hw > 0.01f ? dy / hw : 1.f, l = 0.56f + 0.44f * std::cos(n * 1.5708f); l *= 1.f - 0.34f * smooth01(0.78f, 1.f, n); l *= 0.93f + 0.09f * u;
        if (dy < 0.6f * C && u > 0.6f && u < 0.97f) l *= 0.8f; if (std::fabs(n - 0.74f) * hw < 0.55f * C && u > 0.14f && u < 0.9f) l *= 0.84f;
        o = PxF{l, l, l, cov}; return true; });
    bool hasCab = d.c1 > d.c0 + 0.01f; float span = d.c1 - d.c0;
    if (d.stripe) D.paint(xr, cy - Wd * 0.6f, xr + L, cy + Wd * 0.6f, [&](float px, float py, PxF& o) {
        float u = (px - xr) / L; if (u < 0.02f || u > 0.985f) return false; float hw = hwAt(u), dy = std::fabs(py - cy), cov = covOf(hw - dy); if (cov <= 0) return false; float a = 0; Col c = rgba(255, 255, 255);
        if (d.stripe == 1) a = covOf(0.1f * hw - dy); else if (d.stripe == 2) a = covOf(0.05f * hw - std::fabs(dy - 0.27f * hw));
        else { if (u < 0.5f || u > 0.97f) return false; a = covOf(0.45f * hw - dy) * 0.8f; c = rgba(16, 17, 22); }
        o = PxF{c.r / 255.f, c.g / 255.f, c.b / 255.f, a * cov * 0.9f}; return a > 0; });
    if (d.vents) for (int k = -1; k <= 1; k += 2) D.paint(cx - L * 0.2f, cy - Wd * 0.6f, cx + L * 0.2f, cy + Wd * 0.6f, [&](float px, float py, PxF& o) { float hw = hwAt(0.44f); float s = -sdRRect(px, py, xr + 0.44f * L, cy + k * hw * 0.74f, L * 0.055f, Wd * 0.03f, Wd * 0.02f); o = PxF{0.04f, 0.04f, 0.06f, covOf(s) * 0.85f}; return true; });
    if ((d.kind == 0 || d.kind == 7) && (!d.stripe || d.stripe == 1)) D.paint(xr, cy - Wd * 0.3f, xr + L, cy + Wd * 0.3f, [&](float px, float py, PxF& o) {
        float dd = std::sqrt((px - (xr + 0.76f * L)) * (px - (xr + 0.76f * L)) + (py - cy) * (py - cy)); float r = Wd * 0.13f; if (dd > r + 1) return false;
        o = dd < r * 0.55f ? PxF{0.07f, 0.08f, 0.11f, covOf(r * 0.55f - dd) * 0.92f} : PxF{1, 1, 1, covOf(r - dd) * 0.9f}; return true; });
    D.paint(xr, cy - Wd * 0.6f, xr + L, cy + Wd * 0.6f, [&](float px, float py, PxF& o) {  // gloss
        float u = (px - xr) / L; if (u < 0.02f || u > 0.98f) return false; float hw = hwAt(u), dy = py - cy; if (std::fabs(dy) >= hw) return false; float cov = covOf(hw - std::fabs(dy)); float sd = dy / std::max(hw, 1.f);
        float h1 = std::exp(-((sd + 0.46f) * (sd + 0.46f)) / 0.018f) * 0.3f * (0.5f + 0.5f * smooth01(0.f, 0.5f, u)); float h2 = std::exp(-((sd - 0.55f) * (sd - 0.55f)) / 0.01f) * 0.09f; o = PxF{1, 1, 1, (h1 + h2) * cov}; return true; });
    if (hasCab) D.paint(xr + d.c0 * L - 4, cy - Wd * 0.5f, xr + d.c1 * L + 4, cy + Wd * 0.5f, [&](float px, float py, PxF& o) {
        float u = (px - xr) / L; if (u < d.c0 || u > d.c1) return false; float t = (u - d.c0) / span; float hwg = hwAt(u) * d.cabW * (1.f - 0.3f * smooth01(0.5f, 1.f, t)); float dy = std::fabs(py - cy);
        float cap = std::min(std::sqrt(clampv((u - d.c0) / 0.03f, 0.f, 1.f)), std::sqrt(clampv((d.c1 - u) / 0.035f, 0.f, 1.f))); float cov = covOf(hwg * cap - dy); if (cov <= 0) return false; float dn = dy / std::max(hwg, 1.f);
        if (t >= 0.24f && t <= 0.5f) { if (dn > 0.8f) { o = PxF{0.03f, 0.05f, 0.09f, cov * 0.92f}; return true; } o = PxF{1, 1, 1, 0.1f * (1 - dn) * cov}; return true; }
        float k = 1.f - dn; float r = 0.06f + 0.22f * k, g = 0.1f + 0.34f * k, b = 0.17f + 0.5f * k; float s = ((px - xr) * 0.9f - (py - cy) * 0.5f) / L;
        float streak = std::exp(-((s - 0.5f) * (s - 0.5f)) / 0.0025f) * 0.4f + std::exp(-((s - 0.62f) * (s - 0.62f)) / 0.0008f) * 0.28f; o = PxF{r + streak, g + streak, b + streak, cov * 0.97f}; return true; });
    if (d.cargo) { float um = (d.cg0 + d.cg1) * 0.5f, hx = (d.cg1 - d.cg0) * 0.5f * L, hy = hwAt(um) * 0.94f;
        D.paint(xr + d.cg0 * L - 3, cy - hy - 3, xr + d.cg1 * L + 3, cy + hy + 3, [&](float px, float py, PxF& o) {
            float u = (px - xr) / L; float hyy = std::max(hwAt(u) * 0.94f, 1.f); float sd = -sdRRect(px, py, xr + um * L, cy, hx, hy, 3 * C); float cov = covOf(sd); if (cov <= 0) return false; float dy = (py - cy) / hyy;
            if (d.cargo == 1 || d.cargo == 3) { float l = 0.78f + 0.14f * std::cos(dy * 1.4f); if (std::fmod((px - xr) / (L * 0.055f), 1.f) < 0.06f) l *= 0.86f; o = PxF{l * (d.cargo == 3 ? 1.f : 0.94f), l * (d.cargo == 3 ? 1.f : 0.95f), l, cov};
                if (d.cargo == 3) { float ax = std::fabs(px - (xr + um * L)), ay = std::fabs(py - cy); if ((ax < L * 0.05f && ay < hyy * 0.17f) || (ax < L * 0.017f && ay < hyy * 0.48f)) o = PxF{0.9f, 0.1f, 0.14f, cov}; } }
            else if (d.cargo == 2) { if (sd < 3.2f * C) o = PxF{0.58f, 0.6f, 0.65f, cov}; else { float l = 0.16f + 0.05f * std::sin((px - xr) / (L * 0.03f)); o = PxF{l, l, l * 1.05f, cov}; } }
            else { float l = 0.8f + 0.1f * std::cos(dy * 1.4f); o = PxF{l, l * 1.01f, l * 1.05f, cov}; float adx = std::fabs(dy), seg = std::fmod((px - xr) / (L * 0.07f), 1.f); if (adx > 0.6f && adx < 0.88f && seg > 0.1f) o = PxF{0.08f, 0.14f, 0.24f, cov};
                if (adx < 0.18f) { float hh = std::fmod((px - xr) / (L * 0.22f), 1.f); if (hh > 0.3f && hh < 0.7f) o = PxF{0.55f, 0.6f, 0.68f, cov}; } }
            return true; }); }
    if (d.rider) {
        float ru = d.rider == 1 ? 0.42f : 0.46f, scx = xr + ru * L, hx = xr + (ru + 0.05f) * L, hb = xr + 0.66f * L, hr = Wd * (d.rider == 1 ? 0.3f : 0.2f);
        D.paint(scx - 14 * C, cy - Wd, scx + 14 * C, cy + Wd, [&](float px, float py, PxF& o) { float ex = (px - scx) / (0.085f * L), ey = (py - cy) / (Wd * (d.rider == 1 ? 0.62f : 0.5f)); float dd = std::sqrt(ex * ex + ey * ey); float cov = covOf((1 - dd) * Wd * 0.3f); if (cov <= 0) return false; float l = 0.1f + 0.1f * (1 - std::fabs(ey)); o = PxF{l, l * 1.1f, l * 1.5f, cov}; return true; });
        if (d.rider == 1) { for (int k = -1; k <= 1; k += 2) D.paint(scx, cy - Wd, hb + 4, cy + Wd, [&](float px, float py, PxF& o) { float dd = sdSeg(px, py, scx + 0.03f * L, cy + k * Wd * 0.4f, hb, cy + k * Wd * 0.7f); o = PxF{0.08f, 0.09f, 0.13f, covOf(1.6f * C - dd)}; return true; });
            D.paint(hb - 4 * C, cy - Wd * 0.85f, hb + 4 * C, cy + Wd * 0.85f, [&](float px, float py, PxF& o) { float dd = sdSeg(px, py, hb, cy - Wd * 0.72f, hb, cy + Wd * 0.72f); o = PxF{0.12f, 0.13f, 0.17f, covOf(1.5f * C - dd)}; return true; }); }
        else D.paint(xr + 0.6f * L - 8 * C, cy - Wd * 0.4f, xr + 0.6f * L + 8 * C, cy + Wd * 0.4f, [&](float px, float py, PxF& o) { float dd = std::sqrt((px - (xr + 0.6f * L)) * (px - (xr + 0.6f * L)) + (py - cy) * (py - cy)); o = PxF{0.06f, 0.07f, 0.1f, covOf(1.2f * C - std::fabs(dd - Wd * 0.17f))}; return true; });
        D.paint(hx - hr - 2, cy - hr - 2, hx + hr + 2, cy + hr + 2, [&](float px, float py, PxF& o) { float dd = std::sqrt((px - hx) * (px - hx) + (py - cy) * (py - cy)); float cov = covOf(hr - dd); if (cov <= 0) return false; float vs = (px - hx) / hr; if (vs > 0.2f && dd < hr * 0.9f) o = PxF{0.05f, 0.07f, 0.12f, cov}; else { float l = 0.95f - 0.25f * (dd / hr); o = PxF{l, l, l, cov}; } return true; });
    }
    if (d.extra == 3) for (int k = 0; k < 4; k++) D.paint(xr + (d.c0 - 0.02f) * L, cy - Wd * 0.5f, xr + (d.c1 + 0.02f) * L, cy + Wd * 0.5f, [&](float px, float py, PxF& o) {
        float u0 = d.c0 + 0.03f, u1 = d.c1 - 0.03f, hw = hwAt((u0 + u1) * 0.5f) * 0.82f, dd; if (k == 0) dd = sdSeg(px, py, xr + u0 * L, cy - hw, xr + u1 * L, cy - hw); else if (k == 1) dd = sdSeg(px, py, xr + u0 * L, cy + hw, xr + u1 * L, cy + hw);
        else if (k == 2) dd = sdSeg(px, py, xr + u0 * L, cy - hw, xr + u0 * L, cy + hw); else dd = sdSeg(px, py, xr + u1 * L, cy - hw, xr + u1 * L, cy + hw); o = PxF{0.06f, 0.06f, 0.08f, covOf(1.5f * C - dd)}; return true; });
    if (d.extra == 4) D.paint(xr, cy - Wd * 0.6f, xr + L, cy + Wd * 0.6f, [&](float px, float py, PxF& o) {
        if (d.kind == 2) { float dd = std::sqrt((px - (xr + 0.66f * L)) * (px - (xr + 0.66f * L)) + (py - (cy - Wd * 0.2f)) * (py - (cy - Wd * 0.2f))); float r = Wd * 0.08f; if (dd > r + 1) return false; o = dd < r * 0.65f ? PxF{0.04f, 0.04f, 0.05f, 1} : PxF{0.7f, 0.72f, 0.78f, covOf(r - dd)}; return true; }
        float ex = (px - (xr + 0.7f * L)) / (0.07f * L), ey = (py - cy) / (Wd * 0.26f); float dd = std::sqrt(ex * ex + ey * ey); float cov = covOf((1 - dd) * Wd * 0.2f); if (cov <= 0) return false; o = PxF{0.06f + 0.2f * (1 - dd), 0.07f + 0.2f * (1 - dd), 0.09f + 0.22f * (1 - dd), cov}; return true; });
    if (d.extra == 2) D.paint(xr, cy - Wd * 0.3f, xr + L, cy + Wd * 0.3f, [&](float px, float py, PxF& o) { float s = -sdRRect(px, py, xr + (d.c0 + d.c1) * 0.5f * L, cy, 0.04f * L, Wd * 0.15f, 3 * C); o = PxF{1.f, 0.9f, 0.4f, covOf(s)}; if (std::fabs(py - cy) < 1.2f * C) o = PxF{0.1f, 0.1f, 0.12f, covOf(s)}; return s > -1; });
    if (d.extra == 1) for (int k = -1; k <= 1; k += 2) D.paint(xr, cy - Wd * 0.6f, xr + L, cy + Wd * 0.6f, [&](float px, float py, PxF& o) {
        float um = (d.c0 + d.c1) * 0.5f, hy = hwAt(um) * 0.55f; float s = -sdRRect(px, py, xr + um * L, cy + k * hy * 0.5f, 0.022f * L, hy * 0.5f, 2 * C); o = k < 0 ? PxF{1.f, 0.15f, 0.2f, covOf(s)} : PxF{0.2f, 0.45f, 1.f, covOf(s)}; return s > -1; });
    if (d.spoil) { float xa = xr - (d.spoil == 3 ? 0.03f : 0.008f) * L, xb = xr + (d.spoil == 3 ? 0.105f : d.spoil == 2 ? 0.035f : 0.08f) * L, hh = Wd * (d.spoil == 3 ? 0.53f : d.spoil == 2 ? 0.4f : 0.47f); if (d.kind == 8) hh = Wd * 0.62f;
        D.paint(xa - 3, cy - hh - 3, xb + 3, cy + hh + 3, [&](float px, float py, PxF& o) { float s = -sdRRect(px, py, (xa + xb) * 0.5f, cy, (xb - xa) * 0.5f, hh, 2.2f * C); if (s <= -1) return false; float t = (px - xa) / (xb - xa); float l = 0.1f + 0.14f * t + (std::fabs(py - cy) > hh - 3.5f * C ? 0.14f : 0.f); o = PxF{l, l, l * 1.1f, covOf(s)}; return true; }); }
    for (int k = -1; k <= 1; k += 2) {
        float hy = cy + k * hwAt(0.935f) * 0.55f; D.paint(xr + 0.89f * L, hy - 8 * C, xr + L + 2, hy + 8 * C, [&](float px, float py, PxF& o) { float s = -sdRRect(px, py, xr + 0.935f * L, hy, 0.034f * L, 2.4f * C, 2.2f * C); o = PxF{1.f, 0.97f, 0.8f, covOf(s)}; return s > -1; });
        float ty = cy + k * hwAt(0.04f) * 0.55f; D.paint(xr - 2, ty - 8 * C, xr + 0.06f * L, ty + 8 * C, [&](float px, float py, PxF& o) { float s = -sdRRect(px, py, xr + 0.016f * L, ty, 1.9f * C, 0.05f * Wd, 1.2f * C); o = PxF{1.f, 0.16f, 0.2f, covOf(s)}; return s > -1; });
    }
    if (d.neon) { Col nc = kNeon[d.neon - 1]; D.paint(xr, cy - Wd * 0.6f, xr + L, cy + Wd * 0.6f, [&](float px, float py, PxF& o) {
        float u = (px - xr) / L; if (u < 0.1f || u > 0.96f) return false; float hw = hwAt(u), dy = std::fabs(py - cy); float a = covOf(0.022f * Wd - std::fabs(dy - 0.68f * hw)) * smooth01(0.1f, 0.18f, u) * (1 - smooth01(0.9f, 0.96f, u));
        a = std::max(a, covOf(0.02f * Wd - std::fabs(u - 0.94f) * L * 0.5f) * covOf(0.5f * hw - dy)); if (a <= 0) return false; o = PxF{nc.r / 255.f * 0.8f + 0.2f, nc.g / 255.f * 0.8f + 0.2f, nc.b / 255.f * 0.8f + 0.2f, a}; return true; }); }
    art.body = B.toTexture(rd); art.det = D.toTexture(rd);
}

class Art {
public:
    SDL_Renderer* rd = nullptr; CarArt cars[NCARS]; SDL_Texture* sand = nullptr; SDL_Texture* water = nullptr;
    void init(SDL_Renderer* r) { rd = r; initSpecs(); }
    bool ready(int i) const { return cars[clampv(i, 0, NCARS - 1)].body != nullptr; }
    int count() const { int n = 0; for (auto& c : cars) if (c.body) n++; return n; }
    bool done() const { return sand && water && count() == NCARS; }
    void stepOne(const int* want, int nwant) {  // builds one asset per call, wanted cars first
        for (int k = 0; k < nwant; k++) { int i = clampv(want[k], 0, NCARS - 1); if (!cars[i].body) { genCar(kCars[i], rd, cars[i]); return; } }
        if (!sand) { genSand(); return; } if (!water) { genWater(); return; }
        for (int i = 0; i < NCARS; i++) if (!cars[i].body) { genCar(kCars[i], rd, cars[i]); return; }
    }
private:
    void genSand() {
        Canvas c(256, 256); uint32_t sd = 9;
        auto vn = [&](float x, float y, int per, uint32_t seed) { int ix = (int)std::floor(x), iy = (int)std::floor(y); float fx = x - ix, fy = y - iy; auto hv = [&](int a, int b) { return (hash32((uint32_t)(((a % per) + per) % per) * 73856093u ^ (uint32_t)(((b % per) + per) % per) * 19349663u ^ seed) & 0xFFFF) / 65535.f; };
            float sx = fx * fx * (3 - 2 * fx), sy = fy * fy * (3 - 2 * fy); return hv(ix, iy) * (1 - sx) * (1 - sy) + hv(ix + 1, iy) * sx * (1 - sy) + hv(ix, iy + 1) * (1 - sx) * sy + hv(ix + 1, iy + 1) * sx * sy; };
        c.paint(0, 0, 256, 256, [&](float x, float y, PxF& o) { float n = 0.5f * vn(x / 64, y / 64, 4, 1) + 0.3f * vn(x / 16, y / 16, 16, 2) + 0.2f * vn(x / 4, y / 4, 64, 3); float rip = std::sin((y + 14 * vn(x / 48, y / 48, 5, 4)) * 0.35f) * 0.5f + 0.5f; float gr = frand01(sd) * 0.06f;
            float l = 0.86f + 0.22f * n + 0.03f * rip + gr - 0.03f; o = PxF{0.93f * l, 0.82f * l, 0.6f * l, 1}; return true; });
        sand = c.toTexture(rd);
    }
    void genWater() {
        Canvas c(256, 128); auto hv = [&](int a, int b, int px, int py, uint32_t seed) { return (hash32((uint32_t)(((a % px) + px) % px) * 73856093u ^ (uint32_t)(((b % py) + py) % py) * 19349663u ^ seed) & 0xFFFF) / 65535.f; };
        auto vn = [&](float x, float y, int px, int py, uint32_t seed) { int ix = (int)std::floor(x), iy = (int)std::floor(y); float fx = x - ix, fy = y - iy; float sx = fx * fx * (3 - 2 * fx), sy = fy * fy * (3 - 2 * fy);
            return hv(ix, iy, px, py, seed) * (1 - sx) * (1 - sy) + hv(ix + 1, iy, px, py, seed) * sx * (1 - sy) + hv(ix, iy + 1, px, py, seed) * (1 - sx) * sy + hv(ix + 1, iy + 1, px, py, seed) * sx * sy; };
        c.paint(0, 0, 256, 128, [&](float x, float y, PxF& o) { float n = vn(x / 32, y / 32, 8, 4, 7), n2 = vn(x / 16 + 3, y / 16, 16, 8, 9); float r = 1 - std::fabs(2 * (0.6f * n + 0.4f * n2) - 1); o = PxF{1, 1, 1, std::pow(r, 7.f) * 0.9f}; return true; });
        water = c.toTexture(rd);
    }
};
static Art gArt;

struct CarStyle { int car = 0, color = 0, nitroCol = 0; float wheel = 0, flame = 0, moving = 0, t = 0; bool boost = false; };
static float carHW(int car, float u) { const CarArt& a = gArt.cars[car]; if (a.prof.empty()) return 0.8f; u = clampv(u, 0.f, 1.f); return a.prof[(int)(u * 511)]; }

static void drawCar(Gfx& g, float cx, float cy, float s, const CarStyle& st) {
    int ci = clampv(st.car, 0, NCARS - 1); const CarSpec& d = kCars[ci]; CarArt& a = gArt.cars[ci];
    float L = d.L * s, W = d.W * s, k = s / CAR_SS; Col nc, nc2; nitroCols(st.nitroCol, st.t, nc, nc2); int mode = kNitro[clampv(st.nitroCol, 0, NNITRO - 1)].mode;
    if (!a.body) { g.glowE(cx, cy, L * 0.5f, W * 0.5f, rgba(255, 255, 255, 40 + (int)(30 * std::sin(st.t * 5))), true); return; }
    if (d.hover) cy += std::sin(st.t * 3.f + ci) * 1.8f * s;
    g.glowE(cx + 3 * s, cy + (d.hover ? 16.f : 8.f) * s, L * 0.62f, std::max(W, 22.f * s) * 0.56f, rgba(0, 0, 0, d.hover ? 90 : 150), false);
    if (d.neon) { Col n = kNeon[d.neon - 1]; g.glowE(cx, cy + 2 * s, L * 0.75f, W * 0.8f, withA(n, 130 + (int)(40 * std::sin(st.t * 4))), true); }
    if (st.flame > 0.02f) {
        int nf = d.W < 28 ? 1 : 2;
        for (int i = 0; i < nf; i++) {
            float sgn = nf == 1 ? 0.f : (i ? 1.f : -1.f), fx = cx - L * 0.5f, fy = cy + sgn * W * 0.19f, len = (46 + 38 * st.flame) * s * (nf == 1 ? 0.8f : 1.f), hw = (nf == 1 ? 4.f : 5.5f) * s;
            g.glowE(fx - len * 0.45f, fy, len * 0.62f, 12 * s, withA(nc2, 230), true); g.glowE(fx - len * 0.25f, fy, len * 0.36f, 7 * s, withA(mixc(nc, C_WHITE, 0.5f), 235), true);
            g.tri(fx, fy - hw, fx, fy + hw, fx - len, fy, withA(mixc(nc, C_WHITE, 0.6f), 235), withA(mixc(nc, C_WHITE, 0.6f), 235), withA(nc2, 0));
            if (mode == 2) { uint32_t sd = (uint32_t)(st.t * 28) * 2654435761u + i * 77; for (int q = 0; q < 3; q++) { float x0 = fx, y0 = fy; for (int sg = 0; sg < 5; sg++) { float x1 = x0 - (10 + frand01(sd) * 9) * s, y1 = fy + (frand01(sd) - 0.5f) * 28 * s; g.thickLine(x0, y0, x1, y1, 1.6f, withA(C_WHITE, 220)); g.thickLine(x0, y0, x1, y1, 4.f, withA(nc2, 90)); x0 = x1; y0 = y1; } } }
        }
        g.glowE(cx - L * 0.5f - 18 * s, cy, 60 * s, 32 * s, withA(nc2, 120), true);
    }
    for (int i = 0; i < d.nAx; i++) {
        float u = d.axU[i], wl = (d.axL[i] > 0 ? d.axL[i] : std::max(14.f, d.L * 0.19f)) * s, wh = (d.axH[i] > 0 ? d.axH[i] : std::max(9.f, d.W * 0.2f)) * s, wx = cx + (u - 0.5f) * L;
        int cnt = d.axMode[i] == 1 ? 1 : 2;
        for (int q = 0; q < cnt; q++) {
            float sg = cnt == 1 ? 0.f : (q ? 1.f : -1.f), wy = cy + sg * (carHW(ci, u) * W * 0.5f + wh * 0.05f + (d.axOut[i] + d.whOut) * s * 0.5f * (d.axOut[i] > 0 || d.whOut > 0 ? 1.f : 0.f));
            g.rrect(wx - wl * 0.5f, wy - wh * 0.5f, wl, wh, wh * 0.3f, rgba(16, 17, 22)); float off = std::fmod(st.wheel * wl, wl); if (off < 0) off += wl;
            g.rect(wx - wl * 0.5f + off * 0.86f, wy - wh * 0.38f, std::max(1.5f, 2 * s), wh * 0.76f, rgba(170, 176, 190, 210)); g.rect(wx - wl * 0.4f, wy - wh * 0.5f, wl * 0.8f, std::max(1.f, s), rgba(255, 255, 255, 40));
        }
    }
    float dx = cx - a.cx * k, dy = cy - a.cy * k, dw = a.w * k, dh = a.h * k; Col bc = kBody[clampv(st.color, 0, NCOLORS - 1)];
    g.tex(a.body, dx, dy, dw, dh, bc); g.tex(a.det, dx, dy, dw, dh, rgba(255, 255, 255));
    if (d.extra == 1) { float um = (d.c0 + d.c1) * 0.5f, fl = std::fmod(st.t * 6.f, 1.f) < 0.5f ? 1.f : 0.f, bx = cx + (um - 0.5f) * L; g.glowE(bx, cy - W * 0.12f, 20 * s, 14 * s, rgba(255, 30, 40, (int)(150 * fl)), true); g.glowE(bx, cy + W * 0.12f, 20 * s, 14 * s, rgba(40, 110, 255, (int)(150 * (1 - fl))), true); }
    if (st.moving > 0.15f) g.glowE(cx + L * 0.5f + 20 * s, cy, 46 * s, 22 * s, rgba(255, 240, 190, 70), true);
    g.glowE(cx - L * 0.5f + 2 * s, cy, 9 * s, std::max(7.f, 14 * s * d.W / 44.f), rgba(255, 40, 50, 120), true);
    if (st.boost) g.glowE(cx, cy, L * 0.8f, W * 0.9f, withA(nc2, 60), true);
}

// ---------------- particles (fixed pool, additive sprites) ----------------
struct Particle { float x, y, vx, vy, life, max, size, rot; Col c; uint8_t type; };  // type 0 dust, 1 spark/flame (additive), 2 confetti, 3 smoke
class Particles {
public:
    static constexpr int N = 900; Particle p[N]; int head = 0;
    void emit(float x, float y, float vx, float vy, float life, float size, Col c, uint8_t type) { Particle& q = p[head]; head = (head + 1) % N; q = Particle{x, y, vx, vy, life, life, size, 0, c, type}; }
    void clear() { for (auto& q : p) q.life = 0; }
    void update(float dt) {
        for (auto& q : p) {
            if (q.life <= 0) continue; q.life -= dt; q.x += q.vx * dt; q.y += q.vy * dt; q.rot += dt * 7;
            if (q.type == 2) { q.vy += 300 * dt; q.vx *= (1 - 0.6f * dt); } else { q.vx *= (1 - 1.8f * dt); q.vy *= (1 - 1.8f * dt); }
        }
    }
    void draw(Gfx& g, float camX) {
        for (auto& q : p) {
            if (q.life <= 0) continue; float t = 1 - q.life / q.max, sx = q.x - camX; if (sx < -60 || sx > 1900) continue;
            if (q.type == 0 || q.type == 3) { float r = q.size * (1 + t * 2.2f); g.glowE(sx, q.y, r, r * 0.8f, withA(q.c, (int)(q.c.a * (1 - t) * 0.55f)), false); }
            else if (q.type == 1) { float r = q.size * (1 - t * 0.5f); g.glowE(sx, q.y, r * 2.2f, r * 1.4f, withA(q.c, (int)(q.c.a * (1 - t))), true); }
            else { float c = std::cos(q.rot) * q.size, s = std::sin(q.rot) * q.size * 0.5f; g.quad(sx - c, q.y - s, sx + s, q.y - c * 0.5f, sx + c, q.y + s, sx - s, q.y + c * 0.5f, withA(q.c, (int)(q.c.a * (1 - t * t)))); }
        }
    }
};

// ---------------- beach track ----------------
static constexpr float RV_H = 470, ROAD_Y0 = 132, LANE_H = 56, START_X = 240, WORLD_LEN = 6400, FINISH_X = START_X + WORLD_LEN;
static float laneY(int i) { return ROAD_Y0 + LANE_H * (i + 0.5f); }
static void tileTex(Gfx& g, SDL_Texture* t, float x0, float y0, float x1, float y1, float tw, float th, float offX, float offY, Col c) {
    if (!t) return; SDL_SetTextureColorMod(t, c.r, c.g, c.b); SDL_SetTextureAlphaMod(t, c.a);
    SDL_Rect clip{(int)x0, (int)y0, (int)std::ceil(x1 - x0), (int)std::ceil(y1 - y0)}; SDL_SetRenderClipRect(g.rd, &clip);
    for (float y = y0 - std::fmod(offY, th) - (offY < 0 ? th : 0); y < y1; y += th) for (float x = x0 - std::fmod(offX, tw) - (offX < 0 ? tw : 0); x < x1; x += tw) { SDL_FRect d{x, y, tw + 0.5f, th + 0.5f}; SDL_RenderTexture(g.rd, t, nullptr, &d); }
    SDL_SetRenderClipRect(g.rd, nullptr);
}

static void drawPalm(Gfx& g, float x, float y, float sc, uint32_t seed, float t) {
    g.glowE(x + 12 * sc, y + 14 * sc, 36 * sc, 16 * sc, rgba(0, 0, 0, 110), false);
    float sway = std::sin(t * 1.3f + seed) * 1.5f * sc;
    for (int i = 0; i < 7; i++) {
        float a = (i * 51.4f + (seed % 40)) * 0.0174533f, len = (30 + (hash32(seed + i) % 8)) * sc, ex = x + std::cos(a) * len + sway, ey = y + std::sin(a) * len * 0.8f;
        float nx = -std::sin(a) * 5 * sc, ny = std::cos(a) * 4 * sc;
        g.tri(x + nx * 0.2f, y + ny * 0.2f, x + std::cos(a) * len * 0.55f + nx, y + std::sin(a) * len * 0.44f + ny, ex, ey, rgba(34, 140, 62), rgba(60, 185, 80), rgba(30, 120, 56));
        g.tri(x - nx * 0.2f, y - ny * 0.2f, x + std::cos(a) * len * 0.55f - nx, y + std::sin(a) * len * 0.44f - ny, ex, ey, rgba(26, 110, 50), rgba(46, 150, 66), rgba(30, 120, 56));
    }
    g.circle(x, y, 4 * sc, rgba(110, 72, 36), 10); g.circle(x - 1 * sc, y - 1 * sc, 2 * sc, rgba(150, 100, 52), 8);
}
static void drawUmbrella(Gfx& g, float x, float y, float r, uint32_t seed) {
    static const Col cols[4] = {{235, 70, 70, 255}, {250, 200, 60, 255}, {60, 160, 240, 255}, {60, 200, 150, 255}};
    Col a = cols[seed % 4], b = rgba(250, 250, 245); g.glowE(x + 7, y + 9, r * 1.25f, r * 1.1f, rgba(0, 0, 0, 120), false);
    for (int i = 0; i < 8; i++) {
        float a0 = i * 0.7853982f, a1 = (i + 1) * 0.7853982f; g.tri(x, y, x + std::cos(a0) * r, y + std::sin(a0) * r, x + std::cos(a1) * r, y + std::sin(a1) * r, i % 2 ? a : b, i % 2 ? a : b, i % 2 ? mixc(a, rgba(0, 0, 0), 0.2f) : rgba(225, 225, 220));
    }
    g.circle(x, y, 2.5f, rgba(90, 70, 50), 8);
}
static void drawChecker(Gfx& g, float x, float yTop, float yBot, float camX, float w = 24) {
    int rows = (int)((yBot - yTop) / 14); for (int i = 0; i < rows; i++) for (int k = 0; k < 2; k++) g.rect(x - camX + k * w * 0.5f - w * 0.5f, yTop + i * 14.f, w * 0.5f, 14, (i + k) % 2 ? rgba(20, 20, 24) : rgba(245, 245, 245));
}

static void drawTrack(Gfx& g, float camX, float t, float W) {
    SDL_Rect clip{0, 0, (int)std::ceil(W), (int)RV_H}; SDL_SetRenderClipRect(g.rd, &clip);
    g.vgrad(0, 0, W, 58, rgba(5, 44, 96), rgba(16, 104, 156));                                          // sea (low + calm)
    g.vgrad(0, 34, W, 28, rgba(60, 190, 190, 0), rgba(110, 215, 200, 120));
    tileTex(g, gArt.water, 0, 0, W, 60, 256, 128, camX * 0.1f - t * 16, t * 2, rgba(255, 255, 255, 34));
    tileTex(g, gArt.water, 0, 4, W, 60, 256, 128, 90 - camX * 0.14f + t * 10, 40 - t * 3, rgba(190, 235, 255, 28));
    for (int i = 0; i < 18; i++) { uint32_t h = hash32(i * 7717u + 3); float x = std::fmod((h & 2047) + t * (4 + (h >> 11) % 6) - camX * 0.1f + 4000.f, W + 100.f) - 30, y = 4 + (h >> 16) % 48; float tw = 0.5f + 0.5f * std::sin(t * (1.5f + (h >> 4) % 4) + i); g.glowE(x, y, 4 + tw * 3, 2 + tw, rgba(255, 255, 255, (int)(45 * tw)), true); }
    tileTex(g, gArt.sand, 0, 56, W, 80, 256, 256, camX, 0, rgba(176, 158, 124));                          // wet sand
    for (float x = -20 - std::fmod(camX, 22.f); x < W + 20; x += 22) { float wv = 0.5f + 0.5f * std::sin((x + camX) * 0.03f + t * 1.2f); g.glowE(x, 58 + std::sin((x + camX) * 0.05f + t * 1.1f) * 2.f, 15, 6 + 2 * wv, rgba(255, 255, 255, (int)(70 + 40 * wv)), false); }
    tileTex(g, gArt.sand, 0, 76, W, ROAD_Y0, 256, 256, camX, 0, rgba(226, 208, 168));                    // beach strip
    tileTex(g, gArt.sand, 0, ROAD_Y0, W, ROAD_Y0 + LANE_H * 4, 256, 256, camX, 20, rgba(168, 138, 96)); // darker, dimmer road
    for (int i = 0; i < 4; i++) { g.rect(0, ROAD_Y0 + i * LANE_H, W, LANE_H, (i % 2) ? rgba(34, 20, 8, 56) : rgba(0, 0, 0, 0)); g.rect(0, laneY(i) - 12, W, 4, rgba(60, 40, 18, 40)); g.rect(0, laneY(i) + 8, W, 4, rgba(60, 40, 18, 40)); }
    for (int i = 1; i < 4; i++) { g.rect(0, ROAD_Y0 + i * LANE_H - 2.5f, W, 5, rgba(20, 12, 4, 70)); for (float x = -std::fmod(camX, 72.f) - 72; x < W; x += 72) g.rrect(x, ROAD_Y0 + i * LANE_H - 2.f, 44, 4, 2, rgba(255, 255, 255, 235)); }
    g.vgrad(0, ROAD_Y0, W, 14, rgba(0, 0, 0, 70), rgba(0, 0, 0, 0)); g.vgrad(0, ROAD_Y0 + LANE_H * 4 - 14, W, 14, rgba(0, 0, 0, 0), rgba(0, 0, 0, 60));
    for (float x = -std::fmod(camX, 48.f) - 48; x < W; x += 24) {
        bool red = ((int)std::floor((x + camX) / 24) & 1) != 0; Col c = red ? rgba(214, 44, 50) : rgba(236, 236, 236);
        g.vgrad(x, ROAD_Y0 - 9, 24, 9, mixc(c, C_WHITE, 0.2f), mixc(c, rgba(0, 0, 0), 0.3f)); g.vgrad(x, ROAD_Y0 + LANE_H * 4, 24, 9, mixc(c, C_WHITE, 0.2f), mixc(c, rgba(0, 0, 0), 0.3f));
    }
    g.rect(0, ROAD_Y0 + LANE_H * 4 + 9, W, 3, rgba(0, 0, 0, 60));
    tileTex(g, gArt.sand, 0, ROAD_Y0 + LANE_H * 4 + 12, W, RV_H, 256, 256, camX, 60, rgba(226, 204, 160)); g.vgrad(0, RV_H - 40, W, 40, rgba(80, 50, 20, 0), rgba(80, 50, 20, 70));
    float sl = START_X + 80;
    drawChecker(g, sl, ROAD_Y0, ROAD_Y0 + LANE_H * 4, camX); g.rect(sl - camX + 12, ROAD_Y0, 3, LANE_H * 4, rgba(255, 255, 255, 120));
    drawChecker(g, FINISH_X, ROAD_Y0, ROAD_Y0 + LANE_H * 4, camX, 36);
    if (FINISH_X - camX > -80 && FINISH_X - camX < W + 80) { float fx = FINISH_X - camX; g.rect(fx - 3, ROAD_Y0 - 30, 6, 30, rgba(60, 60, 70)); g.rect(fx - 3, ROAD_Y0 + LANE_H * 4 + 9, 6, 28, rgba(60, 60, 70)); g.rrect(fx - 58, ROAD_Y0 - 44, 116, 18, 5, rgba(16, 16, 22, 235)); g.text(fx, ROAD_Y0 - 42, "FINISH", 1.7f, C_GOLD, 1); }
    if (sl - camX > -80 && sl - camX < W + 80) { float sx = sl - camX; g.rrect(sx - 44, ROAD_Y0 - 44, 88, 18, 5, rgba(16, 16, 22, 235)); g.text(sx, ROAD_Y0 - 42, "START", 1.7f, C_CYAN, 1); }
    int i0 = (int)std::floor((camX - 80) / 130), i1 = (int)std::floor((camX + W + 80) / 130);
    for (int i = i0; i <= i1; i++) {
        uint32_t h = hash32((uint32_t)(i + 1000) * 7919u); float x = i * 130.f + (h % 90) - camX; uint32_t k = (h >> 5) % 6; float by = ROAD_Y0 + LANE_H * 4 + 44 + ((h >> 9) % 14);
        if (k == 0 || k == 1) drawUmbrella(g, x, by, 22, h); else if (k == 2) drawPalm(g, x, by - 4, 1.05f, h, t);
        else if (k == 3) { g.rrect(x - 14, by - 5, 28, 12, 3, mixc(rgba(240, 90, 120), rgba(80, 180, 240), (h >> 3 & 1) * 1.f)); g.rect(x - 14, by - 1, 28, 2, rgba(255, 255, 255, 160)); }
        else if (k == 4) { g.glowE(x + 2, by + 4, 14, 7, rgba(0, 0, 0, 90), false); g.ellipse(x, by, 9, 6, rgba(140, 135, 128)); g.ellipse(x - 2, by - 2, 5, 3, rgba(180, 175, 166)); }
    }
    SDL_SetRenderClipRect(g.rd, nullptr);
}
static void drawPalmsTop(Gfx& g, float camX, float t, float W) {
    SDL_Rect clip{0, 0, (int)std::ceil(W), (int)RV_H}; SDL_SetRenderClipRect(g.rd, &clip);
    int i0 = (int)std::floor((camX - 80) / 130), i1 = (int)std::floor((camX + W + 80) / 130);
    for (int i = i0; i <= i1; i++) { uint32_t h = hash32((uint32_t)(i + 1000) * 7919u); float x = i * 130.f + (h % 90) - camX; if (h % 7 < 3) drawPalm(g, x, 96 + (h >> 8) % 16, 0.9f + ((h >> 12) % 4) * 0.08f, h, t); }
    SDL_SetRenderClipRect(g.rd, nullptr);
}

// ============================================================================
//  11. GAME  (single home hub + race screen; fullscreen, fixed server)
// ============================================================================
static std::string gServerHost = TM_DEFAULT_HOST;  // fixed (override only with --host for developers)
static float lerpf(float a, float b, float t) { return a + (b - a) * t; }
static const int kTimes[4] = {60, 90, 120, 180};

struct HistEntry { int place = 0, players = 1, car = 0, online = 0; float wpm = 0, acc = 0; long long ts = 0; };
struct GameSettings {
    bool soundOn = true; int car = 0, color = 0, nitro = 0, racesDone = 0, wins = 0; float bestWpm = 0; std::string name = "Racer", path; std::vector<HistEntry> hist;
    void load() {
        char* p = SDL_GetPrefPath("TypingMaestro", "Race2"); path = p ? std::string(p) + "profile.cfg" : "tmrace_profile.cfg"; if (p) SDL_free(p);
        FILE* f = fopen(path.c_str(), "r"); if (!f) return; char line[300];
        while (fgets(line, sizeof(line), f)) {
            std::string s(line); while (!s.empty() && (s.back() == '\n' || s.back() == '\r')) s.pop_back(); size_t eq = s.find('='); if (eq == std::string::npos) continue; std::string k = s.substr(0, eq), v = s.substr(eq + 1);
            if (k == "sound") soundOn = v == "1"; else if (k == "car") car = clampv(atoi(v.c_str()), 0, NCARS - 1); else if (k == "color") color = clampv(atoi(v.c_str()), 0, NCOLORS - 1); else if (k == "nitro") nitro = clampv(atoi(v.c_str()), 0, NNITRO - 1);
            else if (k == "races") racesDone = std::max(0, atoi(v.c_str())); else if (k == "wins") wins = std::max(0, atoi(v.c_str())); else if (k == "best") bestWpm = (float)atof(v.c_str()); else if (k == "name" && !v.empty()) name = v.substr(0, 14);
            else if (k == "h") { HistEntry e; if (sscanf(v.c_str(), "%d|%d|%f|%f|%d|%d|%lld", &e.place, &e.players, &e.wpm, &e.acc, &e.car, &e.online, &e.ts) == 7) hist.push_back(e); }
        }
        fclose(f);
    }
    void save() const {
        FILE* f = fopen(path.c_str(), "w"); if (!f) return;
        fprintf(f, "sound=%d\ncar=%d\ncolor=%d\nnitro=%d\nraces=%d\nwins=%d\nbest=%.1f\nname=%s\n", soundOn, car, color, nitro, racesDone, wins, bestWpm, name.c_str());
        for (auto& e : hist) fprintf(f, "h=%d|%d|%.1f|%.1f|%d|%d|%lld\n", e.place, e.players, e.wpm, e.acc, e.car, e.online, e.ts); fclose(f);
    }
};

enum Screen { SC_HOME, SC_RACE };
enum Pending { PA_NONE, PA_QUICK, PA_CREATE, PA_JOIN };
enum HomeMode { HM_IDLE, HM_GARAGE, HM_HISTORY, HM_CONNECT, HM_MATCH, HM_JOIN, HM_ROOM, HM_RESULT };

struct RacerView { bool active = false, fx = false, nitroSeen = false; std::string name; int car = 0, color = 0, nitroCol = 0; float disp = 0, target = 0, speed = 0, wheel = 0, flame = 0, emit = 0, rowY = 0; RacerSnap snap; };
struct RaceView {
    bool live = false, practice = false, goPlayed = false, haveEnd = false, recorded = false; std::string text; std::vector<int> lineStart;
    uint64_t startAtMs = 0, forcedEndMs = 0; double limitSec = 90, endedAt = 0; int mySlot = 255, lastCount = 99; RacerView r[4];
    struct Un { uint32_t seq; char c; }; std::deque<Un> unacked; uint32_t seq = 0, idx = 0, lcorrect = 0, lwrong = 0;
    float wrongFlash = 0, pulse = 0, scroll = 0, dispWpm = 0, dispAcc = 100, nitroFlash = 0; int streak = 0, best = 0, lastPlace = 0; std::vector<float> charX; RaceEndInfo end;
    struct Pop { std::string s; float t = 0, x = 0; Col c; }; Pop pops[6]; int popN = 0;
};
struct Bot { bool on = false; float wpm = 40, acc = 0.95f; int style = 0; double due = 0, nextNitro = 0; uint32_t seq = 0, rng = 1; };

class Game {
public:
    int run(const std::string& host, bool hostGiven, const std::string& fontPath) {
        if (!SDL_Init(SDL_INIT_VIDEO | SDL_INIT_AUDIO)) { fprintf(stderr, "SDL_Init failed: %s\n", SDL_GetError()); return 1; }
        cfg.load(); if (hostGiven) gServerHost = host; shotDir = getenv("TM_SHOT_DIR");
        win = SDL_CreateWindow("Typing Maestro Race", 1280, 720, SDL_WINDOW_FULLSCREEN);  // always fullscreen
        if (!win) { fprintf(stderr, "Window failed: %s\n", SDL_GetError()); return 1; }
        SDL_Renderer* rd = SDL_CreateRenderer(win, nullptr); if (!rd) { fprintf(stderr, "Renderer failed: %s\n", SDL_GetError()); return 1; }
        bool vsync = SDL_SetRenderVSync(rd, 1); if (!g.init(rd, fontPath)) { fprintf(stderr, "Renderer init failed\n"); return 1; }
        gArt.init(rd); updateLayout(); audioOk = audio.init(); net.start(); SDL_StartTextInput(win);
        audio.musicVol = 0.5f; audio.sfxVol = 0.85f; if (!shotDir) net.connectTo(gServerHost, 443, cfg.name);  // connect in the background: online count + wakes the server
        uint64_t last = SDL_GetTicksNS();
        while (running) {
            uint64_t t0 = SDL_GetTicksNS(); dt = clampv((float)((t0 - last) / 1e9), 0.0005f, 0.05f); last = t0; T += dt;
            pollEvents(); pumpNet(); update(); draw(); if (shotDir) shotStep(); SDL_RenderPresent(rd);
            if (!vsync) { uint64_t el = SDL_GetTicksNS() - t0; if (el < 16666666ULL) SDL_DelayNS(16666666ULL - el); }
        }
        if (inRoom) net.send(C_LEAVE); net.disconnect(); std::this_thread::sleep_for(std::chrono::milliseconds(120)); net.stop();
        cfg.save(); audio.shutdown(); g.shutdown(); SDL_DestroyRenderer(rd); SDL_DestroyWindow(win); SDL_Quit(); return 0;
    }

private:
    SDL_Window* win = nullptr; Gfx g; AudioEngine audio; NetworkClient net; GameSettings cfg; bool running = true, audioOk = false; float dt = 0.016f, T = 0, gW = 1280, gOX = 0;
    Screen screen = SC_HOME, nextScreen = SC_HOME; float fade = 1.f; bool fadingOut = false; HomeMode hm = HM_IDLE; HomeMode lastHm = HM_IDLE;
    float mx = 0, my = 0, wheelY = 0; bool down = false, clicked = false, released = false, kBack = false, kEnter = false, kEsc = false, kTab = false; std::string typed;
    int uiId = 0, focusId = -1; float hov[256] = {}; bool uiBlocked = false; std::string toastText; float toastT = 0;
    // online
    Pending pending = PA_NONE; std::string joinCode; bool inRoom = false, haveOffset = false, welcomed = false, confirmLeave = false, editName = false, serverFailed = false; RoomInfo room;
    double offsetMs = 0, minRtt = 1e9; int rttMs = 0, onlineN = 0, racingN = 0; float matchT = 0, slotAnim[4] = {1, 1, 1, 1}; bool prevOcc[4] = {false, false, false, false};
    // race / transitions
    RaceView view; RaceCore core; Bot bots[4]; int pracOpp = 2; Particles fx; float camX = 0, shownEngine = 0, raceT = 0, zoomT = 0, resAlpha = 0; int zoomDir = 0; uint32_t rngState = 77;
    // home panels
    float garageScroll = 0, dragStartY = 0, dragStartScroll = 0; bool dragging = false; int garageCat = 0; const char* shotDir = nullptr; int shotStage = 0, shotWait = 0;

    // ---------------------------------------------------------------- helpers
    void toast(const std::string& s) { toastText = s; toastT = 3.2f; }
    void goTo(Screen s) { if (s == screen && !fadingOut) return; nextScreen = s; fadingOut = true; }
    bool inRect(float x, float y, float w, float h) const { return mx >= x && mx <= x + w && my >= y && my <= y + h; }
    void setContent() { SDL_Rect r{(int)gOX, 0, 1280, 720}; SDL_SetRenderViewport(g.rd, &r); }
    void updateLayout() {
        int pw = 1280, ph = 720; SDL_GetWindowSizeInPixels(win, &pw, &ph); if (ph < 1) ph = 1; float asp = (float)pw / ph;
        gW = clampv(720.f * asp, 1280.f, 1780.f); gOX = (gW - 1280.f) * 0.5f; SDL_SetRenderLogicalPresentation(g.rd, (int)gW, 720, SDL_LOGICAL_PRESENTATION_LETTERBOX); g.setScale(ph > 900 ? 2 : 1);
    }
    void background() {
        SDL_SetRenderViewport(g.rd, nullptr); g.vgrad(0, 0, gW, 720, C_BG1, C_BG0);
        g.glow(300 + gOX + std::sin(T * 0.3f) * 120, 140, 320, rgba(40, 150, 255, 64)); g.glow(1020 + gOX + std::cos(T * 0.25f) * 100, 600, 360, rgba(0, 210, 255, 50));
        for (int i = 0; i < 18; i++) { float x = std::fmod(i * 197.f + T * (260 + i * 18), gW + 240) - 120, y = 30 + std::fmod(i * 91.f, 660.f); g.hgrad(x, y, 80 + i * 9.f, 2, rgba(120, 200, 255, 0), rgba(160, 220, 255, 60)); }
        g.vgrad(0, 640, gW, 80, rgba(0, 0, 0, 0), rgba(0, 0, 0, 110)); setContent();
    }
    void panel(float x, float y, float w, float h, float r = 16.f, Col tint = Col{44, 78, 142, 160}) { g.shadow(x, y + 8, w, h, r, 20, 120); g.rrectG(x, y, w, h, r, tint, rgba(8, 18, 46, 196)); g.gloss(x, y, w, h, r, 34); g.rrectLine(x, y, w, h, r, rgba(140, 205, 255, 95)); }
    bool btn(const char* label, float x, float y, float w, float h, Col acc = C_CYAN, bool en = true, float sc = 2.4f) {
        int id = (++uiId) & 255; bool in = en && !uiBlocked && inRect(x, y, w, h); hov[id] += ((in ? 1.f : 0.f) - hov[id]) * std::min(1.f, dt * 16); float hv = hov[id];
        Col a = en ? acc : rgba(90, 100, 125), top = mixc(rgba(30, 52, 100, 200), withA(a, 190), hv * 0.55f), bot = mixc(rgba(12, 24, 56, 220), withA(a, 90), hv * 0.5f);
        g.rrect(x + 2, y + 4, w, h, 12, rgba(0, 0, 0, 70)); g.rrectG(x, y, w, h, 12, top, bot); g.gloss(x, y, w, h, 12, 40); g.rrectLine(x, y, w, h, 12, withA(mixc(a, C_WHITE, hv * 0.4f), en ? 150 + (int)(hv * 100) : 70));
        g.text(x + w * 0.5f, y + (h - 8 * sc) * 0.5f, label, sc, en ? C_WHITE : rgba(120, 130, 155), 1, en);
        if (in && clicked) { clicked = false; audio.play(SFX_CLICK); return true; } return false;
    }
    void textField(float x, float y, float w, float h, std::string& s, size_t maxLen, int mode, const char* ph, float sc, bool autoFocus = false) {
        int id = ++uiId + 1000; bool in = !uiBlocked && inRect(x, y, w, h); if (autoFocus && focusId < 0) focusId = id;
        if (clicked && !uiBlocked) { if (in) { focusId = id; clicked = false; } else if (focusId == id) focusId = -1; }
        bool foc = focusId == id;
        if (foc) { for (char c : typed) { if (mode == 2) { if (!isdigit((unsigned char)c)) continue; } else if (c < 32 || c > 126 || c == '|' || c == '=') continue; if (s.size() < maxLen) s += c; } if (kBack && !s.empty()) s.pop_back(); if (kEnter) focusId = -1; }
        g.rrect(x, y, w, h, 10, rgba(6, 12, 30, 230)); g.rrectLine(x, y, w, h, 10, foc ? C_CYAN : rgba(120, 170, 230, 90));
        if (s.empty() && !foc) g.text(x + 14, y + (h - 8 * sc) * 0.5f, ph, sc, rgba(110, 125, 160)); else g.text(x + 14, y + (h - 8 * sc) * 0.5f, s.c_str(), sc, C_WHITE);
        if (foc && std::fmod(T, 1.f) < 0.55f) g.rect(x + 16 + g.measure(s.c_str(), sc), y + 8, 3, h - 16, C_CYAN);
    }
    void gradText(float x, float y, const char* s, float sc, Col top, Col bot) {
        float h = 8 * sc, w = g.measure(s, sc) + 6; SDL_Rect c1{(int)(x - 4), (int)(y - 8), (int)w + 8, (int)(h * 0.55f + 8)}, c2{(int)(x - 4), (int)(y - 8 + h * 0.55f + 8), (int)w + 8, (int)h + 16};
        SDL_SetRenderClipRect(g.rd, &c1); g.text(x, y, s, sc, top, 0, true); SDL_SetRenderClipRect(g.rd, &c2); g.text(x, y, s, sc, bot, 0, false); SDL_SetRenderClipRect(g.rd, nullptr);
    }
    // ---------------------------------------------------------------- animated icons (procedural, alive)
    void icon(int k, float cx, float cy, float r, float hv, Col a) {
        float sp = 1.f + hv * 1.6f, t = T * sp; Col w = C_WHITE;
        auto P = [&](float x, float y, float ang) { float c = std::cos(ang), s = std::sin(ang); return SDL_FPoint{cx + (x * c - y * s) * r, cy + (x * s + y * c) * r}; };
        if (k == 0) {  // lightning bolt + orbiting sparks
            float pul = 0.75f + 0.25f * std::sin(t * 5); g.glow(cx, cy, r * 1.6f, withA(a, (int)(120 * pul)));
            SDL_FPoint p0 = P(0.3f, -1, 0), p1 = P(-0.35f, 0.05f, 0), p2 = P(0.3f, 0.0f, 0), p3 = P(-0.25f, 1, 0); g.thickLine(p0.x, p0.y, p1.x, p1.y, r * 0.38f, w); g.thickLine(p1.x, p1.y, p2.x, p2.y, r * 0.38f, w); g.thickLine(p2.x, p2.y, p3.x, p3.y, r * 0.38f, w);
            for (int i = 0; i < 3; i++) { float an = t * 2 + i * 2.094f; g.glow(cx + std::cos(an) * r * 1.25f, cy + std::sin(an) * r * 1.25f, r * 0.45f, withA(a, 200)); }
        } else if (k == 1) {  // key
            float sw = std::sin(t * 2) * 0.18f; g.glow(cx, cy, r * 1.4f, withA(a, 70)); SDL_FPoint rc = P(-0.5f, 0, sw), s0 = P(-0.05f, 0, sw), s1 = P(1.0f, 0, sw), t0 = P(0.62f, 0, sw), t1 = P(0.62f, 0.45f, sw), u0 = P(0.9f, 0, sw), u1 = P(0.9f, 0.32f, sw);
            g.arc(rc.x, rc.y, r * 0.52f, r * 0.2f, 0, 6.283f, w, w); g.thickLine(s0.x, s0.y, s1.x, s1.y, r * 0.2f, w); g.thickLine(t0.x, t0.y, t1.x, t1.y, r * 0.18f, w); g.thickLine(u0.x, u0.y, u1.x, u1.y, r * 0.18f, w);
            float sh = std::fmod(t * 0.8f, 1.f); SDL_FPoint sp2 = P(-0.9f + sh * 2.0f, 0, sw); g.glow(sp2.x, sp2.y, r * 0.5f, rgba(255, 255, 255, 180));
        } else if (k == 2) {  // gear
            g.glow(cx, cy, r * 1.4f, withA(a, 60)); float rot = t * 1.2f; for (int i = 0; i < 8; i++) { float an = rot + i * 0.7854f; g.thickLine(cx + std::cos(an) * r * 0.62f, cy + std::sin(an) * r * 0.62f, cx + std::cos(an) * r * 1.02f, cy + std::sin(an) * r * 1.02f, r * 0.36f, w); }
            g.arc(cx, cy, r * 0.78f, r * 0.26f, 0, 6.283f, w, w); g.circle(cx, cy, r * 0.2f, w);
        } else if (k == 3) {  // clock
            g.glow(cx, cy, r * 1.4f, withA(a, 60)); g.arc(cx, cy, r, r * 0.16f, 0, 6.283f, w, w); for (int i = 0; i < 4; i++) { float an = i * 1.5708f; g.thickLine(cx + std::cos(an) * r * 0.72f, cy + std::sin(an) * r * 0.72f, cx + std::cos(an) * r * 0.88f, cy + std::sin(an) * r * 0.88f, r * 0.1f, w); }
            float am = t * 1.8f, ah = t * 0.15f; g.thickLine(cx, cy, cx + std::cos(am) * r * 0.78f, cy + std::sin(am) * r * 0.78f, r * 0.12f, w); g.thickLine(cx, cy, cx + std::cos(ah) * r * 0.5f, cy + std::sin(ah) * r * 0.5f, r * 0.16f, withA(a, 255)); g.circle(cx, cy, r * 0.12f, w);
        } else if (k == 4) {  // power
            float pul = 0.7f + 0.3f * std::sin(t * 3); g.glow(cx, cy, r * 1.5f, withA(a, (int)(110 * pul))); g.arc(cx, cy, r * 0.95f, r * 0.2f, -1.0f, 4.14f, w, w); g.thickLine(cx, cy - r * 1.05f, cx, cy - r * 0.1f, r * 0.2f, w);
        } else if (k == 5) {  // speaker (sound on/off)
            bool on = cfg.soundOn; g.quad(cx - r * 0.9f, cy - r * 0.3f, cx - r * 0.35f, cy - r * 0.3f, cx - r * 0.35f, cy + r * 0.3f, cx - r * 0.9f, cy + r * 0.3f, w); g.tri(cx - r * 0.35f, cy - r * 0.3f, cx + r * 0.25f, cy - r * 0.8f, cx + r * 0.25f, cy + r * 0.8f, w, w, w); g.tri(cx - r * 0.35f, cy - r * 0.3f, cx - r * 0.35f, cy + r * 0.3f, cx + r * 0.25f, cy + r * 0.8f, w, w, w);
            if (on) for (int i = 0; i < 3; i++) { float ph = std::fmod(t * 1.2f + i * 0.33f, 1.f); g.arc(cx + r * 0.1f, cy, r * (0.55f + 0.5f * ph + 0.3f), r * 0.1f, -0.75f, 0.75f, withA(a, (int)(255 * (1 - ph))), withA(a, (int)(255 * (1 - ph)))); }
            else { g.thickLine(cx + r * 0.4f, cy - r * 0.5f, cx + r * 1.1f, cy + r * 0.5f, r * 0.16f, C_RED); g.thickLine(cx + r * 1.1f, cy - r * 0.5f, cx + r * 0.4f, cy + r * 0.5f, r * 0.16f, C_RED); }
        }
    }
    bool optionCard(float x, float y, float w, float h, const char* title, const char* sub, Col acc, int ic, bool active) {
        int id = (++uiId) & 255; bool in = !uiBlocked && inRect(x, y, w, h); hov[id] += ((in ? 1.f : 0.f) - hov[id]) * std::min(1.f, dt * 12); float hv = hov[id], lift = hv * 3, yy = y - lift;
        g.shadow(x, yy + 6, w, h, 16, 16 + hv * 8, 100 + (int)(hv * 40)); g.rrectG(x, yy, w, h, 16, mixc(rgba(44, 78, 142, 170), withA(acc, 210), hv * 0.35f + (active ? 0.4f : 0.f)), rgba(8, 18, 46, 210)); g.gloss(x, yy, w, h, 16, 38);
        g.rrectLine(x, yy, w, h, 16, withA(mixc(rgba(140, 205, 255), acc, 0.5f), 90 + (int)(hv * 120) + (active ? 100 : 0)));
        float cx = x + 50, cy = yy + h * 0.5f; g.glow(cx, cy, 44, withA(acc, 70 + (int)(hv * 70))); g.circle(cx, cy, 33, rgba(6, 14, 34, 235)); g.arc(cx, cy, 33, 2.5f, 0, 6.283f, withA(acc, 220), withA(acc, 220)); icon(ic, cx, cy, 19, hv, acc);
        g.text(x + 98, cy - 24, title, 2.6f, C_WHITE, 0, true); g.text(x + 98, cy + 8, sub, 1.55f, C_GREY, 0);
        if (in && clicked) { clicked = false; audio.play(SFX_CLICK); return true; } return false;
    }
    void leaveRoom() { if (inRoom) net.send(C_LEAVE); inRoom = false; hm = HM_IDLE; view.live = false; pending = PA_NONE; }

    // ---------------------------------------------------------------- input / frame
    void pollEvents() {
        typed.clear(); kBack = kEnter = kEsc = kTab = false; clicked = released = false; wheelY = 0; SDL_Event e;
        while (SDL_PollEvent(&e)) {
            switch (e.type) {
            case SDL_EVENT_QUIT: running = false; break;
            case SDL_EVENT_TEXT_INPUT: typed += e.text.text; break;
            case SDL_EVENT_KEY_DOWN:
                if (e.key.key == SDLK_BACKSPACE) kBack = true; else if (e.key.key == SDLK_RETURN || e.key.key == SDLK_KP_ENTER) kEnter = true; else if (e.key.key == SDLK_ESCAPE && !e.key.repeat) kEsc = true; else if (e.key.key == SDLK_TAB && !e.key.repeat) kTab = true;
                else if (e.key.key == SDLK_V && (e.key.mod & SDL_KMOD_CTRL)) { char* c = SDL_GetClipboardText(); if (c) { typed += c; SDL_free(c); } } break;
            case SDL_EVENT_MOUSE_MOTION: SDL_ConvertEventToRenderCoordinates(g.rd, &e); mx = e.motion.x - gOX; my = e.motion.y; break;
            case SDL_EVENT_MOUSE_BUTTON_DOWN: SDL_ConvertEventToRenderCoordinates(g.rd, &e); mx = e.button.x - gOX; my = e.button.y; if (e.button.button == SDL_BUTTON_LEFT) { down = true; clicked = true; } break;
            case SDL_EVENT_MOUSE_BUTTON_UP: if (e.button.button == SDL_BUTTON_LEFT) { down = false; released = true; dragging = false; } break;
            case SDL_EVENT_MOUSE_WHEEL: wheelY += e.wheel.y; break;
            case SDL_EVENT_WINDOW_PIXEL_SIZE_CHANGED: case SDL_EVENT_WINDOW_RESIZED: updateLayout(); break;
            default: break;
            }
        }
        if (fadingOut || fade > 0.25f || zoomDir != 0) clicked = false;
    }
    void update() {
        audio.musicOn = cfg.soundOn; audio.sfxOn = cfg.soundOn; audio.musicMode = screen == SC_RACE ? 1 : 0;
        { int want[2] = {cfg.car, 0}; if (!gArt.done()) gArt.stepOne(want, 1); }
        if (fadingOut) { fade += dt * 7; if (fade >= 1) { fade = 1; fadingOut = false; screen = nextScreen; focusId = -1; } } else if (fade > 0) fade = std::max(0.f, fade - dt * 5);
        if (toastT > 0) toastT -= dt;
        for (int i = 0; i < 4; i++) slotAnim[i] = std::min(1.f, slotAnim[i] + dt * 0.8f);
        if (zoomDir > 0) { zoomT += dt / 0.85f; if (zoomT >= 1.f) { zoomT = 0; zoomDir = 0; screen = SC_RACE; } }
        else if (zoomDir < 0) { zoomT -= dt / 0.85f; if (zoomT <= 0.f) { zoomT = 0; zoomDir = 0; } }
        resAlpha = (hm == HM_RESULT && zoomDir == 0) ? std::min(1.f, resAlpha + dt * 3.f) : 0.f;
        if (screen == SC_RACE) updateRace(); else { shownEngine += (0 - shownEngine) * 0.1f; audio.engine = shownEngine; audio.nitroOn = false; }
        if (hm == HM_MATCH) matchT += dt; else matchT = 0;
        fx.update(dt);
    }

    // ---------------------------------------------------------------- home: logo, cards, top bar
    void drawLogo() {
        float x = 26, y = 14; g.glow(150, 56, 170, rgba(40, 160, 255, 46));
        gradText(x, y + 2, "TYPING", 4.3f, rgba(255, 255, 255), rgba(160, 196, 255)); gradText(x, y + 42, "MAESTRO", 3.8f, C_CYAN, C_BLUE);
        float bx = x + 204, by = y + 4; g.rrectG(bx, by, 90, 34, 9, C_GOLD, rgba(255, 130, 40)); g.gloss(bx, by, 90, 34, 9, 80); g.text(bx + 45, by + 8, "RACE", 2.5f, rgba(34, 20, 4), 1);
        for (int i = 0; i < 9; i++) g.rect(bx + i * 10.f, by + 40, 10, 5, i % 2 ? rgba(20, 20, 24) : rgba(240, 240, 240)); for (int i = 0; i < 9; i++) g.rect(bx + i * 10.f, by + 45, 10, 5, i % 2 ? rgba(240, 240, 240) : rgba(20, 20, 24));
        g.hgrad(x, y + 84, 280, 3, withA(C_CYAN, 220), withA(C_CYAN, 0)); for (int i = 0; i < 3; i++) { float sx = std::fmod(T * 120.f + i * 90.f, 290.f); g.hgrad(x + sx, y + 84, 40, 3, rgba(255, 255, 255, 0), rgba(255, 255, 255, 230)); }
    }
    void drawLeftColumn() {
        drawLogo(); bool busy = hm == HM_CONNECT || hm == HM_MATCH || hm == HM_ROOM || hm == HM_JOIN;
        if (optionCard(24, 120, 300, 104, "Quick Match", "Race real players now", C_GOLD, 0, hm == HM_MATCH || (hm == HM_CONNECT && pending == PA_QUICK))) { if (inRoom) leaveRoom(); startOnline(PA_QUICK); }
        // private room: one card, two buttons
        { float x = 24, y = 234, w = 300, h = 136; bool act = hm == HM_ROOM || hm == HM_JOIN || (hm == HM_CONNECT && pending != PA_QUICK); panel(x, y, w, h, 16, act ? Col{60, 120, 200, 200} : Col{44, 78, 142, 170});
            float cx = x + 50, cy = y + 40; g.glow(cx, cy, 44, withA(C_CYAN, 80)); g.circle(cx, cy, 33, rgba(6, 14, 34, 235)); g.arc(cx, cy, 33, 2.5f, 0, 6.283f, C_CYAN, C_CYAN); icon(1, cx, cy, 19, inRect(x, y, w, 80) ? 1.f : 0.f, C_CYAN);
            g.text(x + 98, y + 16, "Private Room", 2.6f, C_WHITE, 0, true); g.text(x + 98, y + 48, "Play with friends (PIN)", 1.55f, C_GREY, 0);
            if (btn("Create", x + 14, y + 84, 134, 40, C_GREEN, true, 2.2f)) { if (inRoom) leaveRoom(); startOnline(PA_CREATE); }
            if (btn("Join", x + 152, y + 84, 134, 40, C_BLUE, true, 2.2f)) { if (inRoom) leaveRoom(); pending = PA_NONE; hm = HM_JOIN; joinCode.clear(); } }
        if (optionCard(24, 380, 300, 96, "Garage", "32 vehicles, 16 nitro FX", C_CYAN, 2, hm == HM_GARAGE)) { if (busy && inRoom) toast("Leave the room first"); else hm = hm == HM_GARAGE ? HM_IDLE : HM_GARAGE; }
        if (optionCard(24, 486, 300, 96, "History", "Your past races", C_BLUE, 3, hm == HM_HISTORY)) { if (busy && inRoom) toast("Leave the room first"); else hm = hm == HM_HISTORY ? HM_IDLE : HM_HISTORY; }
        if (optionCard(24, 592, 300, 64, "Exit", "", C_RED, 4, false)) running = false;
    }
    void drawTopBar() {
        float x = 348, y = 16, h = 56; panel(x, y, 270, h, 16);
        Col bc = kBody[cfg.color]; float acx = x + 32, acy = y + h / 2; g.glow(acx, acy, 30, withA(bc, 90)); g.circle(acx, acy, 20, bc); char in[2] = {(char)toupper((unsigned char)(cfg.name.empty() ? 'R' : cfg.name[0])), 0}; g.text(acx, acy - 12, in, 2.6f, C_WHITE, 1, true);
        if (editName) { textField(x + 62, y + 8, 196, h - 16, cfg.name, 14, 0, "Your name", 2.0f, true); if (focusId < 0) { editName = false; if (cfg.name.empty()) cfg.name = "Racer"; cfg.save(); if (net.state() == NS_CONNECTED) { Writer w; w.str(cfg.name); net.send(C_NAME, w); } } }
        else { g.text(x + 64, y + 10, cfg.name.c_str(), 2.3f, C_WHITE, 0, true); g.text(x + 64, y + 34, "tap to edit name", 1.2f, C_GREY, 0); if (!uiBlocked && inRect(x, y, 270, h) && clicked) { clicked = false; editName = true; focusId = -1; audio.play(SFX_CLICK); } }
        // online players
        float ox = 640, ow = 520; panel(ox, y, ow, h, 16); bool up = net.state() == NS_CONNECTED && welcomed; float pul = 0.6f + 0.4f * std::sin(T * 4);
        if (up) { g.glow(ox + 34, y + h / 2, 16, withA(C_GREEN, (int)(150 * pul))); g.circle(ox + 34, y + h / 2, 7, C_GREEN); char b[48]; snprintf(b, sizeof(b), "%d", std::max(1, onlineN)); g.text(ox + 58, y + 11, b, 3.4f, C_WHITE, 0, true);
            g.text(ox + 58 + g.measure(b, 3.4f) + 10, y + 12, "PLAYERS ONLINE", 1.8f, C_GREEN, 0); char b2[48]; snprintf(b2, sizeof(b2), "%d racing now", racingN); g.text(ox + 58 + g.measure(b, 3.4f) + 10, y + 34, b2, 1.4f, C_GREY, 0); }
        else { bool failed = net.state() == NS_FAILED || net.state() == NS_IDLE; g.circle(ox + 34, y + h / 2, 7, failed ? C_RED : C_GOLD); std::string st = failed ? "Server offline - tap to retry" : net.status(); g.text(ox + 56, y + 11, failed ? "OFFLINE" : "CONNECTING", 2.3f, failed ? C_RED : C_GOLD, 0, true); g.text(ox + 56, y + 36, st.c_str(), 1.4f, C_GREY, 0);
            if (failed && !uiBlocked && inRect(ox, y, ow, h) && clicked) { clicked = false; audio.play(SFX_CLICK); welcomed = false; net.connectTo(gServerHost, 443, cfg.name); } }
        // sound toggle
        float sx = 1180; panel(sx, y, 76, h, 16); icon(5, sx + 38, y + h / 2, 15, inRect(sx, y, 76, h) ? 1.f : 0.f, C_CYAN);
        if (!uiBlocked && inRect(sx, y, 76, h) && clicked) { clicked = false; cfg.soundOn = !cfg.soundOn; cfg.save(); if (cfg.soundOn) audio.play(SFX_CLICK); }
    }

    // ---------------------------------------------------------------- lane scene (lineup / race), referee, cars
    void drawReferee(float x, float y, float t, int state) {
        g.glowE(x + 4, y + 8, 16, 9, rgba(0, 0, 0, 100), false); float ang = state == 1 ? -1.2f : state == 2 ? 0.9f - std::fmod(t * 8, 1.4f) : std::sin(t * 3.f) * 0.6f - 0.4f;
        float fx = x + std::cos(ang) * 6, fy = y + std::sin(ang) * 6, tx = x + std::cos(ang) * 40, ty = y + std::sin(ang) * 40; g.thickLine(fx, fy, tx, ty, 2.2f, rgba(70, 60, 50));
        float wv = std::sin(t * 9) * 4; g.quad(tx, ty, tx + std::cos(ang + 1.57f) * 14 + 8, ty + std::sin(ang + 1.57f) * 14 + wv, tx + std::cos(ang + 1.57f) * 14 + 26, ty + std::sin(ang + 1.57f) * 14 - wv, tx + 18, ty - wv * 0.5f, state == 2 ? rgba(30, 30, 34) : rgba(40, 190, 90), rgba(255, 255, 255), state == 2 ? rgba(240, 240, 240) : rgba(40, 190, 90), rgba(20, 150, 70));
        g.ellipse(x, y, 11, 8, rgba(30, 40, 70)); g.ellipse(x - 1, y - 1, 9, 6, rgba(60, 90, 160)); g.circle(x + 1, y, 6.5f, rgba(250, 205, 160)); g.circle(x + 1, y, 6.5f, rgba(250, 205, 160)); g.ellipse(x + 2, y - 1, 6.5f, 5, rgba(240, 240, 240));
    }
    static float easeOut(float t) { t = clampv(t, 0.f, 1.f); return 1.f - (1.f - t) * (1.f - t) * (1.f - t); }
    void tagPill(float cx, float cy, const char* name, Col c, bool me, bool ready) {
        char tag[40]; snprintf(tag, sizeof(tag), "%s%s", name, me ? "  YOU" : ""); float tw = g.measure(tag, 1.5f) + 22 + (ready ? 14 : 0);
        g.rrect(cx - tw * 0.5f, cy, tw, 19, 9, withA(mixc(c, rgba(0, 0, 0), 0.55f), 220)); g.rrectLine(cx - tw * 0.5f, cy, tw, 19, 9, withA(c, 230)); g.text(cx + (ready ? 7.f : 0.f), cy + 5, tag, 1.5f, C_WHITE, 1);
        if (ready) { g.circle(cx - tw * 0.5f + 12, cy + 9.5f, 5, C_GREEN); }
    }
    void drawLaneScene(float ax, float ay, float aw, float ah, float cam, bool useView) {
        SDL_Rect vp{(int)ax, (int)ay, (int)aw, (int)ah}; SDL_SetRenderViewport(g.rd, &vp); SDL_SetRenderClipRect(g.rd, nullptr); double clock = useView ? raceClock() : 0; float tt = useView ? raceT : T;
        drawTrack(g, cam, tt, aw); float sl = START_X + 80 - cam;
        drawReferee(sl + 26, ROAD_Y0 - 22, T, useView ? (clock < 0 ? 1 : (clock < 1.0 ? 2 : 0)) : 0);
        SDL_Rect clip{0, 0, (int)std::ceil(aw), (int)ah}; SDL_SetRenderClipRect(g.rd, &clip);
        if (useView) {
            for (int i = 0; i < 4; i++) { const RacerView& r = view.r[i]; if (!r.active) continue; float cx = START_X + r.disp * WORLD_LEN - cam, y = laneY(i), x0 = START_X + 80 - cam; if (cx - x0 < 20) continue;
                g.hgrad(x0, y - 12, cx - x0 - 30, 3, rgba(60, 40, 18, 0), rgba(60, 40, 18, 90)); g.hgrad(x0, y + 9, cx - x0 - 30, 3, rgba(60, 40, 18, 0), rgba(60, 40, 18, 90)); }
            fx.draw(g, cam);
            for (int i = 0; i < 4; i++) {
                const RacerView& r = view.r[i]; if (!r.active) continue; CarStyle st; st.car = r.car; st.color = r.color; st.nitroCol = r.nitroCol; st.wheel = r.wheel; st.flame = r.flame; st.moving = r.speed > 8 ? 1.f : 0.f; st.t = raceT + i; st.boost = r.flame > 0.3f;
                float cx = START_X + r.disp * WORLD_LEN - cam, cy = laneY(i) + std::sin(raceT * 22 + i * 2) * 0.5f * std::min(1.f, r.speed / 80.f); if (cx < -220 || cx > aw + 220) continue; drawCar(g, cx, cy, 0.95f, st);
                if (r.speed > 70 || r.flame > 0.2f) for (int k = 0; k < 4; k++) g.hgrad(cx - 70 - r.speed * 0.5f - k * 8, cy - 16 + k * 10.f, 50 + r.speed * 0.45f, 1.5f, rgba(255, 255, 255, 0), rgba(255, 255, 255, std::min(110, (int)(r.speed * 0.5f) + (int)(r.flame * 60))));
                tagPill(cx - kCars[r.car].L * 0.2f, cy - kCars[r.car].W * 0.45f - 24, r.name.c_str(), kBody[r.color], i == view.mySlot, false);
            }
        } else {
            for (int i = 0; i < 4; i++) { const RoomSlotInfo& s = room.slot[i]; if (!inRoom || !s.occupied) continue; float a = easeOut(slotAnim[i]), cx = lerpf(-190.f, START_X, a), cy = laneY(i); CarStyle st; st.car = s.car % NCARS; st.color = (s.color & 15) % NCOLORS; st.nitroCol = std::min<int>(s.color >> 4, NNITRO - 1); st.wheel = (1 - a) * 40 + T * 0.f; st.moving = a < 0.98f ? 1.f : 0.f; st.t = T + i;
                if (a < 0.97f) for (int q = 0; q < 1; q++) fx.emit(cx + cam - 50, cy + (frand01(rngState) - 0.5f) * 20, -40, (frand01(rngState) - 0.5f) * 30, 0.5f, 5, rgba(214, 186, 136, 160), 0);
                drawCar(g, cx, cy, 0.95f, st); tagPill(cx - kCars[st.car].L * 0.2f, cy - kCars[st.car].W * 0.45f - 24, s.name.c_str(), kBody[st.color], i == room.mySlot, s.ready); }
            fx.draw(g, cam);
        }
        SDL_SetRenderClipRect(g.rd, nullptr); drawPalmsTop(g, cam, tt, aw);
        if (useView) { bool myN = view.mySlot < 4 && (view.r[view.mySlot].snap.flags & SF_NITRO);
            if (myN || view.nitroFlash > 0) { Col nc, nc2; nitroCols(view.r[view.mySlot < 4 ? view.mySlot : 0].nitroCol, raceT, nc, nc2); for (int i = 0; i < 24; i++) { float x = std::fmod(i * 131.f - raceT * 1500.f + 20000.f, aw + 300) - 150, y = 10 + std::fmod(i * 53.f, 390.f); g.hgrad(x, y, 120 + (i % 5) * 30.f, 2, rgba(255, 255, 255, 0), rgba(255, 255, 255, 100)); } g.hgrad(0, 0, 150, ah, withA(nc2, 90), withA(nc2, 0)); g.hgrad(aw - 150, 0, 150, ah, withA(nc2, 0), withA(nc2, 90)); } }
        g.hgrad(0, 0, 70, ah, rgba(0, 0, 0, 70), rgba(0, 0, 0, 0)); g.hgrad(aw - 70, 0, 70, ah, rgba(0, 0, 0, 0), rgba(0, 0, 0, 70)); g.vgrad(0, 0, aw, 40, rgba(0, 0, 0, 60), rgba(0, 0, 0, 0));
        SDL_SetRenderViewport(g.rd, nullptr);
    }

    // ---------------------------------------------------------------- stage (the big car card)
    void drawStage(bool skipScene) {
        const float sx = 348, sy = 88, sw = 908, sh = 568; g.shadow(sx, sy + 8, sw, sh, 22, 24, 130); g.rrectG(sx, sy, sw, sh, 22, rgba(20, 38, 76, 238), rgba(6, 12, 30, 248)); g.rrectLine(sx, sy, sw, sh, 22, rgba(140, 205, 255, 100));
        bool sceneMode = hm == HM_MATCH || hm == HM_ROOM || hm == HM_JOIN;
        if (hm != lastHm) { lastHm = hm; }
        switch (hm) {
        case HM_IDLE: drawHero(sx, sy, sw, sh); break;
        case HM_GARAGE: drawGarage(sx, sy, sw, sh); break;
        case HM_HISTORY: drawHistory(sx, sy, sw, sh); break;
        case HM_CONNECT: { float cx = sx + sw / 2, cy = sy + sh / 2; g.arc(cx, cy - 20, 40, 8, T * 5, T * 5 + 4.2f, withA(C_CYAN, 40), C_CYAN); g.text(cx, cy + 40, "Connecting to server...", 2.8f, C_WHITE, 1, true); g.text(cx, cy + 76, net.status().c_str(), 1.8f, C_GREY, 1); g.text(cx, cy + 100, "Free hosting can take ~1 minute to wake up", 1.5f, rgba(120, 150, 200), 1);
            if (btn("Cancel", cx - 90, cy + 130, 180, 44, C_RED, true, 2.2f)) { pending = PA_NONE; hm = HM_IDLE; } if (!net.state()) {} break; }
        case HM_RESULT: drawResultStage(sx, sy, sw, sh); break;
        default: break;
        }
        if (sceneMode) { drawRoomTop(sx, sy, sw); if (!skipScene) { drawLaneScene(gOX + sx + 8, sy + 124, sw - 16, 436, 0, false); setContent(); } }
    }
    void drawHero(float sx, float sy, float sw, float sh) {
        SDL_Rect clip{(int)sx + 2, (int)sy + 2, (int)sw - 4, (int)sh - 4}; SDL_SetRenderClipRect(g.rd, &clip); Col bc = kBody[cfg.color]; float cx = sx + sw / 2, cy = sy + sh / 2 + 10;
        g.glow(cx, cy, 380, withA(bc, 70)); g.glowE(cx, cy + 10, 440, 180, rgba(40, 130, 255, 50), true);
        for (int i = 0; i < 22; i++) { float x = sx + std::fmod(std::fmod(i * 97.f - T * (300.f + i * 14), sw + 200) + sw + 200, sw + 200) - 100, y = sy + 30 + std::fmod(i * 61.f, sh - 60); g.hgrad(x, y, 90 + (i % 4) * 30.f, 2, rgba(150, 210, 255, 0), rgba(180, 225, 255, 50)); }
        g.rrectG(sx + 10, cy - 96, sw - 20, 192, 12, rgba(12, 18, 34, 235), rgba(30, 38, 58, 235)); g.rect(sx + 10, cy - 98, sw - 20, 3, rgba(255, 255, 255, 40)); g.rect(sx + 10, cy + 95, sw - 20, 3, rgba(255, 255, 255, 40));
        for (int i = 0; i < 14; i++) g.rrect(sx + std::fmod(std::fmod(i * 84.f - T * 380.f, 1180.f) + 1180.f, 1180.f) - 140, cy - 3, 58, 6, 3, rgba(255, 255, 255, 200));
        CarStyle st; st.car = cfg.car; st.color = cfg.color; st.nitroCol = cfg.nitro; st.wheel = T * 6; st.moving = 1; st.t = T; st.flame = std::fmod(T, 5.f) < 1.8f ? 0.75f + 0.25f * std::sin(T * 40) : 0.f; st.boost = st.flame > 0.3f;
        float bob = std::sin(T * 16) * 0.9f; drawCar(g, cx, cy + bob, 3.3f, st);
        if (st.flame > 0.1f && kNitro[cfg.nitro].mode >= 3) for (int q = 0; q < 2; q++) { float jx = frand01(rngState), jy = frand01(rngState); Col c1, c2; nitroCols(cfg.nitro, T, c1, c2); int md = kNitro[cfg.nitro].mode; fx.emit(cx - kCars[cfg.car].L * 1.65f - 20, cy + (jy - 0.5f) * 30, -140 - 120 * jx, (jy - 0.5f) * 120 + (md == 5 ? 60.f : 0.f), 0.6f, md == 4 ? 14.f : 5.f, md == 4 ? withA(c2, 110) : withA(c1, 230), md == 4 ? 3 : 1); }
        fx.draw(g, 0); SDL_SetRenderClipRect(g.rd, nullptr); g.rrectLine(sx, sy, sw, sh, 22, rgba(140, 205, 255, 100));
    }
    void drawRoomTop(float sx, float sy, float sw) {
        float y = sy + 12, h = 100; float lx = sx + 12, lw = 430, rx = sx + 454, rw = sw - 466; int n = 0, nready = 0; for (auto& s : room.slot) if (inRoom && s.occupied) { n++; if (s.ready) nready++; }
        if (hm == HM_MATCH) {
            panel(lx, y, lw, h, 16, Col{50, 96, 170, 190}); g.text(lx + 22, y + 12, "MATCHMAKING", 1.9f, C_GOLD, 0, true); char b[16]; snprintf(b, sizeof(b), "%d/4", n); g.text(lx + 22, y + 34, b, 5.4f, C_WHITE, 0, true);
            for (int i = 0; i < 4; i++) { float x = lx + 168 + i * 64.f, cy = y + 56; bool on = inRoom && room.slot[i].occupied; g.circle(x, cy, 24, on ? kBody[(room.slot[i].color & 15) % NCOLORS] : rgba(255, 255, 255, 22)); if (on) { char c[2] = {(char)toupper((unsigned char)room.slot[i].name[0]), 0}; g.text(x, cy - 11, c, 2.6f, C_WHITE, 1, true); } else g.arc(x, cy, 24, 2.5f, T * 3 + i, T * 3 + i + 1.8f, withA(C_CYAN, 40), C_CYAN); }
            panel(rx, y, rw, h, 16); g.text(rx + 20, y + 14, n >= 4 ? "Match found! Starting..." : "Finding players...", 2.1f, n >= 4 ? C_GREEN : C_WHITE, 0, true); g.text(rx + 20, y + 42, "Starts at 4/4, or after a short wait with 2+.", 1.4f, C_GREY, 0);
            if (n <= 1 && matchT > 20.f) { if (btn("Play solo vs AI", rx + 20, y + 58, 200, 34, C_GREEN, true, 1.7f)) { leaveRoom(); beginPractice(true); } }
            if (btn("Cancel", rx + rw - 130, y + 58, 112, 34, C_RED, true, 1.8f)) leaveRoom();
        } else if (hm == HM_ROOM) {
            bool host = room.host == room.mySlot; panel(lx, y, lw, h, 16, Col{50, 96, 170, 190}); g.text(lx + 22, y + 12, "ROOM PIN", 1.9f, C_GOLD, 0, true); std::string pin = room.code; float x0 = lx + 22;
            for (size_t i = 0; i < pin.size(); i++) { g.rrectG(x0 + i * 62.f, y + 36, 54, 54, 12, rgba(30, 50, 100), rgba(8, 16, 40)); g.rrectLine(x0 + i * 62.f, y + 36, 54, 54, 12, withA(C_CYAN, 200)); char c[2] = {pin[i], 0}; g.text(x0 + i * 62.f + 27, y + 46, c, 4.2f, C_WHITE, 1, true); }
            if (btn("Copy", lx + lw - 150, y + 42, 128, 40, C_CYAN, true, 1.9f)) { SDL_SetClipboardText(room.code.c_str()); toast("PIN copied - share it with friends"); }
            panel(rx, y, rw, h, 16);
            if (host) { char b[40]; snprintf(b, sizeof(b), "%d/4 joined  -  %d ready", n, nready); g.text(rx + 18, y + 10, b, 1.7f, C_GREY, 0); bool lobbyOpen = room.state == 0; if (btn("START RACE", rx + 14, y + 36, 190, 52, C_GREEN, lobbyOpen && nready > 0, 2.1f)) net.send(C_START);
                g.text(rx + 222, y + 12, "RACE TIME", 1.4f, C_GREY, 0); int cur = 1; for (int i = 0; i < 4; i++) if (kTimes[i] == room.limit) cur = i; for (int i = 0; i < 4; i++) { char b2[8]; snprintf(b2, sizeof(b2), "%ds", kTimes[i]); float bx = rx + 222 + (i % 2) * 100.f, by = y + 32 + (i / 2) * 32.f; if (btn(b2, bx, by, 92, 28, i == cur ? C_GOLD : C_BLUE, lobbyOpen, 1.6f)) { Writer w; w.u8((uint8_t)i); net.send(C_TIME, w); } } }
            else { bool me = room.slot[room.mySlot & 3].ready; char b[40]; snprintf(b, sizeof(b), "Race time: %d s", room.limit); g.text(rx + 18, y + 10, b, 2.0f, C_GOLD, 0, true); g.text(rx + 18, y + 36, "Waiting for the host to start", 1.5f, C_GREY, 0);
                if (btn(me ? "READY!" : "I'M READY", rx + 14, y + 54, 190, 38, me ? C_GREEN : C_GOLD, room.state == 0, 1.9f)) { Writer w; w.u8(!me); net.send(C_READY, w); } }
            if (btn("Leave", rx + rw - 76, y + 4, 66, 24, C_RED, true, 1.3f)) leaveRoom();
        } else if (hm == HM_JOIN) {
            panel(lx, y, lw, h, 16, Col{50, 96, 170, 190}); g.text(lx + 22, y + 10, "ENTER 4-DIGIT PIN", 1.9f, C_GOLD, 0, true); for (int i = 0; i < 4; i++) { bool has = i < (int)joinCode.size(); g.rrectG(lx + 22 + i * 56.f, y + 36, 48, 54, 12, rgba(30, 50, 100), rgba(8, 16, 40)); g.rrectLine(lx + 22 + i * 56.f, y + 36, 48, 54, 12, has ? C_CYAN : rgba(120, 170, 230, 90)); if (has) { char c[2] = {joinCode[i], 0}; g.text(lx + 22 + i * 56.f + 24, y + 46, c, 4.2f, C_WHITE, 1, true); } }
            if (btn("JOIN", lx + 252, y + 36, 164, 54, C_GREEN, joinCode.size() == 4, 2.4f) || (kEnter && joinCode.size() == 4)) { pending = PA_JOIN; if (net.state() == NS_CONNECTED && welcomed) runPending(); else startOnline(PA_JOIN); }
            panel(rx, y, rw, h, 16); for (int d = 0; d < 10; d++) { char b[4]; snprintf(b, sizeof(b), "%d", (d + 1) % 10); float bx = rx + 14 + (d % 5) * 83.f, by = y + 10 + (d / 5) * 42.f; if (btn(b, bx, by, 76, 36, C_BLUE, joinCode.size() < 4, 2.2f)) joinCode += b[0]; }
            if (kBack && !joinCode.empty()) joinCode.pop_back(); for (char c : typed) if (isdigit((unsigned char)c) && joinCode.size() < 4) joinCode += c;
            if (btn("Back", lx + lw - 90, y + 4, 80, 26, C_GREY, true, 1.4f)) hm = HM_IDLE; if (btn("Del", lx + lw - 178, y + 4, 80, 26, C_GREY, !joinCode.empty(), 1.4f)) joinCode.pop_back();
        }
    }
    void drawGarage(float sx, float sy, float sw, float sh) {
        static const char* cats[5] = {"All", "Cars", "Bikes", "Heavy", "Fun"}; g.text(sx + 24, sy + 18, "GARAGE", 3.4f, C_WHITE, 0, true);
        for (int i = 0; i < 5; i++) if (btn(cats[i], sx + 250 + i * 128.f, sy + 14, 118, 36, garageCat == i ? C_GOLD : C_BLUE, true, 1.7f)) { garageCat = i; garageScroll = 0; }
        // preview + paint + nitro
        float px = sx + 16, py = sy + 64, pw = 300, ph = 488; panel(px, py, pw, ph, 14, Col{36, 66, 124, 170}); SDL_Rect clip{(int)px + 6, (int)py + 6, (int)pw - 12, 190}; SDL_SetRenderClipRect(g.rd, &clip); g.vgrad(px + 6, py + 6, pw - 12, 190, rgba(40, 56, 90), rgba(14, 20, 40));
        g.rect(px + 6, py + 100, pw - 12, 4, rgba(255, 255, 255, 30)); for (int i = 0; i < 6; i++) g.rrect(px + std::fmod(std::fmod(i * 70.f - T * 140.f, 420.f) + 420.f, 420.f) - 60, py + 99, 36, 5, 2, rgba(255, 255, 255, 170));
        CarStyle st; st.car = cfg.car; st.color = cfg.color; st.nitroCol = cfg.nitro; st.wheel = T * 5; st.moving = 1; st.t = T; st.flame = std::fmod(T, 3.f) < 1.5f ? 0.8f : 0.f; float wmax = 0; for (int q = 0; q < kCars[cfg.car].nAx; q++) wmax = std::max(wmax, kCars[cfg.car].axOut[q] + kCars[cfg.car].axH[q] * 0.6f); float sc = std::min({2.0f, 250.f / kCars[cfg.car].L, 150.f / (kCars[cfg.car].W + 2 * wmax + 8)}); drawCar(g, px + pw / 2, py + 100, sc, st); SDL_SetRenderClipRect(g.rd, nullptr);
        g.text(px + pw / 2, py + 204, kCars[cfg.car].name, 2.8f, C_WHITE, 1, true); g.text(px + 16, py + 240, "PAINT", 1.4f, C_GREY, 0);
        for (int i = 0; i < NCOLORS; i++) { float x = px + 28 + (i % 6) * 48.f, y = py + 268 + (i / 6) * 40.f; bool in = inRect(x - 17, y - 17, 34, 34) && !uiBlocked; g.circle(x, y, 13 + (cfg.color == i ? 3 : 0), kBody[i]); if (cfg.color == i) g.rrectLine(x - 19, y - 19, 38, 38, 19, C_WHITE); if (in && clicked) { clicked = false; cfg.color = i; audio.play(SFX_CLICK); cfg.save(); } }
        g.text(px + 16, py + 358, "NITRO FX", 1.4f, C_GREY, 0); g.text(px + pw - 16, py + 358, kNitro[cfg.nitro].name, 1.5f, C_CYAN, 2);
        for (int i = 0; i < NNITRO; i++) { float x = px + 28 + (i % 8) * 34.f, y = py + 392 + (i / 8) * 44.f; bool in = inRect(x - 16, y - 18, 32, 36) && !uiBlocked; Col a = kNitro[i].a, b2 = kNitro[i].b; if (kNitro[i].mode == 1 || kNitro[i].mode == 6) nitroCols(i, T, a, b2); g.glow(x, y, 16, withA(b2, 110)); g.circle(x, y, 8 + (cfg.nitro == i ? 3 : 0), b2); g.circle(x, y, 4, a); if (cfg.nitro == i) g.rrectLine(x - 15, y - 15, 30, 30, 15, C_WHITE); if (in && clicked) { clicked = false; cfg.nitro = i; audio.play(SFX_NITRO); cfg.save(); } }
        // scrolling collection
        float gx = sx + 332, gy = sy + 64, gw = sw - 348, gh = 488, cwid = 132, chgt = 112; int cols = 4; std::vector<int> items; for (int i = 0; i < NCARS; i++) if (garageCat == 0 || kCars[i].cat == garageCat - 1) items.push_back(i);
        int rows = ((int)items.size() + cols - 1) / cols; float total = rows * (chgt + 10), maxS = std::max(0.f, total - gh); bool inGrid = inRect(gx, gy, gw, gh) && !uiBlocked;
        if (inGrid) { garageScroll -= wheelY * 50.f; if (clicked) { dragStartY = my; dragStartScroll = garageScroll; dragging = false; } if (down && std::fabs(my - dragStartY) > 8) dragging = true; if (dragging && down) garageScroll = dragStartScroll - (my - dragStartY); }
        garageScroll = clampv(garageScroll, 0.f, maxS); SDL_Rect gc{(int)gx, (int)gy, (int)gw, (int)gh}; SDL_SetRenderClipRect(g.rd, &gc);
        for (size_t k = 0; k < items.size(); k++) {
            int i = items[k]; float x = gx + (k % cols) * (cwid + 6), y = gy + (k / cols) * (chgt + 10) - garageScroll; if (y < gy - chgt || y > gy + gh) continue; bool sel = cfg.car == i, in = inGrid && inRect(x, y, cwid, chgt) && my > gy && my < gy + gh;
            panel(x, y, cwid, chgt, 12, sel ? Col{40, 130, 205, 210} : (in ? Col{60, 100, 170, 180} : Col{40, 72, 135, 140})); if (sel) g.rrectLine(x - 2, y - 2, cwid + 4, chgt + 4, 14, C_CYAN);
            CarStyle cs; cs.car = i; cs.color = cfg.color; cs.wheel = T * 2; cs.t = T; cs.moving = 0; float s2 = std::min(0.95f, 108.f / kCars[i].L); drawCar(g, x + cwid / 2, y + 48, s2, cs); g.text(x + cwid / 2, y + 86, kCars[i].name, 1.65f, C_WHITE, 1);
            if (kCars[i].neon) g.rrect(x + cwid - 38, y + 6, 32, 13, 5, withA(kNeon[kCars[i].neon - 1], 140));
            if (in && clicked) { clicked = false; cfg.car = i; audio.play(SFX_CLICK); cfg.save(); sendCar(); }
        }
        SDL_SetRenderClipRect(g.rd, nullptr); if (maxS > 0) { float th = gh * gh / total; g.rrect(gx + gw - 6, gy + (gh - th) * (garageScroll / maxS), 4, th, 2, rgba(255, 255, 255, 90)); }
    }
    void drawHistory(float sx, float sy, float sw, float sh) {
        g.text(sx + 24, sy + 18, "HISTORY", 3.4f, C_WHITE, 0, true); float avg = 0; int nw = 0; for (auto& e : cfg.hist) if (e.wpm > 0) { avg += e.wpm; nw++; } avg = nw ? avg / nw : 0; char b[96];
        struct St { const char* k; std::string v; } stv[4] = {{"RACES", std::to_string(cfg.racesDone)}, {"WINS", std::to_string(cfg.wins)}, {"BEST WPM", std::to_string((int)cfg.bestWpm)}, {"AVG WPM", std::to_string((int)avg)}};
        for (int i = 0; i < 4; i++) { float x = sx + 24 + i * 214.f; panel(x, sy + 70, 200, 70, 14); g.text(x + 100, sy + 80, stv[i].k, 1.4f, C_GREY, 1); g.text(x + 100, sy + 102, stv[i].v.c_str(), 3.6f, i == 2 ? C_GOLD : C_CYAN, 1, true); }
        panel(sx + 16, sy + 156, sw - 32, sh - 172, 14, Col{30, 56, 108, 150}); const char* hd[6] = {"DATE", "PLACE", "VEHICLE", "WPM", "ACC", "MODE"}; const float hx[6] = {40, 200, 320, 520, 640, 760}; for (int i = 0; i < 6; i++) g.text(sx + hx[i], sy + 168, hd[i], 1.5f, C_GREY);
        g.rect(sx + 30, sy + 192, sw - 60, 2, rgba(255, 255, 255, 36)); if (cfg.hist.empty()) g.text(sx + sw / 2, sy + 330, "No races yet - play one!", 2.6f, C_GREY, 1);
        SDL_Rect gc{(int)sx + 20, (int)sy + 198, (int)sw - 40, (int)sh - 220}; SDL_SetRenderClipRect(g.rd, &gc); float maxS = std::max(0.f, cfg.hist.size() * 44.f - (sh - 220)); if (inRect(sx, sy + 156, sw, sh - 156) && !uiBlocked) garageScroll = clampv(garageScroll - wheelY * 40.f, 0.f, maxS); if (maxS <= 0) garageScroll = 0;
        for (size_t i = 0; i < cfg.hist.size(); i++) { const HistEntry& e = cfg.hist[i]; float y = sy + 202 + i * 44.f - garageScroll; if (y < sy + 160 || y > sy + sh) continue; SDL_DateTime dtm; SDL_TimeToDateTime((SDL_Time)e.ts * 1000000000LL, &dtm, true); snprintf(b, sizeof(b), "%02d/%02d  %02d:%02d", dtm.day, dtm.month, dtm.hour, dtm.minute);
            if (i % 2 == 0) g.rrect(sx + 28, y - 4, sw - 56, 40, 10, rgba(255, 255, 255, 12)); g.text(sx + hx[0], y + 8, b, 1.7f, C_GREY); snprintf(b, sizeof(b), "%s / %d", kPlace[clampv(e.place, 0, 4)], e.players); g.text(sx + hx[1], y + 6, b, 2.0f, e.place == 1 ? C_GOLD : C_WHITE, 0, true);
            CarStyle cs; cs.car = e.car % NCARS; cs.color = cfg.color; cs.t = T; drawCar(g, sx + hx[2] + 36, y + 16, std::min(0.4f, 70.f / kCars[cs.car].L), cs); snprintf(b, sizeof(b), "%.1f", e.wpm); g.text(sx + hx[3], y + 6, b, 2.2f, C_CYAN, 0); snprintf(b, sizeof(b), "%.1f%%", e.acc); g.text(sx + hx[4], y + 8, b, 1.9f, e.acc >= 95 ? C_GREEN : C_WHITE, 0); g.text(sx + hx[5], y + 8, e.online ? "Online" : "Solo vs AI", 1.8f, C_GREY, 0); }
        SDL_SetRenderClipRect(g.rd, nullptr);
    }
    void drawResultStage(float sx, float sy, float sw, float sh) {
        if (!view.haveEnd) return; const RaceEndInfo& e = view.end; int a = (int)(255 * resAlpha); g.text(sx + sw / 2, sy + 18, "RACE RESULTS", 3.4f, withA(C_GOLD, a), 1, true);
        int order[4], n = 0; for (int i = 0; i < e.n; i++) order[n++] = i; std::sort(order, order + n, [&](int p, int q) { return e.s[p].place < e.s[q].place; });
        const char* hd[5] = {"POS", "DRIVER", "WPM", "ACC", "TIME"}; const float hx[5] = {30, 330, 560, 680, 800}; for (int i = 0; i < 5; i++) g.text(sx + hx[i], sy + 72, hd[i], 1.5f, withA(C_GREY, a)); char b[48];
        for (int k = 0; k < n; k++) { const RacerSnap& s = e.s[order[k]]; const RacerView& r = view.r[s.slot]; float y = sy + 100 + k * 84.f; bool me = s.slot == view.mySlot; float off = (1 - easeOut(resAlpha * 1.4f - k * 0.15f)) * 120.f;
            panel(sx + 16 + off, y, sw - 32, 76, 14, me ? Col{40, 130, 205, (uint8_t)(200 * resAlpha)} : Col{36, 66, 124, (uint8_t)(150 * resAlpha)}); if (me) g.rrectLine(sx + 16 + off, y, sw - 32, 76, 14, withA(C_CYAN, a));
            g.rrectG(sx + 28 + off, y + 18, 62, 40, 10, rgba(34, 40, 58, a), rgba(8, 10, 18, a)); g.rrectLine(sx + 28 + off, y + 18, 62, 40, 10, s.place == 1 ? withA(C_GOLD, a) : rgba(150, 165, 195, a)); g.text(sx + 59 + off, y + 26, kPlace[clampv<int>(s.place, 0, 4)], 2.4f, withA(s.place == 1 ? C_GOLD : C_WHITE, a), 1, true);
            CarStyle cs; cs.car = r.car; cs.color = r.color; cs.t = T; cs.nitroCol = r.nitroCol; cs.wheel = 0; drawCar(g, sx + 190 + off, y + 38, std::min(1.0f, 150.f / kCars[r.car].L), cs);
            g.text(sx + hx[1] + off, y + 14, r.name.c_str(), 2.5f, withA(C_WHITE, a), 0, true); if (me) g.text(sx + hx[1] + off, y + 46, "YOU", 1.5f, withA(C_CYAN, a), 0); else if (s.slot == e.best) g.text(sx + hx[1] + off, y + 46, "BEST PERFORMER", 1.3f, withA(C_GOLD, a), 0);
            snprintf(b, sizeof(b), "%.1f", s.wpm10 / 10.f); g.text(sx + hx[2] + off, y + 22, b, 3.0f, withA(C_CYAN, a), 0, true); snprintf(b, sizeof(b), "%.1f%%", s.acc10 / 10.f); g.text(sx + hx[3] + off, y + 26, b, 2.2f, withA(s.acc10 >= 950 ? C_GREEN : C_WHITE, a), 0);
            if (s.flags & SF_FINISHED) { snprintf(b, sizeof(b), "%.1fs", s.finishMs / 1000.f); g.text(sx + hx[4] + off, y + 26, b, 2.2f, withA(C_WHITE, a), 0); } else g.text(sx + hx[4] + off, y + 26, "DNF", 2.2f, withA(C_RED, a), 0); }
        if (resAlpha > 0.7f) { if (btn("Race Again", sx + sw / 2 - 270, sy + sh - 70, 250, 54, C_GREEN, true, 2.4f)) raceAgain(); if (btn("Home", sx + sw / 2 + 20, sy + sh - 70, 250, 54, C_BLUE, true, 2.4f)) { if (!view.practice) leaveRoom(); else hm = HM_IDLE; view.live = false; } }
    }
    void raceAgain() {
        if (view.practice) { beginPractice(false); return; }
        if (!inRoom) { startOnline(PA_QUICK); return; } hm = room.isPublic ? HM_MATCH : HM_ROOM; view.live = false; if (!room.isPublic) { Writer w; w.u8(1); net.send(C_READY, w); }
    }
    void drawHome() {
        bool zoom = zoomDir != 0; background(); drawLeftColumn(); drawTopBar(); drawStage(zoom);
        if (zoom) { float z = easeOut(zoomT); SDL_SetRenderViewport(g.rd, nullptr); g.rect(0, 0, gW, 720, withA(C_BG0, (int)(clampv(z * 1.5f, 0.f, 1.f) * 255)));
            float x0 = gOX + 356, y0 = 212, w0 = 892, h0 = 436; float x = lerpf(x0, 0, z), y = lerpf(y0, 0, z), w = lerpf(w0, gW, z), h = lerpf(h0, RV_H, z); drawLaneScene(x, y, w, h, zoomDir > 0 ? 0.f : camX, view.live); setContent(); }
    }

    // ---------------------------------------------------------------- race screen (HUD: top bar + docked rank | text | meters)
    void dial(float cx, float cy, float r, float val, float maxv, float redFrom, float major, float needleVal) {
        const float a0 = 2.356f, sweep = 4.712f; g.shadow(cx - r - 9, cy - r - 5, (r + 9) * 2, (r + 9) * 2, r + 9, 12, 140);
        g.circle(cx, cy, r + 10, rgba(8, 10, 16)); g.circle(cx, cy, r + 8, rgba(150, 162, 186)); g.circle(cx, cy, r + 6, rgba(24, 28, 40)); g.circle(cx, cy, r + 1, rgba(7, 15, 36)); g.glowE(cx - r * 0.25f, cy - r * 0.45f, r * 0.85f, r * 0.45f, rgba(150, 200, 255, 34), true);
        g.arc(cx, cy, r - 4, 5, a0, a0 + sweep, rgba(255, 255, 255, 28), rgba(255, 255, 255, 28)); float f = clampv(val / maxv, 0.f, 1.f); if (f > 0.01f) g.arc(cx, cy, r - 4, 5, a0, a0 + sweep * f, C_CYAN, mixc(C_GREEN, f > 0.8f ? C_RED : C_GOLD, f));
        if (redFrom < maxv) g.arc(cx, cy, r - 11, 3, a0 + sweep * redFrom / maxv, a0 + sweep, rgba(255, 60, 70, 150), rgba(255, 60, 70, 220));
        int steps = (int)std::lround(maxv / major) * 5; char b[16];
        for (int i = 0; i <= steps; i++) { float a = a0 + sweep * i / steps; bool mj = i % 5 == 0; float r1 = r - 15, r2 = r - (mj ? 23.f : 19.f); g.thickLine(cx + std::cos(a) * r1, cy + std::sin(a) * r1, cx + std::cos(a) * r2, cy + std::sin(a) * r2, mj ? 2.f : 1.f, mj ? C_WHITE : rgba(160, 175, 205));
            if (mj) { snprintf(b, sizeof(b), "%d", (int)std::lround(i / 5 * major)); g.text(cx + std::cos(a) * (r - 33), cy + std::sin(a) * (r - 33) - 4.f, b, 0.95f, C_GREY, 1); } }
        float na = a0 + sweep * clampv(needleVal / maxv, 0.f, 1.02f), c = std::cos(na), s = std::sin(na), nl = r - 12, px = -s * 3.f, py = c * 3.f;
        g.glowE(cx + c * nl * 0.5f, cy + s * nl * 0.5f, nl * 0.5f, nl * 0.5f, rgba(255, 60, 70, 36), true); g.tri(cx + px, cy + py, cx - px, cy - py, cx + c * nl, cy + s * nl, rgba(255, 70, 80), rgba(255, 70, 80), rgba(255, 150, 150));
        g.thickLine(cx, cy, cx - c * 11, cy - s * 11, 4, rgba(200, 50, 60)); g.circle(cx, cy, 8, rgba(30, 34, 46)); g.circle(cx, cy, 6, rgba(190, 200, 220)); g.circle(cx - 1, cy - 1, 3, rgba(255, 255, 255));
    }
    static std::string fmtTime(double s) { if (s < 0) s = 0; char b[16]; snprintf(b, sizeof(b), "%d:%02d", (int)s / 60, (int)s % 60); return b; }
    void drawTopHud(double clock) {
        int active = 0; for (auto& r : view.r) if (r.active) active++; float y = 10, h = 54;
        panel(16, y, 190, h, 14); if (view.mySlot < 4) { int pl = std::max<int>(1, view.r[view.mySlot].snap.place); Col gold = pl == 1 ? C_GOLD : pl == 2 ? rgba(215, 225, 240) : pl == 3 ? rgba(225, 150, 90) : rgba(120, 190, 255); g.glow(86, y + h / 2, 44, withA(gold, 60)); g.text(30, y + 8, "POS", 1.3f, C_CYAN, 0); g.text(80, y + 9, kPlace[clampv(pl, 1, 4)], 3.6f, gold, 0, true); char b[8]; snprintf(b, sizeof(b), "/%d", active); g.text(190, y + 30, b, 1.8f, C_GREY, 2); } else g.text(111, y + 16, "SPECTATING", 2.0f, C_GOLD, 1);
        double remain = view.limitSec - std::max(0.0, clock); bool fin = false;
        if (view.practice && core.firstFinishAt >= 0) { remain = core.firstFinishAt + RaceCore::kGrace - nowSec(); fin = true; } else if (!view.practice && view.forcedEndMs) { remain = ((double)view.forcedEndMs - ((double)nowMs() + offsetMs)) / 1000.0; fin = true; }
        std::string tm = fmtTime(remain); panel(1074, y, 190, h, 14); if (fin) g.rrectLine(1074, y, 190, h, 14, withA(C_RED, 220)); g.text(1090, y + 9, fin ? "ENDS IN" : "TIME", 1.3f, fin ? C_RED : C_CYAN, 0); g.text(1250, y + 9, tm.c_str(), 3.6f, fin && std::fmod(T, 0.6f) < 0.3f ? C_RED : C_WHITE, 2, true);
        // progress bar (start flag -> finish checker), all racers as lettered markers
        float bx0 = 252, bx1 = 1030, by = 37; g.shadow(222, y, 836, h, 27, 12, 120); g.rrectG(222, y, 836, h, 27, rgba(20, 38, 78, 235), rgba(6, 12, 30, 240)); g.rrectLine(222, y, 836, h, 27, rgba(140, 205, 255, 110));
        g.rrect(bx0, by - 4, bx1 - bx0, 8, 4, rgba(255, 255, 255, 36)); float lead = 0; for (auto& r : view.r) if (r.active) lead = std::max(lead, r.disp); g.rrectG(bx0, by - 4, std::max(8.f, (bx1 - bx0) * lead), 8, 4, C_CYAN, C_BLUE);
        for (int i = 0; i < 5; i++) { g.rect(bx0 - 2, by - 14 + i * 5.6f, 6, 5.6f, i % 2 ? rgba(240, 240, 240) : rgba(20, 20, 24)); } for (int i = 0; i < 6; i++) for (int k = 0; k < 2; k++) g.rect(bx1 + 6 + k * 7.f, by - 15 + i * 5.f, 7, 5, (i + k) % 2 ? rgba(20, 20, 24) : rgba(240, 240, 240));
        for (int i = 0; i < 4; i++) { const RacerView& r = view.r[i]; if (!r.active) continue; float x = bx0 + r.disp * (bx1 - bx0); bool me = i == view.mySlot; float rr = me ? 14.f : 11.f; g.glow(x, by, rr + 8, withA(kBody[r.color], 130)); g.circle(x, by, rr + 1.5f, rgba(10, 14, 26)); g.circle(x, by, rr, kBody[r.color]); if (me) g.rrectLine(x - rr - 2, by - rr - 2, rr * 2 + 4, rr * 2 + 4, rr + 2, C_WHITE);
            char c[2] = {(char)toupper((unsigned char)(r.name.empty() ? '?' : r.name[0])), 0}; g.text(x, by - 6, c, me ? 1.6f : 1.3f, rgba(10, 14, 26), 1); }
    }
    void drawDock(double clock) {
        const float y0 = RV_H, h = 720 - RV_H; SDL_SetRenderViewport(g.rd, nullptr); g.rect(0, y0, gW, h, rgba(5, 10, 24)); setContent();
        g.vgrad(-gOX, y0, gW, h, rgba(18, 34, 70), rgba(6, 12, 30)); g.vgrad(-gOX, y0, gW, 4, withA(C_CYAN, 150), withA(C_CYAN, 0)); g.rect(-gOX, y0, gW, 1, rgba(140, 205, 255, 110)); g.rect(248, y0 + 12, 1, h - 24, rgba(140, 205, 255, 70)); g.rect(968, y0 + 12, 1, h - 24, rgba(140, 205, 255, 70));
        // ---- ranking (left, attached to the text card)
        g.text(124, y0 + 12, "RANKING", 1.5f, C_CYAN, 1); int order[4], n = 0; for (int i = 0; i < 4; i++) if (view.r[i].active) order[n++] = i;
        std::sort(order, order + n, [&](int a, int b) { int pa = view.r[a].snap.place ? view.r[a].snap.place : 9, pb = view.r[b].snap.place ? view.r[b].snap.place : 9; if (pa != pb) return pa < pb; return a < b; });
        for (int k = 0; k < n; k++) { RacerView& r = view.r[order[k]]; float ty = y0 + 40 + k * 48.f; if (r.rowY < 1) r.rowY = ty; r.rowY += (ty - r.rowY) * std::min(1.f, dt * 9); bool me = order[k] == view.mySlot; Col c = kBody[r.color]; float y = r.rowY;
            g.rrect(12, y, 224, 42, 12, withA(c, me ? 120 : 55)); if (me) g.rrectLine(12, y, 224, 42, 12, withA(C_WHITE, 230)); else g.rrectLine(12, y, 224, 42, 12, withA(c, 150)); g.rrect(12, y, 6, 42, 3, c);
            g.circle(44, y + 21, 15, mixc(c, rgba(0, 0, 0), 0.35f)); g.arc(44, y + 21, 15, 2, 0, 6.283f, c, c); char b[4]; snprintf(b, sizeof(b), "%d", k + 1); g.text(44, y + 12, b, 2.0f, C_WHITE, 1, true);
            std::string nm = me ? "YOU" : r.name; if (nm.size() > 9) nm.resize(9); g.text(70, y + 12, nm.c_str(), me ? 2.1f : 1.9f, me ? C_WHITE : withA(mixc(c, C_WHITE, 0.55f), 255), 0, me); }
        // ---- text card (middle): exactly 3 lines, current line always in the middle
        const float tx = 248, tw = 720, sc = 3.0f; if (view.mySlot >= 4) { g.text(tx + tw / 2, y0 + 100, "You are spectating this race", 2.6f, C_GREY, 1); }
        else {
            int nch = (int)view.text.size(), cur = (int)view.idx; size_t L = view.lineStart.size(); int curLine = 0; for (size_t i = 0; i < L; i++) if (view.lineStart[i] <= std::min(cur, nch - 1)) curLine = (int)i;
            view.scroll += ((float)curLine - view.scroll) * (1.f - std::exp(-dt * 9.f)); SDL_Rect clip{(int)(gOX + tx + 8), (int)y0 + 10, (int)tw - 16, (int)h - 20}; SDL_SetRenderClipRect(g.rd, &clip);
            for (size_t li = 0; li < L; li++) {
                float dist = (float)li - view.scroll; if (std::fabs(dist) > 1.7f) continue; float y = y0 + 78 + dist * 56.f; int a = view.lineStart[li], e = li + 1 < L ? view.lineStart[li + 1] : nch; float fade = clampv(1.f - std::fabs(dist) * 0.55f, 0.25f, 1.f); float bx = tx + 34;
                for (int i = a; i < e; i++) {
                    char ch = view.text[i]; float x = bx + view.charX[i], cw = view.charX[i + 1] - view.charX[i];
                    if (i == cur) { Col hc = view.wrongFlash > 0.05f ? rgba(255, 90, 100) : rgba(255, 228, 96); if (ch != ' ') { g.textn(x, y, &ch, 1, sc, hc); g.textn(x + 0.8f, y, &ch, 1, sc, hc); } g.rrect(x, y + 36, std::max(cw, 10.f), 4, 2, view.wrongFlash > 0.05f ? C_RED : C_CYAN); }
                    else if (ch != ' ') g.textn(x, y, &ch, 1, sc, i < cur ? withA(rgba(96, 236, 140), (int)(255 * fade)) : withA(rgba(208, 220, 244), (int)(255 * fade)));
                }
            }
            SDL_SetRenderClipRect(g.rd, nullptr); g.vgrad(tx, y0 + 4, tw, 22, rgba(10, 20, 44, 200), rgba(10, 20, 44, 0)); g.vgrad(tx, y0 + h - 26, tw, 22, rgba(6, 12, 30, 0), rgba(6, 12, 30, 200));
            if (view.streak >= 5) { char b[24]; snprintf(b, sizeof(b), "COMBO x%d", view.streak); g.rrect(tx + tw - 130, y0 + 12, 118, 24, 10, rgba(4, 10, 24, 200)); g.rrectLine(tx + tw - 130, y0 + 12, 118, 24, 10, withA(C_GOLD, 200)); g.text(tx + tw - 71, y0 + 17, b, 1.4f, C_GOLD, 1); }
        }
        // ---- meters (right, attached to the text card): WPM + ACC same size, values below
        const float r = 54, cy = y0 + 92; char b[24]; bool alive = view.mySlot < 4; float wpm = view.dispWpm, acc = view.dispAcc;
        dial(1050, cy, r, wpm, 150, 120, 50, wpm + std::sin(raceT * 30) * (wpm > 5 ? 0.7f : 0.f)); dial(1198, cy, r, acc, 100, 101, 50, acc);
        snprintf(b, sizeof(b), "%.0f", wpm); g.text(1050, cy + r + 16, alive ? b : "-", 3.0f, C_WHITE, 1, true); g.text(1050, cy + r + 44, "WPM", 1.3f, C_CYAN, 1);
        snprintf(b, sizeof(b), "%.0f%%", acc); g.text(1198, cy + r + 16, alive ? b : "-", 3.0f, C_WHITE, 1, true); g.text(1198, cy + r + 44, "ACCURACY", 1.3f, C_CYAN, 1);
        if (alive) { const RacerSnap& s = view.r[view.mySlot].snap; float ny = y0 + h - 26; bool ready = s.nitro >= 50 && !(s.flags & SF_NITRO), act = s.flags & SF_NITRO; Col nc, nc2; nitroCols(view.r[view.mySlot].nitroCol, raceT, nc, nc2); Col nn = mixc(nc, nc2, 0.5f);
            g.text(984, ny + 2, ready ? "NITRO [TAB]" : "NITRO", 1.25f, ready && std::fmod(T, 0.8f) < 0.5f ? nn : C_GREY, 0);
            for (int i = 0; i < 14; i++) { bool on = s.nitro >= (i + 1) * 7 - 3 || act; float x = 1068 + i * 14.f; g.rrect(x, ny, 10, 16, 3, on ? withA(i < 7 ? nc : mixc(nc, nc2, 0.6f), act ? (int)(200 + 55 * std::sin(T * 30)) : 235) : rgba(255, 255, 255, 24)); if (on && ready) g.glow(x + 5, ny + 8, 9, withA(nn, 60)); }
            if (inRect(968, ny - 6, 312, 30) && clicked && !uiBlocked) { clicked = false; doNitro(); } }
        (void)clock;
    }
    void drawRace() {
        double clock = raceClock(); bool myN = view.mySlot < 4 && (view.r[view.mySlot].snap.flags & SF_NITRO); float cam = camX + (myN ? std::sin(raceT * 70) * 1.6f : 0.f);
        SDL_SetRenderViewport(g.rd, nullptr); drawLaneScene(0, 0, gW, RV_H, cam, true); setContent(); drawTopHud(clock); drawDock(clock);
        if (clock < 1.0) { float a = 1.f; char c[8] = ""; if (clock < -3) { snprintf(c, sizeof(c), "READY"); a = 0.9f; } else if (clock < 0) { snprintf(c, sizeof(c), "%d", (int)std::ceil(-clock)); } else { snprintf(c, sizeof(c), "GO!"); a = 1.f - (float)clock; }
            float ph = clock < -3 ? 0.f : (float)(-clock - std::floor(-clock)); float sc = (clock < -3 ? 9.f : 17.f) * (1.f + (clock < 0 && clock >= -3 ? (1.f - ph) * 0.35f : (float)clock * 0.4f)); g.glow(640, 220, 190 + (1 - a) * 80, withA(clock >= 0 ? C_GREEN : C_CYAN, (int)(150 * a))); g.text(640, 220 - sc * 4, c, sc, withA(clock >= 0 ? C_GREEN : C_WHITE, (int)(255 * a)), 1, true); }
        for (auto& p : view.pops) if (p.t > 0) { float u = 1.4f - p.t; g.text(640, 340 - u * 70, p.s.c_str(), 3.6f, withA(p.c, (int)(255 * std::min(1.f, p.t * 2.5f))), 1, true); }
        if (view.mySlot < 4 && (view.r[view.mySlot].snap.flags & SF_FINISHED) && !view.haveEnd) { g.rrect(440, 350, 400, 44, 12, rgba(0, 0, 0, 170)); g.text(640, 361, "Finished! Waiting for the others...", 2.0f, C_WHITE, 1); }
    }

    // ---------------------------------------------------------------- race setup / logic
    void wrapText() {
        const float sc = 3.0f, maxW = 640; view.lineStart.assign(1, 0); view.charX.assign(view.text.size() + 1, 0.f); const char* s = view.text.c_str(); int n = (int)view.text.size(), ls = 0, i = 0;
        while (i < n) { int e = i; while (e < n && s[e] != ' ') e++; int we = std::min(n, e + (e < n ? 1 : 0)); if (we > ls + 1 && g.prefixW(s + ls, e - ls, sc) > maxW && i > ls) { view.lineStart.push_back(i); ls = i; } i = we; }
        for (size_t L = 0; L < view.lineStart.size(); L++) { int a = view.lineStart[L], bnd = L + 1 < view.lineStart.size() ? view.lineStart[L + 1] : n; for (int k = a; k < bnd; k++) view.charX[k] = g.prefixW(s + a, k - a, sc); view.charX[bnd] = g.prefixW(s + a, bnd - a, sc); }
    }
    void resolveColors(const int req[4], const int nit[4]) {
        int used = 0; for (int i = 0; i < 4; i++) { RacerView& r = view.r[i]; if (!r.active) continue; int c = req[i] % NCOLORS; if (used & (1 << c)) { c = -1; for (int k = 0; k < NCOLORS; k++) { int cand = k < 4 ? (i + k) % 4 : k; if (!(used & (1 << cand))) { c = cand; break; } } if (c < 0) c = i; } used |= 1 << c; r.color = c; r.nitroCol = clampv(nit[i], 0, NNITRO - 1); }
    }
    void resetRaceFx() { fx.clear(); camX = 0; raceT = 0; }
    void startZoomOrGo() { if (screen == SC_HOME && (hm == HM_MATCH || hm == HM_ROOM || hm == HM_CONNECT || hm == HM_IDLE)) { zoomDir = 1; zoomT = 0; } else if (screen != SC_RACE) goTo(SC_RACE); }
    void beginOnlineRace(const RaceStartInfo& in) {
        bool same = view.live && !view.practice && view.startAtMs == in.startAtMs;
        if (!same) { view = RaceView(); view.live = true; view.text = in.text; view.startAtMs = in.startAtMs; view.limitSec = in.limitSec; wrapText(); resetRaceFx(); }
        view.mySlot = in.mySlot; int req[4] = {0, 0, 0, 0}, nit[4] = {0, 0, 0, 0};
        for (int i = 0; i < 4; i++) { view.r[i].active = (in.mask >> i) & 1; if (!view.r[i].active) continue; view.r[i].name = in.name[i]; view.r[i].car = in.car[i] % NCARS; req[i] = in.color[i] & 15; nit[i] = in.color[i] >> 4; }
        resolveColors(req, nit); if (!same) { audio.play(SFX_WHOOSH); startZoomOrGo(); } else if (screen != SC_RACE && zoomDir == 0) goTo(SC_RACE);
    }
    void beginPractice(bool zoom) {
        std::mt19937_64 rng(std::random_device{}()); std::string text = genParagraph(1, 1, rng); double limit = 90; core.reset(text, nowSec() + COUNTDOWN_SEC, limit); view = RaceView(); view.live = true; view.practice = true; view.text = text; view.limitSec = limit; view.mySlot = 0; wrapText(); resetRaceFx();
        static const char* nm[3] = {"Turbo-AI", "Blaze-AI", "Nova-AI"}; static const float base[3] = {30, 46, 62}; int req[4] = {cfg.color, 0, 0, 0}, nit[4] = {cfg.nitro, 0, 1, 3};
        core.r[0].active = true; core.r[0].name = cfg.name; view.r[0].active = true; view.r[0].name = cfg.name; view.r[0].car = cfg.car;
        for (int i = 1; i <= 3; i++) { bots[i] = Bot(); if (i > pracOpp) continue; Bot& b = bots[i]; b.on = true; b.rng = (uint32_t)rng(); b.wpm = base[i - 1] * (0.85f + 0.3f * frand01(b.rng)); b.acc = 0.92f + 0.07f * frand01(b.rng); b.style = (i - 1) % 3;
            core.r[i].active = true; core.r[i].name = nm[i - 1]; view.r[i].active = true; view.r[i].name = nm[i - 1]; view.r[i].car = (int)(rng() % NCARS); req[i] = (int)(rng() % NCOLORS); }
        resolveColors(req, nit); if (zoom) { zoomDir = 1; zoomT = 0; } else goTo(SC_RACE);
    }
    double raceClock() const { return view.practice ? nowSec() - core.startAt : ((double)nowMs() + offsetMs - (double)view.startAtMs) / 1000.0; }
    void applySnap(const RacerSnap& s) {
        if (s.slot > 3) return; RacerView& r = view.r[s.slot]; r.snap = s; float len = (float)std::max<size_t>(1, view.text.size()), score = s.score100 / 100.f;
        if (s.slot == view.mySlot) { while (!view.unacked.empty() && view.unacked.front().seq <= s.lastSeq) view.unacked.pop_front(); view.idx = s.progress; view.lcorrect = s.correct; view.lwrong = s.wrong;
            for (auto& u : view.unacked) { if (view.idx < view.text.size() && view.text[view.idx] == u.c) { view.idx++; view.lcorrect++; } else view.lwrong++; } score += (float)view.idx - (float)s.progress; }
        float tgt = score / len; r.target = (s.flags & SF_FINISHED) ? 1.f : std::min(tgt, 0.985f);
    }
    void popup(const std::string& s, Col c) { RaceView::Pop& p = view.pops[view.popN++ % 6]; p.s = s; p.t = 1.4f; p.c = c; p.x = 60 + frand01(rngState) * 600; }
    void typeChar(char c) {
        if (view.idx >= view.text.size()) return; view.seq++; view.unacked.push_back({view.seq, c}); if (view.unacked.size() > 512) view.unacked.pop_front();
        if (view.text[view.idx] == c) { view.idx++; view.lcorrect++; view.pulse = 1; audio.play(SFX_TYPE); view.streak++; view.best = std::max(view.best, view.streak);
            if (view.streak == 10 || view.streak == 25 || view.streak == 50 || view.streak == 100) { popup("COMBO x" + std::to_string(view.streak) + "!", C_GOLD); audio.play(SFX_POS); } }
        else { view.lwrong++; view.wrongFlash = 1; view.streak = 0; audio.play(SFX_ERROR); }
        if (view.practice) core.onChar(0, view.seq, c, nowSec()); else { Writer w; w.u32(view.seq); w.u8((uint8_t)c); net.send(C_TYPE, w); }
    }
    void doNitro() { if (view.mySlot > 3) return; const RacerSnap& s = view.r[view.mySlot].snap; if (s.nitro < 50 || (s.flags & SF_NITRO)) return; if (view.practice) core.onNitro(0, nowSec()); else net.send(C_NITRO); }
    void practiceStep(double clock) {
        double now = nowSec();
        if (clock >= 0) for (int i = 1; i <= 3; i++) { Bot& b = bots[i]; if (!b.on || core.r[i].finished) continue; float w = b.wpm; if (b.style == 1) w *= 1.f + 0.38f * std::sin((float)clock * 0.7f + i); else if (b.style == 2) w *= 1.28f - 0.42f * std::min(1.f, (float)clock / 45.f);
            b.due += dt * w * 5.f / 60.f; while (b.due >= 1.0) { b.due -= 1.0; bool err = frand01(b.rng) > b.acc; char ch = err ? '~' : (core.r[i].progress < core.text.size() ? core.text[core.r[i].progress] : ' '); core.onChar(i, ++b.seq, ch, now); }
            if (core.r[i].nitroCharge >= 60 && now > b.nextNitro) { core.onNitro(i, now); b.nextNitro = now + 4 + 6 * frand01(b.rng); } }
        core.tick(now); for (int i = 0; i < 4; i++) if (core.r[i].active) applySnap(core.snap(i, now));
        if (core.over && !view.haveEnd) { core.computePlaces(); RaceEndInfo e; e.textLen = (uint16_t)core.text.size(); float bs = -1; int n = 0;
            for (int i = 0; i < 4; i++) if (core.r[i].active) { e.s[n++] = core.snap(i, now); if (core.r[i].place == 1) e.winner = (uint8_t)i; float sc = core.wpm(i, now) * core.accuracy(i); if (sc > bs) { bs = sc; e.best = (uint8_t)i; } } e.n = n; view.end = e; view.haveEnd = true; view.endedAt = T; }
    }
    void emitNitro(int slot, float cx, float cy, float L) {
        RacerView& r = view.r[slot]; Col a, b; nitroCols(r.nitroCol, raceT, a, b); int md = kNitro[r.nitroCol].mode;
        for (int q = 0; q < 2; q++) fx.emit(cx - L * 0.5f - 18, cy + (frand01(rngState) - 0.5f) * 14, -170 - 100 * frand01(rngState), (frand01(rngState) - 0.5f) * 70, 0.35f, 4.5f, withA(b, 220), 1);
        if (md == 3) fx.emit(cx - L * 0.5f - 24, cy + (frand01(rngState) - 0.5f) * 34, -120 * frand01(rngState) - 40, (frand01(rngState) - 0.5f) * 90, 0.7f, 6.5f, withA(a, 255), 1);
        else if (md == 4) fx.emit(cx - L * 0.5f - 24, cy + (frand01(rngState) - 0.5f) * 16, -90 - 60 * frand01(rngState), (frand01(rngState) - 0.5f) * 40, 0.8f, 11.f, withA(mixc(b, rgba(120, 120, 130), 0.5f), 110), 3);
        else if (md == 5) fx.emit(cx - L * 0.5f - 20, cy + (frand01(rngState) - 0.5f) * 14, -100 * frand01(rngState) - 50, 60 + 80 * frand01(rngState), 0.6f, 3.5f, withA(a, 255), 1);
    }
    void updateRace() {
        raceT += dt; double clock = raceClock(); if (view.practice) practiceStep(clock);
        if (clock < 0) { int n = clock < -3 ? 4 : (int)std::ceil(-clock); if (n != view.lastCount) { view.lastCount = n; if (n <= 3) audio.play(SFX_BEEP); } }
        if (clock >= 0 && !view.goPlayed) { view.goPlayed = true; audio.play(SFX_GO); }
        view.wrongFlash = std::max(0.f, view.wrongFlash - dt * 4); view.pulse = std::max(0.f, view.pulse - dt * 6); for (auto& p : view.pops) p.t = std::max(0.f, p.t - dt);
        bool canType = view.mySlot < 4 && clock >= 0 && !view.haveEnd && !confirmLeave && view.idx < view.text.size();
        if (shotDir && canType) { for (int q = 0; q < 3 && view.idx < view.text.size(); q++) typeChar(view.text[view.idx]); if (view.r[view.mySlot].snap.nitro >= 50) doNitro(); }
        if (canType) { for (unsigned char ch : typed) if (ch >= 32 && ch <= 126) typeChar((char)ch); if (kTab) doNitro(); }
        if (kEsc) confirmLeave = !confirmLeave;
        float mySpeed = 0; int leader = 0; float lead = -1; const float k = 1.f - std::exp(-dt * 8.f);
        for (int i = 0; i < 4; i++) {
            RacerView& r = view.r[i]; if (!r.active) continue; float tgt = r.target;
            if (i == view.mySlot) { float len = (float)std::max<size_t>(1, view.text.size()); float sc = r.snap.score100 / 100.f + ((float)view.idx - (float)r.snap.progress); tgt = (r.snap.flags & SF_FINISHED) ? 1.f : std::min(sc / len, 0.985f); }
            float prev = r.disp; r.disp += (tgt - r.disp) * k; if (r.disp < prev) r.disp = prev; r.disp = std::min(r.disp, 1.f); float sp = (r.disp - prev) * WORLD_LEN / std::max(dt, 0.001f); r.speed += (sp - r.speed) * 0.15f; r.wheel += r.speed * dt / 36.f; bool nit = r.snap.flags & SF_NITRO;
            r.flame += ((nit ? 1.f + 0.25f * std::sin(raceT * 45 + i) : 0.f) - r.flame) * std::min(1.f, dt * 14);
            if (nit && !r.nitroSeen && i == view.mySlot) { audio.play(SFX_NITRO); view.nitroFlash = 1; popup("NITRO!", kNitro[r.nitroCol].b); } r.nitroSeen = nit;
            float cx = START_X + r.disp * WORLD_LEN, cy = laneY(i), L = kCars[r.car].L; r.emit += dt * r.speed * 0.16f;
            while (r.emit >= 1.f) { r.emit -= 1.f; float jx = frand01(rngState), jy = frand01(rngState); fx.emit(cx - L * 0.5f, cy + (jy - 0.5f) * 30, -30 - r.speed * 0.25f, (jy - 0.5f) * 60, 0.55f, 5 + 4 * jx, rgba(190, 160, 112, 170), 0); }
            if (nit) emitNitro(i, cx, cy, L);
            if ((r.snap.flags & SF_FINISHED) && !r.fx) { r.fx = true; for (int q = 0; q < 70; q++) { static const Col cc[5] = {{255, 90, 90, 255}, {90, 170, 255, 255}, {255, 215, 70, 255}, {90, 230, 130, 255}, {255, 255, 255, 255}}; fx.emit(cx, cy, 160 + frand01(rngState) * 120, -220 * frand01(rngState) - 40, 1.6f, 5, cc[q % 5], 2); } if (i == view.mySlot) { audio.play(SFX_FINISH); popup("FINISHED!", C_GOLD); } }
            if (i == view.mySlot) mySpeed = r.speed; if (r.disp > lead) { lead = r.disp; leader = i; }
        }
        if (view.mySlot < 4) { int pl = view.r[view.mySlot].snap.place; if (view.lastPlace && pl && pl < view.lastPlace && clock > 1) audio.play(SFX_POS); if (pl) view.lastPlace = pl; }
        float eng = view.mySlot < 4 ? clampv(mySpeed / 170.f, 0.f, 1.f) : 0.f; if (clock < 0 || view.haveEnd) eng = clock < 0 ? 0.12f : 0.f; shownEngine += (eng - shownEngine) * 0.12f; audio.engine = shownEngine; audio.nitroOn = view.mySlot < 4 && (view.r[view.mySlot].snap.flags & SF_NITRO);
        int fs = view.mySlot < 4 ? view.mySlot : leader; float want = START_X + view.r[fs].disp * WORLD_LEN - 420.f; want = clampv(want, 0.f, std::max(0.f, FINISH_X + 260.f - gW)); camX += (want - camX) * (1.f - std::exp(-dt * 6.f)); view.nitroFlash = std::max(0.f, view.nitroFlash - dt * 2.2f);
        float wpm = view.mySlot < 4 ? view.r[view.mySlot].snap.wpm10 / 10.f : 0.f; view.dispWpm += (wpm - view.dispWpm) * (1.f - std::exp(-dt * 5.f)); float acc = view.mySlot < 4 ? view.r[view.mySlot].snap.acc10 / 10.f : 100.f; view.dispAcc += (acc - view.dispAcc) * (1.f - std::exp(-dt * 4.f));
        if (view.haveEnd && !view.recorded && T > view.endedAt + 1.4) showResults();
        if (!view.practice && view.live && clock > view.limitSec + 20 && !view.haveEnd) { toast("Race ended"); view.live = false; screen = SC_HOME; hm = inRoom ? (room.isPublic ? HM_MATCH : HM_ROOM) : HM_IDLE; }
    }
    void showResults() {
        view.recorded = true; confirmLeave = false;
        if (view.mySlot < 4) for (int i = 0; i < view.end.n; i++) if (view.end.s[i].slot == view.mySlot) { const RacerSnap& s = view.end.s[i]; cfg.racesDone++; float w = s.wpm10 / 10.f; cfg.bestWpm = std::max(cfg.bestWpm, w); if (s.place == 1 && view.end.n >= 2) cfg.wins++;
            HistEntry e; e.place = s.place; e.players = view.end.n; e.car = view.r[s.slot].car; e.online = !view.practice; e.wpm = w; e.acc = s.acc10 / 10.f; SDL_Time tm = 0; SDL_GetCurrentTime(&tm); e.ts = (long long)(tm / 1000000000LL); cfg.hist.insert(cfg.hist.begin(), e); if (cfg.hist.size() > 40) cfg.hist.resize(40); cfg.save(); }
        screen = SC_HOME; hm = HM_RESULT; zoomDir = -1; zoomT = 1.f; resAlpha = 0; fade = 0; fadingOut = false;
    }

    // ---------------------------------------------------------------- online control + network events
    void startOnline(Pending pa) {
        pending = pa; if (net.state() == NS_CONNECTED && welcomed) { runPending(); return; }
        if (net.state() == NS_IDLE || net.state() == NS_FAILED) { welcomed = false; net.connectTo(gServerHost, 443, cfg.name); } hm = HM_CONNECT;
    }
    void runPending() {
        Pending p = pending; pending = PA_NONE; if (p == PA_QUICK) { Writer w; w.u8(1); w.u8(1); net.send(C_QUICK, w); hm = HM_CONNECT; }
        else if (p == PA_CREATE) { Writer w; w.u8(1); w.u8(1); w.u8(0); w.u8(1); net.send(C_CREATE, w); hm = HM_CONNECT; } else if (p == PA_JOIN) { Writer w; w.str(joinCode); net.send(C_JOIN, w); hm = HM_CONNECT; }
    }
    uint8_t myColorByte() const { return (uint8_t)(cfg.color | (cfg.nitro << 4)); }
    void sendCar() { if (!inRoom) return; Writer w; w.u8((uint8_t)cfg.car); w.u8(myColorByte()); net.send(C_CAR, w); }
    void pumpNet() {
        NetEvent e;
        while (net.poll(e)) {
            switch (e.type) {
            case S_WELCOME: { Reader r(e.data); r.u32(); uint64_t tok = r.u64(), srv = r.u64(); bool resumed = r.u8() != 0; net.setToken(tok); offsetMs = (double)srv - (double)nowMs(); minRtt = 1e9; welcomed = true; serverFailed = false;
                if (!resumed && inRoom) { inRoom = false; view.live = false; toast("Session expired"); hm = HM_IDLE; if (screen == SC_RACE) goTo(SC_HOME); } else if (pending != PA_NONE) runPending(); break; }
            case S_STATS: { Reader r(e.data); onlineN = r.u16(); racingN = r.u16(); break; }
            case S_ROOM: { RoomInfo ri; if (!decodeRoom(e.data, ri)) break; bool wasIn = inRoom; room = ri; inRoom = true;
                if (!wasIn) { hm = ri.isPublic ? HM_MATCH : HM_ROOM; matchT = 0; for (int i = 0; i < 4; i++) { prevOcc[i] = ri.slot[i].occupied; slotAnim[i] = (i == ri.mySlot) ? 0.f : 1.f; } audio.play(SFX_HORN); sendCar(); if (!ri.isPublic && ri.host == ri.mySlot) { Writer w; w.u8(1); net.send(C_READY, w); } }
                else for (int i = 0; i < 4; i++) { if (ri.slot[i].occupied && !prevOcc[i]) { slotAnim[i] = 0; audio.play(SFX_HORN); } prevOcc[i] = ri.slot[i].occupied; }
                break; }
            case S_ERROR: { Reader r(e.data); toast(r.str()); if (hm == HM_CONNECT && !inRoom) hm = (pending == PA_JOIN || !joinCode.empty()) ? HM_JOIN : HM_IDLE; break; }
            case S_KICK: { Reader r(e.data); toast(r.str()); break; }
            case S_RACE_START: { RaceStartInfo si; if (decodeRaceStart(e.data, si)) beginOnlineRace(si); break; }
            case S_RACE_STATE: { RaceStateInfo st; if (decodeRaceState(e.data, st) && view.live && !view.practice) { for (int i = 0; i < st.n; i++) applySnap(st.s[i]); view.forcedEndMs = st.forcedEndMs; } break; }
            case S_RACE_END: { RaceEndInfo re; if (decodeRaceEnd(e.data, re) && view.live && !view.practice) { view.end = re; view.haveEnd = true; view.endedAt = T; for (int i = 0; i < re.n; i++) applySnap(re.s[i]); } break; }
            case S_PONG: { Reader r(e.data); uint64_t ct = r.u64(), srv = r.u64(); double rtt = (double)nowMs() - (double)ct; if (rtt < 0 || rtt > 5000) break; double est = (double)srv + rtt * 0.5 - (double)nowMs();
                if (rtt <= minRtt + 20) offsetMs = haveOffset ? offsetMs * 0.8 + est * 0.2 : est; haveOffset = true; minRtt = std::min(minRtt + 0.5, rtt); rttMs = (int)(rttMs * 0.7 + rtt * 0.3); break; }
            case EV_FAILED: { toast(std::string(e.data.begin(), e.data.end())); inRoom = false; welcomed = false; pending = PA_NONE; view.live = false; if (hm == HM_CONNECT || hm == HM_MATCH || hm == HM_ROOM) hm = HM_IDLE; if (screen == SC_RACE) goTo(SC_HOME); break; }
            case EV_DISCONNECTED: welcomed = false; break;
            default: break;
            }
        }
    }

    // ---------------------------------------------------------------- frame
    void draw() {
        SDL_SetRenderViewport(g.rd, nullptr); g.setCol(C_BG0); SDL_RenderClear(g.rd); uiId = 0; bool overlay = confirmLeave && screen == SC_RACE; uiBlocked = fadingOut || fade > 0.25f || zoomDir != 0 || overlay;
        if (screen == SC_HOME) { if (kEsc && !editName) { if (hm == HM_GARAGE || hm == HM_HISTORY || hm == HM_JOIN || hm == HM_CONNECT) { hm = HM_IDLE; pending = PA_NONE; } } drawHome(); } else drawRace();
        setContent();
        if (overlay) { g.rect(-gOX, 0, gW, 720, rgba(2, 6, 16, 190)); panel(400, 230, 480, 240); g.text(640, 258, "LEAVE THE RACE?", 3.2f, C_WHITE, 1, true); g.text(640, 304, view.practice ? "Your race will be abandoned." : "You will forfeit this race.", 2, C_GREY, 1);
            uiBlocked = false; if (btn("Resume", 430, 370, 190, 56, C_GREEN, true, 2.4f)) confirmLeave = false; if (btn("Leave", 660, 370, 190, 56, C_RED, true, 2.4f)) { confirmLeave = false; if (view.practice) { view.live = false; hm = HM_IDLE; } else leaveRoom(); goTo(SC_HOME); } }
        if (net.state() == NS_RECONNECTING && (inRoom || screen == SC_RACE)) { g.rrect(390, 80, 500, 44, 12, rgba(120, 30, 30, 235)); g.arc(414, 102, 12, 3, T * 6, T * 6 + 4.f, rgba(255, 255, 255, 40), C_WHITE); g.text(640, 93, "Connection lost - reconnecting...", 2.0f, C_WHITE, 1); }
        if (toastT > 0) { float a = std::min(1.f, toastT * 2.f); float w = std::min(1100.f, g.measure(toastText.c_str(), 2.1f) + 60); g.rrect(640 - w / 2, 670, w, 44, 14, rgba(8, 16, 40, (int)(235 * a))); g.rrectLine(640 - w / 2, 670, w, 44, 14, withA(C_CYAN, (int)(200 * a))); g.text(640, 682, toastText.c_str(), 2.1f, withA(C_WHITE, (int)(255 * a)), 1); }
        SDL_SetRenderViewport(g.rd, nullptr); if (fade > 0.001f) g.rect(0, 0, gW, 720, withA(C_BG0, (int)(clampv(fade, 0.f, 1.f) * 255))); g.endFrame(T);
    }

    // ---------------------------------------------------------------- scripted screenshots (verification only, env TM_SHOT_DIR)
    void shot(const char* name) { SDL_Surface* sf = SDL_RenderReadPixels(g.rd, nullptr); if (!sf) return; std::string p = std::string(shotDir) + "/" + name + ".bmp"; SDL_SaveBMP(sf, p.c_str()); SDL_DestroySurface(sf); printf("shot %s\n", name); fflush(stdout); }
    void fakeRoom(bool pub, int n) { room = RoomInfo(); room.code = "4821"; room.mySlot = 0; room.host = 0; room.isPublic = pub; room.limit = 90; for (int i = 0; i < n; i++) { room.slot[i].occupied = true; room.slot[i].name = i == 0 ? cfg.name : i == 1 ? "Aarav" : i == 2 ? "Neha" : "Kabir"; room.slot[i].car = (i * 7 + 3) % NCARS; room.slot[i].color = (uint8_t)((i * 3) | (i << 4)); room.slot[i].ready = i != 1; room.slot[i].connected = true; slotAnim[i] = 1; } inRoom = true; }
    void shotStep() {
        if (fadingOut || fade > 0.02f || zoomDir != 0) return; if (shotWait > 0) { shotWait--; return; }
        switch (shotStage) {
        case 0: if (gArt.done()) { shotWait = 3; shotStage = 1; } break;
        case 1: shot("home"); cfg.car = 21; cfg.color = 5; cfg.nitro = 6; hm = HM_GARAGE; shotStage = 2; shotWait = 3; break;
        case 2: shot("garage"); for (int i = 0; i < 6; i++) { HistEntry e; e.place = 1 + i % 4; e.players = 4; e.car = i * 5; e.wpm = 40.f + i * 7; e.acc = 90.f + i; e.online = i % 2; e.ts = 1764000000LL + i * 5000; cfg.hist.push_back(e); } cfg.racesDone = 6; cfg.wins = 2; hm = HM_HISTORY; shotStage = 3; shotWait = 3; break;
        case 3: shot("history"); fakeRoom(false, 3); hm = HM_ROOM; shotStage = 4; shotWait = 3; break;
        case 4: shot("room"); fakeRoom(true, 2); hm = HM_MATCH; shotStage = 5; shotWait = 3; break;
        case 5: shot("match"); inRoom = false; hm = HM_JOIN; joinCode = "48"; shotStage = 6; shotWait = 3; break;
        case 6: shot("join"); hm = HM_IDLE; cfg.car = 16; beginPractice(false); shotStage = 7; shotWait = 2; break;
        case 7: if (screen == SC_RACE && view.idx > 90) { shot("race_mid"); shotStage = 8; } break;
        case 8: if (screen == SC_RACE && view.r[0].flame > 0.8f) { shot("race_nitro"); shotStage = 9; } else if (screen == SC_HOME) shotStage = 9; break;
        case 9: if (screen == SC_HOME && hm == HM_RESULT && resAlpha > 0.95f) { shot("results"); running = false; } break;
        }
    }
};

#endif  // client-only section
#ifndef TM_SERVER_ONLY
// ============================================================================
//  8. HEADLESS SELF TEST  (real TCP server + real NetworkClient instances)
// ============================================================================
static int runSelfTest() {
    netInit();
    const uint16_t port = 17891;
    NetworkServer srv;
    if (!srv.start(port)) { printf("FAIL: cannot bind port %u\n", port); return 1; }
    std::atomic<bool> stop{false};
    std::thread st([&] { srv.run(stop); });
    struct TC {
        NetworkClient c; RoomInfo room; RaceStartInfo rs; RaceStateInfo state; RaceEndInfo end; std::string err; uint64_t token = 0;
        bool welcome = false, resumed = false, haveRoom = false, haveStart = false, haveState = false, haveEnd = false; uint32_t seq = 0;
    };
    std::vector<std::unique_ptr<TC>> tc; for (int i = 0; i < 5; i++) tc.push_back(std::make_unique<TC>());
    int fails = 0;
    auto check = [&](bool ok, const char* what) { printf("  [%s] %s\n", ok ? "PASS" : "FAIL", what); if (!ok) fails++; };
    auto pump = [&]() {
        for (auto& t : tc) {
            NetEvent e;
            while (t->c.poll(e)) {
                if (e.type == S_WELCOME) { Reader r(e.data); r.u32(); t->token = r.u64(); r.u64(); t->resumed = r.u8(); t->c.setToken(t->token); t->welcome = true; }
                else if (e.type == S_ROOM) t->haveRoom = decodeRoom(e.data, t->room);
                else if (e.type == S_ERROR) { Reader r(e.data); t->err = r.str(); }
                else if (e.type == S_RACE_START) t->haveStart = decodeRaceStart(e.data, t->rs);
                else if (e.type == S_RACE_STATE) t->haveState = decodeRaceState(e.data, t->state);
                else if (e.type == S_RACE_END) t->haveEnd = decodeRaceEnd(e.data, t->end);
            }
        }
    };
    auto waitFor = [&](const std::function<bool()>& f, double sec) { double end = nowSec() + sec; while (nowSec() < end) { pump(); if (f()) return true; std::this_thread::sleep_for(std::chrono::milliseconds(10)); } pump(); return f(); };
    auto typeChar = [&](TC& t, char ch) { Writer w; w.u32(++t.seq); w.u8((uint8_t)ch); t.c.send(C_TYPE, w); };

    printf("Self test: connecting 5 clients\n");
    for (size_t i = 0; i < tc.size(); i++) { tc[i]->c.start(); tc[i]->c.connectTo(i >= 3 ? "ws://127.0.0.1:" + std::to_string(port) : "127.0.0.1", port, "Tester"); }
    check(waitFor([&] { for (auto& t : tc) if (!t->welcome) return false; return true; }, 3), "all clients connected (3 raw TCP + 2 WebSocket) and received WELCOME");

    { Writer w; w.u8(0); w.u8(0); w.u8(0); w.u8(0); tc[0]->c.send(C_CREATE, w); }
    check(waitFor([&] { return tc[0]->haveRoom; }, 2), "room created");
    std::string code = tc[0]->room.code; printf("  room code = %s\n", code.c_str());
    for (int i = 1; i < 5; i++) { Writer w; std::string lc = code; w.str(lc); tc[i]->c.send(C_JOIN, w); waitFor([&] { return tc[i]->haveRoom || !tc[i]->err.empty(); }, 2); }
    check(tc[3]->haveRoom && tc[3]->room.slot[3].occupied, "4 players joined (case-insensitive code)");
    check(!tc[4]->haveRoom && tc[4]->err.find("full") != std::string::npos, "5th player rejected: room full");
    { Writer w; w.u8(1); tc[0]->c.send(C_READY, w); tc[1]->c.send(C_READY, w); }
    waitFor([&] { return tc[2]->room.slot[0].ready && tc[2]->room.slot[1].ready; }, 2);
    tc[1]->err.clear(); tc[1]->c.send(C_START);
    check(waitFor([&] { return !tc[1]->err.empty(); }, 2), "non-host cannot start race");
    tc[0]->c.send(C_START);
    check(waitFor([&] { return tc[0]->haveStart && tc[1]->haveStart && tc[2]->haveStart; }, 2), "RACE_START sent to all members");
    check(tc[0]->rs.mask == 3 && tc[2]->rs.mySlot == 255 && tc[0]->rs.text == tc[1]->rs.text, "2 racers (ready), others spectate; identical paragraph");
    double goAt = tc[0]->rs.startAtMs / 1000.0;
    typeChar(*tc[0], tc[0]->rs.text[0]);  // before GO -> must be ignored
    std::this_thread::sleep_for(std::chrono::milliseconds(300)); pump();
    check(!tc[0]->haveState || tc[0]->state.s[0].progress == 0, "typing before GO is ignored");
    while (nowSec() < goAt + 0.1) { pump(); std::this_thread::sleep_for(std::chrono::milliseconds(10)); }

    // cheater B: duplicate seq, wrong chars, burst flood
    std::string text = tc[0]->rs.text;
    { Writer w; w.u32(5); w.u8((uint8_t)text[0]); tc[1]->seq = 5; tc[1]->c.send(C_TYPE, w); tc[1]->c.send(C_TYPE, w); tc[1]->c.send(C_TYPE, w); }
    for (int i = 0; i < 150; i++) typeChar(*tc[1], text[std::min<size_t>(i + 1, text.size() - 1)]);  // 150-char instant burst
    std::this_thread::sleep_for(std::chrono::milliseconds(400)); pump();
    RacerSnap b = tc[0]->state.s[1];
    printf("  cheater B: progress=%u correct=%u wrong=%u\n", b.progress, b.correct, b.wrong);
    check(b.progress <= 20, "burst/duplicate flood limited by server (no instant progress)");

    // honest A types at ~16 chars/s with a few typos
    auto t0 = nowSec(); size_t i = 0; int typos = 0;
    while (i < text.size() && nowSec() - t0 < 45) {
        if (i == 10 || i == 30) { typeChar(*tc[0], '~'); typos++; }
        typeChar(*tc[0], text[i++]);
        std::this_thread::sleep_for(std::chrono::milliseconds(60)); pump();
    }
    check(waitFor([&] { return tc[0]->haveEnd && tc[2]->haveEnd; }, 12), "race ended; RACE_END delivered to racers and spectators");
    if (tc[0]->haveEnd) {
        const RaceEndInfo& e = tc[0]->end; RacerSnap a{}; for (int k = 0; k < e.n; k++) if (e.s[k].slot == 0) a = e.s[k];
        printf("  A: progress=%u/%zu wpm=%.1f acc=%.1f%% wrong=%u finishMs=%u winner=%u\n", a.progress, text.size(), a.wpm10 / 10.f, a.acc10 / 10.f, a.wrong, a.finishMs, e.winner);
        check(e.winner == 0 && (a.flags & SF_FINISHED) && a.progress == text.size(), "server-validated winner = honest player");
        check(a.wrong == (uint16_t)typos, "server counted exactly the wrong characters sent");
        check(a.wpm10 > 0 && a.place == 1, "server computed WPM and place");
    }
    // oversized frame => dropped
    { sock_t s = socket(AF_INET, SOCK_STREAM, 0); sockaddr_in a; memset(&a, 0, sizeof(a)); a.sin_family = AF_INET; a.sin_port = htons(port); inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
      bool ok = connect(s, (sockaddr*)&a, sizeof(a)) == 0; uint8_t junk[4] = {0xFF, 0xFF, 1, 2}; if (ok) ::send(s, (const char*)junk, 4, MSG_NOSIGNAL);
      char buf[8]; int n = 1; double end = nowSec() + 2; while (nowSec() < end) { n = (int)recv(s, buf, sizeof(buf), 0); if (n <= 0) break; }
      check(ok && n <= 0, "oversized/invalid frame -> connection closed"); sockClose(s); }
    // lobby returns after results; reconnect flow
    check(waitFor([&] { return tc[0]->haveRoom && tc[0]->room.state == 0; }, 9), "room returns to lobby after results");
    tc[3]->haveRoom = false; tc[3]->c.disconnect(); std::this_thread::sleep_for(std::chrono::milliseconds(300));
    waitFor([&] { return tc[0]->room.slot[3].occupied == false; }, 2); pump();
    check(!tc[0]->room.slot[3].occupied, "player leaving the lobby frees the slot");

    { // plain HTTP health check (what Render calls)
      sock_t hs = socket(AF_INET, SOCK_STREAM, 0); sockaddr_in a; memset(&a, 0, sizeof(a)); a.sin_family = AF_INET; a.sin_port = htons(port); inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
      bool ok = connect(hs, (sockaddr*)&a, sizeof(a)) == 0; const char* rq = "GET /health HTTP/1.1\r\nHost: x\r\n\r\n"; if (ok) ::send(hs, rq, (int)strlen(rq), MSG_NOSIGNAL);
      std::string resp; char b[256]; double end = nowSec() + 2; while (nowSec() < end) { int n = (int)recv(hs, b, sizeof(b), 0); if (n <= 0) break; resp.append(b, n); }
      check(resp.find("200 OK") != std::string::npos, "HTTP GET /health returns 200 (Render health check)"); sockClose(hs); }
    printf("Self test %s (%d failure%s)\n", fails ? "FAILED" : "PASSED", fails, fails == 1 ? "" : "s");
    for (auto& t : tc) t->c.stop();
    stop = true; st.join(); netShutdown();
    return fails ? 1 : 0;
}

#endif  // TM_SERVER_ONLY


// ============================================================================
//  12. ENTRY POINT
// ============================================================================
static std::atomic<bool> gStop{false};
static void onSignal(int) { gStop = true; }
static int runServer(int port) {
    netInit(); signal(SIGINT, onSignal); signal(SIGTERM, onSignal);
    NetworkServer srv; if (!srv.start((uint16_t)port)) { fprintf(stderr, "Cannot listen on port %d\n", port); return 1; }
    srv.run(gStop); netShutdown(); return 0;
}
int main(int argc, char** argv) {
    bool server = false, selftest = false, hostGiven = false, portGiven = false; int port = DEFAULT_PORT; std::string host, font;
    if (const char* p = getenv("PORT")) { port = atoi(p); portGiven = true; }
    for (int i = 1; i < argc; i++) {
        std::string a = argv[i];
        if (a == "--server") server = true; else if (a == "--client") server = false; else if (a == "--selftest") selftest = true;
        else if (a == "--port" && i + 1 < argc) { port = atoi(argv[++i]); portGiven = true; } else if (a == "--host" && i + 1 < argc) { host = argv[++i]; hostGiven = true; }
        else if (a == "--font" && i + 1 < argc) font = argv[++i];
        else if (a == "--help") { printf("Typing Maestro Race\n  --server [--port N]       dedicated server (PORT env honored; speaks raw TCP, WebSocket and HTTP health on one port)\n  --client [--host H] [--port N] [--font file.ttf]\n    host: IP, host:port, ws://host[:port], wss://name.onrender.com\n  --selftest                headless protocol test\n"); return 0; }
    }
    if (port < 1 || port > 65535) port = DEFAULT_PORT;
#ifdef TM_SERVER_ONLY
    (void)selftest; (void)host; (void)hostGiven; (void)portGiven; (void)font; (void)server; return runServer(port);
#else
    if (selftest) return runSelfTest();
    if (server) return runServer(port);
    netInit(); Game game; int rc = game.run(host, hostGiven, font); netShutdown(); return rc;
#endif
}
