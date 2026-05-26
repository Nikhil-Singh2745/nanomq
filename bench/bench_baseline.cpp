// bench/bench_baseline.cpp
//
// NanoMQ Baseline Comparison Benchmark
//
// Measures one-way latency for four IPC mechanisms side-by-side:
//   1. NanoMQ SPSC (shared memory ring buffer)
//   2. Unix pipe
//   3. Unix domain socket (AF_UNIX, SOCK_DGRAM)
//   4. TCP loopback (127.0.0.1)
//
// All use the same message size (64 bytes) and sample count.
// Producer and consumer run in separate pthreads.
// Latency is measured producer-side: send timestamp embedded in message,
// consumer echoes back, producer computes RTT/2 as one-way proxy.
//
// Note: For NanoMQ the one-way latency is measured directly via rdtsc
// (producer writes tsc, consumer reads at arrival). For pipe/socket/TCP
// the round-trip is used because the clock on both sides is the same process,
// so RTT/2 is a valid one-way proxy.
//
// Usage: ./bench_baseline [samples]
//   samples : default 1,000,000

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <sched.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

#include "nanomq/common.hpp"
#include "nanomq/spsc_queue.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <numeric>
#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// Benchmark message: 64 bytes to match SPSC slot size
// ---------------------------------------------------------------------------
struct alignas(8) BaseMsg {
    uint64_t tsc;
    uint64_t seq;
    char     _pad[48];
};
static_assert(sizeof(BaseMsg) == 64);

static constexpr int WARMUP = 10'000;

// ---------------------------------------------------------------------------
// CPU pinning
// ---------------------------------------------------------------------------
static void pin_cpu(int cpu) {
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    ::pthread_setaffinity_np(::pthread_self(), sizeof(set), &set);
}

// ---------------------------------------------------------------------------
// Statistics helper
// ---------------------------------------------------------------------------
struct Stats {
    double min, p50, p99, p999, max, mean;
};

static Stats compute_stats(std::vector<double>& v) {
    std::sort(v.begin(), v.end());
    const std::size_t n = v.size();
    const double mean = std::accumulate(v.begin(), v.end(), 0.0) / static_cast<double>(n);
    auto pct = [&](double p) {
        return v[static_cast<std::size_t>(p * static_cast<double>(n - 1))];
    };
    return {v.front(), pct(0.50), pct(0.99), pct(0.999), v.back(), mean};
}

static void print_stats(const char* name, const Stats& s) {
    std::printf("  %-20s %10.1f  %10.1f  %10.1f  %10.1f  %10.1f\n",
                name, s.min, s.p50, s.p99, s.p999, s.mean);
}

// ===========================================================================
// 1. NanoMQ SPSC
// ===========================================================================

using SpscQ = nanomq::SpscQueue<BaseMsg, 131072>;

struct SpscState {
    SpscQ*            q;
    double            ns_per_tick;
    uint64_t          samples;
    double*           latencies;
    std::atomic<int>  ready{0};
};

static void* spsc_producer(void* raw) {
    auto* s = static_cast<SpscState*>(raw);
    pin_cpu(2);
    s->ready.fetch_add(1, std::memory_order_release);
    while (s->ready.load(std::memory_order_acquire) < 2) {}

    const uint64_t total = s->samples + WARMUP;
    for (uint64_t i = 0; i < total; ++i) {
        BaseMsg m{nanomq::rdtsc(), i, {}};
        while (!s->q->try_push(m)) {}
    }
    BaseMsg sentinel{0, UINT64_MAX, {}};
    while (!s->q->try_push(sentinel)) {}
    return nullptr;
}

static void* spsc_consumer(void* raw) {
    auto* s = static_cast<SpscState*>(raw);
    pin_cpu(4);
    s->ready.fetch_add(1, std::memory_order_release);
    while (s->ready.load(std::memory_order_acquire) < 2) {}

    uint64_t count = 0;
    BaseMsg m{};
    while (true) {
        while (!s->q->try_pop(m)) {}
        if (m.seq == UINT64_MAX) break;
        if (count >= static_cast<uint64_t>(WARMUP)) {
            s->latencies[count - WARMUP] =
                static_cast<double>(nanomq::rdtsc() - m.tsc) * s->ns_per_tick;
        }
        ++count;
    }
    return nullptr;
}

static Stats bench_nanomq(uint64_t samples, double ns_per_tick) {
    alignas(64) static char buf[sizeof(SpscQ)];
    SpscQ* q = reinterpret_cast<SpscQ*>(buf);
    SpscQ::init(q);

    std::vector<double> lat(samples);
    SpscState s{q, ns_per_tick, samples, lat.data()};

    pthread_t pt, ct;
    ::pthread_create(&ct, nullptr, spsc_consumer, &s);
    ::pthread_create(&pt, nullptr, spsc_producer, &s);
    ::pthread_join(pt, nullptr);
    ::pthread_join(ct, nullptr);
    return compute_stats(lat);
}

// ===========================================================================
// 2. Unix pipe
// ===========================================================================

struct PipeState {
    int               rd, wr;     // consumer reads rd, producer writes wr
    int               rd2, wr2;   // echo path: consumer writes wr2, producer reads rd2
    uint64_t          samples;
    double            ns_per_tick;
    double*           latencies;
    std::atomic<int>  ready{0};
};

static void* pipe_producer(void* raw) {
    auto* s = static_cast<PipeState*>(raw);
    pin_cpu(2);
    s->ready.fetch_add(1, std::memory_order_release);
    while (s->ready.load(std::memory_order_acquire) < 2) {}

    const uint64_t total = s->samples + WARMUP;
    BaseMsg m{};
    for (uint64_t i = 0; i < total; ++i) {
        m.tsc = nanomq::rdtsc();
        m.seq = i;
        ::write(s->wr, &m, sizeof(m));
        // Wait for echo
        BaseMsg echo{};
        ::read(s->rd2, &echo, sizeof(echo));
        if (i >= static_cast<uint64_t>(WARMUP)) {
            const uint64_t rtt_tsc = nanomq::rdtsc() - m.tsc;
            s->latencies[i - WARMUP] =
                static_cast<double>(rtt_tsc) * s->ns_per_tick * 0.5;
        }
    }
    // Sentinel
    m.seq = UINT64_MAX;
    ::write(s->wr, &m, sizeof(m));
    return nullptr;
}

static void* pipe_consumer(void* raw) {
    auto* s = static_cast<PipeState*>(raw);
    pin_cpu(4);
    s->ready.fetch_add(1, std::memory_order_release);
    while (s->ready.load(std::memory_order_acquire) < 2) {}

    BaseMsg m{};
    while (true) {
        if (::read(s->rd, &m, sizeof(m)) != static_cast<ssize_t>(sizeof(m))) break;
        if (m.seq == UINT64_MAX) break;
        ::write(s->wr2, &m, sizeof(m));  // echo back
    }
    return nullptr;
}

static Stats bench_pipe(uint64_t samples, double ns_per_tick) {
    int p1[2], p2[2];
    if (::pipe(p1) < 0 || ::pipe(p2) < 0) {
        std::fprintf(stderr, "[bench] pipe() failed: %s\n", ::strerror(errno));
        return {};
    }

    std::vector<double> lat(samples);
    PipeState s{p1[0], p1[1], p2[0], p2[1], samples, ns_per_tick, lat.data()};

    pthread_t pt, ct;
    ::pthread_create(&ct, nullptr, pipe_consumer, &s);
    ::pthread_create(&pt, nullptr, pipe_producer, &s);
    ::pthread_join(pt, nullptr);
    ::pthread_join(ct, nullptr);

    ::close(p1[0]); ::close(p1[1]);
    ::close(p2[0]); ::close(p2[1]);
    return compute_stats(lat);
}

// ===========================================================================
// 3. Unix domain socket (AF_UNIX, SOCK_DGRAM)
// ===========================================================================

struct UdsState {
    int               prod_sock, cons_sock;  // producer sends, consumer receives
    int               echo_sock, echo_dst;   // consumer sends echo, producer receives
    uint64_t          samples;
    double            ns_per_tick;
    double*           latencies;
    std::atomic<int>  ready{0};
    // temp paths
    char              prod_path[64];
    char              cons_path[64];
    char              echo_path[64];
    char              echo_dst_path[64];
};

static void* uds_producer(void* raw) {
    auto* s = static_cast<UdsState*>(raw);
    pin_cpu(2);
    s->ready.fetch_add(1, std::memory_order_release);
    while (s->ready.load(std::memory_order_acquire) < 2) {}

    const uint64_t total = s->samples + WARMUP;
    struct sockaddr_un dst{};
    dst.sun_family = AF_UNIX;
    std::strncpy(dst.sun_path, s->cons_path, sizeof(dst.sun_path) - 1);

    struct sockaddr_un echo_src{};
    echo_src.sun_family = AF_UNIX;
    std::strncpy(echo_src.sun_path, s->echo_dst_path, sizeof(echo_src.sun_path) - 1);

    BaseMsg m{};
    for (uint64_t i = 0; i < total; ++i) {
        m.tsc = nanomq::rdtsc();
        m.seq = i;
        ::sendto(s->prod_sock, &m, sizeof(m), 0,
                 reinterpret_cast<sockaddr*>(&dst), sizeof(dst));
        BaseMsg echo{};
        socklen_t len = sizeof(echo_src);
        ::recvfrom(s->echo_sock, &echo, sizeof(echo), 0,
                   reinterpret_cast<sockaddr*>(&echo_src), &len);
        if (i >= static_cast<uint64_t>(WARMUP)) {
            const uint64_t rtt = nanomq::rdtsc() - m.tsc;
            s->latencies[i - WARMUP] =
                static_cast<double>(rtt) * s->ns_per_tick * 0.5;
        }
    }
    m.seq = UINT64_MAX;
    ::sendto(s->prod_sock, &m, sizeof(m), 0,
             reinterpret_cast<sockaddr*>(&dst), sizeof(dst));
    return nullptr;
}

static void* uds_consumer(void* raw) {
    auto* s = static_cast<UdsState*>(raw);
    pin_cpu(4);
    s->ready.fetch_add(1, std::memory_order_release);
    while (s->ready.load(std::memory_order_acquire) < 2) {}

    struct sockaddr_un echo_dst{};
    echo_dst.sun_family = AF_UNIX;
    std::strncpy(echo_dst.sun_path, s->echo_path, sizeof(echo_dst.sun_path) - 1);

    BaseMsg m{};
    struct sockaddr_un src{};
    socklen_t len = sizeof(src);
    while (true) {
        ::recvfrom(s->cons_sock, &m, sizeof(m), 0,
                   reinterpret_cast<sockaddr*>(&src), &len);
        if (m.seq == UINT64_MAX) break;
        ::sendto(s->echo_sock, &m, sizeof(m), 0,
                 reinterpret_cast<sockaddr*>(&echo_dst), sizeof(echo_dst));
    }
    return nullptr;
}

static Stats bench_uds(uint64_t samples, double ns_per_tick) {

    UdsState s{};
    s.samples     = samples;
    s.ns_per_tick = ns_per_tick;

    std::snprintf(s.prod_path,     sizeof(s.prod_path),     "/tmp/nanomq_prod_%d",     (int)::getpid());
    std::snprintf(s.cons_path,     sizeof(s.cons_path),     "/tmp/nanomq_cons_%d",     (int)::getpid());
    std::snprintf(s.echo_path,     sizeof(s.echo_path),     "/tmp/nanomq_echo_%d",     (int)::getpid());
    std::snprintf(s.echo_dst_path, sizeof(s.echo_dst_path), "/tmp/nanomq_echodst_%d",  (int)::getpid());

    // Cleanup any stale sockets
    ::unlink(s.prod_path);
    ::unlink(s.cons_path);
    ::unlink(s.echo_path);
    ::unlink(s.echo_dst_path);

    s.prod_sock = ::socket(AF_UNIX, SOCK_DGRAM, 0);
    s.cons_sock = ::socket(AF_UNIX, SOCK_DGRAM, 0);
    s.echo_sock = ::socket(AF_UNIX, SOCK_DGRAM, 0);
    s.echo_dst  = ::socket(AF_UNIX, SOCK_DGRAM, 0);


    auto bind_sock = [](int fd, const char* path) {
        struct sockaddr_un a{};
        a.sun_family = AF_UNIX;
        std::strncpy(a.sun_path, path, sizeof(a.sun_path) - 1);
        return ::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a));
    };

    bind_sock(s.prod_sock,  s.prod_path);
    bind_sock(s.cons_sock,  s.cons_path);
    bind_sock(s.echo_sock,  s.echo_path);
    bind_sock(s.echo_dst,   s.echo_dst_path);

    std::vector<double> lat(samples);
    s.latencies = lat.data();

    pthread_t pt, ct;
    ::pthread_create(&ct, nullptr, uds_consumer, &s);
    ::pthread_create(&pt, nullptr, uds_producer, &s);
    ::pthread_join(pt, nullptr);
    ::pthread_join(ct, nullptr);

    ::close(s.prod_sock); ::close(s.cons_sock);
    ::close(s.echo_sock); ::close(s.echo_dst);
    ::unlink(s.prod_path); ::unlink(s.cons_path);
    ::unlink(s.echo_path); ::unlink(s.echo_dst_path);

    return compute_stats(lat);
}

// ===========================================================================
// 4. TCP loopback
// ===========================================================================

struct TcpState {
    uint16_t          port;
    uint64_t          samples;
    double            ns_per_tick;
    double*           latencies;
    std::atomic<int>  server_ready{0};
};

static void* tcp_server(void* raw) {
    auto* s = static_cast<TcpState*>(raw);
    pin_cpu(4);

    int lfd = ::socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    ::setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    ::setsockopt(lfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = htons(s->port);
    ::bind(lfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    ::listen(lfd, 1);

    s->server_ready.store(1, std::memory_order_release);

    struct sockaddr_in cli{};
    socklen_t clen = sizeof(cli);
    int cfd = ::accept(lfd, reinterpret_cast<sockaddr*>(&cli), &clen);
    ::setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    ::close(lfd);

    // Echo server
    BaseMsg m{};
    while (true) {
        ssize_t n = ::recv(cfd, &m, sizeof(m), MSG_WAITALL);
        if (n != static_cast<ssize_t>(sizeof(m))) break;
        if (m.seq == UINT64_MAX) break;
        ::send(cfd, &m, sizeof(m), 0);
    }
    ::close(cfd);
    return nullptr;
}

static void* tcp_client(void* raw) {
    auto* s = static_cast<TcpState*>(raw);
    pin_cpu(2);

    // Wait for server
    while (s->server_ready.load(std::memory_order_acquire) == 0) {}

    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = htons(s->port);
    ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));

    const uint64_t total = s->samples + WARMUP;
    BaseMsg m{};
    for (uint64_t i = 0; i < total; ++i) {
        m.tsc = nanomq::rdtsc();
        m.seq = i;
        ::send(fd, &m, sizeof(m), 0);
        BaseMsg echo{};
        ::recv(fd, &echo, sizeof(echo), MSG_WAITALL);
        if (i >= static_cast<uint64_t>(WARMUP)) {
            const uint64_t rtt = nanomq::rdtsc() - m.tsc;
            s->latencies[i - WARMUP] =
                static_cast<double>(rtt) * s->ns_per_tick * 0.5;
        }
    }
    // Sentinel
    m.seq = UINT64_MAX;
    ::send(fd, &m, sizeof(m), 0);
    ::close(fd);
    return nullptr;
}

static Stats bench_tcp(uint64_t samples, double ns_per_tick) {
    std::vector<double> lat(samples);
    TcpState s{};
    s.port        = 19876;
    s.samples     = samples;
    s.ns_per_tick = ns_per_tick;
    s.latencies   = lat.data();

    pthread_t st, ct;
    ::pthread_create(&st, nullptr, tcp_server, &s);
    ::pthread_create(&ct, nullptr, tcp_client, &s);
    ::pthread_join(ct, nullptr);
    ::pthread_join(st, nullptr);
    return compute_stats(lat);
}

// ===========================================================================
// main
// ===========================================================================
int main(int argc, char* argv[]) {
    const uint64_t samples = (argc > 1) ? std::stoull(argv[1]) : 1'000'000ULL;

    std::printf("╔══════════════════════════════════════════════════════╗\n");
    std::printf("║          NanoMQ Baseline Comparison Benchmark        ║\n");
    std::printf("╚══════════════════════════════════════════════════════╝\n");
    std::printf("  Message size : 64 bytes\n");
    std::printf("  Samples      : %llu (+ %d warmup)\n",
                static_cast<unsigned long long>(samples), WARMUP);
    std::printf("  Measurement  : NanoMQ = one-way rdtsc; others = RTT/2\n\n");

    std::printf("  Calibrating TSC... ");
    std::fflush(stdout);
    const double ns_per_tick = nanomq::tsc_ns_per_tick();
    std::printf("%.4f ns/tick\n\n", ns_per_tick);

    std::printf("  %-20s %10s  %10s  %10s  %10s  %10s\n",
                "Method", "min(ns)", "p50(ns)", "p99(ns)", "p99.9(ns)", "mean(ns)");
    std::printf("  %s\n",
        std::string(20 + 5 * 12, '-').c_str());

    std::printf("  Running NanoMQ SPSC...           "); std::fflush(stdout);
    auto s1 = bench_nanomq(samples, ns_per_tick);
    std::printf("done\n");
    print_stats("NanoMQ SPSC", s1);

    std::printf("  Running Unix pipe...              "); std::fflush(stdout);
    auto s2 = bench_pipe(samples, ns_per_tick);
    std::printf("done\n");
    print_stats("Unix pipe", s2);

    std::printf("  Running Unix domain socket...     "); std::fflush(stdout);
    auto s3 = bench_uds(samples, ns_per_tick);
    std::printf("done\n");
    print_stats("Unix domain socket", s3);

    std::printf("  Running TCP loopback...           "); std::fflush(stdout);
    auto s4 = bench_tcp(samples, ns_per_tick);
    std::printf("done\n");
    print_stats("TCP loopback", s4);

    std::printf("\n  Note: pipe/socket/TCP use RTT/2 as one-way latency proxy.\n");
    std::printf("  NanoMQ uses direct rdtsc delta (no echo required).\n\n");

    return 0;
}
