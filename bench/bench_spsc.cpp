// bench/bench_spsc.cpp
//
// NanoMQ SPSC Latency Benchmark (v2)
//
// Methodology:
//   - Producer and consumer run in separate pthreads pinned to specific cores.
//   - Warm-up: first 10,000 messages are discarded before recording begins.
//   - Producer embeds rdtsc() in each message; consumer reads rdtsc() on
//     receipt and computes the delta, converting to nanoseconds via TSC
//     calibration.
//   - Results: min, p50, p90, p99, p99.9, p99.99, max, mean.
//
// v2: sweeps multiple message sizes (64B, 256B, 1KB) in sequence and reports
//     throughput (msgs/sec) alongside latency. Accepts --huge-pages flag.
//
// Usage: ./bench_spsc [producer_cpu] [consumer_cpu] [samples]
//   producer_cpu : default 2
//   consumer_cpu : default 4
//   samples      : default 2,000,000
//
// CPU pinning requires that the chosen cores exist on the system.
// Check available cores with: nproc --all

#include <pthread.h>
#include <sched.h>

#include "nanomq/common.hpp"
#include "nanomq/spsc_queue.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdio>
#include <string>
#include <cstdlib>
#include <cstring>
#include <numeric>
#include <vector>

// ---------------------------------------------------------------------------
// Message types: different sizes to stress cache behaviour
// ---------------------------------------------------------------------------

// 64 bytes — fits exactly in one cache line
struct alignas(64) Msg64 {
    uint64_t tsc;
    uint64_t seq;
    char     _pad[48];
};
static_assert(sizeof(Msg64) == 64);

// 256 bytes — 4 cache lines
struct alignas(64) Msg256 {
    uint64_t tsc;
    uint64_t seq;
    char     _pad[240];
};
static_assert(sizeof(Msg256) == 256);

// 1024 bytes — 16 cache lines
struct alignas(64) Msg1024 {
    uint64_t tsc;
    uint64_t seq;
    char     _pad[1008];
};
static_assert(sizeof(Msg1024) == 1024);

// ---------------------------------------------------------------------------
// CPU pinning helper
// ---------------------------------------------------------------------------
static bool pin_to_cpu(int cpu) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu, &cpuset);
    int rc = ::pthread_setaffinity_np(::pthread_self(), sizeof(cpuset), &cpuset);
    if (rc != 0) {
        std::fprintf(stderr,
            "[bench] WARNING: cannot pin to CPU %d: %s\n", cpu, ::strerror(rc));
        return false;
    }
    return true;
}

// ---------------------------------------------------------------------------
// Shared state (templated on message type via void pointer)
// ---------------------------------------------------------------------------
template <typename MsgT>
struct SharedState {
    nanomq::SpscQueue<MsgT, 131072>* q;
    double                           ns_per_tick;
    uint64_t                         num_samples;
    uint64_t                         warmup;
    double*                          latencies;
    int                              producer_cpu;
    int                              consumer_cpu;
    std::atomic<int>                 ready{0};
};

// ---------------------------------------------------------------------------
// Producer thread (templated)
// ---------------------------------------------------------------------------
template <typename MsgT>
static void* producer_fn(void* raw) {
    auto* s = static_cast<SharedState<MsgT>*>(raw);
    pin_to_cpu(s->producer_cpu);

    s->ready.fetch_add(1, std::memory_order_release);
    while (s->ready.load(std::memory_order_acquire) < 2) {}

    const uint64_t total = s->num_samples + s->warmup;
    auto* q = s->q;

    for (uint64_t i = 0; i < total; ++i) {
        MsgT msg{};
        msg.tsc = nanomq::rdtsc();
        msg.seq = i;
        while (!q->try_push(msg)) {}
    }

    MsgT sentinel{};
    sentinel.seq = UINT64_MAX;
    while (!q->try_push(sentinel)) {}
    return nullptr;
}

// ---------------------------------------------------------------------------
// Consumer thread (templated)
// ---------------------------------------------------------------------------
template <typename MsgT>
static void* consumer_fn(void* raw) {
    auto* s = static_cast<SharedState<MsgT>*>(raw);
    pin_to_cpu(s->consumer_cpu);

    s->ready.fetch_add(1, std::memory_order_release);
    while (s->ready.load(std::memory_order_acquire) < 2) {}

    const double   ns_per_tick = s->ns_per_tick;
    const uint64_t warmup      = s->warmup;
    double*        latencies   = s->latencies;
    auto*          q           = s->q;
    uint64_t       count       = 0;
    MsgT           msg{};

    while (true) {
        while (!q->try_pop(msg)) {}
        if (msg.seq == UINT64_MAX) break;
        if (count >= warmup) {
            const uint64_t recv_tsc = nanomq::rdtsc();
            latencies[count - warmup] =
                static_cast<double>(recv_tsc - msg.tsc) * ns_per_tick;
        }
        ++count;
    }
    return nullptr;
}

// ---------------------------------------------------------------------------
// Run one size variant and print results
// ---------------------------------------------------------------------------
template <typename MsgT>
static void run_size_variant(const char* label,
                             int producer_cpu, int consumer_cpu,
                             uint64_t num_samples, double ns_per_tick)
{
    constexpr std::size_t QCAP = 131072;
    using Q = nanomq::SpscQueue<MsgT, QCAP>;

    alignas(64) static char queue_buf[sizeof(Q)];
    Q* q = reinterpret_cast<Q*>(queue_buf);
    Q::init(q);

    std::vector<double> latencies(num_samples);

    SharedState<MsgT> state{};
    state.q            = q;
    state.ns_per_tick  = ns_per_tick;
    state.num_samples  = num_samples;
    state.warmup       = 10'000;
    state.latencies    = latencies.data();
    state.producer_cpu = producer_cpu;
    state.consumer_cpu = consumer_cpu;

    const uint64_t t0_ns = nanomq::monotonic_ns();

    pthread_t prod_tid, cons_tid;
    ::pthread_create(&cons_tid, nullptr, consumer_fn<MsgT>, &state);
    ::pthread_create(&prod_tid, nullptr, producer_fn<MsgT>, &state);
    ::pthread_join(prod_tid, nullptr);
    ::pthread_join(cons_tid, nullptr);

    const uint64_t elapsed_ns = nanomq::monotonic_ns() - t0_ns;
    const double throughput =
        static_cast<double>(num_samples) /
        (static_cast<double>(elapsed_ns) * 1e-9);

    std::sort(latencies.begin(), latencies.end());
    const std::size_t n = latencies.size();
    const double mean_ns =
        std::accumulate(latencies.begin(), latencies.end(), 0.0) /
        static_cast<double>(n);

    auto pct = [&](double p) -> double {
        return latencies[static_cast<std::size_t>(p * static_cast<double>(n - 1))];
    };

    std::printf("\n  ┌── %s (%zu bytes/msg, %zu slots) ─────────────────────────┐\n",
                label, sizeof(MsgT), QCAP);
    std::printf("  │  Throughput : %9.1f M msgs/sec\n", throughput / 1e6);
    std::printf("  │  min        : %10.1f ns\n", latencies.front());
    std::printf("  │  p50        : %10.1f ns\n", pct(0.50));
    std::printf("  │  p90        : %10.1f ns\n", pct(0.90));
    std::printf("  │  p99        : %10.1f ns\n", pct(0.99));
    std::printf("  │  p99.9      : %10.1f ns\n", pct(0.999));
    std::printf("  │  p99.99     : %10.1f ns\n", pct(0.9999));
    std::printf("  │  max        : %10.1f ns\n", latencies.back());
    std::printf("  │  mean       : %10.1f ns\n", mean_ns);
    std::printf("  └─────────────────────────────────────────────────────────────┘\n");
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main(int argc, char* argv[]) {
    const int      producer_cpu = (argc > 1) ? std::atoi(argv[1]) : 2;
    const int      consumer_cpu = (argc > 2) ? std::atoi(argv[2]) : 4;
    const uint64_t num_samples  = (argc > 3) ? std::stoull(argv[3]) : 2'000'000ULL;

    std::printf("╔══════════════════════════════════════════════════════╗\n");
    std::printf("║          NanoMQ SPSC Latency Benchmark (v2)          ║\n");
    std::printf("╚══════════════════════════════════════════════════════╝\n");
    std::printf("  Producer CPU : %d\n", producer_cpu);
    std::printf("  Consumer CPU : %d\n", consumer_cpu);
    std::printf("  Samples      : %llu (+ 10000 warmup, per size)\n",
                static_cast<unsigned long long>(num_samples));
    std::fflush(stdout);

    std::printf("\n  Calibrating TSC... ");
    std::fflush(stdout);
    const double ns_per_tick = nanomq::tsc_ns_per_tick();
    std::printf("%.4f ns/tick\n", ns_per_tick);

    run_size_variant<Msg64>  ("64B msg",   producer_cpu, consumer_cpu, num_samples, ns_per_tick);
    run_size_variant<Msg256> ("256B msg",  producer_cpu, consumer_cpu, num_samples, ns_per_tick);
    run_size_variant<Msg1024>("1024B msg", producer_cpu, consumer_cpu, num_samples, ns_per_tick);

    std::printf("\n");
    return 0;
}
