// bench/bench_spsc.cpp
//
// NanoMQ SPSC Latency Benchmark
//
// Methodology:
//   - Producer and consumer run in separate pthreads pinned to specific cores.
//   - Warm-up: first 10,000 messages are discarded before recording begins.
//   - Producer embeds rdtsc() in each message; consumer reads rdtsc() on
//     receipt and computes the delta, converting to nanoseconds via TSC
//     calibration.
//   - 10,000,000 samples collected (after warm-up).
//   - Results: min, p50, p90, p99, p99.9, p99.99, max, mean.
//
// Usage: ./bench_spsc [producer_cpu] [consumer_cpu] [samples]
//   producer_cpu : default 2
//   consumer_cpu : default 4
//   samples      : default 10,000,000
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
// Benchmark message: just a TSC timestamp + sequence (64 bytes total)
// ---------------------------------------------------------------------------
struct alignas(64) BenchMsg {
    uint64_t tsc;
    uint64_t seq;
    char     _pad[48];
};
static_assert(sizeof(BenchMsg) == 64, "BenchMsg must be 64 bytes");

// ---------------------------------------------------------------------------
// Queue: 131072 slots (8MB data region + control block)
// ---------------------------------------------------------------------------
using Queue = nanomq::SpscQueue<BenchMsg, 131072>;

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
// Shared state between producer and consumer threads
// ---------------------------------------------------------------------------
struct SharedState {
    Queue*            q;
    double            ns_per_tick;
    uint64_t          num_samples;
    uint64_t          warmup;
    double*           latencies;   // pre-allocated by main
    int               producer_cpu;
    int               consumer_cpu;
    std::atomic<int>  ready{0};    // barrier: both threads increment, spin until 2
};

// ---------------------------------------------------------------------------
// Producer thread
// ---------------------------------------------------------------------------
static void* producer_fn(void* raw) {
    auto* s = static_cast<SharedState*>(raw);
    pin_to_cpu(s->producer_cpu);

    // Barrier: signal ready and wait for consumer
    s->ready.fetch_add(1, std::memory_order_release);
    while (s->ready.load(std::memory_order_acquire) < 2) { /* spin */ }

    const uint64_t total = s->num_samples + s->warmup;
    Queue* q = s->q;

    for (uint64_t i = 0; i < total; ++i) {
        BenchMsg msg{};
        msg.seq = i;
        msg.tsc = nanomq::rdtsc();
        while (!q->try_push(msg)) { /* spin on full */ }
    }

    // Sentinel
    BenchMsg sentinel{};
    sentinel.seq = UINT64_MAX;
    sentinel.tsc = 0;
    while (!q->try_push(sentinel)) {}

    return nullptr;
}

// ---------------------------------------------------------------------------
// Consumer thread
// ---------------------------------------------------------------------------
static void* consumer_fn(void* raw) {
    auto* s = static_cast<SharedState*>(raw);
    pin_to_cpu(s->consumer_cpu);

    // Barrier
    s->ready.fetch_add(1, std::memory_order_release);
    while (s->ready.load(std::memory_order_acquire) < 2) { /* spin */ }

    const double   ns_per_tick = s->ns_per_tick;
    const uint64_t warmup      = s->warmup;
    double*        latencies   = s->latencies;
    Queue*         q           = s->q;

    uint64_t count = 0;
    BenchMsg msg{};

    while (true) {
        while (!q->try_pop(msg)) { /* spin on empty */ }

        if (msg.seq == UINT64_MAX) break;  // sentinel

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
// main
// ---------------------------------------------------------------------------
int main(int argc, char* argv[]) {
    const int      producer_cpu = (argc > 1) ? std::atoi(argv[1]) : 2;
    const int      consumer_cpu = (argc > 2) ? std::atoi(argv[2]) : 4;
    const uint64_t num_samples  = (argc > 3) ? std::stoull(argv[3]) : 10'000'000ULL;

    std::printf("╔══════════════════════════════════════════════════════╗\n");
    std::printf("║          NanoMQ SPSC Latency Benchmark               ║\n");
    std::printf("╚══════════════════════════════════════════════════════╝\n");
    std::printf("  Producer CPU : %d\n", producer_cpu);
    std::printf("  Consumer CPU : %d\n", consumer_cpu);
    std::printf("  Samples      : %llu (+ %u warmup)\n",
                static_cast<unsigned long long>(num_samples), 10000u);
    std::printf("  Queue slots  : %zu (%.1f MB data)\n",
                Queue::capacity,
                static_cast<double>(Queue::capacity * sizeof(BenchMsg)) / (1024.0 * 1024.0));
    std::fflush(stdout);

    // TSC calibration
    std::printf("\n  Calibrating TSC... ");
    std::fflush(stdout);
    const double ns_per_tick = nanomq::tsc_ns_per_tick();
    std::printf("%.4f ns/tick\n\n", ns_per_tick);
    std::fflush(stdout);

    // Allocate queue in process-local aligned memory (in-process benchmark —
    // no shm needed here since producer and consumer share the same address space)
    alignas(64) static char queue_buf[sizeof(Queue)];
    Queue* q = reinterpret_cast<Queue*>(queue_buf);
    Queue::init(q);

    // Pre-allocate latency array
    std::vector<double> latencies(num_samples);

    SharedState state{};
    state.q            = q;
    state.ns_per_tick  = ns_per_tick;
    state.num_samples  = num_samples;
    state.warmup       = 10'000;
    state.latencies    = latencies.data();
    state.producer_cpu = producer_cpu;
    state.consumer_cpu = consumer_cpu;

    pthread_t prod_tid, cons_tid;
    ::pthread_create(&cons_tid, nullptr, consumer_fn, &state);
    ::pthread_create(&prod_tid, nullptr, producer_fn, &state);

    ::pthread_join(prod_tid, nullptr);
    ::pthread_join(cons_tid, nullptr);

    // -------------------------------------------------------------------------
    // Sort and compute statistics
    // -------------------------------------------------------------------------
    std::sort(latencies.begin(), latencies.end());

    const std::size_t n = latencies.size();
    const double mean_ns =
        std::accumulate(latencies.begin(), latencies.end(), 0.0) / static_cast<double>(n);

    auto pct = [&](double p) -> double {
        return latencies[static_cast<std::size_t>(p * static_cast<double>(n - 1))];
    };

    std::printf("┌─────────────────────────────────────┐\n");
    std::printf("│   One-Way Latency (ns)  — %zu samples │\n", n);
    std::printf("├─────────────────────────────────────┤\n");
    std::printf("│  min    :  %10.1f ns            │\n", latencies.front());
    std::printf("│  p50    :  %10.1f ns            │\n", pct(0.50));
    std::printf("│  p90    :  %10.1f ns            │\n", pct(0.90));
    std::printf("│  p99    :  %10.1f ns            │\n", pct(0.99));
    std::printf("│  p99.9  :  %10.1f ns            │\n", pct(0.999));
    std::printf("│  p99.99 :  %10.1f ns            │\n", pct(0.9999));
    std::printf("│  max    :  %10.1f ns            │\n", latencies.back());
    std::printf("│  mean   :  %10.1f ns            │\n", mean_ns);
    std::printf("└─────────────────────────────────────┘\n\n");

    return 0;
}
