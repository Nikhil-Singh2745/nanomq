// bench/bench_mpsc.cpp
//
// NanoMQ MPSC Latency & Throughput Benchmark
//
// Methodology:
//   - N producer threads each pinned to a dedicated CPU, one consumer pinned
//     to its own CPU.
//   - Each producer sends a fixed number of messages carrying its producer ID
//     and a TSC timestamp. The consumer records arrival TSC and computes one-
//     way latency per message.
//   - Warmup: first WARMUP messages per producer are discarded.
//   - Runs for 2, 4, and 8 producers sequentially and prints a comparison table.
//
// Usage:
//   ./bench_mpsc [consumer_cpu] [samples_per_producer]
//   consumer_cpu        : default 0
//   samples_per_producer: default 500,000
//
// Note: producer CPUs are assigned sequentially starting from consumer_cpu+1.
// Adjust if your topology differs.

#include <pthread.h>
#include <sched.h>

#include "nanomq/common.hpp"
#include "nanomq/mpsc_queue.hpp"

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
// Benchmark message (64 bytes)
// ---------------------------------------------------------------------------
struct alignas(64) MpscMsg {
    uint64_t tsc;
    uint64_t seq;
    uint32_t producer_id;
    char     _pad[44];
};
static_assert(sizeof(MpscMsg) == 64);

// ---------------------------------------------------------------------------
// Queue: 131072 slots
// ---------------------------------------------------------------------------
using Queue = nanomq::MpscQueue<MpscMsg, 131072>;

// ---------------------------------------------------------------------------
// CPU pinning
// ---------------------------------------------------------------------------
static bool pin_to_cpu(int cpu) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu, &cpuset);
    int rc = ::pthread_setaffinity_np(::pthread_self(), sizeof(cpuset), &cpuset);
    if (rc != 0) {
        std::fprintf(stderr, "[bench_mpsc] WARNING: cannot pin to CPU %d: %s\n",
                     cpu, ::strerror(rc));
        return false;
    }
    return true;
}

// ---------------------------------------------------------------------------
// Shared state for one benchmark run
// ---------------------------------------------------------------------------
struct RunState {
    Queue*            q;
    double            ns_per_tick;
    uint32_t          num_producers;
    uint64_t          samples_per_producer;
    uint64_t          warmup_per_producer;
    double*           latencies;          // [num_producers * samples_per_producer]
    std::atomic<int>  producers_ready{0};
    std::atomic<bool> consumer_ready{false};
    int               consumer_cpu;
    int*              producer_cpus;      // array of num_producers ints
};

struct ProducerArg {
    RunState* state;
    uint32_t  id;
};

// ---------------------------------------------------------------------------
// Producer thread
// ---------------------------------------------------------------------------
static void* producer_fn(void* raw) {
    auto* arg = static_cast<ProducerArg*>(raw);
    RunState* s = arg->state;
    const uint32_t id = arg->id;

    pin_to_cpu(s->producer_cpus[id]);

    // Signal ready
    s->producers_ready.fetch_add(1, std::memory_order_release);
    // Wait for consumer
    while (!s->consumer_ready.load(std::memory_order_acquire)) { /* spin */ }

    const uint64_t total = s->samples_per_producer + s->warmup_per_producer;
    Queue* q = s->q;

    for (uint64_t i = 0; i < total; ++i) {
        MpscMsg msg{};
        msg.seq         = i;
        msg.producer_id = id;
        msg.tsc         = nanomq::rdtsc();
        while (!q->try_push(msg)) { /* spin on full */ }
    }

    // Sentinel per producer
    MpscMsg sentinel{};
    sentinel.seq         = UINT64_MAX;
    sentinel.producer_id = id;
    sentinel.tsc         = 0;
    while (!q->try_push(sentinel)) {}

    return nullptr;
}

// ---------------------------------------------------------------------------
// Consumer thread
// ---------------------------------------------------------------------------
static void* consumer_fn(void* raw) {
    auto* s = static_cast<RunState*>(raw);
    pin_to_cpu(s->consumer_cpu);

    // Wait for all producers to be ready
    while (s->producers_ready.load(std::memory_order_acquire)
               < static_cast<int>(s->num_producers)) { /* spin */ }

    s->consumer_ready.store(true, std::memory_order_release);

    const double   ns_per_tick = s->ns_per_tick;
    const uint64_t warmup      = s->warmup_per_producer;
    double*        latencies   = s->latencies;
    Queue*         q           = s->q;
    const uint32_t np          = s->num_producers;
    const uint64_t total_msgs  = np * s->samples_per_producer;

    // Per-producer warmup counter and latency index
    std::vector<uint64_t> prod_count(np, 0);
    std::vector<uint64_t> prod_lat_idx(np, 0);
    uint32_t sentinels_seen = 0;
    uint64_t lat_written    = 0;

    MpscMsg msg{};

    while (sentinels_seen < np) {
        while (!q->try_pop(msg)) { /* spin */ }

        if (msg.seq == UINT64_MAX) {
            ++sentinels_seen;
            continue;
        }

        const uint32_t pid = msg.producer_id;
        if (pid >= np) continue;  // safety guard

        const uint64_t cnt = prod_count[pid]++;
        if (cnt >= warmup && lat_written < total_msgs) {
            const uint64_t recv_tsc = nanomq::rdtsc();
            latencies[lat_written++] =
                static_cast<double>(recv_tsc - msg.tsc) * ns_per_tick;
        }
    }

    return nullptr;
}

// ---------------------------------------------------------------------------
// Run one benchmark configuration
// ---------------------------------------------------------------------------
static void run_bench(uint32_t num_producers,
                      int      consumer_cpu,
                      uint64_t samples_per_producer,
                      double   ns_per_tick)
{
    const uint64_t total_samples = num_producers * samples_per_producer;

    std::printf("\n  ┌── %u producer(s) → 1 consumer ──────────────────────────────┐\n",
                num_producers);

    // Assign producer CPUs starting after consumer_cpu
    std::vector<int> prod_cpus(num_producers);
    for (uint32_t i = 0; i < num_producers; ++i) {
        prod_cpus[i] = (consumer_cpu + 1 + static_cast<int>(i)) % 8;
    }

    // Allocate fresh queue and latency buffer
    alignas(64) static char queue_buf[sizeof(Queue)];
    Queue* q = reinterpret_cast<Queue*>(queue_buf);
    Queue::init(q);

    std::vector<double> latencies(total_samples);

    RunState state{};
    state.q                    = q;
    state.ns_per_tick          = ns_per_tick;
    state.num_producers        = num_producers;
    state.samples_per_producer = samples_per_producer;
    state.warmup_per_producer  = 1000;
    state.latencies            = latencies.data();
    state.consumer_cpu         = consumer_cpu;
    state.producer_cpus        = prod_cpus.data();

    std::vector<ProducerArg> args(num_producers);
    for (uint32_t i = 0; i < num_producers; ++i) {
        args[i] = {&state, i};
    }

    pthread_t cons_tid;
    std::vector<pthread_t> prod_tids(num_producers);

    ::pthread_create(&cons_tid, nullptr, consumer_fn, &state);
    for (uint32_t i = 0; i < num_producers; ++i) {
        ::pthread_create(&prod_tids[i], nullptr, producer_fn, &args[i]);
    }

    for (uint32_t i = 0; i < num_producers; ++i) {
        ::pthread_join(prod_tids[i], nullptr);
    }
    ::pthread_join(cons_tid, nullptr);

    // Sort and report
    const std::size_t n = latencies.size();
    std::sort(latencies.begin(), latencies.end());
    const double mean_ns =
        std::accumulate(latencies.begin(), latencies.end(), 0.0) /
        static_cast<double>(n);

    auto pct = [&](double p) -> double {
        return latencies[static_cast<std::size_t>(p * static_cast<double>(n - 1))];
    };

    std::printf("  │  Samples   : %zu\n",       n);
    std::printf("  │  min       : %10.1f ns\n", latencies.front());
    std::printf("  │  p50       : %10.1f ns\n", pct(0.50));
    std::printf("  │  p99       : %10.1f ns\n", pct(0.99));
    std::printf("  │  p99.9     : %10.1f ns\n", pct(0.999));
    std::printf("  │  max       : %10.1f ns\n", latencies.back());
    std::printf("  │  mean      : %10.1f ns\n", mean_ns);
    std::printf("  └─────────────────────────────────────────────────────────────┘\n");
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main(int argc, char* argv[]) {
    const int      consumer_cpu          = (argc > 1) ? std::atoi(argv[1]) : 0;
    const uint64_t samples_per_producer  = (argc > 2) ? std::stoull(argv[2]) : 500'000ULL;

    std::printf("╔══════════════════════════════════════════════════════╗\n");
    std::printf("║          NanoMQ MPSC Latency Benchmark               ║\n");
    std::printf("╚══════════════════════════════════════════════════════╝\n");
    std::printf("  Consumer CPU          : %d\n", consumer_cpu);
    std::printf("  Samples per producer  : %llu\n",
                static_cast<unsigned long long>(samples_per_producer));

    std::printf("\n  Calibrating TSC... ");
    std::fflush(stdout);
    const double ns_per_tick = nanomq::tsc_ns_per_tick();
    std::printf("%.4f ns/tick\n", ns_per_tick);

    for (uint32_t np : {2u, 4u, 8u}) {
        run_bench(np, consumer_cpu, samples_per_producer, ns_per_tick);
    }

    std::printf("\n");
    return 0;
}
