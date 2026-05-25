// examples/consumer.cpp
//
// NanoMQ consumer example.
// Opens an existing shared memory SPSC queue (created by producer.cpp),
// reads all messages, verifies sequence numbers are monotonic, and prints
// basic stats: throughput, per-message latency (TSC-based).
//
// Usage: ./consumer [shm_name]
//   shm_name : default /nanomq_example  (must match producer)
//
// Start AFTER producer has created the queue.

#include "nanomq/common.hpp"
#include "nanomq/spsc_queue.hpp"
#include "nanomq/shm_transport.hpp"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// Message layout — must exactly match producer.cpp
// ---------------------------------------------------------------------------
struct alignas(64) Msg {
    uint64_t seq;
    uint64_t tsc;
    uint64_t timestamp_ns;
    char     payload[40];
};
static_assert(sizeof(Msg) == 64, "Msg must be exactly 64 bytes");

using Queue = nanomq::SpscQueue<Msg, 65536>;

int main(int argc, char* argv[]) {
    const std::string shm_name = (argc > 1) ? argv[1] : "/nanomq_example";

    std::printf("[consumer] Opening shared memory queue '%s'\n", shm_name.c_str());
    std::fflush(stdout);

    // Calibrate TSC → ns conversion (10ms spin)
    const double ns_per_tick = nanomq::tsc_ns_per_tick();
    std::printf("[consumer] TSC calibration: %.4f ns/tick\n", ns_per_tick);

    // Open existing queue (joiner — does not unlink on exit)
    auto handle = nanomq::shm_open_existing<Queue>(shm_name);
    Queue* q    = handle.queue();

    std::printf("[consumer] Attached to queue at %p. Reading...\n",
                static_cast<void*>(q));
    std::fflush(stdout);

    // -------------------------------------------------------------------------
    // Reserve latency sample storage upfront to avoid reallocation on hot path.
    // -------------------------------------------------------------------------
    std::vector<double> latencies_ns;
    latencies_ns.reserve(2'000'000);

    uint64_t prev_seq       = UINT64_MAX;  // will be set on first message
    uint64_t messages_read  = 0;
    bool     sequence_ok    = true;

    const uint64_t t_start_ns = nanomq::monotonic_ns();

    // -------------------------------------------------------------------------
    // Hot path: spin-read until sentinel (seq == UINT64_MAX)
    // -------------------------------------------------------------------------
    Msg msg{};
    while (true) {
        while (!q->try_pop(msg)) {
            // Queue empty — spin
        }

        // Check for sentinel
        if (msg.seq == UINT64_MAX) break;

        // Record latency: TSC delta from send to now, converted to ns
        const uint64_t recv_tsc   = nanomq::rdtsc();
        const double   latency_ns = static_cast<double>(recv_tsc - msg.tsc) * ns_per_tick;
        latencies_ns.push_back(latency_ns);

        // Sequence number monotonicity check
        if (messages_read > 0 && msg.seq != prev_seq + 1) {
            std::fprintf(stderr,
                "[consumer] SEQUENCE ERROR at msg %llu: expected %llu, got %llu\n",
                static_cast<unsigned long long>(messages_read),
                static_cast<unsigned long long>(prev_seq + 1),
                static_cast<unsigned long long>(msg.seq));
            sequence_ok = false;
        }

        prev_seq = msg.seq;
        ++messages_read;
    }

    const uint64_t t_end_ns = nanomq::monotonic_ns();

    // -------------------------------------------------------------------------
    // Statistics
    // -------------------------------------------------------------------------
    const double elapsed_s   = static_cast<double>(t_end_ns - t_start_ns) * 1e-9;
    const double throughput  = static_cast<double>(messages_read) / elapsed_s;

    std::printf("\n[consumer] ─────────────────────────────────────────────\n");
    std::printf("[consumer] Messages received : %llu\n",
                static_cast<unsigned long long>(messages_read));
    std::printf("[consumer] Throughput        : %.2f M msgs/sec\n", throughput / 1e6);
    std::printf("[consumer] Elapsed           : %.3f s\n", elapsed_s);
    std::printf("[consumer] Sequence intact   : %s\n", sequence_ok ? "YES ✓" : "NO ✗");

    if (!latencies_ns.empty()) {
        std::sort(latencies_ns.begin(), latencies_ns.end());
        const std::size_t n   = latencies_ns.size();
        const double mean_ns  = [&]() {
            double sum = 0.0;
            for (double v : latencies_ns) sum += v;
            return sum / static_cast<double>(n);
        }();

        auto percentile = [&](double p) -> double {
            const std::size_t idx = static_cast<std::size_t>(p * static_cast<double>(n - 1));
            return latencies_ns[idx];
        };

        std::printf("\n[consumer] One-way latency (TSC-based, ns):\n");
        std::printf("  min    : %8.1f ns\n",  latencies_ns.front());
        std::printf("  p50    : %8.1f ns\n",  percentile(0.50));
        std::printf("  p90    : %8.1f ns\n",  percentile(0.90));
        std::printf("  p99    : %8.1f ns\n",  percentile(0.99));
        std::printf("  p99.9  : %8.1f ns\n",  percentile(0.999));
        std::printf("  max    : %8.1f ns\n",  latencies_ns.back());
        std::printf("  mean   : %8.1f ns\n",  mean_ns);
    }

    std::printf("[consumer] ─────────────────────────────────────────────\n\n");
    std::fflush(stdout);

    return sequence_ok ? 0 : 1;
}
