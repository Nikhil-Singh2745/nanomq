// examples/producer.cpp
//
// NanoMQ producer example.
// Creates a shared memory SPSC queue and sends N messages in a tight loop.
// Run this FIRST, then start consumer.cpp.
//
// Usage: ./producer [num_messages] [shm_name]
//   num_messages  : default 1,000,000
//   shm_name      : default /nanomq_example
//
// The consumer must be started after this process creates the queue.
// This process owns the shared memory lifetime and unlinks it on exit.

#include "nanomq/common.hpp"
#include "nanomq/spsc_queue.hpp"
#include "nanomq/shm_transport.hpp"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

// ---------------------------------------------------------------------------
// Message layout — exactly 64 bytes (one cache line per message).
// ---------------------------------------------------------------------------
struct alignas(64) Msg {
    uint64_t seq;         // monotonically increasing sequence number
    uint64_t tsc;         // rdtsc at send time (for latency measurement)
    uint64_t timestamp_ns;// wall-clock ns at send time
    char     payload[40]; // padding to hit exactly 64 bytes
};
static_assert(sizeof(Msg) == 64, "Msg must be exactly 64 bytes");

// ---------------------------------------------------------------------------
// Queue type: 65536-slot ring (4 MB of slot data)
// ---------------------------------------------------------------------------
using Queue = nanomq::SpscQueue<Msg, 65536>;

int main(int argc, char* argv[]) {
    const uint64_t    num_messages = (argc > 1) ? std::stoull(argv[1]) : 1'000'000ULL;
    const std::string shm_name     = (argc > 2) ? argv[2]              : "/nanomq_example";

    std::printf("[producer] Creating shared memory queue '%s'\n", shm_name.c_str());
    std::printf("[producer] Queue capacity: %zu slots | Message size: %zu bytes\n",
                Queue::capacity, sizeof(Msg));
    std::printf("[producer] Will send %llu messages\n",
                static_cast<unsigned long long>(num_messages));
    std::fflush(stdout);

    // Create and own the shared memory segment
    auto handle = nanomq::shm_create<Queue>(shm_name);
    Queue* q = handle.queue();

    std::printf("[producer] Queue ready at %p. Start consumer, then press Enter...\n",
                static_cast<void*>(q));
    std::fflush(stdout);
    std::getchar();

    // -------------------------------------------------------------------------
    // Hot path: send messages as fast as possible.
    // We spin on try_push when the queue is full — this keeps the producer
    // running at full throughput and tests back-pressure handling.
    // -------------------------------------------------------------------------
    const uint64_t t_start_ns = nanomq::monotonic_ns();

    for (uint64_t i = 0; i < num_messages; ++i) {
        Msg msg{};
        msg.seq          = i;
        msg.tsc          = nanomq::rdtsc();
        msg.timestamp_ns = nanomq::monotonic_ns();
        std::memcpy(msg.payload, "nanomq", 7);

        while (!q->try_push(msg)) {
            // Queue full — spin. In production you might yield or sleep_for(0).
            // Here we keep it tight to measure raw throughput.
        }
    }

    const uint64_t t_end_ns = nanomq::monotonic_ns();
    const double   elapsed_s = static_cast<double>(t_end_ns - t_start_ns) * 1e-9;
    const double   throughput = static_cast<double>(num_messages) / elapsed_s;

    std::printf("[producer] Sent %llu messages in %.3f s  →  %.2f M msgs/sec\n",
                static_cast<unsigned long long>(num_messages),
                elapsed_s,
                throughput / 1e6);
    std::fflush(stdout);

    // Send a sentinel with seq = UINT64_MAX to signal consumer to stop
    Msg sentinel{};
    sentinel.seq = UINT64_MAX;
    while (!q->try_push(sentinel)) {}

    std::printf("[producer] Sentinel sent. Press Enter to cleanup shared memory...\n");
    std::fflush(stdout);
    std::getchar();

    // ShmHandle destructor will munmap + shm_unlink automatically.
    return 0;
}
