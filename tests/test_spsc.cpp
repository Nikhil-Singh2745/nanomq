// tests/test_spsc.cpp
//
// NanoMQ SPSC Queue Correctness Tests
//
// Test 1 — Single-threaded functional:
//   Fill the queue to capacity, verify full detection, drain it, verify
//   empty detection. Check all values are intact and in order.
//
// Test 2 — Multi-threaded stress:
//   Producer thread sends 1,000,000 messages; consumer verifies every
//   sequence number arrives exactly once and in order.
//
// No external test framework. Uses assert() — exits non-zero on failure.
// Compile with -fsanitize=thread to check for data races.

#include "nanomq/common.hpp"
#include "nanomq/spsc_queue.hpp"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <atomic>

// ---------------------------------------------------------------------------
// Test message
// ---------------------------------------------------------------------------
struct TestMsg {
    uint64_t seq;
    uint64_t checksum;  // seq ^ 0xDEADBEEFCAFEBABEULL — cheap corruption detect
};

// ---------------------------------------------------------------------------
// Test 1: Single-threaded functional
// ---------------------------------------------------------------------------
static void test_single_threaded() {
    std::printf("[test 1] Single-threaded functional... ");
    std::fflush(stdout);

    constexpr std::size_t CAP = 256;
    using Q = nanomq::SpscQueue<TestMsg, CAP>;

    alignas(64) static char buf[sizeof(Q)];
    Q* q = reinterpret_cast<Q*>(buf);
    Q::init(q);

    assert(q->empty());
    assert(!q->full());
    assert(q->size() == 0);

    // Fill to capacity
    for (uint64_t i = 0; i < CAP; ++i) {
        TestMsg m{i, i ^ 0xDEADBEEFCAFEBABEULL};
        assert(q->try_push(m) && "queue should not be full yet");
    }

    // One more push must fail (queue full)
    {
        TestMsg extra{9999, 0};
        assert(!q->try_push(extra) && "queue must be full");
    }

    assert(q->full());
    assert(!q->empty());
    assert(q->size() == CAP);

    // Drain and verify
    for (uint64_t i = 0; i < CAP; ++i) {
        TestMsg m{};
        assert(q->try_pop(m) && "queue should not be empty yet");
        assert(m.seq == i && "sequence mismatch");
        assert(m.checksum == (i ^ 0xDEADBEEFCAFEBABEULL) && "checksum mismatch — data corruption");
    }

    // One more pop must fail (queue empty)
    {
        TestMsg m{};
        assert(!q->try_pop(m) && "queue must be empty");
    }

    assert(q->empty());
    assert(q->size() == 0);

    std::printf("PASS\n");
}

// ---------------------------------------------------------------------------
// Test 2: Multi-threaded stress (producer + consumer threads)
// ---------------------------------------------------------------------------
static void test_multithreaded_stress() {
    std::printf("[test 2] Multi-threaded stress (1M messages)... ");
    std::fflush(stdout);

    constexpr std::size_t CAP      = 4096;
    constexpr uint64_t    N_MSGS   = 1'000'000;
    using Q = nanomq::SpscQueue<TestMsg, CAP>;

    alignas(64) static char buf[sizeof(Q)];
    Q* q = reinterpret_cast<Q*>(buf);
    Q::init(q);

    std::atomic<bool> all_ok{true};

    std::thread producer([&]() {
        for (uint64_t i = 0; i < N_MSGS; ++i) {
            TestMsg m{i, i ^ 0xDEADBEEFCAFEBABEULL};
            while (!q->try_push(m)) { /* spin */ }
        }
        // Sentinel
        TestMsg sentinel{UINT64_MAX, 0};
        while (!q->try_push(sentinel)) {}
    });

    std::thread consumer([&]() {
        uint64_t expected = 0;
        TestMsg m{};
        while (true) {
            while (!q->try_pop(m)) { /* spin */ }
            if (m.seq == UINT64_MAX) break;

            if (m.seq != expected) {
                std::fprintf(stderr,
                    "\n[test 2] SEQUENCE ERROR: expected %llu, got %llu\n",
                    static_cast<unsigned long long>(expected),
                    static_cast<unsigned long long>(m.seq));
                all_ok.store(false);
            }
            if (m.checksum != (m.seq ^ 0xDEADBEEFCAFEBABEULL)) {
                std::fprintf(stderr,
                    "\n[test 2] CHECKSUM ERROR at seq=%llu\n",
                    static_cast<unsigned long long>(m.seq));
                all_ok.store(false);
            }
            ++expected;
        }
        if (expected != N_MSGS) {
            std::fprintf(stderr,
                "\n[test 2] MESSAGE COUNT ERROR: expected %llu, got %llu\n",
                static_cast<unsigned long long>(N_MSGS),
                static_cast<unsigned long long>(expected));
            all_ok.store(false);
        }
    });

    producer.join();
    consumer.join();

    assert(all_ok.load() && "stress test detected data corruption or lost messages");
    std::printf("PASS\n");
}

// ---------------------------------------------------------------------------
// Test 3: Wrap-around correctness (capacity boundary)
// ---------------------------------------------------------------------------
static void test_wraparound() {
    std::printf("[test 3] Wrap-around correctness... ");
    std::fflush(stdout);

    constexpr std::size_t CAP = 8;
    using Q = nanomq::SpscQueue<TestMsg, CAP>;

    alignas(64) static char buf[sizeof(Q)];
    Q* q = reinterpret_cast<Q*>(buf);
    Q::init(q);

    // Do 4 rounds of fill/drain to exercise multiple ring wraps
    for (int round = 0; round < 4; ++round) {
        for (uint64_t i = 0; i < CAP; ++i) {
            const uint64_t seq = i + static_cast<uint64_t>(round) * CAP;
            TestMsg m{seq, seq ^ 0xDEADBEEFCAFEBABEULL};
            assert(q->try_push(m));
        }
        for (uint64_t i = 0; i < CAP; ++i) {
            TestMsg m{};
            assert(q->try_pop(m));
            const uint64_t expected_seq = i + static_cast<uint64_t>(round) * CAP;
            assert(m.seq == expected_seq);
            assert(m.checksum == (expected_seq ^ 0xDEADBEEFCAFEBABEULL));
        }
    }

    std::printf("PASS\n");
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main() {
    std::printf("NanoMQ SPSC Queue — Correctness Tests\n");
    std::printf("──────────────────────────────────────\n");

    test_single_threaded();
    test_wraparound();
    test_multithreaded_stress();

    std::printf("──────────────────────────────────────\n");
    std::printf("All tests PASSED.\n\n");
    return 0;
}
