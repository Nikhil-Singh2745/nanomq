// tests/test_mpsc.cpp
//
// NanoMQ MPSC Queue Correctness Tests
//
// Test 1 — Single-threaded functional:
//   Push N items from a single producer, pop N items, verify all values intact.
//
// Test 2 — Multi-producer stress:
//   4 producers each push 250,000 messages (total 1,000,000).
//   Consumer verifies: every message is received exactly once, no corruption.
//   Uses a bitset (std::vector<bool>) indexed by a global sequence number:
//   each producer uses its own sequence space [id * N .. (id+1)*N - 1].
//
// No external test framework. Uses assert() — exits non-zero on failure.
// Run with -fsanitize=thread to check for data races.

#include "nanomq/common.hpp"
#include "nanomq/mpsc_queue.hpp"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <atomic>
#include <vector>
#include <cstring>

// ---------------------------------------------------------------------------
// Test message
// ---------------------------------------------------------------------------
struct TestMsg {
    uint64_t global_seq;   // unique across all producers
    uint64_t checksum;     // global_seq ^ 0xCAFEBABEDEADBEEFULL
};

// ---------------------------------------------------------------------------
// Test 1: Single-threaded functional
// ---------------------------------------------------------------------------
static void test_single_threaded() {
    std::printf("[test 1] MPSC single-threaded functional... ");
    std::fflush(stdout);

    constexpr std::size_t CAP = 256;
    using Q = nanomq::MpscQueue<TestMsg, CAP>;

    alignas(64) static char buf[sizeof(Q)];
    Q* q = reinterpret_cast<Q*>(buf);
    Q::init(q);

    assert(q->empty());
    assert(q->size() == 0);

    // Push CAP items
    for (uint64_t i = 0; i < CAP; ++i) {
        TestMsg m{i, i ^ 0xCAFEBABEDEADBEEFULL};
        assert(q->try_push(m) && "queue should accept items when not full");
    }

    // Queue should be full now
    {
        TestMsg extra{9999, 0};
        assert(!q->try_push(extra) && "queue must be full — push should fail");
    }

    assert(q->size() == CAP);

    // Pop and verify
    for (uint64_t i = 0; i < CAP; ++i) {
        TestMsg m{};
        assert(q->try_pop(m) && "queue should not be empty during drain");
        assert(m.global_seq == i && "sequence mismatch");
        assert(m.checksum == (i ^ 0xCAFEBABEDEADBEEFULL) && "checksum mismatch — data corruption");
    }

    {
        TestMsg m{};
        assert(!q->try_pop(m) && "queue must be empty after full drain");
    }

    assert(q->empty());
    std::printf("PASS\n");
}

// ---------------------------------------------------------------------------
// Test 2: Multi-producer stress
// ---------------------------------------------------------------------------
static void test_multi_producer_stress() {
    std::printf("[test 2] MPSC multi-producer stress (4 producers × 250K)... ");
    std::fflush(stdout);

    constexpr uint32_t NUM_PRODUCERS   = 4;
    constexpr uint64_t MSGS_PER_PROD   = 250'000;
    constexpr uint64_t TOTAL_MSGS      = NUM_PRODUCERS * MSGS_PER_PROD;

    constexpr std::size_t CAP = 16384;
    using Q = nanomq::MpscQueue<TestMsg, CAP>;

    alignas(64) static char buf[sizeof(Q)];
    Q* q = reinterpret_cast<Q*>(buf);
    Q::init(q);

    // Track which global_seqs were received (indexed by global_seq)
    std::vector<std::atomic<bool>> received(TOTAL_MSGS);
    for (auto& a : received) a.store(false, std::memory_order_relaxed);

    std::atomic<bool> all_ok{true};
    std::atomic<uint32_t> sentinels{0};

    // Consumer thread
    std::thread consumer([&]() {
        uint64_t count = 0;
        TestMsg m{};
        while (count < TOTAL_MSGS) {
            while (!q->try_pop(m)) { /* spin */ }

            // Validate checksum
            if (m.checksum != (m.global_seq ^ 0xCAFEBABEDEADBEEFULL)) {
                std::fprintf(stderr,
                    "\n[test 2] CHECKSUM ERROR at global_seq=%llu\n",
                    static_cast<unsigned long long>(m.global_seq));
                all_ok.store(false, std::memory_order_relaxed);
            }

            // Check for duplicate
            if (m.global_seq >= TOTAL_MSGS) {
                std::fprintf(stderr,
                    "\n[test 2] OUT-OF-RANGE seq=%llu (max=%llu)\n",
                    static_cast<unsigned long long>(m.global_seq),
                    static_cast<unsigned long long>(TOTAL_MSGS));
                all_ok.store(false, std::memory_order_relaxed);
            } else {
                bool was_received = received[m.global_seq].exchange(true, std::memory_order_relaxed);
                if (was_received) {
                    std::fprintf(stderr,
                        "\n[test 2] DUPLICATE seq=%llu\n",
                        static_cast<unsigned long long>(m.global_seq));
                    all_ok.store(false, std::memory_order_relaxed);
                }
            }
            ++count;
        }
    });

    // Producer threads
    std::vector<std::thread> producers;
    producers.reserve(NUM_PRODUCERS);
    for (uint32_t pid = 0; pid < NUM_PRODUCERS; ++pid) {
        producers.emplace_back([&, pid]() {
            const uint64_t base = static_cast<uint64_t>(pid) * MSGS_PER_PROD;
            for (uint64_t i = 0; i < MSGS_PER_PROD; ++i) {
                const uint64_t gs = base + i;
                TestMsg m{gs, gs ^ 0xCAFEBABEDEADBEEFULL};
                while (!q->try_push(m)) { /* spin on full */ }
            }
        });
    }

    for (auto& p : producers) p.join();
    consumer.join();

    // Verify all messages received
    for (uint64_t i = 0; i < TOTAL_MSGS; ++i) {
        if (!received[i].load(std::memory_order_relaxed)) {
            std::fprintf(stderr,
                "\n[test 2] MISSING seq=%llu\n",
                static_cast<unsigned long long>(i));
            all_ok.store(false, std::memory_order_relaxed);
        }
    }

    assert(all_ok.load() && "MPSC stress test detected corruption or missing messages");
    std::printf("PASS\n");
}

// ---------------------------------------------------------------------------
// Test 3: Wrap-around with multiple producers
// ---------------------------------------------------------------------------
static void test_wraparound_mpsc() {
    std::printf("[test 3] MPSC wrap-around (2 producers, small queue)... ");
    std::fflush(stdout);

    constexpr std::size_t CAP = 16;
    constexpr uint32_t NP    = 2;
    constexpr uint64_t EACH  = 500;   // 500 messages per producer, 1000 total

    using Q = nanomq::MpscQueue<TestMsg, CAP>;

    alignas(64) static char buf[sizeof(Q)];
    Q* q = reinterpret_cast<Q*>(buf);
    Q::init(q);

    std::vector<std::atomic<bool>> received(NP * EACH);
    for (auto& a : received) a.store(false, std::memory_order_relaxed);

    std::atomic<bool> all_ok{true};

    std::thread consumer([&]() {
        uint64_t count = 0;
        TestMsg m{};
        while (count < NP * EACH) {
            while (!q->try_pop(m)) {}
            if (m.global_seq >= NP * EACH) {
                all_ok.store(false);
            } else {
                bool was = received[m.global_seq].exchange(true);
                if (was) all_ok.store(false);
                if (m.checksum != (m.global_seq ^ 0xCAFEBABEDEADBEEFULL))
                    all_ok.store(false);
            }
            ++count;
        }
    });

    std::vector<std::thread> producers;
    for (uint32_t pid = 0; pid < NP; ++pid) {
        producers.emplace_back([&, pid]() {
            const uint64_t base = static_cast<uint64_t>(pid) * EACH;
            for (uint64_t i = 0; i < EACH; ++i) {
                const uint64_t gs = base + i;
                TestMsg m{gs, gs ^ 0xCAFEBABEDEADBEEFULL};
                while (!q->try_push(m)) {}
            }
        });
    }
    for (auto& p : producers) p.join();
    consumer.join();

    for (uint64_t i = 0; i < NP * EACH; ++i) {
        assert(received[i].load() && "missing message in wrap-around test");
    }
    assert(all_ok.load());
    std::printf("PASS\n");
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main() {
    std::printf("NanoMQ MPSC Queue — Correctness Tests\n");
    std::printf("──────────────────────────────────────\n");

    test_single_threaded();
    test_wraparound_mpsc();
    test_multi_producer_stress();

    std::printf("──────────────────────────────────────\n");
    std::printf("All tests PASSED.\n\n");
    return 0;
}
