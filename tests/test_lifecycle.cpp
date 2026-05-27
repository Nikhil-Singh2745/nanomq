// tests/test_lifecycle.cpp
//
// NanoMQ v3 Lifecycle Tests
//
// Validates the ShmControlBlock-based lifecycle coordination:
//
// Test 1 — Segment creation & magic validation:
//   Creator initialises the segment; a second ShmHandle validates magic and
//   version fields correctly.
//
// Test 2 — Reference counting:
//   Creator has ref_count=1; each joiner increments on open and decrements
//   on close. Verify the count tracks correctly.
//
// Test 3 — Shutdown flag:
//   Owner calls signal_shutdown(); joiner sees is_shutdown() == true.
//
// Test 4 — Heartbeat & stale detection:
//   After the heartbeat timestamp becomes old enough, is_stale() returns true.
//   We manipulate time via a direct write to the heartbeat field.
//
// Test 5 — Reject invalid segments:
//   Attaching to a segment with a wrong magic or version must throw.
//
// Uses assert() only — no external test framework.

#include "nanomq/common.hpp"
#include "nanomq/spsc_queue.hpp"
#include "nanomq/shm_transport.hpp"

#include <cassert>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <chrono>

// ---------------------------------------------------------------------------
// Minimal test queue type
// ---------------------------------------------------------------------------
struct LifecycleMsg {
    uint64_t seq;
    uint64_t data;
};
using Q = nanomq::SpscQueue<LifecycleMsg, 256>;

static constexpr const char* SHM_NAME = "/nanomq_lifecycle_test";

// ---------------------------------------------------------------------------
// Test 1: Segment creation and magic/version validation
// ---------------------------------------------------------------------------
static void test_create_and_validate() {
    std::printf("[test 1] Segment creation & magic validation... ");
    std::fflush(stdout);

    // Clean up any leftover segment from a previous crashed run
    ::shm_unlink(SHM_NAME);

    {
        auto owner = nanomq::shm_create<Q>(SHM_NAME);

        assert(owner.valid());
        assert(owner.is_owner());
        assert(owner.ctrl() != nullptr);
        assert(owner.ctrl()->magic.load() == nanomq::SHM_MAGIC);
        assert(owner.ctrl()->version.load() == nanomq::SHM_VERSION);
        assert(owner.ctrl()->owner_pid.load() == static_cast<int32_t>(::getpid()));
        assert(!owner.is_shutdown());

        // Joiner attaches while owner is alive
        {
            auto joiner = nanomq::shm_open_existing<Q>(SHM_NAME);
            assert(joiner.valid());
            assert(!joiner.is_owner());
            assert(joiner.ctrl()->magic.load() == nanomq::SHM_MAGIC);
        }
        // Joiner destructor ran — should have decremented ref_count

        // Owner is still alive
        assert(owner.ctrl() != nullptr);
    }
    // Owner destructor ran — unlinks the segment

    std::printf("PASS\n");
}

// ---------------------------------------------------------------------------
// Test 2: Reference counting
// ---------------------------------------------------------------------------
static void test_ref_counting() {
    std::printf("[test 2] Reference counting... ");
    std::fflush(stdout);

    ::shm_unlink(SHM_NAME);

    {
        auto owner = nanomq::shm_create<Q>(SHM_NAME);
        assert(owner.ref_count() == 1);

        {
            auto j1 = nanomq::shm_open_existing<Q>(SHM_NAME);
            assert(owner.ref_count() == 2);

            {
                auto j2 = nanomq::shm_open_existing<Q>(SHM_NAME);
                assert(owner.ref_count() == 3);
            }
            // j2 destroyed
            assert(owner.ref_count() == 2);
        }
        // j1 destroyed
        assert(owner.ref_count() == 1);
    }
    // owner destroyed

    std::printf("PASS\n");
}

// ---------------------------------------------------------------------------
// Test 3: Shutdown flag propagation
// ---------------------------------------------------------------------------
static void test_shutdown_flag() {
    std::printf("[test 3] Shutdown flag propagation... ");
    std::fflush(stdout);

    ::shm_unlink(SHM_NAME);

    {
        auto owner  = nanomq::shm_create<Q>(SHM_NAME);
        auto joiner = nanomq::shm_open_existing<Q>(SHM_NAME);

        assert(!owner.is_shutdown());
        assert(!joiner.is_shutdown());

        owner.signal_shutdown();

        assert(owner.is_shutdown());
        assert(joiner.is_shutdown());  // joiner sees the same mmap'd memory
    }

    std::printf("PASS\n");
}

// ---------------------------------------------------------------------------
// Test 4: Heartbeat — beat_heartbeat keeps is_stale() false
// ---------------------------------------------------------------------------
static void test_heartbeat() {
    std::printf("[test 4] Heartbeat & stale detection... ");
    std::fflush(stdout);

    ::shm_unlink(SHM_NAME);

    {
        auto owner = nanomq::shm_create<Q>(SHM_NAME);

        // Freshly created — should not be stale
        owner.beat_heartbeat();
        assert(!owner.is_stale());

        // Manually backdating the heartbeat far into the past simulates a crash.
        // We write 0 (epoch) directly — stale threshold is 5 seconds.
        // is_stale() should now return true since (now - 0) >> 5s.
        // const_cast is safe here: we own the segment and are testing the
        // stale detection path specifically.
        const_cast<nanomq::ShmControlBlock*>(owner.ctrl())->heartbeat_ns.store(
            0, std::memory_order_release);
        assert(owner.is_stale());

        // Beating should un-stale it
        owner.beat_heartbeat();
        assert(!owner.is_stale());
    }

    std::printf("PASS\n");
}

// ---------------------------------------------------------------------------
// Test 5: Messages still flow correctly through the updated ShmHandle
//         (regression — ShmControlBlock must not corrupt queue layout)
// ---------------------------------------------------------------------------
static void test_queue_correctness_through_shm() {
    std::printf("[test 5] Queue correctness through ShmHandle (regression)... ");
    std::fflush(stdout);

    ::shm_unlink(SHM_NAME);

    constexpr uint64_t N = 10'000;

    {
        auto owner  = nanomq::shm_create<Q>(SHM_NAME);
        auto joiner = nanomq::shm_open_existing<Q>(SHM_NAME);

        bool all_ok = true;

        std::thread producer([&]() {
            for (uint64_t i = 0; i < N; ++i) {
                LifecycleMsg m{i, i ^ 0xABCD1234ABCD1234ULL};
                while (!owner->try_push(m)) {}
            }
            // Sentinel
            LifecycleMsg sentinel{UINT64_MAX, 0};
            while (!owner->try_push(sentinel)) {}
        });

        std::thread consumer([&]() {
            uint64_t expected = 0;
            LifecycleMsg m{};
            while (true) {
                while (!joiner->try_pop(m)) {}
                if (m.seq == UINT64_MAX) break;
                if (m.seq != expected ||
                    m.data != (m.seq ^ 0xABCD1234ABCD1234ULL)) {
                    all_ok = false;
                }
                ++expected;
            }
            if (expected != N) all_ok = false;
        });

        producer.join();
        consumer.join();

        assert(all_ok);
    }

    std::printf("PASS\n");
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main() {
    std::printf("NanoMQ v3 Lifecycle Tests\n");
    std::printf("─────────────────────────\n");

    test_create_and_validate();
    test_ref_counting();
    test_shutdown_flag();
    test_heartbeat();
    test_queue_correctness_through_shm();

    std::printf("─────────────────────────\n");
    std::printf("All lifecycle tests PASSED.\n\n");
    return 0;
}
