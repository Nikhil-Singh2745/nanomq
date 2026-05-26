#pragma once

#include "common.hpp"

#include <atomic>
#include <cstdint>
#include <cstring>    // std::memcpy
#include <concepts>   // std::is_trivially_copyable_v

namespace nanomq {

// ---------------------------------------------------------------------------
// MpscQueue<T, Capacity>
//
// Lock-free Multi-Producer / Single-Consumer ring buffer.
//
// Design — two-phase commit:
//   Phase 1 (claim):  Producer atomically increments head_ with
//                     compare_exchange_weak to claim an exclusive slot index.
//   Phase 2 (commit): Producer writes data to its claimed slot, then sets
//                     slots_[idx].committed with memory_order_release.
//
//   Consumer polls slots_[tail_ & mask].committed with memory_order_acquire.
//   When committed == 1 the consumer reads the item, resets committed to 0,
//   then advances tail_ with memory_order_release.
//
// Key constraints (same as SPSC):
//   - Capacity MUST be a power of two.
//   - T must be trivially copyable.
//   - head_  lives on its own cache line (written by multiple producers via CAS).
//   - tail_  lives on its own cache line (written by single consumer).
//   - Each Slot is cache-line aligned — avoids false sharing between adjacent
//     slots being written by different producers simultaneously.
//
// Memory ordering:
//   - head_ CAS: acq_rel (acquire to read current head, release to publish new head)
//   - Slot write: plain memcpy — safe because the producer owns the slot
//     exclusively after claiming it; the consumer won't read until committed is set.
//   - committed store: release — pairs with consumer's acquire load.
//   - tail_ store: release — signals producers that the slot is now free.
//
// Throughput note:
//   Under high contention the CAS loop degrades gracefully — each producer
//   retries until it wins its claim. The per-slot committed flag decouples
//   the two phases so a slow producer (OS preempted between claim and commit)
//   does not block other producers from claiming further slots; it does stall
//   the consumer at that slot until the preempted producer resumes. This is
//   the classic MPSC two-phase tradeoff: high throughput, bounded tail latency
//   only if producers are never preempted at the worst moment.
// ---------------------------------------------------------------------------

template <typename T, std::size_t Capacity>
    requires std::is_trivially_copyable_v<T>
struct MpscQueue {
    static_assert(is_power_of_two_v<Capacity>,
                  "MpscQueue: Capacity must be a power of two");
    static_assert(sizeof(T) > 0, "MpscQueue: T must have non-zero size");

    // -------------------------------------------------------------------------
    // Public constants
    // -------------------------------------------------------------------------
    static constexpr std::size_t capacity = Capacity;
    static constexpr std::size_t mask     = Capacity - 1;

    // -------------------------------------------------------------------------
    // Per-slot wrapper: data + committed flag.
    // alignas(64) ensures no two slots share a cache line, preventing false
    // sharing between producers writing to adjacent indices simultaneously.
    // -------------------------------------------------------------------------
    struct alignas(CACHE_LINE_SIZE) Slot {
        std::atomic<uint32_t> committed{0};   // 0 = empty / claimed, 1 = ready to read
        char                  _pad_committed[CACHE_LINE_SIZE
                                             - sizeof(std::atomic<uint32_t>)];
        T                     data;
        // Pad so the full Slot is a multiple of CACHE_LINE_SIZE.
        // This keeps data for adjacent slots on separate cache lines.
        static constexpr std::size_t data_pad =
            (sizeof(T) % CACHE_LINE_SIZE == 0)
                ? 0
                : (CACHE_LINE_SIZE - sizeof(T) % CACHE_LINE_SIZE);
        char _pad_data[data_pad == 0 ? CACHE_LINE_SIZE : data_pad];
    };

    // -------------------------------------------------------------------------
    // Control block — head (producers) and tail (consumer), separate lines.
    // -------------------------------------------------------------------------
    alignas(CACHE_LINE_SIZE) std::atomic<uint64_t> head_{0};
    char _pad_head[CACHE_LINE_SIZE - sizeof(std::atomic<uint64_t>)];

    alignas(CACHE_LINE_SIZE) std::atomic<uint64_t> tail_{0};
    char _pad_tail[CACHE_LINE_SIZE - sizeof(std::atomic<uint64_t>)];

    // -------------------------------------------------------------------------
    // Data slots
    // -------------------------------------------------------------------------
    Slot slots_[Capacity];

    // -------------------------------------------------------------------------
    // init() — must be called once by the creating process after mmap.
    // -------------------------------------------------------------------------
    static void init(MpscQueue* q) noexcept {
        q->head_.store(0, std::memory_order_relaxed);
        q->tail_.store(0, std::memory_order_relaxed);
        for (std::size_t i = 0; i < Capacity; ++i) {
            q->slots_[i].committed.store(0, std::memory_order_relaxed);
        }
        std::atomic_thread_fence(std::memory_order_seq_cst);
    }

    // -------------------------------------------------------------------------
    // try_push — producer side (safe for concurrent producers).
    // Returns false if the queue is full (non-blocking).
    // -------------------------------------------------------------------------
    [[nodiscard]] NANOMQ_FORCE_INLINE
    bool try_push(const T& item) noexcept {
        uint64_t head = head_.load(std::memory_order_relaxed);

        while (true) {
            const uint64_t tail = tail_.load(std::memory_order_acquire);
            if (NANOMQ_UNLIKELY(head - tail >= Capacity)) {
                return false;   // full
            }

            // Attempt to claim this slot
            if (head_.compare_exchange_weak(
                    head, head + 1,
                    std::memory_order_acq_rel,
                    std::memory_order_relaxed)) {
                break;  // claimed
            }
            // head was updated by compare_exchange_weak on failure — retry
        }

        Slot& slot = slots_[head & mask];

        // Write data — producer owns this slot exclusively after claiming
        std::memcpy(&slot.data, &item, sizeof(T));

        // Publish: consumer can now read
        slot.committed.store(1, std::memory_order_release);
        return true;
    }

    // -------------------------------------------------------------------------
    // try_pop — consumer side (single consumer only).
    // Returns false immediately if the next slot is not yet committed.
    // -------------------------------------------------------------------------
    [[nodiscard]] NANOMQ_FORCE_INLINE
    bool try_pop(T& item) noexcept {
        const uint64_t tail = tail_.load(std::memory_order_relaxed);
        Slot& slot = slots_[tail & mask];

        // Spin-check: has the producer committed to this slot yet?
        if (slot.committed.load(std::memory_order_acquire) == 0) {
            return false;   // not yet committed (either empty or mid-write)
        }

        std::memcpy(&item, &slot.data, sizeof(T));

        // Reset committed flag before advancing tail so producers see free space
        slot.committed.store(0, std::memory_order_release);
        tail_.store(tail + 1, std::memory_order_release);
        return true;
    }

    // -------------------------------------------------------------------------
    // Observers (approximate)
    // -------------------------------------------------------------------------
    [[nodiscard]] NANOMQ_FORCE_INLINE
    bool empty() const noexcept {
        const uint64_t tail = tail_.load(std::memory_order_relaxed);
        const uint64_t head = head_.load(std::memory_order_acquire);
        return head == tail;
    }

    [[nodiscard]] NANOMQ_FORCE_INLINE
    uint64_t size() const noexcept {
        const uint64_t head = head_.load(std::memory_order_acquire);
        const uint64_t tail = tail_.load(std::memory_order_acquire);
        return head - tail;
    }
};

} // namespace nanomq
