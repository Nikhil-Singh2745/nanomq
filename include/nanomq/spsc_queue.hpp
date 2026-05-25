#pragma once

#include "common.hpp"

#include <atomic>
#include <cstdint>
#include <cstring>   // std::memcpy
#include <concepts>  // std::is_trivially_copyable_v via concept

namespace nanomq {

// ---------------------------------------------------------------------------
// SpscQueue<T, Capacity>
//
// Lock-free Single-Producer / Single-Consumer ring buffer.
//
// Design invariants:
//   - Capacity MUST be a power of two (asserted at compile time).
//   - T must be trivially copyable (no move semantics in the hot path — we
//     operate directly on mmap'd memory, so destructors must be no-ops).
//   - The entire struct is a POD-like region: no vtable, no heap allocation.
//     It is designed to be placement-new'd directly into a mmap'd region.
//   - head_ is written exclusively by the producer.
//   - tail_ is written exclusively by the consumer.
//   - Each lives on its own cache line to eliminate false sharing.
//
// Memory ordering rationale (see BLOG.md):
//   - Producer: relaxed load of own head_ (only writer), acquire load of
//     tail_ (to check space — ensures we see consumer's writes), writes
//     slot data (plain store — no concurrent reader yet), then release
//     store of head_ (publishes the new item to the consumer).
//   - Consumer: relaxed load of own tail_, acquire load of head_ (to see
//     items — pairs with producer's release store), reads slot data, then
//     release store of tail_ (publishes that slot is free to producer).
//   - No seq_cst: it inserts a full memory fence on x86, doubling the cost
//     of atomic operations for no correctness benefit here.
// ---------------------------------------------------------------------------

template <typename T, std::size_t Capacity>
    requires std::is_trivially_copyable_v<T>
struct SpscQueue {
    static_assert(is_power_of_two_v<Capacity>,
                  "SpscQueue: Capacity must be a power of two");
    static_assert(sizeof(T) > 0, "SpscQueue: T must have non-zero size");

    // -------------------------------------------------------------------------
    // Public constants
    // -------------------------------------------------------------------------
    static constexpr std::size_t capacity = Capacity;
    static constexpr std::size_t mask     = Capacity - 1;

    // -------------------------------------------------------------------------
    // Control block — two indices, each isolated on its own cache line.
    // -------------------------------------------------------------------------
    alignas(CACHE_LINE_SIZE) std::atomic<uint64_t> head_{0};  // producer's write cursor
    char _pad0[CACHE_LINE_SIZE - sizeof(std::atomic<uint64_t>)];

    alignas(CACHE_LINE_SIZE) std::atomic<uint64_t> tail_{0};  // consumer's read cursor
    char _pad1[CACHE_LINE_SIZE - sizeof(std::atomic<uint64_t>)];

    // -------------------------------------------------------------------------
    // Data slots — each slot is cache-line aligned to prevent false sharing
    // between adjacent slots accessed by producer and consumer simultaneously.
    // -------------------------------------------------------------------------
    alignas(CACHE_LINE_SIZE) T slots_[Capacity];

    // -------------------------------------------------------------------------
    // init() — must be called once by the creating process after mmap.
    // Placement new on mmap memory does not zero-initialize, so we do it here.
    // -------------------------------------------------------------------------
    static void init(SpscQueue* q) noexcept {
        q->head_.store(0, std::memory_order_relaxed);
        q->tail_.store(0, std::memory_order_relaxed);
        // Slots need not be zeroed — we write before we read.
        // A full memory fence here ensures both processes see initialized state
        // before the consumer opens the segment.
        std::atomic_thread_fence(std::memory_order_seq_cst);
    }

    // -------------------------------------------------------------------------
    // try_push — producer side.
    // Returns false immediately if the queue is full (non-blocking).
    // -------------------------------------------------------------------------
    [[nodiscard]] NANOMQ_FORCE_INLINE
    bool try_push(const T& item) noexcept {
        const uint64_t head = head_.load(std::memory_order_relaxed);
        const uint64_t tail = tail_.load(std::memory_order_acquire);

        if (NANOMQ_UNLIKELY(head - tail >= Capacity)) {
            return false;  // full
        }

        std::memcpy(&slots_[head & mask], &item, sizeof(T));

        head_.store(head + 1, std::memory_order_release);
        return true;
    }

    // -------------------------------------------------------------------------
    // try_pop — consumer side.
    // Returns false immediately if the queue is empty (non-blocking).
    // -------------------------------------------------------------------------
    [[nodiscard]] NANOMQ_FORCE_INLINE
    bool try_pop(T& item) noexcept {
        const uint64_t tail = tail_.load(std::memory_order_relaxed);
        const uint64_t head = head_.load(std::memory_order_acquire);

        if (NANOMQ_UNLIKELY(head == tail)) {
            return false;  // empty
        }

        std::memcpy(&item, &slots_[tail & mask], sizeof(T));

        tail_.store(tail + 1, std::memory_order_release);
        return true;
    }

    // -------------------------------------------------------------------------
    // Observers (approximate — not linearizable across producer/consumer)
    // -------------------------------------------------------------------------
    [[nodiscard]] NANOMQ_FORCE_INLINE
    bool empty() const noexcept {
        const uint64_t tail = tail_.load(std::memory_order_relaxed);
        const uint64_t head = head_.load(std::memory_order_acquire);
        return head == tail;
    }

    [[nodiscard]] NANOMQ_FORCE_INLINE
    bool full() const noexcept {
        const uint64_t head = head_.load(std::memory_order_relaxed);
        const uint64_t tail = tail_.load(std::memory_order_acquire);
        return (head - tail) >= Capacity;
    }

    [[nodiscard]] NANOMQ_FORCE_INLINE
    uint64_t size() const noexcept {
        const uint64_t head = head_.load(std::memory_order_acquire);
        const uint64_t tail = tail_.load(std::memory_order_acquire);
        return head - tail;  // wraps naturally; result is always in [0, Capacity]
    }
};

} // namespace nanomq
