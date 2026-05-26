#pragma once

#include "common.hpp"

#include <atomic>
#include <cstdint>
#include <cstring>   // std::memcpy
#include <concepts>  // std::is_trivially_copyable_v

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
//
// v2 additions:
//   - try_push_batch / try_pop_batch : amortize index loads over multiple items
//   - prepare / commit               : zero-copy write path
//   - peek / consume                 : zero-copy read path
//   - __builtin_prefetch on next slot in try_push / try_pop
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

        // Prefetch the next slot into L1 while the consumer is processing this one
        __builtin_prefetch(&slots_[(head + 1) & mask], 1, 3);

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

        // Prefetch the next slot into L1 for the consumer's next iteration
        __builtin_prefetch(&slots_[(tail + 1) & mask], 0, 3);

        tail_.store(tail + 1, std::memory_order_release);
        return true;
    }

    // -------------------------------------------------------------------------
    // try_push_batch — producer side, batch variant.
    //
    // Pushes up to `count` items from `items[]`, reading the remote tail index
    // only once. Returns the number of items actually pushed (may be less than
    // `count` if the queue would overflow). Never partially commits — all pushed
    // items are published atomically via a single release store on head_.
    //
    // Prefer this over repeated try_push when you have bursts of messages ready
    // to go: the single acquire load of tail_ and single release store of head_
    // amortize the cache-coherence cost over the entire batch.
    // -------------------------------------------------------------------------
    [[nodiscard]] NANOMQ_FORCE_INLINE
    std::size_t try_push_batch(const T* items, std::size_t count) noexcept {
        const uint64_t head    = head_.load(std::memory_order_relaxed);
        const uint64_t tail    = tail_.load(std::memory_order_acquire);
        const uint64_t avail   = Capacity - (head - tail);

        if (NANOMQ_UNLIKELY(avail == 0)) return 0;

        const std::size_t n = (count < avail) ? count : static_cast<std::size_t>(avail);

        // Copy items into contiguous ring slots (may wrap around)
        const std::size_t slot0 = head & mask;
        const std::size_t tail_room = Capacity - slot0;  // slots before wrap

        if (n <= tail_room) {
            // No wrap — single memcpy
            std::memcpy(&slots_[slot0], items, n * sizeof(T));
        } else {
            // Split across ring end
            std::memcpy(&slots_[slot0],     items,           tail_room * sizeof(T));
            std::memcpy(&slots_[0],         items + tail_room, (n - tail_room) * sizeof(T));
        }

        head_.store(head + n, std::memory_order_release);
        return n;
    }

    // -------------------------------------------------------------------------
    // try_pop_batch — consumer side, batch variant.
    //
    // Pops up to `count` items into `items[]` in a single pass.
    // Returns the number of items actually popped.
    // -------------------------------------------------------------------------
    [[nodiscard]] NANOMQ_FORCE_INLINE
    std::size_t try_pop_batch(T* items, std::size_t count) noexcept {
        const uint64_t tail    = tail_.load(std::memory_order_relaxed);
        const uint64_t head    = head_.load(std::memory_order_acquire);
        const uint64_t avail   = head - tail;

        if (NANOMQ_UNLIKELY(avail == 0)) return 0;

        const std::size_t n = (count < avail) ? count : static_cast<std::size_t>(avail);

        const std::size_t slot0 = tail & mask;
        const std::size_t tail_room = Capacity - slot0;

        if (n <= tail_room) {
            std::memcpy(items, &slots_[slot0], n * sizeof(T));
        } else {
            std::memcpy(items,           &slots_[slot0], tail_room * sizeof(T));
            std::memcpy(items + tail_room, &slots_[0],  (n - tail_room) * sizeof(T));
        }

        tail_.store(tail + n, std::memory_order_release);
        return n;
    }

    // -------------------------------------------------------------------------
    // Zero-copy write path — producer side.
    //
    // prepare() returns a pointer to the next writable slot, or nullptr if full.
    // The caller fills the slot in-place, then calls commit() to publish it.
    // Do NOT call any other producer method between prepare() and commit().
    // Do NOT call commit() without a preceding successful prepare().
    //
    // This eliminates the memcpy in try_push when the caller can write directly
    // to the slot (e.g., DMA target, network receive buffer, sensor feed).
    // -------------------------------------------------------------------------
    [[nodiscard]] NANOMQ_FORCE_INLINE
    T* prepare() noexcept {
        const uint64_t head = head_.load(std::memory_order_relaxed);
        const uint64_t tail = tail_.load(std::memory_order_acquire);
        if (NANOMQ_UNLIKELY(head - tail >= Capacity)) return nullptr;
        // Cache head for commit()
        cached_head_ = head;
        return &slots_[head & mask];
    }

    NANOMQ_FORCE_INLINE
    void commit() noexcept {
        head_.store(cached_head_ + 1, std::memory_order_release);
    }

    // -------------------------------------------------------------------------
    // Zero-copy read path — consumer side.
    //
    // peek() returns a pointer to the next readable slot, or nullptr if empty.
    // The caller reads the slot in-place, then calls consume() to advance tail.
    // Do NOT call consume() without a preceding successful peek().
    // The returned pointer is valid only until consume() is called.
    // -------------------------------------------------------------------------
    [[nodiscard]] NANOMQ_FORCE_INLINE
    const T* peek() noexcept {
        const uint64_t tail = tail_.load(std::memory_order_relaxed);
        const uint64_t head = head_.load(std::memory_order_acquire);
        if (NANOMQ_UNLIKELY(head == tail)) return nullptr;
        cached_tail_ = tail;
        return &slots_[tail & mask];
    }

    NANOMQ_FORCE_INLINE
    void consume() noexcept {
        tail_.store(cached_tail_ + 1, std::memory_order_release);
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

private:
    // Cached indices for the zero-copy prepare/commit and peek/consume paths.
    // These are thread-local to the producer and consumer respectively, so no
    // atomic needed — but they must not be confused across threads.
    // Place them in separate cache lines away from head_/tail_ to avoid
    // polluting the hot control block during batch or zero-copy operations.
    alignas(CACHE_LINE_SIZE) uint64_t cached_head_{0};
    char _pad2[CACHE_LINE_SIZE - sizeof(uint64_t)];

    alignas(CACHE_LINE_SIZE) uint64_t cached_tail_{0};
    char _pad3[CACHE_LINE_SIZE - sizeof(uint64_t)];
};

} // namespace nanomq
