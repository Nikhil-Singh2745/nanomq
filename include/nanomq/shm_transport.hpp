#pragma once

#include "common.hpp"
#include "spsc_queue.hpp"

#include <sys/mman.h>    // mmap, munmap, MAP_SHARED, PROT_READ, PROT_WRITE
#include <sys/stat.h>    // mode constants
#include <fcntl.h>       // O_CREAT, O_RDWR, O_EXCL
#include <unistd.h>      // ftruncate, close, getpid
#include <cerrno>
#include <cstring>       // strerror
#include <cstdio>        // fprintf, stderr
#include <ctime>         // clock_gettime
#include <stdexcept>     // std::runtime_error
#include <string>
#include <utility>       // std::move
#include <atomic>
#include <cstdint>

namespace nanomq {

// ---------------------------------------------------------------------------
// Huge page support
//
// MAP_HUGETLB asks the kernel to back the mmap with 2MB huge pages instead
// of 4KB pages. This matters because:
//   - A 4096-slot queue of 64-byte messages = 256 KB of slot data alone.
//     That's 64 × 4KB pages = 64 TLB entries. A 2MB huge page covers the
//     entire region in a single TLB entry.
//   - TLB misses cost ~10–20 cycles each (L1 TLB miss) up to ~100+ cycles
//     (full page table walk). In the hot path this translates directly to
//     tail latency spikes.
//
// To enable huge pages on Linux:
//   echo 64 | sudo tee /proc/sys/vm/nr_hugepages
//
// On WSL2, huge page support is limited (kernel may not expose them).
// shm_transport falls back gracefully to normal pages if MAP_HUGETLB fails.
// ---------------------------------------------------------------------------

#ifdef MAP_HUGETLB
#  ifdef MAP_HUGE_2MB
#    define NANOMQ_HUGE_FLAGS (MAP_HUGETLB | MAP_HUGE_2MB)
#  else
#    define NANOMQ_HUGE_FLAGS MAP_HUGETLB
#  endif
#  define NANOMQ_HUGE_PAGES_AVAILABLE 1
#else
#  define NANOMQ_HUGE_FLAGS 0
#  define NANOMQ_HUGE_PAGES_AVAILABLE 0
#endif

// ---------------------------------------------------------------------------
// v3: ShmControlBlock
//
// A fixed-size header prepended before the QueueT in shared memory.
// Provides lifecycle coordination between the owner process and joiners
// without requiring any kernel synchronisation on the hot path.
//
// Layout:
//   [ShmControlBlock][padding to CACHE_LINE_SIZE][QueueT]
//
// Fields:
//   magic        : Sentinel value (0xNANOMQ42) — joiners check this before
//                  attaching to detect uninitialised or corrupted segments.
//   version      : Protocol version. Joiners reject mismatches.
//   owner_pid    : PID of the creating process. Joiners can check whether
//                  the owner is still alive (kill(pid, 0) == 0).
//   ref_count    : Number of processes currently attached. Creator initialises
//                  to 1; each joiner increments on open, decrements on close.
//                  Allows the last process to decide whether to unlink.
//   heartbeat_ns : Owner updates this periodically (e.g., every 100ms) with
//                  monotonic_ns(). Joiners can detect stale segments where the
//                  owner crashed without calling shm_unlink — if
//                  (now - heartbeat_ns) > STALE_THRESHOLD_NS, the segment is
//                  considered orphaned and may be reclaimed.
//   shutdown     : Set to 1 by the owner to signal all consumers/producers to
//                  drain and exit cleanly. Checked on the hot path by the demo
//                  application; not checked inside the lock-free queue itself
//                  (policy stays in user code, mechanism here).
//
// Note: all fields are std::atomic for safe cross-process visibility across
// the mmap'd region. This adds ~3 cache lines of overhead at the front of
// the segment — a one-time cost, not on the message hot path.
// ---------------------------------------------------------------------------

constexpr uint32_t SHM_MAGIC   = 0xA42042A4u;  // sentinel for segment validity
constexpr uint32_t SHM_VERSION = 3u;            // incremented each breaking change
constexpr uint64_t STALE_THRESHOLD_NS = 5'000'000'000ULL;  // 5 seconds

struct alignas(CACHE_LINE_SIZE) ShmControlBlock {
    // --- Slot 0: identity ---
    std::atomic<uint32_t> magic{0};
    std::atomic<uint32_t> version{0};
    std::atomic<int32_t>  owner_pid{0};
    char _pad0[CACHE_LINE_SIZE - 3 * sizeof(std::atomic<uint32_t>)];

    // --- Slot 1: lifecycle counters ---
    alignas(CACHE_LINE_SIZE) std::atomic<int32_t>  ref_count{0};
    char _pad1[CACHE_LINE_SIZE - sizeof(std::atomic<int32_t>)];

    // --- Slot 2: heartbeat (updated by owner) ---
    alignas(CACHE_LINE_SIZE) std::atomic<uint64_t> heartbeat_ns{0};
    char _pad2[CACHE_LINE_SIZE - sizeof(std::atomic<uint64_t>)];

    // --- Slot 3: shutdown flag ---
    alignas(CACHE_LINE_SIZE) std::atomic<uint32_t> shutdown{0};
    char _pad3[CACHE_LINE_SIZE - sizeof(std::atomic<uint32_t>)];
};

static_assert(sizeof(ShmControlBlock) == 4 * 64,
              "ShmControlBlock must be exactly 4 cache lines (256 bytes)");

// ---------------------------------------------------------------------------
// ShmRegion layout helpers
//
// The shared memory region is structured as:
//   [ShmControlBlock (256 bytes)] [QueueT (sizeof(QueueT) bytes)]
//
// We round the control block up to a multiple of CACHE_LINE_SIZE for
// alignment before the queue — already guaranteed by sizeof(ShmControlBlock).
// ---------------------------------------------------------------------------

constexpr std::size_t SHM_REGION_SIZE(std::size_t queue_size) noexcept {
    return sizeof(ShmControlBlock) + queue_size;
}

// ---------------------------------------------------------------------------
// ShmHandle<QueueT>
//
// RAII wrapper around a POSIX shared memory segment containing a QueueT.
//
// Creator path (create=true):
//   shm_open(O_CREAT|O_RDWR) → ftruncate → mmap → init ShmControlBlock →
//   QueueT::init() → mlock (optional)
//   On destruction: signal_shutdown(), munmap, shm_unlink (owns segment)
//
// Joiner path (create=false):
//   shm_open(O_RDWR) → mmap → validate magic/version → increment ref_count
//   On destruction: decrement ref_count, munmap (does not unlink)
//
// v3 additions:
//   - ShmControlBlock at front of segment (256 bytes overhead)
//   - signal_shutdown() / is_shutdown()
//   - beat_heartbeat()  — call periodically from owner's main loop
//   - is_stale()        — joiners call this to detect orphaned segments
//   - ref_count tracking for clean multi-process teardown
//
// Options:
//   use_mlock      : mlock(2) the region to prevent page faults on hot path.
//   use_huge_pages : back the mmap with 2MB huge pages (MAP_HUGETLB).
//
// Design notes:
//   - Non-copyable to prevent double-unmap/unlink.
//   - Movable so it can be returned from factory functions.
// ---------------------------------------------------------------------------

template <typename QueueT>
class ShmHandle {
public:
    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------
    explicit ShmHandle(const std::string& name,
                       bool create,
                       bool use_mlock      = false,
                       bool use_huge_pages = false)
        : name_(name), is_owner_(create), ctrl_(nullptr), queue_(nullptr)
        , fd_(-1), mmap_ptr_(nullptr)
        , shm_size_(SHM_REGION_SIZE(sizeof(QueueT)))
    {
        int flags = O_RDWR;
        if (create) flags |= O_CREAT | O_TRUNC;

        fd_ = ::shm_open(name.c_str(), flags, 0666);
        if (fd_ < 0) {
            throw std::runtime_error(
                std::string("shm_open(") + name + ") failed: " + ::strerror(errno));
        }

        if (create) {
            if (::ftruncate(fd_, static_cast<off_t>(shm_size_)) < 0) {
                ::close(fd_);
                ::shm_unlink(name.c_str());
                throw std::runtime_error(
                    std::string("ftruncate failed: ") + ::strerror(errno));
            }
        }

        void* ptr = try_mmap(use_huge_pages);
        if (ptr == MAP_FAILED) {
            ::close(fd_);
            if (create) ::shm_unlink(name.c_str());
            throw std::runtime_error(
                std::string("mmap failed: ") + ::strerror(errno));
        }

        ::close(fd_);
        fd_ = -1;

        mmap_ptr_ = ptr;
        ctrl_  = reinterpret_cast<ShmControlBlock*>(ptr);
        queue_ = reinterpret_cast<QueueT*>(
            reinterpret_cast<char*>(ptr) + sizeof(ShmControlBlock));

        if (create) {
            // Initialise control block
            ctrl_->magic.store(SHM_MAGIC,           std::memory_order_relaxed);
            ctrl_->version.store(SHM_VERSION,        std::memory_order_relaxed);
            ctrl_->owner_pid.store(static_cast<int32_t>(::getpid()),
                                                     std::memory_order_relaxed);
            ctrl_->ref_count.store(1,                std::memory_order_relaxed);
            ctrl_->heartbeat_ns.store(monotonic_ns(),std::memory_order_relaxed);
            ctrl_->shutdown.store(0,                 std::memory_order_relaxed);
            std::atomic_thread_fence(std::memory_order_seq_cst);

            // Initialise the queue
            QueueT::init(queue_);
        } else {
            // Validate that we're attaching to a valid, compatible segment
            if (ctrl_->magic.load(std::memory_order_acquire) != SHM_MAGIC) {
                ::munmap(ptr, shm_size_);
                throw std::runtime_error(
                    "shm segment '" + name + "' has invalid magic — not a NanoMQ segment");
            }
            if (ctrl_->version.load(std::memory_order_relaxed) != SHM_VERSION) {
                ::munmap(ptr, shm_size_);
                throw std::runtime_error(
                    "shm segment '" + name + "' version mismatch");
            }
            // Increment ref count
            ctrl_->ref_count.fetch_add(1, std::memory_order_acq_rel);
        }

        if (use_mlock) {
            if (::mlock(ptr, shm_size_) < 0) {
                ::fprintf(stderr,
                    "[nanomq] WARNING: mlock failed (%s). "
                    "Page faults may occur on hot path.\n",
                    ::strerror(errno));
            }
        }
    }

    // Non-copyable
    ShmHandle(const ShmHandle&)            = delete;
    ShmHandle& operator=(const ShmHandle&) = delete;

    // Movable
    ShmHandle(ShmHandle&& other) noexcept
        : name_(std::move(other.name_))
        , is_owner_(other.is_owner_)
        , ctrl_(other.ctrl_)
        , queue_(other.queue_)
        , fd_(other.fd_)
        , mmap_ptr_(other.mmap_ptr_)
        , shm_size_(other.shm_size_)
    {
        other.ctrl_     = nullptr;
        other.queue_    = nullptr;
        other.fd_       = -1;
        other.mmap_ptr_ = nullptr;
        other.is_owner_ = false;
    }

    ShmHandle& operator=(ShmHandle&& other) noexcept {
        if (this != &other) {
            cleanup();
            name_     = std::move(other.name_);
            is_owner_ = other.is_owner_;
            ctrl_     = other.ctrl_;
            queue_    = other.queue_;
            fd_       = other.fd_;
            mmap_ptr_ = other.mmap_ptr_;
            shm_size_ = other.shm_size_;
            other.ctrl_     = nullptr;
            other.queue_    = nullptr;
            other.fd_       = -1;
            other.mmap_ptr_ = nullptr;
            other.is_owner_ = false;
        }
        return *this;
    }

    ~ShmHandle() { cleanup(); }

    // -------------------------------------------------------------------------
    // Queue accessors
    // -------------------------------------------------------------------------
    [[nodiscard]] QueueT* queue() noexcept { return queue_; }
    [[nodiscard]] const QueueT* queue() const noexcept { return queue_; }

    [[nodiscard]] QueueT* operator->() noexcept { return queue_; }
    [[nodiscard]] const QueueT* operator->() const noexcept { return queue_; }

    [[nodiscard]] QueueT& operator*() noexcept { return *queue_; }
    [[nodiscard]] const QueueT& operator*() const noexcept { return *queue_; }

    [[nodiscard]] bool valid() const noexcept { return queue_ != nullptr; }
    [[nodiscard]] bool is_owner() const noexcept { return is_owner_; }
    [[nodiscard]] const std::string& name() const noexcept { return name_; }
    [[nodiscard]] std::size_t shm_size() const noexcept { return shm_size_; }

    // -------------------------------------------------------------------------
    // v3 Lifecycle API
    // -------------------------------------------------------------------------

    // signal_shutdown(): Owner calls this to tell all attached processes to
    // drain queues and exit. Does not forcibly terminate anything.
    void signal_shutdown() noexcept {
        if (ctrl_) ctrl_->shutdown.store(1, std::memory_order_release);
    }

    // is_shutdown(): Called by producers/consumers on their loop condition.
    [[nodiscard]] bool is_shutdown() const noexcept {
        if (!ctrl_) return true;
        return ctrl_->shutdown.load(std::memory_order_acquire) != 0;
    }

    // beat_heartbeat(): Owner calls periodically (e.g., every 100ms) so
    // joiners can detect a crash vs. a live owner.
    void beat_heartbeat() noexcept {
        if (ctrl_ && is_owner_)
            ctrl_->heartbeat_ns.store(monotonic_ns(), std::memory_order_release);
    }

    // is_stale(): Returns true if the heartbeat has not been updated in
    // STALE_THRESHOLD_NS nanoseconds — suggesting the owner crashed.
    [[nodiscard]] bool is_stale() const noexcept {
        if (!ctrl_) return true;
        const uint64_t last_beat = ctrl_->heartbeat_ns.load(std::memory_order_acquire);
        return (monotonic_ns() - last_beat) > STALE_THRESHOLD_NS;
    }

    // ref_count(): Current number of attached processes (approx — no fence).
    [[nodiscard]] int32_t ref_count() const noexcept {
        if (!ctrl_) return 0;
        return ctrl_->ref_count.load(std::memory_order_relaxed);
    }

    // owner_pid(): PID of the segment creator.
    [[nodiscard]] int32_t owner_pid() const noexcept {
        if (!ctrl_) return -1;
        return ctrl_->owner_pid.load(std::memory_order_relaxed);
    }

    // ctrl(): Direct access to the control block for inspection.
    [[nodiscard]] const ShmControlBlock* ctrl() const noexcept { return ctrl_; }

private:
    void* try_mmap(bool use_huge_pages) noexcept {
        void* ptr = MAP_FAILED;

#if NANOMQ_HUGE_PAGES_AVAILABLE
        if (use_huge_pages) {
            ptr = ::mmap(nullptr, shm_size_,
                         PROT_READ | PROT_WRITE,
                         MAP_SHARED | NANOMQ_HUGE_FLAGS,
                         fd_, 0);
            if (ptr == MAP_FAILED) {
                ::fprintf(stderr,
                    "[nanomq] WARNING: MAP_HUGETLB failed (%s). "
                    "Falling back to normal pages.\n"
                    "         To enable: echo 64 | sudo tee /proc/sys/vm/nr_hugepages\n",
                    ::strerror(errno));
            }
        }
#else
        if (use_huge_pages) {
            ::fprintf(stderr,
                "[nanomq] WARNING: Huge pages not available on this platform. "
                "Falling back to normal pages.\n");
        }
        (void)use_huge_pages;
#endif

        if (ptr == MAP_FAILED) {
            ptr = ::mmap(nullptr, shm_size_,
                         PROT_READ | PROT_WRITE, MAP_SHARED,
                         fd_, 0);
        }
        return ptr;
    }

    void cleanup() noexcept {
        if (ctrl_ && !is_owner_) {
            // Joiner: decrement reference count
            ctrl_->ref_count.fetch_sub(1, std::memory_order_acq_rel);
        }
        if (mmap_ptr_ != nullptr) {
            ::munmap(mmap_ptr_, shm_size_);
            mmap_ptr_ = nullptr;
            ctrl_     = nullptr;
            queue_    = nullptr;
        }
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
        if (is_owner_ && !name_.empty()) {
            ::shm_unlink(name_.c_str());
            is_owner_ = false;
        }
    }

    std::string        name_;
    bool               is_owner_;
    ShmControlBlock*   ctrl_;
    QueueT*            queue_;
    int                fd_;
    void*              mmap_ptr_;
    std::size_t        shm_size_;
};

// ---------------------------------------------------------------------------
// Convenience factory functions
// ---------------------------------------------------------------------------

template <typename QueueT>
[[nodiscard]] ShmHandle<QueueT> shm_create(
    const std::string& name,
    bool use_mlock      = false,
    bool use_huge_pages = false)
{
    return ShmHandle<QueueT>(name, /*create=*/true, use_mlock, use_huge_pages);
}

template <typename QueueT>
[[nodiscard]] ShmHandle<QueueT> shm_open_existing(
    const std::string& name,
    bool use_mlock      = false,
    bool use_huge_pages = false)
{
    return ShmHandle<QueueT>(name, /*create=*/false, use_mlock, use_huge_pages);
}

} // namespace nanomq
