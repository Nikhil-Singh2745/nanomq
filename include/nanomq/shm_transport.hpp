#pragma once

#include "common.hpp"
#include "spsc_queue.hpp"

#include <sys/mman.h>    // mmap, munmap, MAP_SHARED, PROT_READ, PROT_WRITE
#include <sys/stat.h>    // mode constants
#include <fcntl.h>       // O_CREAT, O_RDWR, O_EXCL
#include <unistd.h>      // ftruncate, close
#include <cerrno>
#include <cstring>       // strerror
#include <cstdio>        // fprintf, stderr
#include <stdexcept>     // std::runtime_error
#include <string>
#include <utility>       // std::move

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
// ShmHandle<QueueT>
//
// RAII wrapper around a POSIX shared memory segment containing a QueueT.
//
// Creator path (create=true):
//   shm_open(O_CREAT|O_RDWR) → ftruncate → mmap → QueueT::init()
//   On destruction: munmap + shm_unlink (owns the segment)
//
// Joiner path (create=false):
//   shm_open(O_RDWR) → mmap
//   On destruction: munmap only (does not unlink — creator owns lifetime)
//
// Options (v2):
//   use_mlock      : mlock(2) the region to prevent page faults on hot path.
//                    Requires RLIMIT_MEMLOCK or CAP_IPC_LOCK (or root).
//   use_huge_pages : back the mmap with 2MB huge pages (MAP_HUGETLB).
//                    Falls back to normal pages if the kernel refuses.
//
// Design notes:
//   - Non-copyable to prevent double-unmap/unlink.
//   - Movable so it can be returned from factory functions.
// ---------------------------------------------------------------------------

template <typename QueueT>
class ShmHandle {
public:
    // -------------------------------------------------------------------------
    // Constructor — opens or creates the shared memory segment.
    //
    // name           : POSIX shm name, e.g. "/nanomq_spsc". Must start with '/'.
    // create         : true = creator (will init queue and own lifetime of segment)
    //                  false = joiner (attaches to existing segment)
    // use_mlock      : lock pages into RAM. Needs elevated privileges or high
    //                  RLIMIT_MEMLOCK. Warn and continue if unavailable.
    // use_huge_pages : request MAP_HUGETLB for 2MB huge pages. Gracefully falls
    //                  back to normal pages if the kernel refuses.
    // -------------------------------------------------------------------------
    explicit ShmHandle(const std::string& name,
                       bool create,
                       bool use_mlock      = false,
                       bool use_huge_pages = false)
        : name_(name), is_owner_(create), queue_(nullptr), fd_(-1), mmap_ptr_(nullptr)
        , shm_size_(sizeof(QueueT))
    {
        int flags = O_RDWR;
        if (create) flags |= O_CREAT | O_TRUNC;

        // Open / create the shared memory object
        fd_ = ::shm_open(name.c_str(), flags, 0666);
        if (fd_ < 0) {
            throw std::runtime_error(
                std::string("shm_open(") + name + ") failed: " + ::strerror(errno));
        }

        if (create) {
            // Size the segment to exactly fit the queue
            if (::ftruncate(fd_, static_cast<off_t>(shm_size_)) < 0) {
                ::close(fd_);
                ::shm_unlink(name.c_str());
                throw std::runtime_error(
                    std::string("ftruncate failed: ") + ::strerror(errno));
            }
        }

        // Map into our address space — attempt huge pages first if requested
        void* ptr = try_mmap(use_huge_pages);
        if (ptr == MAP_FAILED) {
            ::close(fd_);
            if (create) ::shm_unlink(name.c_str());
            throw std::runtime_error(
                std::string("mmap failed: ") + ::strerror(errno));
        }

        // fd is no longer needed after mmap on Linux
        ::close(fd_);
        fd_ = -1;

        mmap_ptr_ = ptr;
        queue_ = reinterpret_cast<QueueT*>(ptr);

        if (create) {
            QueueT::init(queue_);
        }

        if (use_mlock) {
            if (::mlock(ptr, shm_size_) < 0) {
                // Non-fatal — warn and continue
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
        , queue_(other.queue_)
        , fd_(other.fd_)
        , mmap_ptr_(other.mmap_ptr_)
        , shm_size_(other.shm_size_)
    {
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
            queue_    = other.queue_;
            fd_       = other.fd_;
            mmap_ptr_ = other.mmap_ptr_;
            shm_size_ = other.shm_size_;
            other.queue_    = nullptr;
            other.fd_       = -1;
            other.mmap_ptr_ = nullptr;
            other.is_owner_ = false;
        }
        return *this;
    }

    ~ShmHandle() { cleanup(); }

    // -------------------------------------------------------------------------
    // Accessors
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

private:
    // -------------------------------------------------------------------------
    // try_mmap — attempt huge pages, fall back to normal pages gracefully.
    // -------------------------------------------------------------------------
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
                // Fall through to normal mmap below
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
        if (mmap_ptr_ != nullptr) {
            ::munmap(mmap_ptr_, shm_size_);
            mmap_ptr_ = nullptr;
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

    std::string  name_;
    bool         is_owner_;
    QueueT*      queue_;
    int          fd_;
    void*        mmap_ptr_;
    std::size_t  shm_size_;
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
