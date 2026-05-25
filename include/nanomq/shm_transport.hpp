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
// Design notes:
//   - Non-copyable to prevent double-unmap/unlink.
//   - Movable so it can be returned from factory functions.
//   - mlock is optional (requires RLIMIT_MEMLOCK or CAP_IPC_LOCK). Off by
//     default in v1 to avoid needing elevated privileges.
// ---------------------------------------------------------------------------

template <typename QueueT>
class ShmHandle {
public:
    // -------------------------------------------------------------------------
    // Constructor — opens or creates the shared memory segment.
    //
    // name    : POSIX shm name, e.g. "/nanomq_spsc". Must start with '/'.
    // create  : true = creator (will init queue and own lifetime of segment)
    //           false = joiner (attaches to existing segment)
    // use_mlock : lock pages into RAM. Default off; see note above.
    // -------------------------------------------------------------------------
    explicit ShmHandle(const std::string& name, bool create, bool use_mlock = false)
        : name_(name), is_owner_(create), queue_(nullptr), fd_(-1), mmap_ptr_(nullptr)
    {
        constexpr std::size_t shm_size = sizeof(QueueT);

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
            if (::ftruncate(fd_, static_cast<off_t>(shm_size)) < 0) {
                ::close(fd_);
                ::shm_unlink(name.c_str());
                throw std::runtime_error(
                    std::string("ftruncate failed: ") + ::strerror(errno));
            }
        }

        // Map into our address space
        void* ptr = ::mmap(nullptr, shm_size,
                           PROT_READ | PROT_WRITE, MAP_SHARED,
                           fd_, 0);
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
            if (::mlock(ptr, shm_size) < 0) {
                // Non-fatal in v1 — warn and continue
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

private:
    void cleanup() noexcept {
        if (mmap_ptr_ != nullptr) {
            ::munmap(mmap_ptr_, sizeof(QueueT));
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

    std::string name_;
    bool        is_owner_;
    QueueT*     queue_;
    int         fd_;
    void*       mmap_ptr_;
};

// ---------------------------------------------------------------------------
// Convenience factory functions
// ---------------------------------------------------------------------------

template <typename QueueT>
[[nodiscard]] ShmHandle<QueueT> shm_create(const std::string& name, bool use_mlock = false) {
    return ShmHandle<QueueT>(name, /*create=*/true, use_mlock);
}

template <typename QueueT>
[[nodiscard]] ShmHandle<QueueT> shm_open_existing(const std::string& name, bool use_mlock = false) {
    return ShmHandle<QueueT>(name, /*create=*/false, use_mlock);
}

} // namespace nanomq
