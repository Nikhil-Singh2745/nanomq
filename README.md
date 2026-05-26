# NanoMQ

Lock-free inter-process message queue over POSIX shared memory, built from scratch in C++20.

![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)
![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)

---

## Key Metrics — SPSC

| Metric  | WSL2 (Intel i3-1315U) | Native Fedora (Ryzen 9 7900 - Isolated) |
|---------|-----------------------|-----------------------------------------|
| min     | 18 ns                 | **7.1 ns**                              |
| p50     | ~40 µs                | **11.2 ns**                             |
| p99     | ~238 µs               | **24.5 ns**                             |

> We test across two environments: a thin-and-light laptop running WSL2 (intentionally constrained to observe scheduler behavior) and a native Fedora desktop (Ryzen 9 7900) configured with core isolation (`isolcpus`) and huge pages. On native hardware with isolated cores, the p50 and p99 stay extremely low (~11.2 ns and ~24.5 ns), showcasing the queue's true lock-free potential.

---

## What is this?

NanoMQ is a lock-free SPSC/MPSC ring buffer over POSIX shared memory (`shm_open`/`mmap`). It eliminates the kernel from the messaging hot path entirely — no syscalls, no locks, no copies beyond the slot write. Built as an HFT systems project to demonstrate practical knowledge of cache coherence, memory ordering, and why the kernel is the enemy of latency.

---

## Architecture

```
Producer Process                  Consumer Process
┌──────────────┐                  ┌──────────────┐
│  try_push()  │                  │  try_pop()   │
│      │       │                  │      ▲       │
│      ▼       │                  │      │       │
│  ┌────────┐  │   /dev/shm       │  ┌────────┐  │
│  │  head  │──┼──►┌──────────┐◄──┼──│  tail  │  │
│  └────────┘  │   │ Slot [0] │   │  └────────┘  │
│  (cacheline) │   │ Slot [1] │   │  (cacheline) │
│              │   │   ...    │   │              │
│              │   │ Slot [N] │   │              │
│              │   └──────────┘   │              │
└──────────────┘   (mmap region)  └──────────────┘
```

- `head` and `tail` are `std::atomic<uint64_t>`, each on its **own 64-byte cache line** — false sharing eliminated.
- Slots are `alignas(64)`, one message per cache line — no inter-slot interference.
- Ring size is always a power of 2 — `index & (capacity - 1)` instead of `%`.
- Memory ordering: `acquire` loads / `release` stores — **no `seq_cst`**, no unnecessary fences.
- The queue struct is a pure POD region: no vtable, no heap allocation — lives directly in `mmap`'d memory.

---

## Features

- **Lock-free SPSC** — single-producer/single-consumer, zero locks on the hot path
- **Lock-free MPSC** — multi-producer/single-consumer via CAS slot-claim + per-slot committed flag
- **POSIX shared memory** — `shm_open` + `mmap`, works across processes, no kernel involvement per message
- **Huge page support** — `MAP_HUGETLB | MAP_HUGE_2MB` with graceful fallback; eliminates TLB pressure on large queues
- **`mlock` support** — optional page locking to prevent hot-path page faults
- **Zero-copy API** — `prepare()`/`commit()` and `peek()`/`consume()` write/read directly into ring slots
- **Batch push/pop** — `try_push_batch` / `try_pop_batch` amortize index cache-coherence costs over runs
- **Zero external dependencies** — POSIX only; no Boost, no abseil, nothing
- **Cache-line isolation** — `head`/`tail` on separate lines; slots aligned to prevent inter-slot false sharing
- **Precise latency measurement** — inline `rdtsc` + TSC calibration; percentile reporting (p50/p99/p99.9/p99.99)
- **Strict memory ordering** — acquire/release only; `seq_cst` is explicitly banned (see [BLOG.md](BLOG.md))
- **RAII transport handle** — `ShmHandle<Q>` owns the segment lifetime; creator unlinks on destruction
- **Optional message envelope** — `MsgHeader` with sequence, timestamp, payload_size, flags; zero-overhead fixed-T path remains default
- **C++20** — concepts, `requires`, no legacy workarounds

---

## Benchmark Results

### NanoMQ vs Kernel IPC (SPSC)

We evaluate SPSC queue latency under two different configurations to show how bare-metal core-isolation and hypervisor-constrained CPU sharing affect lock-free ring buffers.

#### Environment A: Bare-Metal Desktop (Native Fedora 40, Ryzen 9 7900, DDR5 6000MHz)
*64-byte messages, 2M samples, pinned to cores on the same CCX (Cores 2 & 4), `-O3 -march=native`*
*Pipe/socket/TCP use RTT/2 as one-way latency proxy. NanoMQ SPSC measured with direct `rdtsc`.*

| Method | min (ns) | p50 (ns) | p99 (ns) | p99.9 (ns) | mean (ns) |
|:---|:---|:---|:---|:---|:---|
| **NanoMQ SPSC (Isolated Cores)** | **7.1** | **11.2** | **24.5** | **48.2** | **12.4** |
| **NanoMQ SPSC (Standard Cores)** | **7.8** | **85** | **1,820** | **8,540** | **110** |
| Unix pipe | 580 | 3,240 | 8,420 | 12,800 | 3,550 |
| Unix domain socket | 1,850 | 4,210 | 11,400 | 18,900 | 4,680 |
| TCP loopback | 2,120 | 5,880 | 15,300 | 28,400 | 6,240 |

> **CCX Locality & Isolation**: With `isolcpus` active, NanoMQ stays entirely within the Ryzen L3 cache hierarchy without yielding or preemption. SPSC p99 latency remains at a pristine 24.5 ns.

#### Environment B: Hypervisor-Constrained Laptop (WSL2, Intel i3-1315U, Linux 6.6)
*64-byte messages, 500K samples, pinned to CPU 0 & 1, `-O3 -march=native`*

| Method | min (ns) | p50 (ns) | p99 (ns) | p99.9 (ns) | mean (ns) |
|:---|:---|:---|:---|:---|:---|
| **NanoMQ SPSC** | **18** | **39,712** | **238,103** | **240,597** | **68,417** |
| Unix pipe | 725 | 12,823 | 33,332 | 66,965 | 13,644 |
| Unix domain socket | 4,150 | 14,586 | 52,044 | 115,858 | 17,872 |
| TCP loopback | 3,977 | 16,733 | 67,233 | 179,763 | 22,545 |

> **Why NanoMQ's WSL2 p50 looks worse than pipe**: Under high-contention hypervisor environments, NanoMQ's busy-spin polling is vulnerable to CPU preemption. A blocking `read()` on a pipe yields CPU control, allowing the OS scheduler to wake it efficiently. In contrast, bare metal with isolation ensures NanoMQ never yields, letting it run 200–300× faster at the median.

### MPSC Scaling

We scale the producers to measure the lock-free CAS-claim algorithm under contention.

#### AMD Ryzen 9 7900 (Native Fedora 40 - 2M samples total)
Producers and consumer are pinned within the same CCX for 2/4 producers, and cross the CCX boundary for 8 producers.

| Producers | min (ns) | p50 (ns) | p99 (ns) | mean (ns) | CCX Boundary |
|:---|:---|:---|:---|:---|:---|
| 2 Producers | 7.9 | 42 | 115 | 48 | Same CCX (Core-Local) |
| 4 Producers | 11.2 | 64 | 172 | 76 | Same CCX (Core-Local) |
| 8 Producers | 18.5 | 188 | 435 | 202 | Cross-CCX (Infinity Fabric) |

#### Intel i3-1315U (WSL2 - 200K samples per producer)

| Producers | min (ns) | p50 (ns) | p99 (ns) | mean (ns) |
|:---|:---|:---|:---|:---|
| 2 Producers | 15 | 206 | 174,916 | 7,863 |
| 4 Producers | 29 | 267 | 38,734 | 1,897 |
| 8 Producers | 38 | 2,891,250 | — | — |

---

## Quick Start

**Requirements:** CMake 3.16+, g++ 13+ or clang 16+ (C++20), Linux.

```bash
git clone <repo>
cd nanomq

# Build all targets
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel

# Correctness tests
ctest --test-dir build --output-on-failure

# SPSC benchmark: 2M samples, 3 message sizes (64B / 256B / 1KB)
./build/bench_spsc 0 1 2000000

# MPSC benchmark: 2, 4, 8 producers
./build/bench_mpsc 0 500000

# Baseline comparison: NanoMQ vs pipe vs socket vs TCP
./build/bench_baseline 500000

# Inter-process example (two terminals)
./build/producer 500000 /nanomq_demo   # terminal 1
./build/consumer /nanomq_demo          # terminal 2

# ThreadSanitizer build
cmake -B build_tsan -DTSAN=ON
cmake --build build_tsan --parallel
./build_tsan/test_spsc && ./build_tsan/test_mpsc
```

**Enabling huge pages (bare metal):**
```bash
echo 64 | sudo tee /proc/sys/vm/nr_hugepages
# Then use ShmHandle with use_huge_pages=true in your code
```

---

## API Overview

### Standard push/pop

```cpp
#include "nanomq/spsc_queue.hpp"
#include "nanomq/shm_transport.hpp"

struct Msg { uint64_t seq; uint64_t tsc; char data[48]; };
using Queue = nanomq::SpscQueue<Msg, 65536>;  // must be power of 2

// --- Producer process ---
auto shm = nanomq::shm_create<Queue>("/my_queue");
Msg m{.seq = 42, .tsc = nanomq::rdtsc()};
while (!shm->try_push(m)) {}  // spin; returns false only when full

// --- Consumer process ---
auto shm = nanomq::shm_open_existing<Queue>("/my_queue");
Msg m{};
while (!shm->try_pop(m)) {}   // spin; returns false only when empty
```

### Batch push/pop (v2)

```cpp
std::array<Msg, 64> burst;
// ... fill burst ...
std::size_t sent = shm->try_push_batch(burst.data(), burst.size());

Msg drain[64];
std::size_t got = shm->try_pop_batch(drain, 64);
```

### Zero-copy write/read (v2)

```cpp
// Producer: write directly into ring slot, no memcpy
if (Msg* slot = shm->prepare()) {
    slot->seq = next_seq++;
    slot->tsc = nanomq::rdtsc();
    shm->commit();   // publish
}

// Consumer: read directly from ring slot, no memcpy
if (const Msg* slot = shm->peek()) {
    process(*slot);
    shm->consume();  // advance tail
}
```

### MPSC (v2)

```cpp
#include "nanomq/mpsc_queue.hpp"
using MQ = nanomq::MpscQueue<Msg, 65536>;
auto shm = nanomq::shm_create<MQ>("/mpsc_queue");

// Multiple producer threads (each calls try_push concurrently — safe)
// Single consumer thread calls try_pop
```

---

## Design Decisions

See [BLOG.md](BLOG.md) for the full engineering journal:
- Why power-of-2 and not `%` (latency, not aesthetics)
- Memory ordering choices — the exact acquire/release contract and why `seq_cst` is banned
- `shm_open` vs `memfd_create` tradeoff
- False sharing: what the padding arrays actually do
- TSC calibration and why you don't use `CLOCK_MONOTONIC` on the hot path
- Huge pages: TLB math and why 2MB pages matter at queue scale
- MPSC two-phase commit: CAS claim + per-slot committed flag, and the preemption edge case
- The comparison chart: what the numbers mean on WSL2 vs bare metal

---

## License

MIT — see [LICENSE](LICENSE).
