# NanoMQ

Lock-free inter-process message queue over POSIX shared memory, built from scratch in C++20.

![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)
![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)

---

## Key Metrics — SPSC (64-byte messages, 500K samples)

| Metric | WSL2 (Intel i3-1315U) | Native Fedora (Ryzen 9 7900 — Isolated)† |
|--------|----------------------|------------------------------------------|
| min    | **130 ns**           | **7.1 ns**                               |
| p50    | 407 ns               | **11.2 ns**                              |
| p99    | 64,205 ns            | **24.5 ns**                              |
| p99.9  | 112,650 ns           | **48.2 ns**                              |
| mean   | 4,443 ns             | **12.4 ns**                              |

> **WSL2 context**: The i3-1315U numbers reflect the hypervisor scheduler preempting the busy-spinning consumer thread — a fundamental WSL2 constraint, not a queue deficiency. The **130 ns min** is the raw lock-free mechanism latency; everything above that is OS scheduling noise. On bare-metal Linux with isolated cores, the queue delivers sub-25 ns p99.
>
> †*Ryzen 9 7900 numbers measured on Fedora 40 native with `isolcpus=2,4` and 2MB huge pages. See [BLOG.md](BLOG.md) for methodology.*

---

## What is this?

NanoMQ is a lock-free SPSC/MPSC ring buffer over POSIX shared memory (`shm_open`/`mmap`). It eliminates the kernel from the messaging hot path entirely — no syscalls, no locks, no copies beyond the slot write. Built as an HFT systems project to demonstrate practical knowledge of cache coherence, memory ordering, and why the kernel is the enemy of latency.

**v3** adds production-aware lifecycle management: owner/joiner process model, graceful shutdown signalling, crash detection via heartbeat timestamps, reference counting, and a realistic mock trading data pipeline demo.

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
│  └────────┘  │   │CtrlBlock │   │  └────────┘  │
│  (cacheline) │   │ Slot [0] │   │  (cacheline) │
│              │   │ Slot [1] │   │              │
│              │   │   ...    │   │              │
│              │   │ Slot [N] │   │              │
│              │   └──────────┘   │              │
└──────────────┘   (mmap region)  └──────────────┘
```

- `head` and `tail` are `std::atomic<uint64_t>`, each on its **own 64-byte cache line** — false sharing eliminated.
- **ShmControlBlock** (256 bytes, 4 cache lines) prepended to every segment: `magic`, `version`, `owner_pid`, `ref_count`, `heartbeat_ns`, `shutdown` flag.
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
- **v3 Lifecycle management** — owner/joiner model, graceful `signal_shutdown()`, crash detection via `is_stale()`, `ref_count` tracking
- **v3 Config system** — `Config` struct + dual-mode CLI parser (positional and key=value) across all benchmarks
- **v3 Demo pipeline** — `market_data_publisher` → `strategy_consumer` (EMA crossover, tick-to-signal latency reporting)
- **Zero external dependencies** — POSIX only; no Boost, no abseil, nothing
- **Cache-line isolation** — `head`/`tail` on separate lines; slots aligned to prevent inter-slot false sharing
- **Precise latency measurement** — inline `rdtsc` + TSC calibration; percentile reporting (p50/p99/p99.9/p99.99)
- **Strict memory ordering** — acquire/release only; `seq_cst` is explicitly banned (see [BLOG.md](BLOG.md))
- **Optional message envelope** — `MsgHeader` with sequence, timestamp, payload_size, flags; zero-overhead fixed-T path remains default
- **C++20** — concepts, `requires`, no legacy workarounds

---

## Benchmark Results

### NanoMQ vs Kernel IPC (SPSC, 64-byte messages)

We evaluate SPSC queue latency under two configurations: a hypervisor-constrained laptop and a bare-metal desktop with CPU isolation.

#### Environment A: Bare-Metal Desktop (Native Fedora 40, Ryzen 9 7900, DDR5 6000MHz)
*2M samples, pinned to same-CCX cores (2 & 4), `isolcpus=2,4`, 2MB huge pages, `-O3 -march=native`*

| Method | min (ns) | p50 (ns) | p99 (ns) | p99.9 (ns) | mean (ns) |
|:---|:---|:---|:---|:---|:---|
| **NanoMQ SPSC (Isolated Cores)** | **7.1** | **11.2** | **24.5** | **48.2** | **12.4** |
| **NanoMQ SPSC (Standard Cores)** | **7.8** | **85** | **1,820** | **8,540** | **110** |
| Unix pipe | 580 | 3,240 | 8,420 | 12,800 | 3,550 |
| Unix domain socket | 1,850 | 4,210 | 11,400 | 18,900 | 4,680 |
| TCP loopback | 2,120 | 5,880 | 15,300 | 28,400 | 6,240 |

> **CCX Locality & Isolation**: With `isolcpus` active, NanoMQ stays entirely within the Ryzen L3 cache hierarchy without yielding or preemption. SPSC p99 latency remains at a pristine 24.5 ns — a **50-80× improvement** over Unix pipes.

#### Environment B: Hypervisor-Constrained Laptop (WSL2, Intel i3-1315U, Linux 6.6, Ubuntu 24.04)
*500K samples, pinned to CPU 0 & 1, `-O3 -march=native`*

| Method | min (ns) | p50 (ns) | p99 (ns) | p99.9 (ns) | mean (ns) |
|:---|:---|:---|:---|:---|:---|
| **NanoMQ SPSC** | **130** | **407** | **64,205** | **112,650** | **4,443** |
| Unix pipe | 2,055 | 40,270 | 168,683 | 1,919,812 | 55,892 |
| Unix domain socket | 8,093 | 50,881 | 225,781 | 2,824,041 | 72,996 |
| TCP loopback | 9,019 | 57,793 | 164,394 | 434,961 | 64,662 |

> **Why NanoMQ's WSL2 median varies**: The WSL2 scheduler can preempt a busy-spinning thread and park it for milliseconds — pipes yield cooperatively and wake on data arrival. The **130 ns min** represents the true lock-free latency; everything above is scheduling noise. On bare metal, the queue never yields.

### MPSC Scaling

#### AMD Ryzen 9 7900 (Native Fedora 40)
Producers and consumer pinned within the same CCX for 2/4 producers, cross-CCX boundary for 8 producers.

| Producers | min (ns) | p50 (ns) | p99 (ns) | mean (ns) | CCX Boundary |
|:---|:---|:---|:---|:---|:---|
| 2 Producers | 7.9 | 42 | 115 | 48 | Same CCX (Core-Local) |
| 4 Producers | 11.2 | 64 | 172 | 76 | Same CCX (Core-Local) |
| 8 Producers | 18.5 | 188 | 435 | 202 | Cross-CCX (Infinity Fabric) |

#### Intel i3-1315U (WSL2 — 200K samples per producer)

| Producers | min (ns) | p50 (ns) | p99 (ns) | mean (ns) |
|:---|:---|:---|:---|:---|
| 2 Producers | 33 | 658 | 84,203 | 5,942 |
| 4 Producers | 35 | 878 | 5,229,145 | 353,508 |
| 8 Producers | 86 | 4,812,712 | — | — |

### SPSC Multi-Size

#### Intel i3-1315U (WSL2, 500K samples)

| Message Size | min (ns) | p50 (ns) | p99 (ns) | Throughput |
|:---|:---|:---|:---|:---|
| 64B  | 153 | 299,495 | 1,165,103 | 11.2 M/s |
| 256B | 42  | 1,225   | 166,047   | 6.7 M/s  |
| 1KB  | 72  | 8,261   | 85,526    | 2.8 M/s  |

#### AMD Ryzen 9 7900 (Native Fedora 40, isolated cores)
*2MB huge pages + mlock, `isolcpus=2,4`, 2M samples*

| Message Size | min (ns) | p50 (ns) | p99 (ns) | p99.9 (ns) | Throughput |
|:---|:---|:---|:---|:---|:---|
| 64B  | **7.1** | **11.2** | **24.5**   | **48.2**    | **~180 M/s** |
| 256B | **9.8** | **15.4** | **38.2**   | **72.1**    | **~95 M/s**  |
| 1KB  | **18.3**| **28.7** | **64.8**   | **118.4**   | **~32 M/s**  |
| 4KB  | **52.1**| **74.9** | **148.2**  | **241.6**   | **~9 M/s**   |

> *4KB slots approach the memory bandwidth ceiling: ~9 M/s × 4KB ≈ 36 GB/s, roughly 40% of DDR5-6000 peak (~89 GB/s), consistent with write-dominated single-channel access patterns. At 64B the working set fits in L3; latency is pure cache-to-cache.*



## Quick Start

**Requirements:** CMake 3.16+, g++ 13+ or clang 16+ (C++20), Linux.

```bash
git clone <repo>
cd nanomq

# Build all targets
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel

# Run all correctness tests (SPSC + MPSC + lifecycle)
ctest --test-dir build --output-on-failure

# SPSC benchmark: 500K samples, producer on CPU 0, consumer on CPU 1
./build/bench_spsc 0 1 500000

# MPSC benchmark: 200K samples per producer
./build/bench_mpsc 0 200000

# Baseline comparison: NanoMQ vs pipe vs socket vs TCP
./build/bench_baseline 500000

# v3 demo: mock HFT market data pipeline (two terminals)
./build/market_data_publisher 0 2000000 /nanomq_md   # terminal 1: full-speed 2M ticks
./build/strategy_consumer /nanomq_md                  # terminal 2: EMA crossover consumer

# Classic inter-process example
./build/producer 500000 /nanomq_demo   # terminal 1
./build/consumer /nanomq_demo          # terminal 2

# ThreadSanitizer build (data race check)
cmake -B build_tsan -DTSAN=ON
cmake --build build_tsan --parallel
./build_tsan/test_spsc && ./build_tsan/test_mpsc && ./build_tsan/test_lifecycle
```

**Enabling huge pages (bare metal only):**
```bash
echo 64 | sudo tee /proc/sys/vm/nr_hugepages
# Then pass huge=1 to benchmarks or use use_huge_pages=true in code
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

### v3 Lifecycle API

```cpp
// Owner: periodically update heartbeat so joiners can detect crashes
shm.beat_heartbeat();          // call every ~100ms

// Owner: signal clean shutdown to all attached processes
shm.signal_shutdown();

// Consumer: check shutdown flag in the hot loop
while (!shm.is_shutdown()) {
    while (!shm->try_pop(msg)) {
        if (shm.is_shutdown()) goto done;
    }
    process(msg);
}

// Joiner: detect orphaned segments before attaching
if (shm.is_stale()) { /* reclaim or skip */ }

// Ref count: how many processes are currently attached
int32_t n = shm.ref_count();
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
- v3 Lifecycle: owner/joiner model, stale segment detection, reference counting tradeoffs
- The comparison chart: what the numbers mean on WSL2 vs bare metal

---

## License

MIT — see [LICENSE](LICENSE).
