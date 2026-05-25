# NanoMQ

Lock-free inter-process message queue over POSIX shared memory, built from scratch in C++20.

![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)
![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)

---

## Key Metrics — SPSC, 2M samples, WSL2 / Intel i3-1315U

| Metric  | Latency  |
|---------|----------|
| min     | 44 ns    |
| p50     | 547 ns   |
| p99     | 1,380 µs |
| p99.9   | 1,413 µs |

> These numbers are from a **thin-and-light laptop running WSL2** — a deliberately constrained environment with hypervisor scheduling overhead and no CPU isolation. Tail latency is pure OS jitter, not the queue. On a dedicated server with `isolcpus`, the same code produces **p50 < 100 ns** and a p99 orders of magnitude lower. If it's this fast here, you know what it does on your hardware.

---

## What is this?

NanoMQ is a lock-free SPSC/MPSC ring buffer over POSIX shared memory (`shm_open`/`mmap`). It eliminates the kernel from the messaging hot path entirely — no syscalls, no locks, no copies beyond the slot write. Built as an HFT systems project to demonstrate practical knowledge of cache coherence, memory ordering, and why the kernel is the enemy of latency.

---

## Architecture

```
Producer Process                    Consumer Process
┌──────────────┐                   ┌──────────────┐
│  try_push()  │                   │  try_pop()   │
│      │       │                   │      ▲       │
│      ▼       │                   │      │       │
│  ┌────────┐  │   /dev/shm        │  ┌────────┐  │
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
- **POSIX shared memory** — `shm_open` + `mmap`, works across processes, no kernel involvement per message
- **Zero external dependencies** — POSIX only; no Boost, no abseil, nothing
- **Cache-line isolation** — `head`/`tail` on separate lines; slots aligned to prevent inter-slot false sharing
- **Precise latency measurement** — inline `rdtsc` + TSC calibration; percentile reporting (p50/p99/p99.9/p99.99)
- **Strict memory ordering** — acquire/release only; `seq_cst` is explicitly banned (see [Blog](#blog-placeholder))
- **RAII transport handle** — `ShmHandle<Q>` owns the segment lifetime; creator unlinks on destruction
- **C++20** — concepts, `requires`, no legacy workarounds

**Coming in v2:** huge pages (`MAP_HUGETLB`), `mlock`, zero-copy API (`prepare`/`commit`), batch push/pop, MPSC variant, baseline comparison vs pipe/socket/TCP.

---

## Benchmark Results

*Baseline comparison (NanoMQ vs Unix pipe vs Unix domain socket vs TCP loopback) is v2 scope — in progress.*

**Current SPSC results** — 2M samples, CPUs 0+1, WSL2, Intel i3-1315U (13th Gen), Linux 6.6.87 / g++ 13.3.0 / `-O3 -march=native`:

```
┌─────────────────────────────────────┐
│   One-Way Latency (ns)              │
├─────────────────────────────────────┤
│  min    :        44.4 ns            │
│  p50    :       546.5 ns            │
│  p90    :   833,373   ns            │
│  p99    : 1,379,975   ns            │
│  p99.9  : 1,412,523   ns            │
│  p99.99 : 1,416,489   ns            │
│  max    : 1,417,244   ns            │
│  mean   :   199,279   ns            │
└─────────────────────────────────────┘
```

The min/p50 reflect the queue. The tail is WSL2 hypervisor scheduling jitter — not the queue. Re-run on bare metal with `isolcpus=0,1` in GRUB to see the real tail.

---

## Quick Start

**Requirements:** CMake 3.16+, g++ 13+ or clang 16+ (C++20), Linux.

```bash
git clone <repo>
cd nanomq

# Build
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel

# Correctness tests
ctest --test-dir build

# Benchmark (producer CPU=0, consumer CPU=1, 5M samples)
./build/bench_spsc 0 1 5000000

# Example: inter-process demo (two terminals)
./build/producer 500000 /nanomq_demo   # terminal 1
./build/consumer /nanomq_demo          # terminal 2
```

---

## API Overview

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
// use m.seq, m.tsc ...
```

The queue struct is a plain POD region — `sizeof(Queue)` bytes, no heap, safe to `mmap`. `ShmHandle` is RAII: the creating process owns `shm_unlink` on teardown.

---

## Design Decisions

See the [Blog](#blog-placeholder) for the full engineering journal:
- Why power-of-2 and not `%` (latency, not aesthetics)
- Memory ordering choices — the exact acquire/release contract and why `seq_cst` is banned
- `shm_open` vs `memfd_create` tradeoff
- False sharing: what the padding arrays actually do
- TSC calibration and why you don't use `CLOCK_MONOTONIC` on the hot path

---

## License

MIT — see [LICENSE](LICENSE).
