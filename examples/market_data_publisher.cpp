// examples/market_data_publisher.cpp
//
// NanoMQ v3 — Mock Market Data Publisher
//
// Generates synthetic tick data (symbol, bid/ask, last price, volume,
// timestamp) at a configurable rate and pushes each tick into a NanoMQ
// SPSC queue backed by POSIX shared memory.
//
// This demonstrates a realistic HFT feed handler pattern:
//   - Fixed-size message struct fits in two cache lines (128 bytes)
//   - TSC timestamp embedded in each tick for end-to-end latency measurement
//   - Configurable tick rate: 0 = as-fast-as-possible, N = N ticks/second
//   - Shutdown flag checked every tick so strategy_consumer can trigger clean stop
//
// Usage:
//   ./market_data_publisher [ticks_per_second] [total_ticks] [shm_name]
//   ./market_data_publisher 0 5000000 /nanomq_md     # full speed, 5M ticks
//   ./market_data_publisher 100000 1000000            # 100k ticks/sec, 1M total
//
// Start BEFORE strategy_consumer. This process owns the shm segment.

#include "nanomq/common.hpp"
#include "nanomq/spsc_queue.hpp"
#include "nanomq/shm_transport.hpp"
#include "nanomq/config.hpp"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <string>
#include <csignal>
#include <atomic>

// ---------------------------------------------------------------------------
// Tick message layout — 128 bytes (2 cache lines)
//
// Fixed size to avoid any dynamic allocation on the hot path.
// All price/size fields use integer representation (price × 10000) to stay
// trivially copyable and avoid floating-point atomicity issues.
// ---------------------------------------------------------------------------
struct alignas(64) Tick {
    // Cache line 0: timing & identity (hot for latency measurement)
    uint64_t tsc_send;       // rdtsc() at publish time
    uint64_t seq;            // monotonic tick counter
    uint64_t timestamp_ns;   // CLOCK_MONOTONIC at publish time
    char     symbol[8];      // symbol name, NUL-padded (e.g. "AAPL\0\0\0\0")
    uint64_t _reserved0;     // future: exchange source ID

    // Cache line 1: market data fields (hot for strategy logic)
    int64_t  bid_price;      // bid × 10000 (e.g. 1502350 = $150.2350)
    int64_t  ask_price;      // ask × 10000
    int64_t  last_price;     // last trade × 10000
    uint32_t bid_size;       // shares on bid
    uint32_t ask_size;       // shares on ask
    uint32_t last_size;      // last trade size
    uint32_t flags;          // bit 0: is_trade, bit 1: is_open, bit 2: is_close
    uint64_t _reserved1;     // future: venue/session flags
};
static_assert(sizeof(Tick) == 128, "Tick must be exactly 128 bytes (2 cache lines)");

// ---------------------------------------------------------------------------
// Queue: 32768-slot ring (32768 × 128B = 4MB of slot data)
// At 1M ticks/sec this is ~32ms of buffer — enough to absorb burst spikes.
// ---------------------------------------------------------------------------
using Queue = nanomq::SpscQueue<Tick, 32768>;

// ---------------------------------------------------------------------------
// Synthetic market data generator
//
// Simple random walk on mid-price with realistic bid/ask spread.
// Not a simulation of real market microstructure — just plausible numbers.
// ---------------------------------------------------------------------------
struct PriceSimulator {
    // Use a fast xorshift64 instead of std::mt19937 — no malloc, no state bloat
    uint64_t state;

    explicit PriceSimulator(uint64_t seed) noexcept : state(seed ? seed : 1) {}

    uint64_t next_rand() noexcept {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        return state;
    }

    // Returns a signed delta in the range [-range, +range]
    int64_t rand_delta(int64_t range) noexcept {
        const int64_t r = static_cast<int64_t>(next_rand() % static_cast<uint64_t>(2 * range + 1));
        return r - range;
    }
};

// ---------------------------------------------------------------------------
// Global signal handler for clean Ctrl-C shutdown
// ---------------------------------------------------------------------------
static std::atomic<bool> g_shutdown{false};
static void on_signal(int) noexcept { g_shutdown.store(true, std::memory_order_release); }

int main(int argc, char* argv[]) {
    std::signal(SIGINT,  on_signal);
    std::signal(SIGTERM, on_signal);

    // Parse args: [ticks_per_second] [total_ticks] [shm_name]
    const uint64_t    ticks_per_sec = (argc > 1) ? static_cast<uint64_t>(std::atoll(argv[1])) : 0ULL;
    const uint64_t    total_ticks   = (argc > 2) ? static_cast<uint64_t>(std::atoll(argv[2])) : 2'000'000ULL;
    const std::string shm_name      = (argc > 3) ? argv[3] : "/nanomq_md";

    std::printf("[publisher] NanoMQ Market Data Publisher v3\n");
    std::printf("[publisher] SHM name    : %s\n", shm_name.c_str());
    std::printf("[publisher] Total ticks : %llu\n", static_cast<unsigned long long>(total_ticks));
    std::printf("[publisher] Tick rate   : %s\n",
        ticks_per_sec == 0 ? "full speed" :
        (std::to_string(ticks_per_sec) + " ticks/sec").c_str());
    std::printf("[publisher] Queue size  : %zu slots × %zu bytes = %zu KB\n",
        Queue::capacity, sizeof(Tick), (Queue::capacity * sizeof(Tick)) / 1024);
    std::fflush(stdout);

    // Create the shared memory segment (owner)
    auto handle = nanomq::shm_create<Queue>(shm_name);
    Queue* q    = handle.queue();

    std::printf("[publisher] Queue created at %p\n", static_cast<void*>(q));
    std::printf("[publisher] Start strategy_consumer, then press Enter...\n");
    std::fflush(stdout);
    std::getchar();

    // -------------------------------------------------------------------------
    // Simulate 5 symbols: two large-cap equities, two mid-cap, one ETF
    // -------------------------------------------------------------------------
    static const char* const SYMBOLS[5] = {"AAPL", "MSFT", "NVDA", "META", "SPY"};
    // Starting mid-prices in × 10000: $189.50, $415.80, $875.25, $512.30, $524.00
    int64_t mid_prices[5] = {1'895'000LL, 4'158'000LL, 8'752'500LL, 5'123'000LL, 5'240'000LL};
    const int64_t SPREAD = 500;  // $0.0500 half-spread (tight, like a large-cap)

    PriceSimulator sim(nanomq::rdtsc());

    // Inter-tick interval in nanoseconds (0 = no throttle)
    const uint64_t tick_interval_ns = (ticks_per_sec > 0)
        ? (1'000'000'000ULL / ticks_per_sec)
        : 0ULL;

    uint64_t published   = 0;
    uint64_t seq         = 0;
    uint64_t next_tick_time = nanomq::monotonic_ns();
    const uint64_t t_start = nanomq::monotonic_ns();

    // -------------------------------------------------------------------------
    // Hot path — tick generation and publication loop
    // -------------------------------------------------------------------------
    while (published < total_ticks &&
           !g_shutdown.load(std::memory_order_relaxed) &&
           !handle.is_shutdown()) {

        // Rate limiting: spin until next tick time
        if (tick_interval_ns > 0) {
            while (nanomq::monotonic_ns() < next_tick_time) {
                // tight spin — realistic for a feed handler hot loop
            }
            next_tick_time += tick_interval_ns;
        }

        // Pick symbol round-robin (in production: driven by market feed)
        const int sym_idx = static_cast<int>(seq % 5);

        // Random walk: ±2 ticks on mid, ±5% volume
        mid_prices[sym_idx] += sim.rand_delta(100);  // ±$0.0100 per tick
        // Clamp to keep prices sane
        if (mid_prices[sym_idx] < 10'000LL)  mid_prices[sym_idx] = 10'000LL;
        if (mid_prices[sym_idx] > 100'000'000LL) mid_prices[sym_idx] = 100'000'000LL;

        const int64_t mid  = mid_prices[sym_idx];
        const int64_t bid  = mid - SPREAD;
        const int64_t ask  = mid + SPREAD;

        Tick tick{};
        tick.tsc_send    = nanomq::rdtsc();
        tick.seq         = seq;
        tick.timestamp_ns= nanomq::monotonic_ns();
        std::memcpy(tick.symbol, SYMBOLS[sym_idx], std::strlen(SYMBOLS[sym_idx]) + 1);
        tick.bid_price   = bid;
        tick.ask_price   = ask;
        tick.last_price  = mid + sim.rand_delta(SPREAD / 2);
        tick.bid_size    = static_cast<uint32_t>(100 + sim.next_rand() % 900);
        tick.ask_size    = static_cast<uint32_t>(100 + sim.next_rand() % 900);
        tick.last_size   = static_cast<uint32_t>(1 + sim.next_rand() % 499);
        tick.flags       = (seq % 17 == 0) ? 0x1u : 0u;  // mark every 17th as a trade

        // Spin until queue has space (back-pressure)
        while (!q->try_push(tick) &&
               !g_shutdown.load(std::memory_order_relaxed)) {}

        ++seq;
        ++published;

        // Update heartbeat every 100k ticks
        if (NANOMQ_UNLIKELY(published % 100'000 == 0)) {
            handle.beat_heartbeat();
        }
    }

    // Publish sentinel: seq == UINT64_MAX signals consumer to exit
    Tick sentinel{};
    sentinel.seq = UINT64_MAX;
    while (!q->try_push(sentinel)) {}

    const uint64_t t_end   = nanomq::monotonic_ns();
    const double   elapsed = static_cast<double>(t_end - t_start) * 1e-9;
    const double   rate    = static_cast<double>(published) / elapsed;

    std::printf("\n[publisher] ─────────────────────────────────────────────\n");
    std::printf("[publisher] Ticks published : %llu\n",
        static_cast<unsigned long long>(published));
    std::printf("[publisher] Elapsed         : %.3f s\n", elapsed);
    std::printf("[publisher] Publish rate    : %.2f M ticks/sec\n", rate / 1e6);
    std::printf("[publisher] ─────────────────────────────────────────────\n\n");
    std::fflush(stdout);

    std::printf("[publisher] Press Enter to release shared memory...\n");
    std::getchar();

    // ShmHandle destructor: signal_shutdown not needed (sentinel already sent),
    // then munmap + shm_unlink.
    return 0;
}
