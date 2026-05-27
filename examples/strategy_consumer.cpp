// examples/strategy_consumer.cpp
//
// NanoMQ v3 — Mock Strategy Consumer
//
// Reads Tick messages from the NanoMQ SPSC queue created by
// market_data_publisher, applies a minimal signal (dual exponential moving
// average crossover per symbol), and logs "order" events with the full
// end-to-end latency from tick publish to signal generation.
//
// EMA crossover signal:
//   fast_ema (n=8)  crosses above slow_ema (n=32)  → BUY signal
//   fast_ema (n=8)  crosses below slow_ema (n=32)  → SELL signal
//
// This is intentionally a toy strategy — the point is to show the
// tick-to-signal pipeline latency and demonstrate that the queue is suitable
// for latency-sensitive signal processing.
//
// Usage:
//   ./strategy_consumer [shm_name]
//   ./strategy_consumer /nanomq_md
//
// Start AFTER market_data_publisher has created the queue.

#include "nanomq/common.hpp"
#include "nanomq/spsc_queue.hpp"
#include "nanomq/shm_transport.hpp"

#include <algorithm>   // std::sort
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <csignal>
#include <atomic>

// ---------------------------------------------------------------------------
// Tick definition — must exactly match market_data_publisher.cpp
// ---------------------------------------------------------------------------
struct alignas(64) Tick {
    uint64_t tsc_send;
    uint64_t seq;
    uint64_t timestamp_ns;
    char     symbol[8];
    uint64_t _reserved0;

    int64_t  bid_price;
    int64_t  ask_price;
    int64_t  last_price;
    uint32_t bid_size;
    uint32_t ask_size;
    uint32_t last_size;
    uint32_t flags;
    uint64_t _reserved1;
};
static_assert(sizeof(Tick) == 128, "Tick size mismatch — must match publisher");

using Queue = nanomq::SpscQueue<Tick, 32768>;

// ---------------------------------------------------------------------------
// Per-symbol EMA state
// ---------------------------------------------------------------------------
struct SymbolState {
    char     symbol[8]{};
    double   fast_ema{0.0};   // n=8  → alpha = 2/(8+1) ≈ 0.222
    double   slow_ema{0.0};   // n=32 → alpha = 2/(32+1) ≈ 0.059
    bool     initialised{false};
    int      last_signal{0};  // +1 = long, -1 = short, 0 = flat
    uint64_t signals_fired{0};
};

static constexpr double FAST_ALPHA = 2.0 / (8.0  + 1.0);
static constexpr double SLOW_ALPHA = 2.0 / (32.0 + 1.0);
static constexpr int    N_SYMBOLS  = 5;

static const char* const SYMBOL_NAMES[N_SYMBOLS] = {
    "AAPL", "MSFT", "NVDA", "META", "SPY"
};

// ---------------------------------------------------------------------------
// Order log entry — accumulated during run, printed in summary
// ---------------------------------------------------------------------------
struct OrderEvent {
    uint64_t seq;
    double   latency_ns;   // tick publish (tsc_send) to signal generation
    char     symbol[8];
    int      side;          // +1 buy, -1 sell
    double   price;         // mid-price at signal time (×0.0001 from int)
};

static std::atomic<bool> g_shutdown{false};
static void on_signal(int) noexcept { g_shutdown.store(true, std::memory_order_release); }

// ---------------------------------------------------------------------------
// Helper: find SymbolState by name (linear scan over N_SYMBOLS — fine here)
// ---------------------------------------------------------------------------
static SymbolState* find_state(SymbolState* states, const char* sym) noexcept {
    for (int i = 0; i < N_SYMBOLS; ++i) {
        if (std::strncmp(states[i].symbol, sym, 8) == 0) return &states[i];
    }
    return nullptr;
}

int main(int argc, char* argv[]) {
    std::signal(SIGINT,  on_signal);
    std::signal(SIGTERM, on_signal);

    const std::string shm_name = (argc > 1) ? argv[1] : "/nanomq_md";

    std::printf("[strategy] NanoMQ Strategy Consumer v3\n");
    std::printf("[strategy] Attaching to SHM '%s'...\n", shm_name.c_str());
    std::fflush(stdout);

    // Calibrate TSC → nanoseconds (10ms spin)
    const double ns_per_tick = nanomq::tsc_ns_per_tick();
    std::printf("[strategy] TSC calibration : %.4f ns/tick\n", ns_per_tick);

    // Open existing queue (joiner — does not unlink)
    auto handle = nanomq::shm_open_existing<Queue>(shm_name);
    Queue* q    = handle.queue();

    std::printf("[strategy] Attached at %p. Processing ticks...\n\n",
        static_cast<void*>(q));
    std::fflush(stdout);

    // -------------------------------------------------------------------------
    // Initialise per-symbol state
    // -------------------------------------------------------------------------
    SymbolState states[N_SYMBOLS];
    for (int i = 0; i < N_SYMBOLS; ++i) {
        std::memcpy(states[i].symbol, SYMBOL_NAMES[i], 8);
    }

    // Pre-allocate order log to avoid hot-path allocation
    std::vector<OrderEvent> order_log;
    order_log.reserve(50'000);

    // Latency samples for percentile report
    std::vector<double> latency_samples;
    latency_samples.reserve(2'000'000);

    uint64_t ticks_processed = 0;
    const uint64_t t_start   = nanomq::monotonic_ns();

    // -------------------------------------------------------------------------
    // Hot path — tick consumption and signal generation
    // -------------------------------------------------------------------------
    Tick tick{};
    while (!g_shutdown.load(std::memory_order_relaxed)) {
        // Spin on the queue
        while (!q->try_pop(tick)) {
            if (g_shutdown.load(std::memory_order_relaxed) ||
                handle.is_shutdown()) goto done;
        }

        // Sentinel check
        if (tick.seq == UINT64_MAX) break;

        // Measure tick-to-signal latency
        const uint64_t tsc_recv    = nanomq::rdtsc();
        const double   latency_ns  = static_cast<double>(tsc_recv - tick.tsc_send) * ns_per_tick;
        latency_samples.push_back(latency_ns);

        // Locate symbol state
        SymbolState* st = find_state(states, tick.symbol);
        if (!st) { ++ticks_processed; continue; }

        // Price used for EMA: mid-price in floating-point dollars
        const double price = static_cast<double>(tick.bid_price + tick.ask_price) * 0.5 * 0.0001;

        // EMA update
        if (!st->initialised) {
            st->fast_ema    = price;
            st->slow_ema    = price;
            st->initialised = true;
        } else {
            st->fast_ema = FAST_ALPHA * price + (1.0 - FAST_ALPHA) * st->fast_ema;
            st->slow_ema = SLOW_ALPHA * price + (1.0 - SLOW_ALPHA) * st->slow_ema;
        }

        // Signal logic: detect crossover
        const int new_signal = (st->fast_ema > st->slow_ema) ? +1 : -1;
        if (new_signal != st->last_signal && st->signals_fired > 0) {
            // Crossover detected — log an order
            if (order_log.size() < order_log.capacity()) {
                OrderEvent ev{};
                ev.seq        = tick.seq;
                ev.latency_ns = latency_ns;
                std::memcpy(ev.symbol, tick.symbol, 8);
                ev.side       = new_signal;
                ev.price      = price;
                order_log.push_back(ev);
            }
        }
        if (new_signal != st->last_signal) {
            st->last_signal = new_signal;
        }
        st->signals_fired++;

        ++ticks_processed;
    }

done:
    const uint64_t t_end  = nanomq::monotonic_ns();
    const double   elapsed = static_cast<double>(t_end - t_start) * 1e-9;

    // -------------------------------------------------------------------------
    // Results
    // -------------------------------------------------------------------------
    std::printf("\n[strategy] ─────────────────────────────────────────────\n");
    std::printf("[strategy] Ticks processed : %llu\n",
        static_cast<unsigned long long>(ticks_processed));
    std::printf("[strategy] Elapsed         : %.3f s\n", elapsed);
    std::printf("[strategy] Throughput      : %.2f M ticks/sec\n",
        static_cast<double>(ticks_processed) / elapsed / 1e6);
    std::printf("[strategy] Orders logged   : %zu\n", order_log.size());

    if (!latency_samples.empty()) {
        std::sort(latency_samples.begin(), latency_samples.end());
        const std::size_t n   = latency_samples.size();
        double sum = 0.0;
        for (double v : latency_samples) sum += v;
        const double mean = sum / static_cast<double>(n);

        auto pct = [&](double p) -> double {
            return latency_samples[static_cast<std::size_t>(p * static_cast<double>(n - 1))];
        };

        std::printf("\n[strategy] Tick-to-signal latency (TSC-based, ns):\n");
        std::printf("  min    : %8.1f ns\n", latency_samples.front());
        std::printf("  p50    : %8.1f ns\n", pct(0.50));
        std::printf("  p90    : %8.1f ns\n", pct(0.90));
        std::printf("  p99    : %8.1f ns\n", pct(0.99));
        std::printf("  p99.9  : %8.1f ns\n", pct(0.999));
        std::printf("  max    : %8.1f ns\n", latency_samples.back());
        std::printf("  mean   : %8.1f ns\n", mean);
    }

    // Print last 10 orders as sample
    std::printf("\n[strategy] Last %zu order event(s):\n",
        std::min(order_log.size(), std::size_t{10}));
    const std::size_t start = order_log.size() > 10 ? order_log.size() - 10 : 0;
    for (std::size_t i = start; i < order_log.size(); ++i) {
        const auto& ev = order_log[i];
        char sym_buf[9]{};
        std::memcpy(sym_buf, ev.symbol, 8);
        std::printf("  seq=%-10llu  %-6s  %s  price=%.4f  latency=%.1f ns\n",
            static_cast<unsigned long long>(ev.seq),
            sym_buf,
            ev.side > 0 ? "BUY " : "SELL",
            ev.price,
            ev.latency_ns);
    }

    std::printf("[strategy] ─────────────────────────────────────────────\n\n");
    std::fflush(stdout);

    return 0;
}
