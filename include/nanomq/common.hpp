#pragma once

#include <cstdint>
#include <cstddef>
#include <ctime>
#include <type_traits>

// ---------------------------------------------------------------------------
// Cache-line size (x86_64 standard; adjust for other ISAs if needed)
// ---------------------------------------------------------------------------
namespace nanomq {

inline constexpr std::size_t CACHE_LINE_SIZE = 64;

// ---------------------------------------------------------------------------
// Power-of-two compile-time check
// ---------------------------------------------------------------------------
template <std::size_t N>
struct is_power_of_two : std::bool_constant<(N >= 1) && ((N & (N - 1)) == 0)> {};

template <std::size_t N>
inline constexpr bool is_power_of_two_v = is_power_of_two<N>::value;

// Convenience macro for static_assert in template bodies
#define NANOMQ_ASSERT_POWER_OF_TWO(N) \
    static_assert(::nanomq::is_power_of_two_v<(N)>, \
                  "Capacity must be a power of two")

// ---------------------------------------------------------------------------
// Force-inline attribute
// ---------------------------------------------------------------------------
#if defined(__GNUC__) || defined(__clang__)
#  define NANOMQ_FORCE_INLINE [[gnu::always_inline]] inline
#else
#  define NANOMQ_FORCE_INLINE inline
#endif

// ---------------------------------------------------------------------------
// Branch prediction hints (C++20 attributes, but also macros for older paths)
// ---------------------------------------------------------------------------
#define NANOMQ_LIKELY(x)   (__builtin_expect(!!(x), 1))
#define NANOMQ_UNLIKELY(x) (__builtin_expect(!!(x), 0))

// ---------------------------------------------------------------------------
// rdtsc — cycle-accurate timestamp counter (x86_64 only)
// ---------------------------------------------------------------------------
#ifdef __x86_64__

NANOMQ_FORCE_INLINE uint64_t rdtsc() noexcept {
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_ia32_rdtsc();
#else
    uint32_t lo, hi;
    asm volatile("rdtsc" : "=a"(lo), "=d"(hi));
    return (static_cast<uint64_t>(hi) << 32) | lo;
#endif
}

#else  // Non-x86 fallback: use CLOCK_MONOTONIC nanoseconds
NANOMQ_FORCE_INLINE uint64_t rdtsc() noexcept {
    struct timespec ts{};
    ::clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ULL + static_cast<uint64_t>(ts.tv_nsec);
}
#endif

// ---------------------------------------------------------------------------
// CLOCK_MONOTONIC wall-clock nanoseconds helper
// ---------------------------------------------------------------------------
NANOMQ_FORCE_INLINE uint64_t monotonic_ns() noexcept {
    struct timespec ts{};
    ::clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ULL + static_cast<uint64_t>(ts.tv_nsec);
}

// ---------------------------------------------------------------------------
// TSC calibration — returns nanoseconds per TSC tick
// Spin for ~10ms comparing CLOCK_MONOTONIC vs rdtsc.
// Call once at startup; result is stable per CPU frequency state.
// ---------------------------------------------------------------------------
inline double tsc_ns_per_tick() noexcept {
    constexpr uint64_t CALIBRATION_NS = 10'000'000ULL;  // 10ms

    const uint64_t t0_ns  = monotonic_ns();
    const uint64_t t0_tsc = rdtsc();

    while ((monotonic_ns() - t0_ns) < CALIBRATION_NS)
        ; // spin

    const uint64_t t1_ns  = monotonic_ns();
    const uint64_t t1_tsc = rdtsc();

    const uint64_t delta_ns  = t1_ns  - t0_ns;
    const uint64_t delta_tsc = t1_tsc - t0_tsc;

    if (NANOMQ_UNLIKELY(delta_tsc == 0)) return 1.0;
    return static_cast<double>(delta_ns) / static_cast<double>(delta_tsc);
}

} // namespace nanomq
