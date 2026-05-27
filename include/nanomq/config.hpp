#pragma once

// ---------------------------------------------------------------------------
// nanomq/config.hpp — v3 Runtime Configuration
//
// Provides a Config struct and a minimal key=value / positional argument
// parser for benchmarks and examples. No external libraries required.
//
// Positional arg convention used by benchmarks:
//   ./bench_spsc <producer_cpu> <consumer_cpu> <num_samples> [use_huge_pages]
//
// Key=value convention (also supported):
//   ./bench_spsc producer_cpu=0 consumer_cpu=1 samples=1000000
// ---------------------------------------------------------------------------

#include <cstdint>
#include <cstdlib>   // std::atoi, std::atoll
#include <cstring>   // std::strncmp
#include <string>
#include <string_view>

namespace nanomq {

// ---------------------------------------------------------------------------
// Config
//
// Collects all runtime-tunable parameters in one place so benchmarks and
// examples share a single source of truth. The fixed-T template parameters
// (Capacity, T) remain compile-time constants — Config is purely for
// operational knobs.
//
// Defaults are conservative: no huge pages, no mlock, run on CPUs 0 and 1.
// ---------------------------------------------------------------------------
struct Config {
    std::string shm_name     = "/nanomq_default";  // POSIX shm object name
    int         producer_cpu = 0;                   // CPU to pin producer thread
    int         consumer_cpu = 1;                   // CPU to pin consumer thread
    uint64_t    num_samples  = 500'000;             // benchmark sample count
    bool        use_huge_pages = false;             // request MAP_HUGETLB
    bool        use_mlock      = false;             // mlock the shm region
    int         num_producers  = 1;                 // for MPSC benchmarks
    bool        verbose        = false;             // extra diagnostic output
};

// ---------------------------------------------------------------------------
// parse_args
//
// Accepts both positional and key=value forms. Unknown keys are silently
// ignored so forward/backward compatibility is not fragile.
//
// Positional form (argc/argv from main):
//   arg[1] = producer_cpu
//   arg[2] = consumer_cpu
//   arg[3] = num_samples
//   arg[4] = num_producers  (MPSC only)
//   arg[5] = use_huge_pages (0|1)
//
// Key=value form (any position, overrides positional):
//   name=/my_queue  producer_cpu=2  consumer_cpu=4  samples=2000000
//   huge=1  mlock=1  producers=4  verbose=1
// ---------------------------------------------------------------------------
inline Config parse_args(int argc, char* argv[]) noexcept {
    Config cfg;

    // --- Positional pass (first, overridden by key=value below) ---
    if (argc > 1 && std::strchr(argv[1], '=') == nullptr)
        cfg.producer_cpu = std::atoi(argv[1]);
    if (argc > 2 && std::strchr(argv[2], '=') == nullptr)
        cfg.consumer_cpu = std::atoi(argv[2]);
    if (argc > 3 && std::strchr(argv[3], '=') == nullptr)
        cfg.num_samples  = static_cast<uint64_t>(std::atoll(argv[3]));
    if (argc > 4 && std::strchr(argv[4], '=') == nullptr)
        cfg.num_producers = std::atoi(argv[4]);
    if (argc > 5 && std::strchr(argv[5], '=') == nullptr)
        cfg.use_huge_pages = (std::atoi(argv[5]) != 0);

    // --- Key=value pass ---
    auto match = [](const char* arg, const char* key) -> const char* {
        const std::size_t klen = std::strlen(key);
        if (std::strncmp(arg, key, klen) == 0 && arg[klen] == '=')
            return arg + klen + 1;
        return nullptr;
    };

    for (int i = 1; i < argc; ++i) {
        const char* v = nullptr;
        if ((v = match(argv[i], "name")))          cfg.shm_name      = v;
        if ((v = match(argv[i], "producer_cpu")))  cfg.producer_cpu  = std::atoi(v);
        if ((v = match(argv[i], "consumer_cpu")))  cfg.consumer_cpu  = std::atoi(v);
        if ((v = match(argv[i], "samples")))       cfg.num_samples   = static_cast<uint64_t>(std::atoll(v));
        if ((v = match(argv[i], "producers")))     cfg.num_producers = std::atoi(v);
        if ((v = match(argv[i], "huge")))          cfg.use_huge_pages = (std::atoi(v) != 0);
        if ((v = match(argv[i], "mlock")))         cfg.use_mlock      = (std::atoi(v) != 0);
        if ((v = match(argv[i], "verbose")))       cfg.verbose        = (std::atoi(v) != 0);
    }

    return cfg;
}

} // namespace nanomq
