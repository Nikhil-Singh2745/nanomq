cmake_minimum_required(VERSION 3.16)
project(nanomq CXX)

# ─── C++20 ───────────────────────────────────────────────────────────────────
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)     # -std=c++20, not -std=gnu++20

# ─── Default build type ───────────────────────────────────────────────────────
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

# ─── Compiler flags ───────────────────────────────────────────────────────────
# Aggressive optimization + strict warnings. Applied to all targets via
# the nanomq interface library so they inherit without repetition.
set(NANOMQ_COMPILE_OPTIONS
    -O3
    -march=native
    -Wall
    -Wextra
    -Wpedantic
    -Wno-unused-result          # tolerate ignored return values in examples
)

# ─── nanomq — header-only interface library ───────────────────────────────────
add_library(nanomq INTERFACE)
target_include_directories(nanomq INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)
target_compile_options(nanomq INTERFACE ${NANOMQ_COMPILE_OPTIONS})
target_compile_features(nanomq INTERFACE cxx_std_20)

# POSIX real-time library (shm_open, shm_unlink live here on Linux)
target_link_libraries(nanomq INTERFACE rt pthread)

# ─── Helper macro: add a single-file executable linked against nanomq ─────────
macro(nanomq_executable target source)
    add_executable(${target} ${source})
    target_link_libraries(${target} PRIVATE nanomq)
endmacro()

# ─── Examples ─────────────────────────────────────────────────────────────────
nanomq_executable(producer  examples/producer.cpp)
nanomq_executable(consumer  examples/consumer.cpp)

# ─── Benchmarks ───────────────────────────────────────────────────────────────
nanomq_executable(bench_spsc bench/bench_spsc.cpp)

# ─── Tests ────────────────────────────────────────────────────────────────────
nanomq_executable(test_spsc tests/test_spsc.cpp)
# Tests must NOT be compiled with -DNDEBUG — assert() is the entire test mechanism.
# Remove the NDEBUG definition that CMake adds automatically in Release/MinSizeRel builds.
target_compile_options(test_spsc PRIVATE -UNDEBUG)

# Optional ThreadSanitizer build: cmake -B build_tsan -DTSAN=ON
option(TSAN "Build with ThreadSanitizer" OFF)
if(TSAN)
    message(STATUS "  ThreadSanitizer: ON")
    target_compile_options(test_spsc PRIVATE -fsanitize=thread -g -O1)
    target_link_options(test_spsc PRIVATE -fsanitize=thread)
endif()

# Register with CTest so `ctest` works
enable_testing()
add_test(NAME spsc_correctness COMMAND test_spsc)

# ─── Optional: export compile_commands.json for IDEs / clangd ────────────────
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ─── Summary ──────────────────────────────────────────────────────────────────
message(STATUS "")
message(STATUS "NanoMQ v1")
message(STATUS "  Build type   : ${CMAKE_BUILD_TYPE}")
message(STATUS "  C++ standard : ${CMAKE_CXX_STANDARD}")
message(STATUS "  Targets      : producer consumer bench_spsc test_spsc")
message(STATUS "")
