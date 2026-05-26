#pragma once

#include <cstdint>

namespace nanomq {

// ---------------------------------------------------------------------------
// MsgHeader — optional envelope for variable-intent messages.
//
// Use this when you need to multiplex different payload types through the
// same queue without changing the fixed-T template parameter.
//
// The zero-overhead fixed-T path (SpscQueue<MyStruct, N>) remains the
// primary API. This header is opt-in for cases where you need:
//   - Sequence tracking across restarts
//   - Nanosecond timestamps without embedding them in every T
//   - Payload-size hints (for dynamic consumers)
//   - Flag bits (e.g., begin/end of burst, heartbeat, flush marker)
//
// Usage pattern (inline payload, fixed max size):
//   struct Envelope {
//       nanomq::MsgHeader hdr;
//       char              payload[MAX_PAYLOAD];
//   };
//   SpscQueue<Envelope, 4096> q;
//
// Producers fill hdr and payload; consumers inspect hdr.flags to dispatch.
//
// Layout: 24 bytes total, no padding needed on 64-bit platforms.
// ---------------------------------------------------------------------------

struct MsgHeader {
    uint64_t sequence;       // monotonic counter; can be used to detect gaps
    uint64_t timestamp_ns;   // CLOCK_MONOTONIC nanoseconds at write time
    uint32_t payload_size;   // actual bytes used in payload (0 = fixed-size T)
    uint32_t flags;          // application-defined; reserve low 8 bits for nanomq

    // Predefined flag bits (low byte)
    static constexpr uint32_t FLAG_HEARTBEAT  = 0x01;  // no-op keep-alive
    static constexpr uint32_t FLAG_FLUSH      = 0x02;  // consumer should flush output
    static constexpr uint32_t FLAG_BEGIN_BURST= 0x04;  // first message of a burst
    static constexpr uint32_t FLAG_END_BURST  = 0x08;  // last message of a burst
    static constexpr uint32_t FLAG_SHUTDOWN   = 0x10;  // graceful shutdown signal
};

static_assert(sizeof(MsgHeader) == 24, "MsgHeader layout mismatch");

} // namespace nanomq
