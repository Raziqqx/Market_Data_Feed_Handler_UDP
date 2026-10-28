#pragma once
// The element that travels from the feed thread to the book thread through the SPSC queue.
//
// It is exactly one 64-byte cache line: the producer writes one line, the consumer reads one
// line, and two neighbouring slots never share a line. It carries the decoded book event plus
// the timestamps needed for the per-stage latency breakdown.

#include <cstdint>
#include <cstring>

#include "itch/book_builder.hpp"
#include "itch/itch.hpp"

namespace feed {

// Non-ITCH control kinds (ITCH types are ASCII letters, so these cannot collide).
enum Control : char {
  kReset = 1,         // throw the books away; a snapshot follows
  kSnapshotDone = 2,  // the snapshot is complete; live updates resume
  kEndOfSession = 3,  // publisher said the session is over
};

// Flags.
enum : std::uint8_t {
  kFromSnapshot = 1,  // came from a TCP snapshot, not from the wire
  kReplayed = 2,      // was buffered while recovering: its latency includes the recovery time
  kNoKernelTs = 4,    // the kernel gave no receive timestamp for this packet
};

struct alignas(64) Update {
  std::uint64_t seq;      // MoldUDP64 sequence number of this message
  std::uint64_t ref;      // order reference
  union {
    std::uint64_t new_ref;  // 'U'
    char stock[8];          // 'A', 'F', 'R', 'H'
  };
  std::uint64_t t_rx_tsc;    // TSC when recvmmsg returned the packet
  std::uint64_t t_push_tsc;  // TSC when this update was published to the queue
  std::uint32_t shares;
  std::uint32_t price;
  std::uint32_t wire_ns;  // kernel receive timestamp -> user space (CLOCK_REALTIME, ns)
  std::uint16_t locate;
  std::uint16_t tracking;
  char type;            // ITCH message type, or a Control value
  char aux;             // side ('B'/'S'), trading state, system event code, or printable flag
  std::uint8_t flags;
};
static_assert(sizeof(Update) == 64, "Update should be one cache line");

// Types the book cares about. Everything else (trades, NOII, ...) is sequenced and counted but
// never crosses the queue.
inline bool book_relevant(std::uint8_t t) {
  switch (t) {
    case 'A': case 'F': case 'E': case 'C': case 'X': case 'D': case 'U': case 'S': case 'R': case 'H':
      return true;
    default:
      return false;
  }
}

// Decode one ITCH message straight out of the receive buffer into a queue slot.
// m points at the type byte; the caller has already validated the length.
inline void decode_into(const std::uint8_t* m, Update& u) {
  using namespace itch;
  u.type = char(m[0]);
  u.locate = be16(m + 1);
  u.tracking = be16(m + 3);
  switch (m[0]) {
    case 'A':
    case 'F':
      u.ref = be64(m + 11);
      u.aux = char(m[19]);
      u.shares = be32(m + 20);
      std::memcpy(u.stock, m + 24, 8);
      u.price = be32(m + 32);
      break;
    case 'E':
      u.ref = be64(m + 11);
      u.shares = be32(m + 19);
      break;
    case 'C':
      u.ref = be64(m + 11);
      u.shares = be32(m + 19);
      u.aux = char(m[31]);
      u.price = be32(m + 32);
      break;
    case 'X':
      u.ref = be64(m + 11);
      u.shares = be32(m + 19);
      break;
    case 'D':
      u.ref = be64(m + 11);
      break;
    case 'U':
      u.ref = be64(m + 11);
      u.new_ref = be64(m + 19);
      u.shares = be32(m + 27);
      u.price = be32(m + 31);
      break;
    case 'S':
      u.aux = char(m[11]);
      break;
    case 'R':
      std::memcpy(u.stock, m + 11, 8);
      break;
    case 'H':
      std::memcpy(u.stock, m + 11, 8);
      u.aux = char(m[19]);
      break;
  }
}

// Apply one decoded update to the P3 book builder (same handler calls the file replay uses).
inline void apply(itch::BookBuilder& b, const Update& u) {
  const itch::Header h{u.type, u.locate, u.tracking, 0};
  switch (u.type) {
    case 'A':
    case 'F':
      b.on_add(h, u.ref, u.aux, u.shares, u.stock, u.price);
      break;
    case 'E':
      b.on_execute(h, u.ref, u.shares);
      break;
    case 'C':
      b.on_execute_with_price(h, u.ref, u.shares, u.aux, u.price);
      break;
    case 'X':
      b.on_cancel(h, u.ref, u.shares);
      break;
    case 'D':
      b.on_delete(h, u.ref);
      break;
    case 'U':
      b.on_replace(h, u.ref, u.new_ref, u.shares, u.price);
      break;
    case 'S':
      b.on_system_event(h, u.aux);
      break;
    case 'R':
      b.on_stock_directory(h, u.stock);
      break;
    case 'H':
      b.on_trading_action(h, u.stock, u.aux);
      break;
  }
}

}  // namespace feed
