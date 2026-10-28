#pragma once
// MoldUDP64 framing: how NASDAQ itself carries ITCH over UDP multicast.
//
//   Downstream packet (all integers big-endian):
//     offset  0  Session          10 bytes  ASCII, identifies the feed session/day
//     offset 10  Sequence Number   8 bytes  sequence number of the FIRST message in this packet
//     offset 18  Message Count     2 bytes  messages in this packet
//     offset 20  Message blocks:  [2-byte length][ITCH message] * Message Count
//
//   Message Count = 0      -> heartbeat. Sequence Number = the next sequence to be sent, so a
//                             receiver that missed the tail of a burst still sees the gap.
//   Message Count = 0xFFFF -> end of session. Sequence Number = one past the last message.
//
// Sequence numbers count MESSAGES, not packets, and start at 1. A receiver that has applied
// everything up to N-1 expects the next packet to start at N; anything larger is a gap.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "itch/itch.hpp"

namespace feed::mold {

inline constexpr std::size_t kSessionLen = 10;
inline constexpr std::size_t kHeaderLen = 20;
inline constexpr std::uint16_t kHeartbeat = 0;
inline constexpr std::uint16_t kEndOfSession = 0xFFFF;
// 1400 bytes keeps a packet under a 1500-byte Ethernet MTU after IP (20) + UDP (8) headers,
// so no IP fragmentation (a lost fragment loses the whole datagram).
inline constexpr std::size_t kDefaultMaxPayload = 1400;

inline void put_be16(std::uint8_t* p, std::uint16_t v) {
  p[0] = std::uint8_t(v >> 8);
  p[1] = std::uint8_t(v);
}
inline void put_be32(std::uint8_t* p, std::uint32_t v) {
  v = __builtin_bswap32(v);
  std::memcpy(p, &v, 4);
}
inline void put_be64(std::uint8_t* p, std::uint64_t v) {
  v = __builtin_bswap64(v);
  std::memcpy(p, &v, 8);
}

// Parsed view of a packet header. Points into the receive buffer (zero-copy).
struct PacketView {
  const std::uint8_t* session;
  std::uint64_t seq;
  std::uint16_t count;
  const std::uint8_t* blocks;  // first message block
  const std::uint8_t* end;     // end of the datagram

  bool heartbeat() const { return count == kHeartbeat; }
  bool end_of_session() const { return count == kEndOfSession; }
  std::uint16_t messages() const { return (heartbeat() || end_of_session()) ? 0 : count; }
};

inline bool parse_header(const std::uint8_t* p, std::size_t len, PacketView& out) {
  if (len < kHeaderLen) return false;
  out.session = p;
  out.seq = itch::be64(p + 10);
  out.count = itch::be16(p + 18);
  out.blocks = p + kHeaderLen;
  out.end = p + len;
  return true;
}

// Builds packets into a caller-owned buffer.
class PacketBuilder {
 public:
  PacketBuilder(const char* session, std::size_t max_payload = kDefaultMaxPayload) : max_(max_payload) {
    std::memset(session_, ' ', kSessionLen);
    std::memcpy(session_, session, std::min(std::strlen(session), kSessionLen));
  }

  // Start a new packet whose first message has sequence `seq`.
  void begin(std::uint8_t* buf, std::uint64_t seq) {
    buf_ = buf;
    std::memcpy(buf_, session_, kSessionLen);
    put_be64(buf_ + 10, seq);
    count_ = 0;
    len_ = kHeaderLen;
  }

  bool fits(std::size_t msg_len) const { return len_ + 2 + msg_len <= max_ && count_ < 0xFFFE; }

  // Append one ITCH message (without its length prefix). Caller checks fits() first.
  void add(const std::uint8_t* msg, std::size_t msg_len) {
    put_be16(buf_ + len_, std::uint16_t(msg_len));
    std::memcpy(buf_ + len_ + 2, msg, msg_len);
    len_ += 2 + msg_len;
    ++count_;
  }

  // Finish the packet; returns its length in bytes.
  std::size_t finish() {
    put_be16(buf_ + 18, count_);
    return len_;
  }

  // Header-only packet (heartbeat or end of session).
  std::size_t control(std::uint8_t* buf, std::uint64_t next_seq, std::uint16_t kind) {
    begin(buf, next_seq);
    count_ = kind;
    return finish();
  }

  // Forget the current packet (after finish(): the next one starts with begin()).
  void reset() {
    count_ = 0;
    len_ = 0;
  }

  std::uint16_t count() const { return count_; }
  std::size_t size() const { return len_; }
  std::size_t max_payload() const { return max_; }

 private:
  char session_[kSessionLen];
  std::size_t max_;
  std::uint8_t* buf_ = nullptr;
  std::uint16_t count_ = 0;
  std::size_t len_ = 0;
};

}  // namespace feed::mold
