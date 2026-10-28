#pragma once
// NASDAQ TotalView-ITCH 5.0 decoder.
//
// File format (the "BinaryFILE" format NASDAQ uses for the sample files): a stream of
//     [2-byte big-endian length][message of that length]
// Every message starts with the same 11-byte header:
//     offset 0  Message Type     1 byte  (ASCII letter)
//     offset 1  Stock Locate     2 bytes (per-day integer id of the instrument, 0 = none)
//     offset 3  Tracking Number  2 bytes
//     offset 5  Timestamp        6 bytes (nanoseconds since midnight)
// All integers are big-endian; prices are 4-byte integers with 4 implied decimals.
//
// The decoder is zero-copy: it reads fields straight out of the buffer and calls a handler.
// Handler methods are resolved at compile time (template parameter), so there is no virtual
// call per message.

#include <cstdint>
#include <cstring>

#include "lob/common.hpp"

namespace itch {

LOB_ALWAYS_INLINE std::uint16_t be16(const std::uint8_t* p) { return std::uint16_t(p[0] << 8 | p[1]); }
LOB_ALWAYS_INLINE std::uint32_t be32(const std::uint8_t* p) {
  std::uint32_t v;
  std::memcpy(&v, p, 4);
  return __builtin_bswap32(v);
}
LOB_ALWAYS_INLINE std::uint64_t be64(const std::uint8_t* p) {
  std::uint64_t v;
  std::memcpy(&v, p, 8);
  return __builtin_bswap64(v);
}
LOB_ALWAYS_INLINE std::uint64_t be48(const std::uint8_t* p) {
  return std::uint64_t(be16(p)) << 32 | be32(p + 2);
}

// Exact message sizes from the ITCH 5.0 specification (excluding the 2-byte length prefix).
// 0 = unknown type.
struct SizeTable {
  std::uint8_t size[256] = {};
  constexpr SizeTable() {
    size['S'] = 12;  // System Event
    size['R'] = 39;  // Stock Directory
    size['H'] = 25;  // Stock Trading Action
    size['Y'] = 20;  // Reg SHO Restriction
    size['L'] = 26;  // Market Participant Position
    size['V'] = 35;  // MWCB Decline Level
    size['W'] = 12;  // MWCB Status
    size['K'] = 28;  // IPO Quoting Period Update
    size['J'] = 35;  // LULD Auction Collar
    size['h'] = 21;  // Operational Halt
    size['A'] = 36;  // Add Order (no MPID attribution)
    size['F'] = 40;  // Add Order with MPID attribution
    size['E'] = 31;  // Order Executed
    size['C'] = 36;  // Order Executed With Price
    size['X'] = 23;  // Order Cancel
    size['D'] = 19;  // Order Delete
    size['U'] = 35;  // Order Replace
    size['P'] = 44;  // Trade (non-cross)
    size['Q'] = 40;  // Cross Trade
    size['B'] = 19;  // Broken Trade
    size['I'] = 50;  // NOII
    size['N'] = 20;  // Retail Price Improvement Indicator
  }
};
inline constexpr SizeTable kSizes{};

struct Header {
  char type;
  std::uint16_t locate;
  std::uint16_t tracking;
  std::uint64_t timestamp;  // ns since midnight
};

LOB_ALWAYS_INLINE Header header(const std::uint8_t* m) {
  return Header{char(m[0]), be16(m + 1), be16(m + 3), be48(m + 5)};
}

// Handler with no-op defaults. Derive (no virtuals needed) and hide the ones you care about.
struct NullHandler {
  void on_system_event(const Header&, char /*event_code*/) {}
  void on_stock_directory(const Header&, const char* /*stock8*/) {}
  void on_trading_action(const Header&, const char* /*stock8*/, char /*state*/) {}
  void on_add(const Header&, std::uint64_t /*ref*/, char /*side*/, std::uint32_t /*shares*/, const char* /*stock8*/,
              std::uint32_t /*price*/) {}
  void on_execute(const Header&, std::uint64_t /*ref*/, std::uint32_t /*shares*/) {}
  void on_execute_with_price(const Header&, std::uint64_t /*ref*/, std::uint32_t /*shares*/, char /*printable*/,
                             std::uint32_t /*price*/) {}
  void on_cancel(const Header&, std::uint64_t /*ref*/, std::uint32_t /*shares*/) {}
  void on_delete(const Header&, std::uint64_t /*ref*/) {}
  void on_replace(const Header&, std::uint64_t /*old_ref*/, std::uint64_t /*new_ref*/, std::uint32_t /*shares*/,
                  std::uint32_t /*price*/) {}
  void on_other(const Header&) {}
  void on_bad_message(const std::uint8_t* /*msg*/, std::size_t /*len*/) {}
};

// Decode one message (m points at the type byte, len from the length prefix).
template <class H>
LOB_ALWAYS_INLINE void dispatch(const std::uint8_t* m, std::size_t len, H& h) {
  const std::uint8_t t = m[0];
  if (LOB_UNLIKELY(kSizes.size[t] == 0 || kSizes.size[t] != len)) {
    h.on_bad_message(m, len);
    return;
  }
  const Header hd = header(m);
  switch (t) {
    case 'A':
      h.on_add(hd, be64(m + 11), char(m[19]), be32(m + 20), reinterpret_cast<const char*>(m + 24), be32(m + 32));
      break;
    case 'F':  // same layout as 'A' plus a 4-byte MPID we do not need
      h.on_add(hd, be64(m + 11), char(m[19]), be32(m + 20), reinterpret_cast<const char*>(m + 24), be32(m + 32));
      break;
    case 'E':
      h.on_execute(hd, be64(m + 11), be32(m + 19));
      break;
    case 'C':
      h.on_execute_with_price(hd, be64(m + 11), be32(m + 19), char(m[31]), be32(m + 32));
      break;
    case 'X':
      h.on_cancel(hd, be64(m + 11), be32(m + 19));
      break;
    case 'D':
      h.on_delete(hd, be64(m + 11));
      break;
    case 'U':
      h.on_replace(hd, be64(m + 11), be64(m + 19), be32(m + 27), be32(m + 31));
      break;
    case 'S':
      h.on_system_event(hd, char(m[11]));
      break;
    case 'R':
      h.on_stock_directory(hd, reinterpret_cast<const char*>(m + 11));
      break;
    case 'H':
      h.on_trading_action(hd, reinterpret_cast<const char*>(m + 11), char(m[19]));
      break;
    default:
      h.on_other(hd);
      break;
  }
}

// Walk every complete message in [p, end). Returns bytes consumed; a trailing partial message
// is left for the caller to carry over into the next buffer.
template <class H>
std::size_t for_each_message(const std::uint8_t* p, const std::uint8_t* end, H& h, std::uint64_t& count) {
  const std::uint8_t* start = p;
  while (end - p >= 2) {
    std::size_t len = be16(p);
    if (std::size_t(end - p) < 2 + len) break;
    dispatch(p + 2, len, h);
    p += 2 + len;
    ++count;
  }
  return std::size_t(p - start);
}

// Copy an 8-char space-padded ITCH symbol into a NUL-terminated string.
inline void symbol_to_cstr(const char* s8, char out[9]) {
  int n = 8;
  while (n > 0 && s8[n - 1] == ' ') --n;
  std::memcpy(out, s8, n);
  out[n] = '\0';
}

}  // namespace itch
