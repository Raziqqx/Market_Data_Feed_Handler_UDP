#pragma once
// TCP snapshot protocol, snapshot serialisation, and the book digest.
//
// A snapshot is expressed as ordinary ITCH messages: one 'S' (if continuous trading has started),
// then per instrument an 'R' (stock directory), an 'H' (if it is not in the default 'T' state)
// and one 'A' per resting order, best price first and oldest first within a price. Replaying
// those messages into an empty book rebuilds exactly the same book, time priority included.
// That means the client parses a snapshot with the very same decoder as the live feed.
//
// Requests (client -> server), 17 bytes: kind(1) a(8) b(8), big-endian:
//   'S' min_seq  -      "a snapshot that covers at least every message < min_seq"
//   'R' from     to     "retransmit messages [from, to)" (MoldUDP64-style gap fill)
//   'D' at_seq   -      "the digest of your books once you have applied every message < at_seq"
// Responses (server -> client), big-endian:
//   'S' next_seq(8) msg_count(8) payload_bytes(8)  then payload_bytes of [len16][ITCH msg]
//   'R' first_seq(8) msg_count(8) payload_bytes(8) then the messages; msg_count 0 = no longer
//                                                  available, ask for a snapshot instead
//   'D' next_seq(8) digest(8) orders(8) books(8)
// next_seq = the first sequence number NOT reflected in the snapshot/digest.

#include <cstdint>
#include <cstring>
#include <vector>

#include "feed/moldudp64.hpp"
#include "itch/book_builder.hpp"

namespace feed::snap {

inline constexpr std::size_t kRequestLen = 17;
inline constexpr std::size_t kSnapshotHeaderLen = 25;
inline constexpr std::size_t kDigestLen = 33;

inline void encode_request(std::uint8_t out[kRequestLen], char kind, std::uint64_t a, std::uint64_t b = 0) {
  out[0] = std::uint8_t(kind);
  mold::put_be64(out + 1, a);
  mold::put_be64(out + 9, b);
}

class Writer {
 public:
  explicit Writer(std::vector<std::uint8_t>& out) : out_(out) {}

  std::uint8_t* message(std::size_t len) {
    std::size_t at = out_.size();
    out_.resize(at + 2 + len);
    mold::put_be16(out_.data() + at, std::uint16_t(len));
    ++count_;
    return out_.data() + at + 2;
  }
  std::uint64_t count() const { return count_; }

 private:
  std::vector<std::uint8_t>& out_;
  std::uint64_t count_ = 0;
};

inline void put_header(std::uint8_t* m, char type, std::uint16_t locate) {
  m[0] = std::uint8_t(type);
  mold::put_be16(m + 1, locate);
  mold::put_be16(m + 3, 0);    // tracking
  std::memset(m + 5, 0, 6);    // timestamp: not meaningful for a snapshot
}

inline void put_stock(std::uint8_t* p, const char* sym) {
  std::memset(p, ' ', 8);
  std::memcpy(p, sym, std::min<std::size_t>(std::strlen(sym), 8));
}

// Serialise every book into `out` (header first). Returns the number of ITCH messages.
inline std::uint64_t serialize(const itch::BookBuilder& b, std::uint64_t next_seq, std::vector<std::uint8_t>& out) {
  out.clear();
  out.resize(kSnapshotHeaderLen);
  Writer w(out);
  if (b.continuous()) {
    std::uint8_t* m = w.message(12);
    put_header(m, 'S', 0);
    m[11] = 'Q';
  }
  b.for_each_book([&](std::uint16_t locate, const lob::OrderBook& book) {
    std::uint8_t* r = w.message(39);
    std::memset(r, 0, 39);
    put_header(r, 'R', locate);
    put_stock(r + 11, b.symbol(locate));
    if (b.trading_state(locate) != 'T') {
      std::uint8_t* h = w.message(25);
      std::memset(h, 0, 25);
      put_header(h, 'H', locate);
      put_stock(h + 11, b.symbol(locate));
      h[19] = std::uint8_t(b.trading_state(locate));
    }
    for (lob::Side s : {lob::Side::Buy, lob::Side::Sell}) {
      book.for_each_order(s, [&](const lob::Order& o) {
        std::uint8_t* a = w.message(36);
        put_header(a, 'A', locate);
        mold::put_be64(a + 11, o.id);
        a[19] = s == lob::Side::Buy ? 'B' : 'S';
        mold::put_be32(a + 20, o.qty);
        put_stock(a + 24, b.symbol(locate));
        mold::put_be32(a + 32, o.price);
      });
    }
  });
  out[0] = 'S';
  mold::put_be64(out.data() + 1, next_seq);
  mold::put_be64(out.data() + 9, w.count());
  mold::put_be64(out.data() + 17, out.size() - kSnapshotHeaderLen);
  return w.count();
}

struct Digest {
  std::uint64_t hash = 0xcbf29ce484222325ull;  // FNV-1a offset basis
  std::uint64_t orders = 0;
  std::uint64_t books = 0;

  void mix(std::uint64_t v) {
    // FNV-1a over 64-bit words, then a final avalanche in value(). Order-sensitive on purpose:
    // two books with the same orders in a different time priority must not match.
    hash ^= v;
    hash *= 0x100000001b3ull;
  }
  std::uint64_t value() const {
    std::uint64_t z = hash + 0x9e3779b97f4a7c15ull;
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ull;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebull;
    return z ^ (z >> 31);
  }
};

// Hash of every resting order of every book, in priority order. Two processes that hold the
// same books (same orders, same queue positions) produce the same digest.
inline Digest digest(const itch::BookBuilder& b) {
  Digest d;
  b.for_each_book([&](std::uint16_t locate, const lob::OrderBook& book) {
    ++d.books;
    d.mix(locate);
    for (lob::Side s : {lob::Side::Buy, lob::Side::Sell}) {
      d.mix(0xB0B0 + std::uint64_t(s));
      book.for_each_order(s, [&](const lob::Order& o) {
        ++d.orders;
        d.mix(o.id);
        d.mix(std::uint64_t(o.price) << 32 | o.qty);
      });
    }
  });
  return d;
}

}  // namespace feed::snap
