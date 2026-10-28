// Unit tests: SPSC queue, MoldUDP64 framing, ITCH decode into queue updates, and the snapshot
// round trip (serialise books -> replay into empty books -> identical digest, priority included).
// The network paths are exercised end to end by scripts/run_demo.sh.

#include <cinttypes>
#include <cstdio>
#include <random>
#include <thread>
#include <vector>

#include "feed/moldudp64.hpp"
#include "feed/pending_buffer.hpp"
#include "feed/snapshot.hpp"
#include "feed/spsc_queue.hpp"
#include "feed/update.hpp"

using namespace feed;

static int g_fail = 0, g_pass = 0;
#define CHECK(c)                                                          \
  do {                                                                    \
    if (c) ++g_pass;                                                      \
    else {                                                                \
      ++g_fail;                                                           \
      std::fprintf(stderr, "FAIL %s:%d  %s\n", __FILE__, __LINE__, #c);   \
    }                                                                     \
  } while (0)

// ---- synthetic ITCH message builders (big-endian, exact spec sizes) ----
struct Msg {
  std::vector<std::uint8_t> b;
  Msg(char t, std::size_t len, std::uint16_t locate) : b(len, 0) {
    b[0] = std::uint8_t(t);
    mold::put_be16(&b[1], locate);
  }
};
static void stock8(std::uint8_t* p, const char* s) {
  std::memset(p, ' ', 8);
  std::memcpy(p, s, std::strlen(s));
}
static Msg R(std::uint16_t loc, const char* sym) {
  Msg m('R', 39, loc);
  stock8(&m.b[11], sym);
  return m;
}
static Msg A(std::uint16_t loc, std::uint64_t ref, char side, std::uint32_t qty, const char* sym, std::uint32_t px) {
  Msg m('A', 36, loc);
  mold::put_be64(&m.b[11], ref);
  m.b[19] = std::uint8_t(side);
  mold::put_be32(&m.b[20], qty);
  stock8(&m.b[24], sym);
  mold::put_be32(&m.b[32], px);
  return m;
}
static Msg E(std::uint16_t loc, std::uint64_t ref, std::uint32_t qty) {
  Msg m('E', 31, loc);
  mold::put_be64(&m.b[11], ref);
  mold::put_be32(&m.b[19], qty);
  return m;
}
static Msg X(std::uint16_t loc, std::uint64_t ref, std::uint32_t qty) {
  Msg m('X', 23, loc);
  mold::put_be64(&m.b[11], ref);
  mold::put_be32(&m.b[19], qty);
  return m;
}
static Msg D(std::uint16_t loc, std::uint64_t ref) {
  Msg m('D', 19, loc);
  mold::put_be64(&m.b[11], ref);
  return m;
}
static Msg U(std::uint16_t loc, std::uint64_t old_ref, std::uint64_t new_ref, std::uint32_t qty, std::uint32_t px) {
  Msg m('U', 35, loc);
  mold::put_be64(&m.b[11], old_ref);
  mold::put_be64(&m.b[19], new_ref);
  mold::put_be32(&m.b[27], qty);
  mold::put_be32(&m.b[31], px);
  return m;
}

// Feed a message through the same path the subscriber uses: decode into an Update, apply.
static void feed_msg(itch::BookBuilder& b, const Msg& m) {
  Update u{};
  decode_into(m.b.data(), u);
  apply(b, u);
}

static itch::BuilderConfig small_cfg() {
  itch::BuilderConfig c;
  c.max_orders = 1 << 16;
  c.max_levels = 1 << 14;
  c.max_books = 64;
  c.window = 1 << 10;
  return c;
}

// ---------------------------------------------------------------------------------------------

static void test_spsc_basic() {
  SpscQueue<std::uint64_t> q(4);
  std::uint64_t v;
  CHECK(!q.try_pop(v));
  for (std::uint64_t i = 0; i < 4; ++i) CHECK(q.try_push(i));
  CHECK(!q.try_push(99));  // full
  CHECK(q.size() == 4);
  for (int round = 0; round < 1000; ++round) {  // wrap many times
    CHECK(q.try_pop(v) && v == std::uint64_t(round));
    CHECK(q.try_push(std::uint64_t(round) + 4));
  }
  bool threw = false;
  try {
    SpscQueue<int> bad(3);
  } catch (const std::invalid_argument&) {
    threw = true;
  }
  CHECK(threw);
}

static void test_spsc_threads() {
  SpscQueue<Update> q(1024);
  const std::uint64_t n = 5'000'000;
  std::uint64_t bad = 0;
  std::thread c([&] {
    for (std::uint64_t want = 0; want < n;) {
      if (Update* u = q.front()) {
        if (u->seq != want || u->ref != want * 3) ++bad;
        q.pop();
        ++want;
      }
    }
  });
  for (std::uint64_t i = 0; i < n;) {
    if (Update* u = q.claim()) {
      u->seq = i;
      u->ref = i * 3;
      q.publish();
      ++i;
    }
  }
  c.join();
  CHECK(bad == 0);
  CHECK(q.size() == 0);
}

static void test_mold() {
  std::uint8_t buf[1500];
  mold::PacketBuilder pb("TESTSESS", 100);
  pb.begin(buf, 42);
  Msg a = A(1, 7, 'B', 100, "AAPL", 1000000), d = D(1, 7);
  CHECK(pb.fits(a.b.size()));
  pb.add(a.b.data(), a.b.size());
  CHECK(pb.fits(d.b.size()));
  pb.add(d.b.data(), d.b.size());
  CHECK(!pb.fits(a.b.size()));  // 20 + 38 + 21 + 38 > 100
  std::size_t len = pb.finish();
  CHECK(len == 20 + 2 + 36 + 2 + 19);

  mold::PacketView v{};
  CHECK(mold::parse_header(buf, len, v));
  CHECK(v.seq == 42 && v.count == 2 && v.messages() == 2 && !v.heartbeat() && !v.end_of_session());
  CHECK(std::memcmp(v.session, "TESTSESS  ", 10) == 0);
  CHECK(itch::be16(v.blocks) == 36 && v.blocks[2] == 'A');
  CHECK(itch::be16(v.blocks + 38) == 19 && v.blocks[40] == 'D');

  std::size_t hl = pb.control(buf, 77, mold::kHeartbeat);
  CHECK(hl == 20 && mold::parse_header(buf, hl, v) && v.heartbeat() && v.seq == 77 && v.messages() == 0);
  hl = pb.control(buf, 78, mold::kEndOfSession);
  CHECK(mold::parse_header(buf, hl, v) && v.end_of_session() && v.seq == 78);
  CHECK(!mold::parse_header(buf, 19, v));
}

static void test_decode() {
  Update u{};
  Msg a = A(5, 123456789012ull, 'S', 300, "MSFT", 1234500);
  decode_into(a.b.data(), u);
  CHECK(u.type == 'A' && u.locate == 5 && u.ref == 123456789012ull && u.aux == 'S' && u.shares == 300 &&
        u.price == 1234500 && std::memcmp(u.stock, "MSFT    ", 8) == 0);
  Msg r = U(5, 1, 2, 50, 999);
  decode_into(r.b.data(), u);
  CHECK(u.type == 'U' && u.ref == 1 && u.new_ref == 2 && u.shares == 50 && u.price == 999);
  Msg e = E(5, 9, 10);
  decode_into(e.b.data(), u);
  CHECK(u.type == 'E' && u.ref == 9 && u.shares == 10);
  CHECK(book_relevant('A') && book_relevant('U') && !book_relevant('P') && !book_relevant('I'));
  for (const Msg* m : {&a, &r, &e}) CHECK(itch::kSizes.size[m->b[0]] == m->b.size());
}

// Apply a random but valid ITCH-like stream to two builders; snapshot one mid-stream and rebuild
// a third from the snapshot. All three must agree, and keep agreeing as the stream continues.
static void test_snapshot_roundtrip() {
  const char* syms[4] = {"AAPL", "MSFT", "NVDA", "TSLA"};
  std::vector<Msg> stream;
  for (std::uint16_t i = 0; i < 4; ++i) stream.push_back(R(std::uint16_t(i + 1), syms[i]));
  std::mt19937_64 rng(12345);
  std::vector<std::pair<std::uint64_t, std::uint16_t>> live;  // (ref, locate)
  std::uint64_t next_ref = 1;
  for (int i = 0; i < 20000; ++i) {
    int k = int(rng() % 10);
    if (live.empty() || k < 5) {
      std::uint16_t loc = std::uint16_t(1 + rng() % 4);
      char side = rng() % 2 ? 'B' : 'S';
      std::uint32_t px = std::uint32_t(1000000 + (side == 'B' ? -1 : 1) * std::int64_t(100 * (1 + rng() % 50)));
      stream.push_back(A(loc, next_ref, side, std::uint32_t(100 * (1 + rng() % 5)), syms[loc - 1], px));
      live.push_back({next_ref++, loc});
    } else {
      std::size_t j = rng() % live.size();
      auto [ref, loc] = live[j];
      if (k == 5) {
        stream.push_back(E(loc, ref, 100));  // might fully execute; builder handles both
      } else if (k == 6) {
        stream.push_back(X(loc, ref, 100));
      } else if (k == 7) {
        stream.push_back(U(loc, ref, next_ref, 200, std::uint32_t(1000000 + 100 * (rng() % 20))));
        live[j].first = next_ref++;
        continue;
      } else {
        stream.push_back(D(loc, ref));
      }
      live[j] = live.back();
      live.pop_back();
    }
  }

  itch::BookBuilder truth(small_cfg());
  const std::size_t cut = stream.size() / 2;
  for (std::size_t i = 0; i < cut; ++i) feed_msg(truth, stream[i]);
  // Over-executions/unknown refs above are fine (an E or X of an order already gone is counted,
  // not applied). What matters is that every replica agrees.

  std::vector<std::uint8_t> snapbuf;
  std::uint64_t nmsgs = snap::serialize(truth, cut + 1, snapbuf);
  CHECK(snapbuf[0] == 'S');
  CHECK(itch::be64(&snapbuf[1]) == cut + 1);
  CHECK(itch::be64(&snapbuf[9]) == nmsgs);
  CHECK(itch::be64(&snapbuf[17]) == snapbuf.size() - snap::kSnapshotHeaderLen);

  // The replica already holds stale books (it applied only part of the stream, as a feed
  // handler that hit a gap would). clear() + snapshot must still produce the exact truth.
  itch::BookBuilder replica(small_cfg());
  for (std::size_t i = 0; i < cut / 3; ++i) feed_msg(replica, stream[i]);
  CHECK(snap::digest(replica).orders > 0);
  replica.clear();
  CHECK(snap::digest(replica).orders == 0);
  const std::uint64_t unknown_before = replica.stats().unknown_refs;
  std::uint64_t count = 0, bad = 0;
  const std::uint8_t* p = snapbuf.data() + snap::kSnapshotHeaderLen;
  const std::uint8_t* end = snapbuf.data() + snapbuf.size();
  while (end - p >= 2) {
    std::size_t len = itch::be16(p);
    const std::uint8_t* m = p + 2;
    p += 2 + len;
    ++count;
    if (itch::kSizes.size[m[0]] != len) ++bad;
    Update u{};
    decode_into(m, u);
    apply(replica, u);
  }
  CHECK(count == nmsgs && bad == 0);
  CHECK(replica.stats().unknown_refs == unknown_before && replica.stats().add_rejects == 0);

  snap::Digest dt = snap::digest(truth), dr = snap::digest(replica);
  CHECK(dt.orders > 100);
  CHECK(dt.books == 4 && dr.books == 4);
  CHECK(dt.orders == dr.orders);
  CHECK(dt.value() == dr.value());

  for (std::size_t i = cut; i < stream.size(); ++i) {
    feed_msg(truth, stream[i]);
    feed_msg(replica, stream[i]);
  }
  CHECK(snap::digest(truth).value() == snap::digest(replica).value());
  std::printf("  snapshot round trip: %" PRIu64 " messages, %zu bytes, %" PRIu64 " orders at the cut\n", nmsgs,
              snapbuf.size(), dt.orders);
}

// The digest must notice a difference in time priority alone.
static void test_digest_priority() {
  itch::BookBuilder b1(small_cfg()), b2(small_cfg());
  for (auto* b : {&b1, &b2}) feed_msg(*b, R(1, "AAPL"));
  feed_msg(b1, A(1, 1, 'B', 100, "AAPL", 1000000));
  feed_msg(b1, A(1, 2, 'B', 100, "AAPL", 1000000));
  feed_msg(b2, A(1, 2, 'B', 100, "AAPL", 1000000));
  feed_msg(b2, A(1, 1, 'B', 100, "AAPL", 1000000));
  CHECK(snap::digest(b1).value() != snap::digest(b2).value());
  CHECK(snap::digest(b1).orders == snap::digest(b2).orders);
}

// FIFO order across chunk boundaries, entries stay put while more are appended, the cap is
// enforced, and chunks are recycled once consumed.
static void test_pending_buffer() {
  PendingBuffer pb(4096, 4 * 4096);
  std::uint8_t pkt[1500];
  std::uint64_t pushed = 0, popped = 0;
  bool order_ok = true;
  // Interleave pushes and pops; hold a pointer to the front while pushing more.
  for (int round = 0; round < 50; ++round) {
    for (int i = 0; i < 7; ++i) {
      std::uint32_t len = std::uint32_t(20 + (pushed * 37) % 900);
      std::memset(pkt, int(pushed & 0xFF), len);
      if (pb.push(pkt, len, std::int64_t(pushed), 0, pushed)) ++pushed;
    }
    const PendingBuffer::Entry* e = pb.front();
    for (int i = 0; i < 3 && e; ++i) {
      if (std::uint64_t(e->kernel_ns) != popped || e->data()[0] != std::uint8_t(popped & 0xFF) ||
          e->data()[e->len - 1] != std::uint8_t(popped & 0xFF))
        order_ok = false;
      pb.pop();
      ++popped;
      e = pb.front();
    }
  }
  while (const PendingBuffer::Entry* e = pb.front()) {
    if (std::uint64_t(e->kernel_ns) != popped) order_ok = false;
    pb.pop();
    ++popped;
  }
  CHECK(order_ok);
  CHECK(pushed == popped && pb.empty() && pb.count() == 0 && pb.bytes() == 0);
  CHECK(pb.peak_bytes() <= 4 * 4096);

  // Cap: 4 chunks of 4 KB hold a bounded number of 1000-byte packets, then push fails.
  int stored = 0;
  std::memset(pkt, 1, sizeof pkt);
  while (pb.push(pkt, 1000, 0, 0, 0)) ++stored;
  CHECK(stored == 12);  // 3 x 1032-byte entries per 4 KB chunk
  while (pb.front()) pb.pop();
  CHECK(pb.push(pkt, 1000, 0, 0, 0));  // space is reusable after draining
}

int main() {
  test_spsc_basic();
  test_spsc_threads();
  test_mold();
  test_decode();
  test_snapshot_roundtrip();
  test_digest_priority();
  test_pending_buffer();
  std::printf("%d checks passed, %d failed\n", g_pass, g_fail);
  return g_fail ? 1 : 0;
}
