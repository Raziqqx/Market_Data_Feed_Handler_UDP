#pragma once
// Limit order book for one instrument, plus a price-time-priority matching engine.
//
// Data layout
//   * Orders live in a shared preallocated Pool<Order> and are found by id through a shared
//     OrderIdMap. Both are owned by OrderStore so one id space spans every book (as in ITCH,
//     where order reference numbers are unique across all stocks for the day).
//   * Each price level is a PriceLevel (from a shared pool) holding an intrusive doubly linked
//     FIFO of orders. Appending to the tail = time priority; unlinking = O(1) cancel.
//   * Each side has a flat array ("window") of level handles indexed by
//         tick_key(price) - base_key
//     so locating a level is one subtraction and one array load. A two-level bitmap tracks
//     which slots are non-empty so the next best level is found with a few bit instructions.
//   * Prices that fall outside the window, or are not on the tick grid, go to a small sorted
//     "overflow" vector. These are rare (far-from-touch orders) and keep the window small.
//
// Nothing on the hot path calls malloc. The only exception is the overflow vector growing past
// its reserved capacity, which is counted (overflow_grows()) so it can be reported.

#include <algorithm>
#include <cassert>
#include <string>
#include <utility>
#include <vector>

#include "lob/arena.hpp"
#include "lob/bitset.hpp"
#include "lob/common.hpp"
#include "lob/order_id_map.hpp"
#include "lob/pool.hpp"

namespace lob {

// Maps a price to a dense, monotonic integer "tick key".
//
// Prices below `boundary` move in steps of `fine`, prices at or above it in steps of `coarse`.
// US equities (Reg NMS Rule 612): $0.0001 ticks below $1.00 and $0.01 ticks from $1.00 up. With
// that rule a $150 stock has key 10000 + 14900 = 24900 instead of 1,500,000, so a modest window
// covers a wide price range.
class TickRule {
 public:
  TickRule(Price boundary, Price fine, Price coarse)
      : boundary_(boundary), fine_(fine), coarse_(coarse), fine_m_(magic(fine)), coarse_m_(magic(coarse)),
        boundary_key_(boundary / fine) {
    assert(fine > 0 && coarse > 0 && boundary % fine == 0);
  }
  static TickRule uniform(Price tick) { return TickRule(0, tick, tick); }
  static TickRule us_equities() { return TickRule(kPriceScale, 1, 100); }

  // False if the price is not a multiple of the applicable tick.
  LOB_ALWAYS_INLINE bool key(Price p, std::uint64_t& k) const {
    if (p < boundary_) {
      std::uint32_t q = div(p, fine_, fine_m_);
      k = q;
      return q * fine_ == p;
    }
    std::uint32_t d = p - boundary_;
    std::uint32_t q = div(d, coarse_, coarse_m_);
    k = boundary_key_ + q;
    return q * coarse_ == d;
  }
  bool on_grid(Price p) const {
    std::uint64_t k;
    return key(p, k);
  }

 private:
  // Division by a runtime constant via multiply-high: exact for all 32-bit numerators.
  static std::uint64_t magic(std::uint32_t d) { return d == 1 ? 0 : ~0ull / d + 1; }
  __extension__ typedef unsigned __int128 u128;
  LOB_ALWAYS_INLINE static std::uint32_t div(std::uint32_t x, std::uint32_t d, std::uint64_t m) {
    if (d == 1) return x;
    return static_cast<std::uint32_t>((static_cast<u128>(x) * m) >> 64);
  }

  Price boundary_, fine_, coarse_;
  std::uint64_t fine_m_, coarse_m_;
  std::uint64_t boundary_key_;
};

// Pools and the id index shared by all books.
class OrderStore {
 public:
  OrderStore(std::size_t max_orders, std::size_t max_levels, IdHash hash = IdHash::Mix)
      : orders(max_orders), levels(max_levels), ids(max_orders, orders, hash) {
    for (auto& v : scratch) v.reserve(1 << 16);
  }

  void prefault() {
    orders.prefault();
    levels.prefault();
    ids.prefault();
  }

  Pool<Order> orders;
  Pool<PriceLevel> levels;
  OrderIdMap ids;
  std::vector<Handle> scratch[2];  // reused by OrderBook::rebase, so recentering does not allocate
};

// Preallocated window memory (level-handle slots + bitmaps) for up to `max_books` books.
class LadderArena {
 public:
  LadderArena(std::size_t max_books, std::uint32_t window)
      : window_(window), bit_words_(HierBitset::words_needed(window)), max_books_(max_books),
        slots_(max_books * 2 * std::size_t(window)), bits_(max_books * 2 * bit_words_) {
    assert(window % 64 == 0 && window > 0);
  }
  Handle* slots(std::size_t book, Side s) { return slots_.data() + (book * 2 + std::size_t(s)) * window_; }
  std::uint64_t* bits(std::size_t book, Side s) { return bits_.data() + (book * 2 + std::size_t(s)) * bit_words_; }
  std::uint32_t window() const { return window_; }
  std::size_t max_books() const { return max_books_; }
  std::size_t bytes() const { return slots_.bytes() + bits_.bytes(); }
  void prefault() {
    slots_.prefault();
    bits_.prefault();
  }

 private:
  std::uint32_t window_;
  std::size_t bit_words_;
  std::size_t max_books_;
  Arena<Handle> slots_;
  Arena<std::uint64_t> bits_;
};

enum class SubmitStatus : std::uint8_t { Accepted, Rejected };

struct SubmitResult {
  SubmitStatus status = SubmitStatus::Accepted;
  Qty filled = 0;     // executed immediately
  Qty resting = 0;    // now resting on the book
  Qty cancelled = 0;  // remainder dropped (IOC / market / pool exhaustion)
};

struct NullListener {
  void on_trade(const Trade&) {}
};

class OrderBook {
 public:
  OrderBook(BookId id, OrderStore& store, LadderArena& arena, std::size_t arena_index, TickRule rule)
      : id_(id), store_(store), rule_(rule), window_(arena.window()) {
    for (Side s : {Side::Buy, Side::Sell}) {
      Ladder& L = side_[std::size_t(s)];
      L.slots = arena.slots(arena_index, s);
      L.bits = HierBitset(arena.bits(arena_index, s), window_);
      L.overflow.reserve(kOverflowReserve);  // startup allocation, not on the hot path
    }
  }
  OrderBook(const OrderBook&) = delete;
  OrderBook& operator=(const OrderBook&) = delete;

  // ------------------------------------------------------------------------------------------
  // Feed-style operations (used by ITCH replay). These never match: the exchange already did.
  // ------------------------------------------------------------------------------------------

  // Add a resting order. Returns its handle, or kNull if rejected (qty 0, duplicate id, or a
  // pool is exhausted).
  Handle add(OrderId id, Side side, Price price, Qty qty) {
    if (LOB_UNLIKELY(qty == 0)) return kNull;
    Handle h = store_.orders.alloc();
    if (LOB_UNLIKELY(h == kNull)) return kNull;
    Order& o = store_.orders[h];
    o.id = id;
    o.price = price;
    o.qty = qty;
    o.book = id_;
    o.side = side;
    if (LOB_UNLIKELY(!store_.ids.insert_unique(id, h))) {
      store_.orders.free(h);
      return kNull;
    }
    Handle lv = get_or_create_level(side, price);
    if (LOB_UNLIKELY(lv == kNull)) {
      store_.ids.erase(id);
      store_.orders.free(h);
      return kNull;
    }
    link_tail(lv, h);
    return h;
  }

  // Reduce an order's open quantity (execution or partial cancel). Removes it at zero.
  // Returns the quantity actually removed (clamped to the open quantity).
  Qty reduce(Handle h, Qty qty) {
    Order& o = store_.orders[h];
    if (qty >= o.qty) {
      Qty q = o.qty;
      remove(h);
      return q;
    }
    o.qty -= qty;
    store_.levels[o.level].total -= qty;
    return qty;
  }

  // Remove an order entirely. O(1): unlink from its level's list, erase from the id map.
  void remove(Handle h) {
    Order& o = store_.orders[h];
    Handle lv = o.level;
    Side s = o.side;
    unlink(lv, h);
    store_.ids.erase(o.id);
    store_.orders.free(h);
    if (store_.levels[lv].count == 0) remove_level(s, lv);
  }

  // ITCH "Order Replace": cancel the old order and add a new one (new id, loses priority).
  Handle replace(Handle h, OrderId new_id, Price price, Qty qty) {
    Side s = store_.orders[h].side;
    remove(h);
    return add(new_id, s, price, qty);
  }

  // ------------------------------------------------------------------------------------------
  // Matching-engine operations.
  // Listener must provide on_trade(const Trade&) and must not modify the book from the callback.
  // ------------------------------------------------------------------------------------------

  template <class Listener>
  SubmitResult submit(OrderId id, Side side, OrderType type, TimeInForce tif, Price price, Qty qty,
                      Listener& listener) {
    SubmitResult r;
    const bool market = type == OrderType::Market;
    if (qty == 0 || store_.ids.find(id) != kNull || (!market && !rule_.on_grid(price))) {
      r.status = SubmitStatus::Rejected;
      return r;
    }
    r.filled = match(id, side, price, market, qty, listener);
    Qty rem = qty - r.filled;
    if (rem == 0) return r;
    if (!market && tif == TimeInForce::GTC && add(id, side, price, rem) != kNull) {
      r.resting = rem;
    } else {
      r.cancelled = rem;
    }
    return r;
  }

  template <class Listener>
  SubmitResult limit(OrderId id, Side side, Price price, Qty qty, Listener& l) {
    return submit(id, side, OrderType::Limit, TimeInForce::GTC, price, qty, l);
  }
  template <class Listener>
  SubmitResult ioc(OrderId id, Side side, Price price, Qty qty, Listener& l) {
    return submit(id, side, OrderType::Limit, TimeInForce::IOC, price, qty, l);
  }
  template <class Listener>
  SubmitResult market(OrderId id, Side side, Qty qty, Listener& l) {
    return submit(id, side, OrderType::Market, TimeInForce::IOC, 0, qty, l);
  }

  // Cancel by id. O(1).
  bool cancel(OrderId id) {
    Handle h = lookup(id);
    if (h == kNull) return false;
    remove(h);
    return true;
  }

  // Modify price and/or open quantity, with standard exchange semantics:
  //   * same price and quantity down -> amended in place, KEEPS time priority;
  //   * price change or quantity up  -> cancel/replace, LOSES priority, and may trade if the
  //     new price crosses the spread.
  //   * new_qty == 0                 -> cancel.
  template <class Listener>
  SubmitResult modify(OrderId id, Price new_price, Qty new_qty, Listener& listener) {
    SubmitResult r;
    Handle h = lookup(id);
    if (h == kNull) {
      r.status = SubmitStatus::Rejected;
      return r;
    }
    Order& o = store_.orders[h];
    if (new_qty == 0) {
      r.cancelled = o.qty;
      remove(h);
      return r;
    }
    if (new_price == o.price && new_qty <= o.qty) {
      store_.levels[o.level].total -= (o.qty - new_qty);
      o.qty = new_qty;
      r.resting = new_qty;
      return r;
    }
    if (!rule_.on_grid(new_price)) {
      r.status = SubmitStatus::Rejected;
      return r;
    }
    Side s = o.side;
    remove(h);
    return submit(id, s, OrderType::Limit, TimeInForce::GTC, new_price, new_qty, listener);
  }

  // ------------------------------------------------------------------------------------------
  // Queries
  // ------------------------------------------------------------------------------------------

  Handle lookup(OrderId id) const {
    Handle h = store_.ids.find(id);
    return (h != kNull && store_.orders[h].book == id_) ? h : kNull;
  }
  const Order& order(Handle h) const { return store_.orders[h]; }

  Handle best_level(Side s) const { return side_[std::size_t(s)].best; }
  const PriceLevel& level(Handle h) const { return store_.levels[h]; }

  TopOfBook top() const {
    TopOfBook t;
    if (Handle b = side_[0].best) {
      t.bid_price = store_.levels[b].price;
      t.bid_qty = store_.levels[b].total;
    }
    if (Handle a = side_[1].best) {
      t.ask_price = store_.levels[a].price;
      t.ask_qty = store_.levels[a].total;
    }
    return t;
  }

  // Full depth as (price, total qty), best first. Allocates: for tests and verification only.
  std::vector<std::pair<Price, std::uint64_t>> depth(Side s) const {
    std::vector<std::pair<Price, std::uint64_t>> out;
    const Ladder& L = side_[std::size_t(s)];
    for (std::int64_t i = L.bits.find_next(0); i >= 0; i = L.bits.find_next(std::size_t(i) + 1)) {
      const PriceLevel& lv = store_.levels[L.slots[i]];
      out.emplace_back(lv.price, lv.total);
    }
    for (const auto& e : L.overflow) out.emplace_back(e.price, store_.levels[e.level].total);
    if (s == Side::Buy)
      std::sort(out.begin(), out.end(), [](auto& a, auto& b) { return a.first > b.first; });
    else
      std::sort(out.begin(), out.end(), [](auto& a, auto& b) { return a.first < b.first; });
    return out;
  }

  // Remove every resting order, keeping all memory mapped. O(live orders).
  // [feed handler addition: rebuilding from a snapshot must not unmap and re-fault the arenas]
  void clear() {
    for (Side s : {Side::Buy, Side::Sell})
      while (Handle lv = side_[std::size_t(s)].best) remove(store_.levels[lv].head);
  }

  // Visit every resting order on one side: best price first, oldest first within a level.
  // Allocates (collects and sorts the levels): for snapshots and verification only.
  // [feed handler addition: the snapshot server serialises books in exact priority order]
  template <class F>
  void for_each_order(Side s, F&& f) const {
    std::vector<Handle> lvls;
    const Ladder& L = side_[std::size_t(s)];
    for (std::int64_t i = L.bits.find_next(0); i >= 0; i = L.bits.find_next(std::size_t(i) + 1))
      lvls.push_back(L.slots[i]);
    for (const auto& e : L.overflow) lvls.push_back(e.level);
    std::sort(lvls.begin(), lvls.end(), [&](Handle a, Handle b) {
      return s == Side::Buy ? store_.levels[a].price > store_.levels[b].price
                            : store_.levels[a].price < store_.levels[b].price;
    });
    for (Handle lv : lvls)
      for (Handle h = store_.levels[lv].head; h != kNull; h = store_.orders[h].next) f(store_.orders[h]);
  }

  std::size_t level_count(Side s) const { return side_[std::size_t(s)].levels; }
  BookId id() const { return id_; }
  std::uint64_t overflow_levels_created() const { return overflow_created_; }
  std::uint64_t overflow_grows() const { return overflow_grows_; }
  std::uint64_t recenters() const { return recenters_; }
  std::size_t overflow_size(Side s) const { return side_[std::size_t(s)].overflow.size(); }

  // Exhaustive self-check of every invariant. Slow; used by tests and --verify.
  bool check_invariants(std::string* err = nullptr) const;

 private:
  static constexpr std::size_t kOverflowReserve = 64;  // ~9 MB across a full day of books

  struct OverflowEntry {
    Price price;
    Handle level;
  };
  struct Ladder {
    Handle* slots = nullptr;
    HierBitset bits;
    std::vector<OverflowEntry> overflow;  // sorted ascending by price
    Handle best = kNull;
    std::size_t levels = 0;
  };

  static bool better(Side s, Price a, Price b) { return s == Side::Buy ? a > b : a < b; }

  // Window slot for a price, or -1 if it belongs in the overflow.
  LOB_ALWAYS_INLINE std::int64_t window_index(Price p) const {
    std::uint64_t k;
    if (LOB_UNLIKELY(!rule_.key(p, k))) return -1;
    std::uint64_t idx = k - base_key_;  // wraps to huge if k < base_key_
    return idx < window_ ? std::int64_t(idx) : -1;
  }

  Handle get_or_create_level(Side s, Price p) {
    Ladder& L = side_[std::size_t(s)];
    if (LOB_UNLIKELY(side_[0].levels + side_[1].levels == 0)) anchor(p);
    std::int64_t idx = window_index(p);
    Handle lv;
    if (LOB_LIKELY(idx >= 0)) {
      lv = L.slots[idx];
      if (lv != kNull) return lv;
      lv = new_level(p);
      if (lv == kNull) return kNull;
      L.slots[idx] = lv;
      L.bits.set(std::size_t(idx));
    } else {
      auto it = std::lower_bound(L.overflow.begin(), L.overflow.end(), p,
                                 [](const OverflowEntry& e, Price x) { return e.price < x; });
      if (it != L.overflow.end() && it->price == p) return it->level;
      lv = new_level(p);
      if (lv == kNull) return kNull;
      if (L.overflow.size() == L.overflow.capacity()) ++overflow_grows_;
      L.overflow.insert(it, OverflowEntry{p, lv});
      ++overflow_created_;
    }
    ++L.levels;
    if (L.best == kNull || better(s, p, store_.levels[L.best].price)) {
      L.best = lv;
      if (LOB_UNLIKELY(idx < 0)) maybe_recenter();  // the touch moved outside the window
    }
    return lv;
  }

  Handle new_level(Price p) {
    Handle lv = store_.levels.alloc();
    if (lv == kNull) return kNull;
    PriceLevel& l = store_.levels[lv];
    l.price = p;
    l.count = 0;
    l.total = 0;
    l.head = l.tail = kNull;
    return lv;
  }

  void remove_level(Side s, Handle lv) {
    Ladder& L = side_[std::size_t(s)];
    Price p = store_.levels[lv].price;
    std::int64_t idx = window_index(p);
    if (LOB_LIKELY(idx >= 0)) {
      L.slots[idx] = kNull;
      L.bits.clear(std::size_t(idx));
    } else {
      auto it = std::lower_bound(L.overflow.begin(), L.overflow.end(), p,
                                 [](const OverflowEntry& e, Price x) { return e.price < x; });
      assert(it != L.overflow.end() && it->level == lv);
      L.overflow.erase(it);
    }
    store_.levels.free(lv);
    --L.levels;
    if (L.best == lv) {
      L.best = find_best(s, idx);
      if (LOB_UNLIKELY(L.best != kNull && window_index(store_.levels[L.best].price) < 0)) maybe_recenter();
    }
  }

  // Recompute the best level after the old best (at window index `from`, or -1 if it was in the
  // overflow) was removed. Everything better than the removed level is already empty.
  Handle find_best(Side s, std::int64_t from) const {
    const Ladder& L = side_[std::size_t(s)];
    Handle w = kNull, o = kNull;
    if (s == Side::Buy) {
      std::int64_t i = L.bits.find_prev(from >= 0 ? std::size_t(from) : window_ - 1);
      if (i >= 0) w = L.slots[i];
      if (!L.overflow.empty()) o = L.overflow.back().level;
    } else {
      std::int64_t i = L.bits.find_next(from >= 0 ? std::size_t(from) : 0);
      if (i >= 0) w = L.slots[i];
      if (!L.overflow.empty()) o = L.overflow.front().level;
    }
    if (w == kNull) return o;
    if (o == kNull) return w;
    return better(s, store_.levels[o].price, store_.levels[w].price) ? o : w;
  }

  // Anchor the window around `p`. Only legal while the book is completely empty.
  void anchor(Price p) {
    std::uint64_t k;
    if (!rule_.key(p, k)) return;
    base_key_ = k > window_ / 2 ? k - window_ / 2 : 0;
  }

  // A best level lies outside the window. If the two touches fit comfortably in one window,
  // re-anchor the window around them so the active part of the book is back on the fast path.
  // If the spread itself is wider than half a window (an illiquid book), leave things alone:
  // recentering would just chase one side and then the other.
  void maybe_recenter() {
    std::uint64_t kb = 0, ka = 0;
    const bool hb = side_[0].best != kNull;
    const bool ha = side_[1].best != kNull;
    // An off-grid touch can never live in the window, so moving the window cannot help.
    if (hb && !rule_.key(store_.levels[side_[0].best].price, kb)) return;
    if (ha && !rule_.key(store_.levels[side_[1].best].price, ka)) return;
    auto inside = [&](std::uint64_t k) { return k - base_key_ < window_; };
    if ((!hb || inside(kb)) && (!ha || inside(ka))) return;  // both touches already on the fast path
    std::uint64_t center;
    if (hb && ha) {
      std::uint64_t lo = kb < ka ? kb : ka, hi = kb < ka ? ka : kb;
      if (hi - lo >= window_ / 2) return;
      center = lo + (hi - lo) / 2;
    } else if (hb) {
      center = kb;
    } else if (ha) {
      center = ka;
    } else {
      return;
    }
    std::uint64_t nb = center > window_ / 2 ? center - window_ / 2 : 0;
    if (nb != base_key_) rebase(nb);
  }

  // Move the window to start at key `nb`: pull every level out, then put each one back in its new
  // slot or in the overflow. O(levels + window/64). Level handles do not change, so `best` and
  // every order's `level` stay valid.
  LOB_NOINLINE void rebase(std::uint64_t nb) {
    ++recenters_;
    for (Side s : {Side::Buy, Side::Sell}) {
      Ladder& L = side_[std::size_t(s)];
      std::vector<Handle>& tmp = store_.scratch[std::size_t(s)];
      tmp.clear();
      for (std::int64_t i = L.bits.find_next(0); i >= 0; i = L.bits.find_next(std::size_t(i) + 1)) {
        tmp.push_back(L.slots[i]);
        L.slots[i] = kNull;
        L.bits.clear(std::size_t(i));
      }
      for (const OverflowEntry& e : L.overflow) tmp.push_back(e.level);
      L.overflow.clear();
    }
    base_key_ = nb;
    for (Side s : {Side::Buy, Side::Sell}) {
      Ladder& L = side_[std::size_t(s)];
      for (Handle lv : store_.scratch[std::size_t(s)]) {
        Price p = store_.levels[lv].price;
        std::int64_t idx = window_index(p);
        if (idx >= 0) {
          L.slots[idx] = lv;
          L.bits.set(std::size_t(idx));
        } else {
          if (L.overflow.size() == L.overflow.capacity()) ++overflow_grows_;
          L.overflow.push_back(OverflowEntry{p, lv});
        }
      }
      std::sort(L.overflow.begin(), L.overflow.end(),
                [](const OverflowEntry& a, const OverflowEntry& b) { return a.price < b.price; });
    }
  }

  LOB_ALWAYS_INLINE void link_tail(Handle lv, Handle h) {
    PriceLevel& l = store_.levels[lv];
    Order& o = store_.orders[h];
    o.level = lv;
    o.next = kNull;
    o.prev = l.tail;
    if (l.tail != kNull)
      store_.orders[l.tail].next = h;
    else
      l.head = h;
    l.tail = h;
    ++l.count;
    l.total += o.qty;
  }

  LOB_ALWAYS_INLINE void unlink(Handle lv, Handle h) {
    PriceLevel& l = store_.levels[lv];
    Order& o = store_.orders[h];
    if (o.prev != kNull)
      store_.orders[o.prev].next = o.next;
    else
      l.head = o.next;
    if (o.next != kNull)
      store_.orders[o.next].prev = o.prev;
    else
      l.tail = o.prev;
    --l.count;
    l.total -= o.qty;
  }

  // Walk the opposite side best-first, filling against resting orders in FIFO order.
  template <class Listener>
  Qty match(OrderId taker, Side s, Price limit, bool market, Qty qty, Listener& listener) {
    const Side os = opposite(s);
    Ladder& opp = side_[std::size_t(os)];
    Qty filled = 0;
    while (qty > 0 && opp.best != kNull) {
      const Handle lvh = opp.best;
      PriceLevel& lv = store_.levels[lvh];
      if (!market && (s == Side::Buy ? lv.price > limit : lv.price < limit)) break;  // no longer crosses
      while (qty > 0 && lv.head != kNull) {
        const Handle mh = lv.head;
        Order& m = store_.orders[mh];
        const Qty fill = qty < m.qty ? qty : m.qty;
        listener.on_trade(Trade{taker, m.id, lv.price, fill, s});
        qty -= fill;
        filled += fill;
        m.qty -= fill;
        lv.total -= fill;
        if (m.qty == 0) {
          lv.head = m.next;
          if (lv.head != kNull)
            store_.orders[lv.head].prev = kNull;
          else
            lv.tail = kNull;
          --lv.count;
          store_.ids.erase(m.id);
          store_.orders.free(mh);
        }
      }
      if (lv.count == 0) remove_level(os, lvh);
    }
    return filled;
  }

  BookId id_;
  OrderStore& store_;
  TickRule rule_;
  std::uint32_t window_;
  std::uint64_t base_key_ = 0;
  Ladder side_[2];
  std::uint64_t overflow_created_ = 0;
  std::uint64_t overflow_grows_ = 0;
  std::uint64_t recenters_ = 0;
};

inline bool OrderBook::check_invariants(std::string* err) const {
  auto fail = [&](const std::string& m) {
    if (err) *err = "book " + std::to_string(id_) + ": " + m;
    return false;
  };
  for (Side s : {Side::Buy, Side::Sell}) {
    const Ladder& L = side_[std::size_t(s)];
    std::size_t nlevels = 0;
    Handle best = kNull;
    auto visit = [&](Handle lvh, Price expect_price) -> bool {
      const PriceLevel& lv = store_.levels[lvh];
      if (lv.price != expect_price) return fail("level price mismatch");
      if (lv.count == 0) return fail("empty level left in ladder");
      std::uint64_t total = 0;
      std::uint32_t count = 0;
      Handle prev = kNull;
      for (Handle h = lv.head; h != kNull; h = store_.orders[h].next) {
        const Order& o = store_.orders[h];
        if (o.prev != prev) return fail("broken prev link");
        if (o.level != lvh || o.price != lv.price || o.side != s || o.book != id_) return fail("order/level mismatch");
        if (o.qty == 0) return fail("zero-qty order resting");
        if (store_.ids.find(o.id) != h) return fail("id map does not point at order");
        total += o.qty;
        ++count;
        prev = h;
        if (count > lv.count) return fail("list longer than count");
      }
      if (prev != lv.tail) return fail("tail mismatch");
      if (count != lv.count || total != lv.total) return fail("level count/total mismatch");
      ++nlevels;
      if (best == kNull || better(s, lv.price, store_.levels[best].price)) best = lvh;
      return true;
    };
    for (std::size_t i = 0; i < window_; ++i) {
      bool has = L.slots[i] != kNull;
      if (has != L.bits.test(i)) return fail("bitmap disagrees with slots");
      if (has) {
        const PriceLevel& lv = store_.levels[L.slots[i]];
        if (window_index(lv.price) != std::int64_t(i)) return fail("level in wrong slot");
        if (!visit(L.slots[i], lv.price)) return false;
      }
    }
    for (std::size_t i = 0; i < L.overflow.size(); ++i) {
      if (i && L.overflow[i - 1].price >= L.overflow[i].price) return fail("overflow not sorted");
      if (window_index(L.overflow[i].price) >= 0) return fail("overflow entry belongs in window");
      if (!visit(L.overflow[i].level, L.overflow[i].price)) return false;
    }
    if (nlevels != L.levels) return fail("level count mismatch");
    if (best != L.best) return fail("cached best is not the best level");
  }
  return true;
}

}  // namespace lob
