#pragma once
// Rebuilds every instrument's limit order book from an ITCH 5.0 stream.
//
// ITCH is a market-data feed: NASDAQ's own engine has already matched everything, and the feed
// tells us the consequences (order added, executed, cancelled, deleted, replaced). So replay
// never matches; it applies feed events to the books using the O(1) handle operations.
//
// Besides building books, it counts things that should never happen if the book is right:
//   * an execute/cancel/delete/replace referring to an order we do not know;
//   * cancelling or executing more shares than the order has open;
//   * during continuous trading, an 'E' execution of an order that is not at the best price of
//     its side. NASDAQ fills price-first, so every execution of a displayed order in continuous
//     trading must hit the touch. This checks our book against the exchange's own matching.

#include <array>
#include <memory>
#include <vector>

#include "itch/itch.hpp"
#include "lob/order_book.hpp"

namespace itch {

struct BuilderConfig {
  std::size_t max_orders = std::size_t(1) << 24;
  std::size_t max_levels = std::size_t(1) << 22;
  std::size_t max_books = 16384;
  std::uint32_t window = 1u << 14;
  lob::IdHash id_hash = lob::IdHash::Identity;  // ITCH refs are sequential: see order_id_map.hpp
};

struct BuilderStats {
  std::uint64_t adds = 0, executes = 0, executes_with_price = 0, cancels = 0, deletes = 0, replaces = 0;
  std::uint64_t add_rejects = 0;          // duplicate id or pool exhausted
  std::uint64_t unknown_refs = 0;         // E/C/X/D/U for an order not in the book
  std::uint64_t over_reductions = 0;      // executed/cancelled more than open qty
  std::uint64_t locate_mismatches = 0;    // message locate != the order's book
  std::uint64_t bad_messages = 0;         // unknown type or wrong length
  std::uint64_t books_dropped = 0;        // more instruments than max_books
  std::uint64_t exec_continuous = 0;      // 'E' during continuous trading for a 'T' stock
  std::uint64_t exec_not_at_touch = 0;    // ...of which the order was not at the best level
};

class BookBuilder : public NullHandler {
 public:
  explicit BookBuilder(const BuilderConfig& cfg)
      : cfg_(cfg), store_(cfg.max_orders, cfg.max_levels, cfg.id_hash), arena_(cfg.max_books, cfg.window), books_(65536) {
    state_.fill('T');
    for (auto& s : symbols_) s[0] = '\0';
  }

  void prefault() {
    store_.prefault();
    arena_.prefault();
  }

  // Empty every book but keep the books, the symbols and all memory. Used by the feed handler
  // before it applies a snapshot. [feed handler addition]
  void clear() {
    for (auto& b : books_)
      if (b) b->clear();
    state_.fill('T');
    continuous_ = false;
  }

  // ---- ITCH handler ----

  void on_system_event(const Header&, char code) {
    if (code == 'Q') continuous_ = true;  // start of market hours (09:30)
    if (code == 'M') continuous_ = false;  // end of market hours (16:00)
  }

  void on_stock_directory(const Header& h, const char* stock) { create(h.locate, stock); }

  void on_trading_action(const Header& h, const char*, char state) { state_[h.locate] = state; }

  LOB_ALWAYS_INLINE void on_add(const Header& h, std::uint64_t ref, char side, std::uint32_t shares, const char* stock,
                                std::uint32_t price) {
    ++stats_.adds;
    lob::OrderBook* b = books_[h.locate].get();
    if (LOB_UNLIKELY(!b)) b = create(h.locate, stock);
    if (LOB_UNLIKELY(!b || b->add(ref, side == 'B' ? lob::Side::Buy : lob::Side::Sell, price, shares) == lob::kNull))
      ++stats_.add_rejects;
  }

  LOB_ALWAYS_INLINE void on_execute(const Header& h, std::uint64_t ref, std::uint32_t shares) {
    ++stats_.executes;
    lob::Handle o = find(h, ref);
    if (LOB_UNLIKELY(o == lob::kNull)) return;
    lob::OrderBook& b = *books_[store_.orders[o].book];
    if (continuous_ && state_[h.locate] == 'T') {
      ++stats_.exec_continuous;
      const lob::Order& ord = store_.orders[o];
      if (b.best_level(ord.side) != ord.level) ++stats_.exec_not_at_touch;
    }
    reduce(b, o, shares);
  }

  LOB_ALWAYS_INLINE void on_execute_with_price(const Header& h, std::uint64_t ref, std::uint32_t shares, char,
                                               std::uint32_t) {
    ++stats_.executes_with_price;
    lob::Handle o = find(h, ref);
    if (LOB_UNLIKELY(o == lob::kNull)) return;
    reduce(*books_[store_.orders[o].book], o, shares);
  }

  LOB_ALWAYS_INLINE void on_cancel(const Header& h, std::uint64_t ref, std::uint32_t shares) {
    ++stats_.cancels;
    lob::Handle o = find(h, ref);
    if (LOB_UNLIKELY(o == lob::kNull)) return;
    reduce(*books_[store_.orders[o].book], o, shares);
  }

  LOB_ALWAYS_INLINE void on_delete(const Header& h, std::uint64_t ref) {
    ++stats_.deletes;
    lob::Handle o = find(h, ref);
    if (LOB_UNLIKELY(o == lob::kNull)) return;
    books_[store_.orders[o].book]->remove(o);
  }

  LOB_ALWAYS_INLINE void on_replace(const Header& h, std::uint64_t old_ref, std::uint64_t new_ref,
                                    std::uint32_t shares, std::uint32_t price) {
    ++stats_.replaces;
    lob::Handle o = find(h, old_ref);
    if (LOB_UNLIKELY(o == lob::kNull)) return;
    if (books_[store_.orders[o].book]->replace(o, new_ref, price, shares) == lob::kNull) ++stats_.add_rejects;
  }

  void on_bad_message(const std::uint8_t*, std::size_t) { ++stats_.bad_messages; }

  // ---- software prefetch pipeline ----
  // Processing an execute/cancel/delete/replace costs a chain of dependent cache misses:
  // id-table slot -> order -> price level. The replay loop calls these for messages that are
  // 16, 8 and 4 positions ahead, so by the time a message is processed its memory is in cache.
  // Prefetches are hints: a stale or wrong address costs nothing but bandwidth.

  static LOB_ALWAYS_INLINE bool refers_to_order(std::uint8_t t) {
    return t == 'E' || t == 'C' || t == 'X' || t == 'D' || t == 'U';
  }
  LOB_ALWAYS_INLINE void prefetch_slot(const std::uint8_t* m) const {
    if (refers_to_order(m[0])) __builtin_prefetch(store_.ids.home_slot(be64(m + 11)));
  }
  LOB_ALWAYS_INLINE void prefetch_order(const std::uint8_t* m) const {
    if (!refers_to_order(m[0])) return;
    lob::Handle h = store_.ids.peek(be64(m + 11));
    if (h != lob::kNull) __builtin_prefetch(&store_.orders[h]);
  }
  LOB_ALWAYS_INLINE void prefetch_level(const std::uint8_t* m) const {
    if (!refers_to_order(m[0])) return;
    lob::Handle h = store_.ids.peek(be64(m + 11));
    if (h != lob::kNull) __builtin_prefetch(&store_.levels[store_.orders[h].level]);
  }

  // ---- access ----

  lob::OrderBook* book(std::uint16_t locate) const { return books_[locate].get(); }
  const char* symbol(std::uint16_t locate) const { return symbols_[locate].data(); }
  char trading_state(std::uint16_t locate) const { return state_[locate]; }
  bool continuous() const { return continuous_; }
  const BuilderStats& stats() const { return stats_; }
  const lob::OrderStore& store() const { return store_; }
  const lob::LadderArena& arena() const { return arena_; }
  std::size_t book_count() const { return next_arena_index_; }

  template <class F>
  void for_each_book(F&& f) const {
    for (std::size_t i = 0; i < books_.size(); ++i)
      if (books_[i]) f(std::uint16_t(i), *books_[i]);
  }

 private:
  lob::OrderBook* create(std::uint16_t locate, const char* stock) {
    if (books_[locate]) return books_[locate].get();
    if (next_arena_index_ >= cfg_.max_books) {
      ++stats_.books_dropped;
      return nullptr;
    }
    books_[locate] = std::make_unique<lob::OrderBook>(locate, store_, arena_, next_arena_index_++,
                                                      lob::TickRule::us_equities());
    symbol_to_cstr(stock, symbols_[locate].data());
    return books_[locate].get();
  }

  LOB_ALWAYS_INLINE lob::Handle find(const Header& h, std::uint64_t ref) {
    lob::Handle o = store_.ids.find(ref);
    if (LOB_UNLIKELY(o == lob::kNull)) {
      ++stats_.unknown_refs;
      return lob::kNull;
    }
    if (LOB_UNLIKELY(store_.orders[o].book != h.locate)) ++stats_.locate_mismatches;
    return o;
  }

  LOB_ALWAYS_INLINE void reduce(lob::OrderBook& b, lob::Handle o, std::uint32_t shares) {
    if (LOB_UNLIKELY(shares > store_.orders[o].qty)) ++stats_.over_reductions;
    b.reduce(o, shares);
  }

  BuilderConfig cfg_;
  lob::OrderStore store_;
  lob::LadderArena arena_;
  std::vector<std::unique_ptr<lob::OrderBook>> books_;  // indexed by stock locate
  std::array<std::array<char, 9>, 65536> symbols_;
  std::array<char, 65536> state_;
  std::size_t next_arena_index_ = 0;
  bool continuous_ = false;
  BuilderStats stats_;
};

}  // namespace itch
