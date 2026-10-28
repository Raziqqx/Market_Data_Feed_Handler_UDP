#pragma once
// Core types shared by the order book, the matching engine and the ITCH replay.

#include <cstddef>
#include <cstdint>

#if defined(__GNUC__)
#define LOB_LIKELY(x) __builtin_expect(!!(x), 1)
#define LOB_UNLIKELY(x) __builtin_expect(!!(x), 0)
#define LOB_ALWAYS_INLINE inline __attribute__((always_inline))
#define LOB_NOINLINE __attribute__((noinline))
#else
#define LOB_LIKELY(x) (x)
#define LOB_UNLIKELY(x) (x)
#define LOB_ALWAYS_INLINE inline
#define LOB_NOINLINE
#endif

namespace lob {

using OrderId = std::uint64_t;  // ITCH order reference numbers are 64-bit
using Price = std::uint32_t;    // fixed point, 4 implied decimals (ITCH Price(4)): $1.00 == 10000
using Qty = std::uint32_t;      // shares
using BookId = std::uint16_t;   // ITCH stock locate code
using Handle = std::uint32_t;   // index into a preallocated pool; 0 is the null handle

inline constexpr Handle kNull = 0;
inline constexpr Price kPriceScale = 10000;

enum class Side : std::uint8_t { Buy = 0, Sell = 1 };

inline constexpr Side opposite(Side s) { return s == Side::Buy ? Side::Sell : Side::Buy; }

enum class OrderType : std::uint8_t { Limit, Market };

// GTC: rest whatever is not filled. IOC: fill what you can immediately, cancel the rest.
enum class TimeInForce : std::uint8_t { GTC, IOC };

// A resting order. Exactly 32 bytes so two fit in a cache line and the pool is dense.
struct Order {
  OrderId id;
  Price price;
  Qty qty;        // remaining (open) quantity
  Handle prev;    // intrusive doubly-linked FIFO within the price level
  Handle next;
  Handle level;   // the PriceLevel this order rests in -> O(1) cancel without a price lookup
  BookId book;
  Side side;
  std::uint8_t pad_;
};
static_assert(sizeof(Order) == 32, "Order should stay 32 bytes");

// All resting orders at one price on one side, in time priority (head = oldest).
struct PriceLevel {
  Price price;
  std::uint32_t count;  // number of orders
  std::uint64_t total;  // aggregate open quantity
  Handle head;
  Handle tail;
};
static_assert(sizeof(PriceLevel) == 24, "PriceLevel should stay 24 bytes");

struct Trade {
  OrderId taker;
  OrderId maker;
  Price price;
  Qty qty;
  Side taker_side;

  bool operator==(const Trade& o) const {
    return taker == o.taker && maker == o.maker && price == o.price && qty == o.qty && taker_side == o.taker_side;
  }
};

struct TopOfBook {
  Price bid_price = 0;
  std::uint64_t bid_qty = 0;  // 0 => no bid
  Price ask_price = 0;
  std::uint64_t ask_qty = 0;  // 0 => no ask

  bool operator==(const TopOfBook& o) const {
    return bid_qty == o.bid_qty && ask_qty == o.ask_qty && (bid_qty == 0 || bid_price == o.bid_price) &&
           (ask_qty == 0 || ask_price == o.ask_price);
  }
  bool operator!=(const TopOfBook& o) const { return !(*this == o); }
};

}  // namespace lob
