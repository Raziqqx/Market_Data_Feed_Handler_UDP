#pragma once
// OrderId -> order Handle, open addressing with linear probing.
//
// Each table slot is just a 4-byte Handle. The key (order id) is not stored in the table; it is
// read from the order itself in the pool. That makes the table 4x smaller than storing
// {uint64 key, uint32 value} pairs, and on a hit the "extra" memory access is the order we were
// about to touch anyway.
//
// Deletion uses backward-shift (no tombstones), so probe sequences never degrade over a long day
// of adds and deletes. The table is sized to at least 2x the order pool, so load factor <= 0.5.
//
// Two hash functions:
//   Mix      - Fibonacci hashing. Safe default for arbitrary ids.
//   Identity - slot = id mod capacity. ITCH assigns order reference numbers sequentially, so
//              orders added close together in time land in adjacent slots (same cache lines),
//              and most deletes hit lines that are still hot. Measured ~18% faster on a real
//              ITCH day. Terrible for adversarial ids (e.g. all multiples of 2^20), which is
//              why it is opt-in.

#include <cassert>
#include <cstdint>

#include "lob/arena.hpp"
#include "lob/common.hpp"
#include "lob/pool.hpp"

namespace lob {

enum class IdHash : std::uint8_t { Mix, Identity };

class OrderIdMap {
 public:
  OrderIdMap(std::size_t max_orders, const Pool<Order>& pool, IdHash hash = IdHash::Mix)
      : pool_(pool), identity_(hash == IdHash::Identity) {
    std::size_t cap = 16;
    while (cap < max_orders * 2) cap <<= 1;
    table_ = Arena<Handle>(cap);
    mask_ = cap - 1;
    shift_ = 64 - __builtin_ctzll(cap);
  }

  LOB_ALWAYS_INLINE Handle find(OrderId id) const {
    for (std::size_t i = home(id);; i = (i + 1) & mask_) {
      Handle h = table_[i];
      if (h == kNull) return kNull;
      if (pool_[h].id == id) return h;
    }
  }

  // Precondition: pool_[h].id == id and id not present (callers check find() first).
  LOB_ALWAYS_INLINE void insert(OrderId id, Handle h) {
    assert(pool_[h].id == id);
    std::size_t i = home(id);
    while (table_[i] != kNull) i = (i + 1) & mask_;
    table_[i] = h;
    ++size_;
  }

  // Insert unless the id is already present (one probe sequence instead of find() + insert()).
  LOB_ALWAYS_INLINE bool insert_unique(OrderId id, Handle h) {
    std::size_t i = home(id);
    for (;; i = (i + 1) & mask_) {
      Handle e = table_[i];
      if (e == kNull) break;
      if (pool_[e].id == id) return false;
    }
    table_[i] = h;
    ++size_;
    return true;
  }

  // Must be called while pool_[h] still holds the id (i.e. before freeing the order).
  LOB_ALWAYS_INLINE bool erase(OrderId id) {
    std::size_t i = home(id);
    for (;; i = (i + 1) & mask_) {
      Handle h = table_[i];
      if (h == kNull) return false;
      if (pool_[h].id == id) break;
    }
    // Backward-shift: pull later entries of the cluster into the hole if that does not move
    // them before their home slot.
    std::size_t j = i;
    for (;;) {
      j = (j + 1) & mask_;
      Handle h = table_[j];
      if (h == kNull) break;
      std::size_t k = home(pool_[h].id);
      if (((j - k) & mask_) >= ((j - i) & mask_)) {
        table_[i] = h;
        i = j;
      }
    }
    table_[i] = kNull;
    --size_;
    return true;
  }

  // Prefetch support: the home slot of an id, and a non-validating peek at it (may return a
  // different order if the id collided; that only makes a prefetch useless, never wrong).
  LOB_ALWAYS_INLINE const Handle* home_slot(OrderId id) const { return &table_[home(id)]; }
  LOB_ALWAYS_INLINE Handle peek(OrderId id) const { return table_[home(id)]; }

  std::size_t size() const { return size_; }
  std::size_t capacity() const { return mask_ + 1; }
  void prefault() { table_.prefault(); }

 private:
  LOB_ALWAYS_INLINE std::size_t home(OrderId id) const {
    if (identity_) return static_cast<std::size_t>(id & mask_);
    // Fibonacci hashing: multiply by 2^64/phi and keep the top bits.
    return static_cast<std::size_t>((id * 0x9E3779B97F4A7C15ull) >> shift_);
  }

  const Pool<Order>& pool_;
  bool identity_;
  Arena<Handle> table_;
  std::size_t mask_ = 0;
  unsigned shift_ = 0;
  std::size_t size_ = 0;
};

}  // namespace lob
