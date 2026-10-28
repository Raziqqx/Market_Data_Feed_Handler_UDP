#pragma once
// Two-level bitmap over a price window: bit i set <=> window slot i holds a non-empty level.
//
// When the best level empties we need "the next non-empty level below/above". Scanning the slot
// array would be O(window). Instead: level 0 has one bit per slot, level 1 has one bit per
// level-0 word ("this word is non-zero"). Finding the next set bit is a couple of
// count-leading/trailing-zero instructions plus, at worst, a short scan of level 1
// (window/4096 words: 16 words for a 65536-slot window).
//
// The bitmap does not own memory; it points into a preallocated arena.

#include <cstdint>

#include "lob/common.hpp"

namespace lob {

class HierBitset {
 public:
  static constexpr std::size_t words_needed(std::size_t bits) {
    std::size_t l0 = (bits + 63) / 64;
    return l0 + (l0 + 63) / 64;
  }

  HierBitset() = default;
  HierBitset(std::uint64_t* mem, std::size_t bits)
      : l0_(mem), l1_(mem + (bits + 63) / 64), n0_((bits + 63) / 64), n1_((n0_ + 63) / 64), bits_(bits) {}

  LOB_ALWAYS_INLINE void set(std::size_t i) {
    std::size_t w = i >> 6;
    l0_[w] |= 1ull << (i & 63);
    l1_[w >> 6] |= 1ull << (w & 63);
  }

  LOB_ALWAYS_INLINE void clear(std::size_t i) {
    std::size_t w = i >> 6;
    l0_[w] &= ~(1ull << (i & 63));
    if (l0_[w] == 0) l1_[w >> 6] &= ~(1ull << (w & 63));
  }

  LOB_ALWAYS_INLINE bool test(std::size_t i) const { return (l0_[i >> 6] >> (i & 63)) & 1; }

  // Highest set index <= i, or -1.
  LOB_ALWAYS_INLINE std::int64_t find_prev(std::size_t i) const {
    std::size_t w = i >> 6;
    std::uint64_t m = l0_[w] & ((2ull << (i & 63)) - 1);  // bits 0..i%64 (wraps to all-ones at 63)
    if (m) return static_cast<std::int64_t>(w * 64 + 63 - __builtin_clzll(m));
    if (w == 0) return -1;
    std::size_t w2 = w - 1;
    std::size_t s = w2 >> 6;
    std::uint64_t m1 = l1_[s] & ((2ull << (w2 & 63)) - 1);
    while (!m1) {
      if (s == 0) return -1;
      m1 = l1_[--s];
    }
    std::size_t ww = s * 64 + 63 - __builtin_clzll(m1);
    return static_cast<std::int64_t>(ww * 64 + 63 - __builtin_clzll(l0_[ww]));
  }

  // Lowest set index >= i, or -1.
  LOB_ALWAYS_INLINE std::int64_t find_next(std::size_t i) const {
    std::size_t w = i >> 6;
    if (w >= n0_) return -1;
    std::uint64_t m = l0_[w] & (~0ull << (i & 63));
    if (m) return static_cast<std::int64_t>(w * 64 + __builtin_ctzll(m));
    std::size_t w2 = w + 1;
    if (w2 >= n0_) return -1;
    std::size_t s = w2 >> 6;
    std::uint64_t m1 = l1_[s] & (~0ull << (w2 & 63));
    while (!m1) {
      if (++s >= n1_) return -1;
      m1 = l1_[s];
    }
    std::size_t ww = s * 64 + __builtin_ctzll(m1);
    return static_cast<std::int64_t>(ww * 64 + __builtin_ctzll(l0_[ww]));
  }

  std::size_t size() const { return bits_; }

 private:
  std::uint64_t* l0_ = nullptr;
  std::uint64_t* l1_ = nullptr;
  std::size_t n0_ = 0;
  std::size_t n1_ = 0;
  std::size_t bits_ = 0;
};

}  // namespace lob
