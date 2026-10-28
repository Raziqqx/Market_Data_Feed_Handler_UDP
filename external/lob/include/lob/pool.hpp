#pragma once
// Fixed-capacity object pool addressed by 32-bit handles.
//
// Handles instead of pointers: half the size (4 vs 8 bytes) so Order stays at 32 bytes, and
// they stay valid no matter where the arena lives. Handle 0 is reserved as null.
//
// Allocation order: reuse the most recently freed slot first (LIFO free list; that slot is
// probably still in cache), otherwise bump into never-used memory. No malloc, ever.

#include <cassert>
#include <cstdint>

#include "lob/arena.hpp"
#include "lob/common.hpp"

namespace lob {

template <class T>
class Pool {
 public:
  explicit Pool(std::size_t capacity) : slots_(capacity + 1) {}

  // Returns kNull when the pool is exhausted; the caller must handle it (reject the order).
  LOB_ALWAYS_INLINE Handle alloc() {
    if (free_head_ != kNull) {
      Handle h = free_head_;
      free_head_ = next_free(h);
      ++live_;
      return h;
    }
    if (LOB_UNLIKELY(bump_ >= slots_.size())) return kNull;
    ++live_;
    if (live_ > peak_) peak_ = live_;
    return static_cast<Handle>(bump_++);
  }

  LOB_ALWAYS_INLINE void free(Handle h) {
    assert(h != kNull && h < bump_);
    next_free(h) = free_head_;
    free_head_ = h;
    --live_;
  }

  LOB_ALWAYS_INLINE T& operator[](Handle h) { return slots_[h]; }
  LOB_ALWAYS_INLINE const T& operator[](Handle h) const { return slots_[h]; }

  std::size_t capacity() const { return slots_.size() - 1; }
  std::size_t live() const { return live_; }
  std::size_t peak() const { return peak_; }
  void prefault() { slots_.prefault(); }

 private:
  static_assert(sizeof(T) >= sizeof(Handle), "slot must be able to hold a free-list link");
  // A free slot's first 4 bytes store the next free handle (the object is dead, so reuse it).
  Handle& next_free(Handle h) { return *reinterpret_cast<Handle*>(&slots_[h]); }

  Arena<T> slots_;
  Handle free_head_ = kNull;
  std::size_t bump_ = 1;  // slot 0 is the null handle
  std::size_t live_ = 0;
  std::size_t peak_ = 0;
};

}  // namespace lob
