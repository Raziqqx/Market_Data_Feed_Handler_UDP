#pragma once
// Bounded lock-free single-producer / single-consumer ring buffer (the "P2 queue").
//
// * Indices are free-running 64-bit counters; slot = index & mask (capacity is a power of two).
//   They never wrap in practice (2^64 messages), so full/empty need no extra flag.
// * The producer owns tail_, the consumer owns head_. Each lives on its own cache line, so the
//   two threads never write the same line (no false sharing).
// * Each side keeps a private cached copy of the other side's index and only re-reads the shared
//   atomic when the cache says "full" (producer) or "empty" (consumer). In steady state that
//   removes almost all cross-core cache-line traffic.
// * Memory order: the producer writes the slot, then publishes tail_ with release; the consumer
//   reads tail_ with acquire, so it is guaranteed to see the slot contents. Symmetrically for
//   head_ when a slot is handed back.
// * Zero-copy API: claim() hands the producer a pointer to the slot so it can build the element
//   in place, and front() lets the consumer read it in place. No intermediate copies.

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <stdexcept>
#include <type_traits>

namespace feed {

inline constexpr std::size_t kCacheLine = 64;

template <class T>
class SpscQueue {
  static_assert(std::is_trivially_copyable_v<T>, "slots are reused in place; keep T trivially copyable");

 public:
  explicit SpscQueue(std::size_t capacity) : mask_(capacity - 1) {
    if (capacity < 2 || (capacity & (capacity - 1)) != 0)
      throw std::invalid_argument("SpscQueue capacity must be a power of two >= 2");
    slots_.reset(static_cast<T*>(::operator new[](capacity * sizeof(T), std::align_val_t(alignof(T) < kCacheLine ? kCacheLine : alignof(T)))));
    // Touch every page now so the first lap around the ring does not page-fault.
    std::memset(static_cast<void*>(slots_.get()), 0, capacity * sizeof(T));
  }

  SpscQueue(const SpscQueue&) = delete;
  SpscQueue& operator=(const SpscQueue&) = delete;

  // ---- producer side ----

  // Returns a slot to fill in place, or nullptr if the queue is full. Call publish() when done.
  T* claim() noexcept {
    const std::uint64_t t = tail_.load(std::memory_order_relaxed);
    if (t - head_cache_ > mask_) {
      head_cache_ = head_.load(std::memory_order_acquire);
      if (t - head_cache_ > mask_) return nullptr;
    }
    return &slots_.get()[t & mask_];
  }
  void publish() noexcept { tail_.store(tail_.load(std::memory_order_relaxed) + 1, std::memory_order_release); }

  bool try_push(const T& v) noexcept {
    T* s = claim();
    if (!s) return false;
    *s = v;
    publish();
    return true;
  }

  // ---- consumer side ----

  // Returns the oldest element (read it in place), or nullptr if empty. Call pop() when done.
  T* front() noexcept {
    const std::uint64_t h = head_.load(std::memory_order_relaxed);
    if (h == tail_cache_) {
      tail_cache_ = tail_.load(std::memory_order_acquire);
      if (h == tail_cache_) return nullptr;
    }
    return &slots_.get()[h & mask_];
  }
  void pop() noexcept { head_.store(head_.load(std::memory_order_relaxed) + 1, std::memory_order_release); }

  bool try_pop(T& out) noexcept {
    T* s = front();
    if (!s) return false;
    out = *s;
    pop();
    return true;
  }

  // ---- either side (approximate while the other side is running) ----
  std::size_t size() const noexcept {
    return std::size_t(tail_.load(std::memory_order_acquire) - head_.load(std::memory_order_acquire));
  }
  std::size_t capacity() const noexcept { return mask_ + 1; }

 private:
  struct Deleter {
    void operator()(T* p) const noexcept {
      ::operator delete[](p, std::align_val_t(alignof(T) < kCacheLine ? kCacheLine : alignof(T)));
    }
  };

  const std::uint64_t mask_;
  std::unique_ptr<T, Deleter> slots_;

  alignas(kCacheLine) std::atomic<std::uint64_t> head_{0};  // written by the consumer
  std::uint64_t tail_cache_ = 0;                            // consumer's private copy of tail_

  alignas(kCacheLine) std::atomic<std::uint64_t> tail_{0};  // written by the producer
  std::uint64_t head_cache_ = 0;                            // producer's private copy of head_

  char pad_[kCacheLine - sizeof(std::uint64_t)];  // keep the next object off tail_'s line
};

}  // namespace feed
