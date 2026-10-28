#pragma once
// Cheap per-event latency measurement.
//
// * rdtsc reads the CPU's time-stamp counter: ~20 cycles, no syscall. On modern x86 the TSC
//   ticks at a constant rate regardless of frequency scaling ("constant_tsc"/"nonstop_tsc" in
//   /proc/cpuinfo), so ticks convert to nanoseconds with one calibrated factor.
// * LatencyHistogram is log-linear (HdrHistogram-style): exact below 64 ticks, then 32
//   sub-buckets per power of two (< 3.2% relative error). Fixed size, so recording never
//   allocates and costs one increment.

#include <x86intrin.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <thread>

#include "lob/common.hpp"

namespace lob {

LOB_ALWAYS_INLINE std::uint64_t rdtsc() { return __rdtsc(); }

// Measures TSC ticks per nanosecond against the steady clock.
inline double calibrate_tsc_ghz(std::chrono::milliseconds dur = std::chrono::milliseconds(200)) {
  auto t0 = std::chrono::steady_clock::now();
  std::uint64_t c0 = rdtsc();
  std::this_thread::sleep_for(dur);
  auto t1 = std::chrono::steady_clock::now();
  std::uint64_t c1 = rdtsc();
  double ns = std::chrono::duration<double, std::nano>(t1 - t0).count();
  return double(c1 - c0) / ns;
}

class LatencyHistogram {
 public:
  static constexpr int kSubBits = 5;  // 32 sub-buckets per octave
  static constexpr int kExact = 64;   // values below this are recorded exactly
  static constexpr int kBuckets = kExact + (64 - 6) * (1 << kSubBits);

  LOB_ALWAYS_INLINE void record(std::uint64_t v) {
    ++counts_[bucket(v)];
    ++n_;
    sum_ += v;
    if (v > max_) max_ = v;
  }

  // Value (in recorded units) at percentile p in [0, 100].
  std::uint64_t percentile(double p) const {
    if (n_ == 0) return 0;
    std::uint64_t target = std::uint64_t(p / 100.0 * double(n_));
    if (target >= n_) target = n_ - 1;
    std::uint64_t seen = 0;
    for (int b = 0; b < kBuckets; ++b) {
      seen += counts_[b];
      if (seen > target) return b == bucket(max_) ? max_ : bucket_mid(b);
    }
    return max_;
  }

  void merge(const LatencyHistogram& o) {
    for (int b = 0; b < kBuckets; ++b) counts_[b] += o.counts_[b];
    n_ += o.n_;
    sum_ += o.sum_;
    if (o.max_ > max_) max_ = o.max_;
  }

  std::uint64_t count() const { return n_; }
  std::uint64_t max() const { return max_; }
  double mean() const { return n_ ? double(sum_) / double(n_) : 0.0; }

 private:
  LOB_ALWAYS_INLINE static int bucket(std::uint64_t v) {
    if (v < std::uint64_t(kExact)) return int(v);
    int e = 63 - __builtin_clzll(v);  // >= 6
    int sub = int((v >> (e - kSubBits)) & ((1 << kSubBits) - 1));
    return kExact + (e - 6) * (1 << kSubBits) + sub;
  }
  static std::uint64_t bucket_mid(int b) {
    if (b < kExact) return std::uint64_t(b);
    int e = (b - kExact) / (1 << kSubBits) + 6;
    int sub = (b - kExact) % (1 << kSubBits);
    std::uint64_t lo = (std::uint64_t(1) << e) | (std::uint64_t(sub) << (e - kSubBits));
    std::uint64_t width = std::uint64_t(1) << (e - kSubBits);
    return lo + width / 2;
  }

  std::array<std::uint64_t, kBuckets> counts_{};
  std::uint64_t n_ = 0, sum_ = 0, max_ = 0;
};

}  // namespace lob
