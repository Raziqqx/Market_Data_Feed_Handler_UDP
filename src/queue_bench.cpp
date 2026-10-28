// queue_bench: the SPSC queue on its own, between two pinned threads, with the real 64-byte
// Update element. Gives a floor for stage 3 ("queue hop") of the feed handler.
//
//   throughput  producer pushes N updates flat out, consumer pops them
//   one-way     producer pushes one update stamped with rdtsc, waits for it to be consumed,
//               consumer records rdtsc(now) - stamp: the cost of one cache-line hand-off

#include <cinttypes>
#include <thread>

#include "feed/spsc_queue.hpp"
#include "feed/update.hpp"
#include "feed/util.hpp"
#include "lob/latency.hpp"

using namespace feed;

int main(int argc, char** argv) {
  Options o(argc, argv, {{"n", "50000000"}, {"samples", "1000000"}, {"cpu-a", "2"}, {"cpu-b", "4"}, {"queue", "65536"}},
            "usage: queue_bench [--n N] [--samples N] [--cpu-a N] [--cpu-b N] [--queue N]\n");
  const double ghz = lob::calibrate_tsc_ghz();
  const std::uint64_t n = std::uint64_t(o.num("n"));

  {
    SpscQueue<Update> q(std::size_t(o.num("queue")));
    std::uint64_t sum = 0;
    std::thread c([&] {
      pin_this_thread(int(o.num("cpu-b")), "consumer");
      for (std::uint64_t i = 0; i < n;) {
        if (Update* u = q.front()) {
          sum += u->seq;
          q.pop();
          ++i;
        }
      }
    });
    pin_this_thread(int(o.num("cpu-a")), "producer");
    std::int64_t t0 = monotonic_ns();
    for (std::uint64_t i = 0; i < n;) {
      if (Update* u = q.claim()) {
        u->seq = i++;
        q.publish();
      }
    }
    c.join();
    double s = double(monotonic_ns() - t0) / 1e9;
    std::printf("throughput: %" PRIu64 " x 64-byte updates in %.3f s = %.1f M/s (%.2f ns/update)  [check %" PRIu64 "]\n",
                n, s, double(n) / s / 1e6, s * 1e9 / double(n), sum);
  }

  {
    SpscQueue<Update> q(std::size_t(o.num("queue")));
    std::atomic<std::uint64_t> done{0};
    const std::uint64_t samples = std::uint64_t(o.num("samples"));
    lob::LatencyHistogram h;
    std::thread c([&] {
      pin_this_thread(int(o.num("cpu-b")), "consumer");
      for (std::uint64_t i = 0; i < samples;) {
        if (Update* u = q.front()) {
          std::uint64_t now = lob::rdtsc();
          // Cross-core TSC readings can disagree by a few ns: clamp instead of wrapping.
          h.record(now > u->t_push_tsc ? std::uint64_t(double(now - u->t_push_tsc) / ghz) : 0);
          q.pop();
          done.store(++i, std::memory_order_release);
        }
      }
    });
    for (std::uint64_t i = 0; i < samples; ++i) {
      Update* u;
      while (!(u = q.claim())) {
      }
      u->t_push_tsc = lob::rdtsc();
      q.publish();
      while (done.load(std::memory_order_acquire) <= i) {
      }
    }
    c.join();
    std::printf("one-way hand-off (idle consumer spinning), ns: p50 %" PRIu64 "  p90 %" PRIu64 "  p99 %" PRIu64
                "  p99.9 %" PRIu64 "  max %" PRIu64 "\n",
                h.percentile(50), h.percentile(90), h.percentile(99), h.percentile(99.9), h.max());
  }
  return 0;
}
