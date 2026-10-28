#pragma once
// FIFO of received packets that arrive while the handler is recovering (and while it works
// through that backlog afterwards).
//
// Why not just keep them in the receive ring? Ring slots are fixed at 2 KB (the largest
// datagram) but a typical packet is 40-100 bytes, and a big snapshot can take hundreds of ms.
// At 500k msg/s that is ~250k packets: 500 MB of slots, versus ~20 MB packed tightly here.
//
// Storage is a list of fixed-size chunks. Entries never move once written, so a pointer to the
// entry being processed stays valid while new packets are appended behind it (the feed thread
// keeps draining the socket even in the middle of processing). Consumed chunks are recycled,
// so after the first big recovery no further allocation happens. Copying is acceptable here:
// this is the recovery path, not the hot path.

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <memory>
#include <vector>

namespace feed {

class PendingBuffer {
 public:
  struct Entry {
    std::uint32_t len;
    std::uint32_t pad_;
    std::int64_t kernel_ns;
    std::int64_t user_ns;
    std::uint64_t rx_tsc;
    const std::uint8_t* data() const { return reinterpret_cast<const std::uint8_t*>(this + 1); }
  };

  explicit PendingBuffer(std::size_t chunk_bytes = std::size_t(4) << 20, std::size_t max_bytes = std::size_t(1) << 30)
      : chunk_bytes_(chunk_bytes), max_chunks_(max_bytes / chunk_bytes) {}

  // Append a copy of one packet. Returns false (packet not stored) if the buffer is at its cap:
  // the caller then simply loses that packet, which shows up later as another gap.
  bool push(const std::uint8_t* data, std::uint32_t len, std::int64_t kernel_ns, std::int64_t user_ns,
            std::uint64_t rx_tsc) {
    const std::size_t need = entry_size(len);
    if (chunks_.empty() || chunks_.back().used + need > chunk_bytes_) {
      if (chunks_.size() >= max_chunks_ || need > chunk_bytes_) return false;
      chunks_.push_back(take_chunk());
    }
    Chunk& c = chunks_.back();
    Entry* e = reinterpret_cast<Entry*>(c.mem.get() + c.used);
    e->len = len;
    e->kernel_ns = kernel_ns;
    e->user_ns = user_ns;
    e->rx_tsc = rx_tsc;
    std::memcpy(e + 1, data, len);
    c.used += need;
    ++count_;
    bytes_ += need;
    if (bytes_ > peak_bytes_) peak_bytes_ = bytes_;
    return true;
  }

  // Oldest entry, or nullptr if empty. Valid until pop().
  const Entry* front() {
    while (!chunks_.empty()) {
      Chunk& c = chunks_.front();
      if (rd_ < c.used) return reinterpret_cast<const Entry*>(c.mem.get() + rd_);
      if (chunks_.size() == 1) {  // fully consumed: rewind the only chunk
        c.used = 0;
        rd_ = 0;
        return nullptr;
      }
      free_.push_back(std::move(c));
      chunks_.pop_front();
      rd_ = 0;
    }
    return nullptr;
  }

  void pop() {
    const Entry* e = reinterpret_cast<const Entry*>(chunks_.front().mem.get() + rd_);
    const std::size_t sz = entry_size(e->len);
    rd_ += sz;
    bytes_ -= sz;
    --count_;
  }

  bool empty() const { return count_ == 0; }
  std::size_t count() const { return count_; }
  std::size_t bytes() const { return bytes_; }
  std::size_t peak_bytes() const { return peak_bytes_; }

 private:
  struct Chunk {
    std::unique_ptr<std::uint8_t[]> mem;
    std::size_t used = 0;
  };

  static std::size_t entry_size(std::uint32_t len) { return (sizeof(Entry) + len + 7) & ~std::size_t(7); }

  Chunk take_chunk() {
    if (!free_.empty()) {
      Chunk c = std::move(free_.back());
      free_.pop_back();
      c.used = 0;
      return c;
    }
    Chunk c;
    c.mem.reset(new std::uint8_t[chunk_bytes_]);
    return c;
  }

  const std::size_t chunk_bytes_;
  const std::size_t max_chunks_;
  std::deque<Chunk> chunks_;
  std::vector<Chunk> free_;
  std::size_t rd_ = 0;  // read offset in chunks_.front()
  std::size_t count_ = 0, bytes_ = 0, peak_bytes_ = 0;
};

}  // namespace feed
