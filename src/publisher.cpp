// feed_publisher: replays a NASDAQ ITCH 5.0 file as MoldUDP64 packets over UDP multicast, at a
// controllable speed, and runs the TCP snapshot server that subscribers recover from.
//
// Threads (each pinned):
//   publisher  walks the mmap'd file, assigns sequence numbers, packs messages into packets,
//              paces them, optionally drops some on purpose, and sends them with sendmmsg.
//   snapshot   owns the "truth": a P3 BookBuilder fed every message (dropped ones included)
//              through a P2 SPSC queue of pointers into the mmap'd file (zero-copy). It serves
//              snapshot, retransmission and digest requests over non-blocking TCP with epoll.
//              Retransmission history is a ring of those same pointers: no message is copied
//              until it is written to a client.

#include <signal.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <atomic>
#include <cinttypes>
#include <memory>
#include <random>
#include <thread>
#include <unordered_map>

#include "feed/moldudp64.hpp"
#include "feed/net.hpp"
#include "feed/snapshot.hpp"
#include "feed/spsc_queue.hpp"
#include "feed/util.hpp"
#include "itch/book_builder.hpp"
#include "lob/latency.hpp"

using namespace feed;

namespace {

std::atomic<bool> g_stop{false};
void on_signal(int) { g_stop.store(true); }

const char* kUsage =
    "usage: feed_publisher <file.itch> [options]\n"
    "  --group IP            multicast group (239.1.1.1)\n"
    "  --port N              multicast port (30001)\n"
    "  --iface IP            outgoing interface address (127.0.0.1)\n"
    "  --session NAME        MoldUDP64 session name (ITCH50)\n"
    "  --speed X             replay at X times real time using ITCH timestamps (0 = off)\n"
    "  --rate N              replay at N messages per second (0 = off)\n"
    "                        (neither: as fast as possible)\n"
    "  --skip N              fast-forward N messages into the snapshot server without sending\n"
    "  --count N             send at most N messages after the skip (0 = whole file)\n"
    "  --max-payload N       max UDP payload bytes per packet (1400)\n"
    "  --drop-every N        deliberately drop every Nth packet (0 = off)\n"
    "  --drop-burst N        ...and the N-1 packets after it (1)\n"
    "  --drop-prob P         deliberately drop each packet with probability P (0)\n"
    "  --seed N              RNG seed for --drop-prob (1)\n"
    "  --heartbeat-ms N      heartbeat when idle this long (1000)\n"
    "  --snap-ip IP          snapshot server listen address (127.0.0.1)\n"
    "  --snap-port N         snapshot server port (31001)\n"
    "  --linger S            keep the snapshot server up S seconds after the end (10)\n"
    "  --pub-cpu N           pin the publisher thread (6, -1 = no pin)\n"
    "  --snap-cpu N          pin the snapshot thread (8, -1 = no pin)\n";

struct FeedMsg {
  std::uint64_t seq;
  const std::uint8_t* msg;  // points into the mmap'd file: never copied
  std::uint64_t len;
};

// ---------------------------------------------------------------------------------------------
// Snapshot server
// ---------------------------------------------------------------------------------------------
class SnapshotServer {
 public:
  static constexpr std::size_t kHistory = std::size_t(1) << 21;  // messages kept for retransmission

  SnapshotServer(SpscQueue<FeedMsg>& q, const std::string& ip, int port)
      : q_(q),
        builder_(std::make_unique<itch::BookBuilder>(itch::BuilderConfig{})),
        history_(new FeedMsg[kHistory]()),
        listen_fd_(net::tcp_listen(ip, port)) {
    ep_.add(listen_fd_, EPOLLIN);
  }

  void run(int cpu, const std::atomic<bool>& stop) {
    pin_this_thread(cpu, "snapshot");
    epoll_event evs[16];
    while (!stop.load(std::memory_order_relaxed)) {
      std::size_t applied = drain_feed(8192);
      int n = ep_.wait(evs, 16, applied ? 0 : 1);
      for (int i = 0; i < n; ++i) {
        if (evs[i].data.fd == listen_fd_) accept_clients();
        else on_client(evs[i].data.fd, evs[i].events);
      }
      serve_ready();
    }
    for (auto& [fd, c] : clients_) ::close(fd);
    ::close(listen_fd_);
  }

  std::uint64_t snapshots() const { return snapshots_; }
  std::uint64_t retransmissions() const { return retransmissions_; }
  std::uint64_t digests() const { return digests_; }

 private:
  struct Client {
    std::uint8_t in[snap::kRequestLen];
    std::size_t in_len = 0;
    char want = 0;               // 'S', 'R' or 'D' once a request has arrived
    std::uint64_t want_seq = 0;  // answer once every message < want_seq has been applied
    std::uint64_t from_seq = 0;  // 'R' only
    std::vector<std::uint8_t> out;
    std::size_t out_off = 0;
  };

  std::size_t drain_feed(std::size_t max) {
    std::size_t n = 0;
    while (n < max) {
      FeedMsg* m = q_.front();
      if (!m) break;
      itch::dispatch(m->msg, m->len, *builder_);
      history_[m->seq & (kHistory - 1)] = *m;
      next_seq_ = m->seq + 1;
      q_.pop();
      ++n;
    }
    return n;
  }

  void accept_clients() {
    for (;;) {
      int fd = ::accept4(listen_fd_, nullptr, nullptr, SOCK_NONBLOCK);
      if (fd < 0) return;
      net::tcp_tune(fd);
      clients_.emplace(fd, Client{});
      ep_.add(fd, EPOLLIN | EPOLLRDHUP);
    }
  }

  void close_client(int fd) {
    ep_.del(fd);
    ::close(fd);
    clients_.erase(fd);
  }

  void on_client(int fd, std::uint32_t events) {
    auto it = clients_.find(fd);
    if (it == clients_.end()) return;
    Client& c = it->second;
    if (events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
      if (c.out.empty() || c.out_off == c.out.size()) return close_client(fd);
    }
    if (events & EPOLLIN) {
      while (c.in_len < snap::kRequestLen) {
        ssize_t r = ::recv(fd, c.in + c.in_len, snap::kRequestLen - c.in_len, 0);
        if (r == 0) return close_client(fd);
        if (r < 0) break;
        c.in_len += std::size_t(r);
      }
      if (c.in_len == snap::kRequestLen && !c.want) {
        c.want = char(c.in[0]);
        c.want_seq = itch::be64(c.in + 1);
        if (c.want == 'R') {
          c.from_seq = itch::be64(c.in + 1);
          c.want_seq = itch::be64(c.in + 9);
          if (c.want_seq < c.from_seq) return close_client(fd);
        } else if (c.want != 'S' && c.want != 'D') {
          return close_client(fd);
        }
      }
    }
    if (events & EPOLLOUT) flush(fd, c);
  }

  // Answer every request whose sequence requirement is now met. The server never answers with a
  // state older than what the client asked for: that is what makes the hand-off gap-free.
  void serve_ready() {
    for (auto it = clients_.begin(); it != clients_.end();) {
      int fd = it->first;
      Client& c = it->second;
      ++it;
      if (!c.want || !c.out.empty() || next_seq_ < c.want_seq) continue;
      if (c.want == 'S') {
        std::int64_t t0 = monotonic_ns();
        c.out.reserve(last_snapshot_bytes_ + last_snapshot_bytes_ / 8);  // avoid regrowing 60+ MB
        std::uint64_t msgs = snap::serialize(*builder_, next_seq_, c.out);
        last_snapshot_bytes_ = c.out.size();
        ++snapshots_;
        std::printf("[snapshot] served seq<%" PRIu64 ": %" PRIu64 " msgs, %.1f MB, built in %.1f ms\n", next_seq_,
                    msgs, double(c.out.size()) / 1e6, double(monotonic_ns() - t0) / 1e6);
      } else if (c.want == 'R') {
        retransmit(c);
      } else {
        snap::Digest d = snap::digest(*builder_);
        c.out.resize(snap::kDigestLen);
        c.out[0] = 'D';
        mold::put_be64(c.out.data() + 1, next_seq_);
        mold::put_be64(c.out.data() + 9, d.value());
        mold::put_be64(c.out.data() + 17, d.orders);
        mold::put_be64(c.out.data() + 25, d.books);
        ++digests_;
        std::printf("[snapshot] digest at seq<%" PRIu64 ": %016" PRIx64 " (%" PRIu64 " orders, %" PRIu64 " books)\n",
                    next_seq_, d.value(), d.orders, d.books);
      }
      std::fflush(stdout);
      // Only writability matters now (EPOLLERR/EPOLLHUP are always reported). Keeping EPOLLRDHUP
      // would make a client that half-closed mid-transfer wake us up on every loop.
      ep_.mod(fd, EPOLLOUT);
      flush(fd, c);
    }
  }

  // Messages [from_seq, want_seq) straight from the history ring. If the oldest one has already
  // been overwritten, answer with a count of 0 and let the client fall back to a snapshot.
  void retransmit(Client& c) {
    const std::uint64_t n = c.want_seq - c.from_seq;
    const bool available = c.from_seq >= 1 && n <= kHistory && next_seq_ - c.from_seq <= kHistory;
    c.out.resize(snap::kSnapshotHeaderLen);
    std::uint64_t count = 0;
    if (available) {
      for (std::uint64_t s = c.from_seq; s < c.want_seq; ++s) {
        const FeedMsg& m = history_[s & (kHistory - 1)];
        const std::size_t at = c.out.size();
        c.out.resize(at + 2 + m.len);
        mold::put_be16(c.out.data() + at, std::uint16_t(m.len));
        std::memcpy(c.out.data() + at + 2, m.msg, m.len);
        ++count;
      }
    }
    c.out[0] = 'R';
    mold::put_be64(c.out.data() + 1, c.from_seq);
    mold::put_be64(c.out.data() + 9, count);
    mold::put_be64(c.out.data() + 17, c.out.size() - snap::kSnapshotHeaderLen);
    ++retransmissions_;
    if (!available)
      std::printf("[snapshot] retransmission of [%" PRIu64 ", %" PRIu64 ") refused: older than history\n", c.from_seq,
                  c.want_seq);
  }

  // Non-blocking write of whatever the socket will take; EPOLLOUT brings us back for the rest.
  void flush(int fd, Client& c) {
    while (c.out_off < c.out.size()) {
      ssize_t w = ::send(fd, c.out.data() + c.out_off, c.out.size() - c.out_off, MSG_NOSIGNAL);
      if (w < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) return;
        return close_client(fd);
      }
      c.out_off += std::size_t(w);
    }
    if (!c.out.empty()) close_client(fd);  // one request per connection
  }

  SpscQueue<FeedMsg>& q_;
  std::unique_ptr<itch::BookBuilder> builder_;
  std::unique_ptr<FeedMsg[]> history_;
  net::Epoll ep_;
  int listen_fd_;
  std::unordered_map<int, Client> clients_;
  std::uint64_t next_seq_ = 1;  // every message < next_seq_ has been applied
  std::uint64_t snapshots_ = 0, digests_ = 0, retransmissions_ = 0;
  std::size_t last_snapshot_bytes_ = 0;
};

// ---------------------------------------------------------------------------------------------
// Publisher
// ---------------------------------------------------------------------------------------------
class Publisher {
 public:
  static constexpr int kBatch = 32;

  Publisher(const Options& o, SpscQueue<FeedMsg>& q)
      : q_(q),
        fd_(net::multicast_sender(o.str("iface"), 0, 4 << 20)),
        dst_(net::endpoint(o.str("group"), int(o.num("port")))),
        session_(o.str("session")),
        pb_(session_.c_str(), std::size_t(o.num("max-payload"))),
        speed_(o.real("speed")),
        rate_(o.real("rate")),
        drop_every_(std::uint64_t(o.num("drop-every"))),
        drop_burst_(std::uint64_t(std::max(1LL, o.num("drop-burst")))),
        drop_prob_(o.real("drop-prob")),
        rng_(std::uint64_t(o.num("seed"))),
        hb_ns_(o.num("heartbeat-ms") * 1'000'000) {
    for (int i = 0; i < kBatch; ++i) {
      iov_[i].iov_base = bufs_[i];
      hdrs_[i] = mmsghdr{};
      hdrs_[i].msg_hdr.msg_name = &dst_;
      hdrs_[i].msg_hdr.msg_namelen = sizeof dst_;
      hdrs_[i].msg_hdr.msg_iov = &iov_[i];
      hdrs_[i].msg_hdr.msg_iovlen = 1;
    }
  }

  void run(const std::uint8_t* p, const std::uint8_t* end, std::uint64_t skip, std::uint64_t count) {
    // Fast-forward: the snapshot server learns these messages, the wire never sees them. A
    // subscriber that starts now therefore joins mid-stream and must recover via snapshot.
    while (skip && end - p >= 2) {
      std::size_t len = itch::be16(p);
      enqueue(p + 2, len);
      p += 2 + len;
      --skip;
    }
    std::printf("[publisher] starting at seq %" PRIu64 "\n", seq_);
    std::fflush(stdout);

    const bool paced = speed_ > 0 || rate_ > 0;
    const std::int64_t t_start = monotonic_ns();
    std::int64_t last_report = t_start;
    std::uint64_t first_ts = 0, sent_msgs = 0, last_report_msgs = 0;
    last_send_ns_ = t_start;

    while (end - p >= 2 && (count == 0 || sent_msgs < count) && !g_stop.load(std::memory_order_relaxed)) {
      std::size_t len = itch::be16(p);
      if (std::size_t(end - p) < 2 + len) break;
      const std::uint8_t* m = p + 2;

      if (paced) {
        std::int64_t due;
        if (speed_ > 0) {
          std::uint64_t ts = itch::be48(m + 5);
          if (first_ts == 0) first_ts = ts;
          due = t_start + std::int64_t(double(ts - first_ts) / speed_);
        } else {
          due = t_start + std::int64_t(double(sent_msgs) * 1e9 / rate_);
        }
        std::int64_t now = monotonic_ns();
        if (due > now) {
          close_packet();
          flush_batch();
          wait_until(due);
        }
      }

      if (pb_.count() && !pb_.fits(len)) close_packet();
      if (!pb_.count()) open_packet();
      pb_.add(m, len);
      enqueue(m, len);
      p += 2 + len;
      ++sent_msgs;

      if ((sent_msgs & 0xFFFF) == 0) {
        std::int64_t now = monotonic_ns();
        if (now - last_report >= 1'000'000'000) {
          std::printf("[publisher] seq %" PRIu64 "  %.0f msg/s  packets %" PRIu64 "  dropped %" PRIu64 "\n", seq_,
                      double(sent_msgs - last_report_msgs) * 1e9 / double(now - last_report), packets_,
                      dropped_packets_);
          std::fflush(stdout);
          last_report = now;
          last_report_msgs = sent_msgs;
        }
      }
    }
    close_packet();
    flush_batch();
    double secs = double(monotonic_ns() - t_start) / 1e9;

    send_control(mold::kEndOfSession);  // repeated by linger(): UDP may lose any single copy
    std::printf("[publisher] done: %" PRIu64 " msgs in %.2f s (%.0f msg/s), %" PRIu64 " packets, deliberately "
                "dropped %" PRIu64 " packets (%" PRIu64 " msgs), last seq %" PRIu64 "\n",
                sent_msgs, secs, double(sent_msgs) / secs, packets_, dropped_packets_, dropped_msgs_, seq_ - 1);
    std::fflush(stdout);
  }

  std::uint64_t next_seq() const { return seq_; }

  // After the last message: keep repeating End of Session (as MoldUDP64 servers do), so a
  // subscriber that lost the tail, or was busy recovering, still learns the final sequence
  // number, detects the gap and recovers before the snapshot server goes away.
  void linger(std::int64_t seconds) {
    const std::int64_t until = monotonic_ns() + seconds * 1'000'000'000;
    while (!g_stop.load() && monotonic_ns() < until) {
      std::this_thread::sleep_for(std::chrono::nanoseconds(std::min<std::int64_t>(hb_ns_, 100'000'000)));
      send_control(mold::kEndOfSession);
    }
  }

 private:
  void enqueue(const std::uint8_t* m, std::size_t len) {
    FeedMsg* s;
    while (!(s = q_.claim())) {
      // The snapshot server is the source of truth and must see every message, so we apply
      // backpressure instead of dropping. Only matters when replaying flat out.
    }
    *s = FeedMsg{seq_++, m, len};
    q_.publish();
  }

  void open_packet() { pb_.begin(bufs_[nbatch_], seq_); }

  void close_packet() {
    if (!pb_.count()) return;
    std::size_t len = pb_.finish();
    std::uint16_t msgs = pb_.count();
    pb_.reset();
    ++packets_;
    if (should_drop()) {  // the sequence numbers are spent: receivers will see a gap
      ++dropped_packets_;
      dropped_msgs_ += msgs;
      return;
    }
    iov_[nbatch_].iov_len = len;
    if (++nbatch_ == kBatch) flush_batch();
  }

  // --drop-every N --drop-burst B drops packets N..N+B-1, 2N..2N+B-1, ... (1-based).
  bool should_drop() {
    if (drop_every_ && packets_ >= drop_every_ && packets_ % drop_every_ < drop_burst_) return true;
    return drop_prob_ > 0 && std::uniform_real_distribution<double>(0, 1)(rng_) < drop_prob_;
  }

  void flush_batch() {
    int off = 0;
    while (off < nbatch_) {
      int n = ::sendmmsg(fd_, hdrs_ + off, unsigned(nbatch_ - off), 0);
      if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == ENOBUFS) continue;  // spin: buffer full
        net::die("sendmmsg");
      }
      off += n;
    }
    if (nbatch_) last_send_ns_ = monotonic_ns();
    nbatch_ = 0;
  }

  void send_control(std::uint16_t kind) {
    std::uint8_t buf[mold::kHeaderLen];
    mold::PacketBuilder b(session_.c_str());
    std::size_t len = b.control(buf, seq_, kind);
    ::sendto(fd_, buf, len, 0, reinterpret_cast<const sockaddr*>(&dst_), sizeof dst_);
    last_send_ns_ = monotonic_ns();
  }

  // Busy-wait for short gaps (precise), sleep for long ones (do not burn a core for minutes of
  // pre-market silence). Heartbeats keep receivers informed while we are idle.
  void wait_until(std::int64_t due) {
    for (;;) {
      std::int64_t now = monotonic_ns();
      if (now >= due || g_stop.load(std::memory_order_relaxed)) return;
      if (now - last_send_ns_ >= hb_ns_) send_control(mold::kHeartbeat);
      std::int64_t left = due - now;
      if (left > 200'000) {
        std::int64_t nap = std::min<std::int64_t>(left - 100'000, hb_ns_ / 2);
        timespec ts{time_t(nap / 1'000'000'000), long(nap % 1'000'000'000)};
        nanosleep(&ts, nullptr);
      } else {
        __builtin_ia32_pause();
      }
    }
  }

  SpscQueue<FeedMsg>& q_;
  int fd_;
  sockaddr_in dst_;
  std::string session_;
  mold::PacketBuilder pb_;
  double speed_, rate_;
  std::uint64_t drop_every_, drop_burst_;
  double drop_prob_;
  std::mt19937_64 rng_;
  std::int64_t hb_ns_;
  std::int64_t last_send_ns_ = 0;

  std::uint8_t bufs_[kBatch][2048];
  iovec iov_[kBatch];
  mmsghdr hdrs_[kBatch];
  int nbatch_ = 0;

  std::uint64_t seq_ = 1;
  std::uint64_t packets_ = 0, dropped_packets_ = 0, dropped_msgs_ = 0;
};

}  // namespace

int main(int argc, char** argv) {
  Options o(argc, argv,
            {{"group", "239.1.1.1"}, {"port", "30001"}, {"iface", "127.0.0.1"}, {"session", "ITCH50"},
             {"speed", "0"}, {"rate", "0"}, {"skip", "0"}, {"count", "0"}, {"max-payload", "1400"},
             {"drop-every", "0"}, {"drop-burst", "1"}, {"drop-prob", "0"}, {"seed", "1"},
             {"heartbeat-ms", "1000"}, {"snap-ip", "127.0.0.1"}, {"snap-port", "31001"}, {"linger", "10"},
             {"pub-cpu", "6"}, {"snap-cpu", "8"}},
            kUsage);
  if (o.positional().size() != 1) o.fail("expected exactly one ITCH file");
  signal(SIGINT, on_signal);
  signal(SIGTERM, on_signal);

  const std::string path = o.positional()[0];
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) net::die("open " + path);
  struct stat st;
  fstat(fd, &st);
  auto* base = static_cast<const std::uint8_t*>(::mmap(nullptr, std::size_t(st.st_size), PROT_READ, MAP_SHARED, fd, 0));
  if (base == MAP_FAILED) net::die("mmap");
  ::madvise(const_cast<std::uint8_t*>(base), std::size_t(st.st_size), MADV_SEQUENTIAL);

  SpscQueue<FeedMsg> q(1 << 20);
  SnapshotServer server(q, o.str("snap-ip"), int(o.num("snap-port")));
  std::atomic<bool> server_stop{false};
  std::thread snap_thread([&] { server.run(int(o.num("snap-cpu")), server_stop); });

  std::printf("[publisher] %s (%.2f GB) -> %s:%lld via %s, snapshot server on %s:%lld\n", path.c_str(),
              double(st.st_size) / 1e9, o.str("group").c_str(), o.num("port"), o.str("iface").c_str(),
              o.str("snap-ip").c_str(), o.num("snap-port"));
  {
    Publisher pub(o, q);
    std::thread pub_thread([&] {
      pin_this_thread(int(o.num("pub-cpu")), "publisher");
      pub.run(base, base + st.st_size, std::uint64_t(o.num("skip")), std::uint64_t(o.num("count")));
    });
    pub_thread.join();
    pub.linger(o.num("linger"));  // the snapshot server keeps serving meanwhile
  }
  server_stop.store(true);
  snap_thread.join();
  std::printf("[publisher] snapshot server: %" PRIu64 " snapshots, %" PRIu64 " retransmissions, %" PRIu64
              " digests served\n",
              server.snapshots(), server.retransmissions(), server.digests());
  ::munmap(const_cast<std::uint8_t*>(base), std::size_t(st.st_size));
  ::close(fd);
  return 0;
}
