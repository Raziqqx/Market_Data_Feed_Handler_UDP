// feed_handler: receives the MoldUDP64/ITCH multicast feed, detects gaps, recovers from the TCP
// snapshot server, and maintains every order book. Reports a per-stage latency breakdown.
//
// Threads (each pinned):
//   feed  epoll loop over the non-blocking UDP socket and (while recovering) the TCP snapshot
//         connection. recvmmsg writes packets straight into a ring of preallocated slots; the
//         parser reads fields out of those slots (zero-copy) and decodes each book event directly
//         into a slot of the SPSC queue.
//   book  pops updates and applies them to the P3 order books, stamping the latency of every
//         stage along the way.
//
// Recovery: from the moment a gap is seen, every arriving packet is copied into a compact pending
// buffer instead of being processed. When the snapshot is done, the pending packets are replayed
// in order (messages already covered by the snapshot are skipped by sequence number), and only
// when the backlog is empty does the handler go back to processing straight from the ring.

#include <linux/net_tstamp.h>
#include <signal.h>

#include <atomic>
#include <cstdarg>
#include <cinttypes>
#include <memory>
#include <random>
#include <thread>

#include "feed/moldudp64.hpp"
#include "feed/net.hpp"
#include "feed/pending_buffer.hpp"
#include "feed/snapshot.hpp"
#include "feed/spsc_queue.hpp"
#include "feed/update.hpp"
#include "feed/util.hpp"
#include "itch/book_builder.hpp"
#include "lob/latency.hpp"

using namespace feed;

namespace {

std::atomic<bool> g_stop{false};
void on_signal(int) { g_stop.store(true); }

const char* kUsage =
    "usage: feed_handler [options]\n"
    "  --group IP          multicast group (239.1.1.1)\n"
    "  --port N            multicast port (30001)\n"
    "  --iface IP          interface address to join on (127.0.0.1)\n"
    "  --snap-ip IP        snapshot server address (127.0.0.1)\n"
    "  --snap-port N       snapshot server port (31001)\n"
    "  --feed-cpu N        pin the feed thread (2, -1 = no pin)\n"
    "  --book-cpu N        pin the book thread (4, -1 = no pin)\n"
    "  --queue N           SPSC queue capacity, power of two (65536)\n"
    "  --ring N            packet ring slots, power of two (16384)\n"
    "  --batch N           max datagrams per recvmmsg (64)\n"
    "  --rcvbuf BYTES      requested socket receive buffer (8388608)\n"
    "  --blocking          epoll_wait blocks instead of busy-polling\n"
    "  --drop-prob P       simulate receive loss: ignore each packet with probability P (0)\n"
    "  --drop-every N      simulate receive loss: ignore every Nth packet (0)\n"
    "  --seed N            RNG seed for --drop-prob (7)\n"
    "  --retransmit-max N  gaps of at most N messages are filled by retransmission; larger ones,\n"
    "                      late joins and expired history use a snapshot (50000; 0 = snapshot only)\n"
    "  --idle-exit S       give up after S seconds without packets, once started (10)\n"
    "  --max-orders N      order pool size; the order-id table gets 2x this many slots (16777216)\n"
    "  --max-levels N      price level pool size (2097152)\n"
    "  --max-books N       instruments (12288)\n"
    "  --window N          price-ladder slots per side per book (4096)\n"
    "  --prefault          touch all book memory up front (no page faults on the hot path)\n"
    "  --no-digest         skip the end-of-session digest check against the server\n"
    "  --report FILE       also write the final report to FILE\n";

struct RecoveryRecord {
  char kind = 0;                       // 'R' retransmission or 'S' snapshot (after any fallback)
  std::uint64_t expected = 0;          // what we were waiting for
  std::uint64_t seen = 0;              // sequence that revealed the gap
  std::uint64_t resume_seq = 0;        // first sequence taken from the live feed afterwards
  std::uint64_t msgs = 0, bytes = 0;   // size of the recovery response
  std::uint64_t buffered_packets = 0;  // packets that arrived while recovering
  double request_ms = 0;               // gap detected -> response header received
  double total_ms = 0;                 // gap detected -> response fully applied
  int attempts = 0;
};

struct FeedStats {
  std::uint64_t packets = 0, bytes = 0, messages = 0, pushed = 0;
  std::uint64_t heartbeats = 0, duplicate_packets = 0, bad_packets = 0, bad_messages = 0;
  std::uint64_t sim_dropped = 0, kernel_drops = 0, ring_full = 0, queue_full_waits = 0;
  std::uint64_t gaps = 0, missed_msgs = 0, snapshot_msgs = 0, snapshots = 0;
  std::uint64_t retransmissions = 0, retransmitted_msgs = 0;
  std::uint64_t pending_overflow = 0, pending_peak_bytes = 0;
  std::uint64_t final_seq = 0;
  bool end_of_session = false;
  std::vector<RecoveryRecord> recoveries;
};

// ---------------------------------------------------------------------------------------------
// Feed thread
// ---------------------------------------------------------------------------------------------
class FeedHandler {
 public:
  FeedHandler(const Options& o, SpscQueue<Update>& q)
      : q_(q),
        ring_n_(std::size_t(o.num("ring"))),
        batch_(unsigned(o.num("batch"))),
        busy_(!o.flag("blocking")),
        snap_ip_(o.str("snap-ip")),
        snap_port_(int(o.num("snap-port"))),
        drop_prob_(o.real("drop-prob")),
        drop_every_(std::uint64_t(o.num("drop-every"))),
        retransmit_max_(std::uint64_t(o.num("retransmit-max"))),
        rng_(std::uint64_t(o.num("seed"))),
        idle_exit_ns_(o.num("idle-exit") * 1'000'000'000) {
    if (ring_n_ & (ring_n_ - 1)) o.fail("--ring must be a power of two");
    udp_ = net::multicast_receiver(o.str("group"), int(o.num("port")), o.str("iface"), int(o.num("rcvbuf")),
                                   &rcvbuf_got_);
    ep_.add(udp_, EPOLLIN);

    // All receive memory is allocated (and touched, so the first lap does not page-fault: 32 MB
    // of first-touch faults was enough to overflow the socket buffer) once, here.
    slots_.reset(new Slot[ring_n_]());
    hdrs_.reset(new mmsghdr[ring_n_]);
    iovs_.reset(new iovec[ring_n_]);
    ctrl_.reset(new Ctrl[ring_n_]);
    for (std::size_t i = 0; i < ring_n_; ++i) {
      iovs_[i] = iovec{slots_[i].data, sizeof slots_[i].data};
      hdrs_[i] = mmsghdr{};
      hdrs_[i].msg_hdr.msg_iov = &iovs_[i];
      hdrs_[i].msg_hdr.msg_iovlen = 1;
      hdrs_[i].msg_hdr.msg_control = ctrl_[i].buf;
    }
    snap_buf_.resize(4 << 20);
  }

  ~FeedHandler() {
    if (tcp_ >= 0) ::close(tcp_);
    ::close(udp_);
  }

  int rcvbuf() const { return rcvbuf_got_; }
  const FeedStats& stats() const {
    st_.pending_peak_bytes = pend_.peak_bytes();
    return st_;
  }

  void run(int cpu) {
    pin_this_thread(cpu, "feed");
    epoll_event evs[8];
    last_packet_ns_ = monotonic_ns();
    std::uint32_t tick = 0;
    while (!st_.end_of_session && !g_stop.load(std::memory_order_relaxed)) {
      int n = ep_.wait(evs, 8, busy_ ? 0 : 50);
      for (int i = 0; i < n; ++i) {
        if (evs[i].data.fd == udp_) drain_udp();
        else if (evs[i].data.fd == tcp_) on_tcp(evs[i].events);
      }
      process();
      if ((++tick & 0x3FF) == 0 || !busy_) housekeeping();
    }
    if (!st_.end_of_session) {
      std::fprintf(stderr, "[feed] stopping without end of session (idle or signal)\n");
      push_control(kEndOfSession);
    }
  }

 private:
  struct Slot {
    std::uint8_t data[2048];
    std::int64_t kernel_ns;  // SO_TIMESTAMPNS (0 = none)
    std::int64_t user_ns;    // CLOCK_REALTIME right after recvmmsg returned
    std::uint64_t rx_tsc;    // TSC right after recvmmsg returned
    std::uint32_t len;       // 0 = deliberately dropped (simulated loss)
  };
  struct Ctrl {
    alignas(cmsghdr) char buf[128];
  };
  // What handle_packet needs, whether the packet sits in a ring slot or in the pending buffer.
  struct PacketRef {
    const std::uint8_t* data;
    std::uint32_t len;
    std::int64_t kernel_ns, user_ns;
    std::uint64_t rx_tsc;
  };
  static PacketRef ref(const Slot& s) { return {s.data, s.len, s.kernel_ns, s.user_ns, s.rx_tsc}; }
  static PacketRef ref(const PendingBuffer::Entry& e) { return {e.data(), e.len, e.kernel_ns, e.user_ns, e.rx_tsc}; }

  // While recovering, and until the backlog is worked off, new packets go to the pending buffer
  // so that they are processed strictly after everything that arrived before them.
  bool buffering() const { return recovering_ || !pend_.empty(); }

  // ---- receive ------------------------------------------------------------------------------

  // Pull every waiting datagram into free ring slots (never consumes; process() does that). In
  // buffering mode the slots are only a landing zone: packets are copied to pend_ immediately.
  void drain_udp() {
    for (;;) {
      std::size_t used = std::size_t(tail_ - head_);
      if (used == ring_n_) {
        if (!ring_was_full_) ++st_.ring_full;  // the kernel keeps buffering; then drops -> gap
        ring_was_full_ = true;
        return;
      }
      ring_was_full_ = false;
      std::size_t start = std::size_t(tail_) & (ring_n_ - 1);
      unsigned k = unsigned(std::min<std::size_t>({batch_, ring_n_ - used, ring_n_ - start}));
      for (unsigned i = 0; i < k; ++i) hdrs_[start + i].msg_hdr.msg_controllen = sizeof(Ctrl::buf);
      int n = ::recvmmsg(udp_, &hdrs_[start], k, MSG_DONTWAIT, nullptr);
      if (n <= 0) {
        if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) net::die("recvmmsg");
        return;
      }
      const std::uint64_t tsc = lob::rdtsc();
      const std::int64_t now = realtime_ns();
      for (int i = 0; i < n; ++i) {
        Slot& s = slots_[start + std::size_t(i)];
        s.len = hdrs_[start + std::size_t(i)].msg_len;
        s.rx_tsc = tsc;
        s.user_ns = now;
        s.kernel_ns = 0;
        msghdr& mh = hdrs_[start + std::size_t(i)].msg_hdr;
        for (cmsghdr* c = CMSG_FIRSTHDR(&mh); c; c = CMSG_NXTHDR(&mh, c)) {
          if (c->cmsg_level != SOL_SOCKET) continue;
          if (c->cmsg_type == SCM_TIMESTAMPNS) {
            timespec ts;
            std::memcpy(&ts, CMSG_DATA(c), sizeof ts);
            s.kernel_ns = std::int64_t(ts.tv_sec) * 1'000'000'000 + ts.tv_nsec;
          } else if (c->cmsg_type == SO_RXQ_OVFL) {
            std::uint32_t drops;
            std::memcpy(&drops, CMSG_DATA(c), sizeof drops);
            st_.kernel_drops = drops;  // cumulative count for this socket
          }
        }
        ++rx_count_;
        if ((drop_every_ && rx_count_ % drop_every_ == 0) ||
            (drop_prob_ > 0 && std::uniform_real_distribution<double>(0, 1)(rng_) < drop_prob_)) {
          s.len = 0;
          ++st_.sim_dropped;
        }
      }
      if (buffering()) {
        for (int i = 0; i < n; ++i) {
          const Slot& s = slots_[start + std::size_t(i)];
          if (s.len && !pend_.push(s.data, s.len, s.kernel_ns, s.user_ns, s.rx_tsc)) ++st_.pending_overflow;
        }
      } else {
        tail_ += std::uint64_t(n);
      }
      last_packet_ns_ = monotonic_ns();
      if (unsigned(n) < k) return;  // socket is empty
    }
  }

  // ---- live processing ----------------------------------------------------------------------

  // Backlog first (it is older), then the ring. On a gap the packet that revealed it is not
  // consumed: it stays at the front of the pending buffer and is replayed after the snapshot.
  void process() {
    while (!recovering_ && !st_.end_of_session) {
      if (const PendingBuffer::Entry* e = pend_.front()) {
        if (!handle_packet(ref(*e), kReplayed)) return;
        pend_.pop();
        continue;
      }
      if (head_ == tail_) return;
      const Slot& s = slots_[std::size_t(head_) & (ring_n_ - 1)];
      if (!handle_packet(ref(s), 0)) return;
      ++head_;
    }
  }

  // Returns false if the packet revealed a gap (recovery started, packet not consumed).
  bool handle_packet(const PacketRef& s, std::uint8_t extra_flags) {
    if (s.len == 0) return true;
    mold::PacketView pk;
    if (!mold::parse_header(s.data, s.len, pk)) {
      ++st_.bad_packets;
      return true;
    }
    ++st_.packets;
    st_.bytes += s.len;

    if (pk.seq > expected_) {
      start_recovery(pk.seq);
      return false;
    }
    if (pk.end_of_session()) {
      st_.final_seq = pk.seq;
      st_.end_of_session = true;
      push_control(kEndOfSession);
      return true;
    }
    if (pk.heartbeat()) {
      ++st_.heartbeats;
      return true;
    }
    if (pk.seq + pk.count <= expected_) {
      ++st_.duplicate_packets;  // entirely old (e.g. buffered during a recovery that covered it)
      return true;
    }

    const std::uint32_t wire_ns =
        s.kernel_ns ? std::uint32_t(std::max<std::int64_t>(0, s.user_ns - s.kernel_ns)) : 0;
    const std::uint8_t flags = std::uint8_t(extra_flags | (s.kernel_ns ? 0 : kNoKernelTs));

    const std::uint8_t* p = pk.blocks;
    std::uint64_t seq = pk.seq;
    for (std::uint16_t i = 0; i < pk.count; ++i, ++seq) {
      if (pk.end - p < 2) return bad_packet();
      const std::size_t len = itch::be16(p);
      const std::uint8_t* m = p + 2;
      p += 2 + len;
      if (p > pk.end) return bad_packet();
      if (seq < expected_) continue;  // already covered by a snapshot
      expected_ = seq + 1;
      ++st_.messages;
      if (len == 0 || itch::kSizes.size[m[0]] != len) {
        ++st_.bad_messages;
        continue;
      }
      if (!book_relevant(m[0])) continue;
      Update& u = claim();
      decode_into(m, u);
      u.seq = seq;
      u.t_rx_tsc = s.rx_tsc;
      u.wire_ns = wire_ns;
      u.flags = flags;
      u.t_push_tsc = lob::rdtsc();
      q_.publish();
      ++st_.pushed;
    }
    return true;
  }

  bool bad_packet() {
    ++st_.bad_packets;
    return true;
  }

  // Get a queue slot. If the book thread is behind, keep the socket drained while we wait so
  // a slow consumer does not turn into kernel packet loss.
  Update& claim() {
    if (Update* u = q_.claim()) return *u;
    ++st_.queue_full_waits;
    for (;;) {
      drain_udp();
      if (Update* u = q_.claim()) return *u;
    }
  }

  void push_control(Control c) {
    Update& u = claim();
    u = Update{};
    u.type = char(c);
    u.t_push_tsc = lob::rdtsc();
    q_.publish();
  }

  // ---- recovery -----------------------------------------------------------------------------

  // Two-tier recovery, as real feeds do it:
  //   'R' retransmission: a small gap asks for just the missing messages [expected, seen). The
  //       books are untouched; cost is one TCP round trip plus the few messages.
  //   'S' snapshot: a large gap, a late join, or a retransmission the server can no longer serve
  //       (too old for its history) rebuilds the books from a full snapshot.
  void start_recovery(std::uint64_t seen) {
    recovering_ = true;
    ++st_.gaps;
    st_.missed_msgs += seen - expected_;
    rec_ = RecoveryRecord{};
    rec_.expected = expected_;
    rec_.seen = seen;
    rec_.kind = (seen - expected_ <= retransmit_max_) ? 'R' : 'S';
    rec_t0_ = monotonic_ns();
    want_seq_ = seen;
    // Move whatever is waiting in the ring (starting with the packet that revealed the gap) into
    // the pending buffer; from now on drain_udp() appends there too. If the gap was found while
    // replaying the pending buffer, the ring is already empty (nothing lands there meanwhile).
    for (; head_ != tail_; ++head_) {
      const Slot& s = slots_[std::size_t(head_) & (ring_n_ - 1)];
      if (s.len && !pend_.push(s.data, s.len, s.kernel_ns, s.user_ns, s.rx_tsc)) ++st_.pending_overflow;
    }
    std::printf("[feed] GAP: expected seq %" PRIu64 ", got %" PRIu64 " (%" PRIu64 " msgs missing) -> requesting %s\n",
                expected_, seen, seen - expected_, rec_.kind == 'R' ? "retransmission" : "snapshot");
    std::fflush(stdout);
    connect_recovery();
  }

  void connect_recovery() {
    ++rec_.attempts;
    tcp_ = net::tcp_connect_nb(snap_ip_, snap_port_);
    if (tcp_ < 0) {
      retry_at_ = monotonic_ns() + 200'000'000;
      return;
    }
    connected_ = false;
    have_header_ = false;
    snap_len_ = 0;
    ep_.add(tcp_, EPOLLOUT | EPOLLIN | EPOLLRDHUP);
  }

  void close_recovery() {
    ep_.del(tcp_);
    ::close(tcp_);
    tcp_ = -1;
  }

  void fail_recovery(const char* why) {
    std::fprintf(stderr, "[feed] recovery attempt failed (%s), retrying\n", why);
    close_recovery();
    retry_at_ = monotonic_ns() + 200'000'000;
  }

  void on_tcp(std::uint32_t events) {
    if (!connected_ && (events & (EPOLLOUT | EPOLLERR | EPOLLHUP))) {
      if (net::socket_error(tcp_) != 0) return fail_recovery("connect");
      std::uint8_t req[snap::kRequestLen];
      if (rec_.kind == 'R') snap::encode_request(req, 'R', expected_, want_seq_);
      else snap::encode_request(req, 'S', want_seq_);
      if (::send(tcp_, req, sizeof req, MSG_NOSIGNAL) != ssize_t(sizeof req)) return fail_recovery("send");
      connected_ = true;
      ep_.mod(tcp_, EPOLLIN | EPOLLRDHUP);
    }
    if (!(events & (EPOLLIN | EPOLLRDHUP | EPOLLHUP))) return;
    for (;;) {
      drain_udp();  // a big snapshot takes a while: keep the multicast socket from overflowing
      ssize_t r = ::recv(tcp_, snap_buf_.data() + snap_len_, snap_buf_.size() - snap_len_, 0);
      if (r == 0) return fail_recovery("server closed early");
      if (r < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) return;
        return fail_recovery("recv");
      }
      snap_len_ += std::size_t(r);
      if (consume_recovery()) return finish_recovery();
      if (tcp_ < 0) return;  // rejected, or switched from retransmission to snapshot
    }
  }

  // Parse what has arrived so far, push it to the book thread, keep any partial message.
  // Returns true once the whole response has been consumed.
  bool consume_recovery() {
    std::size_t off = 0;
    if (!have_header_) {
      if (snap_len_ < snap::kSnapshotHeaderLen) return false;
      if (snap_buf_[0] != std::uint8_t(rec_.kind)) {
        fail_recovery("bad header");
        return false;
      }
      snap_seq_ = itch::be64(&snap_buf_[1]);  // 'S': next_seq   'R': first message's seq
      snap_msgs_ = itch::be64(&snap_buf_[9]);
      snap_bytes_ = itch::be64(&snap_buf_[17]);
      snap_done_msgs_ = 0;
      have_header_ = true;
      off = snap::kSnapshotHeaderLen;
      rec_.request_ms = double(monotonic_ns() - rec_t0_) / 1e6;
      if (rec_.kind == 'R' && snap_msgs_ == 0) {
        // Server no longer has those messages: fall back to a snapshot, right away.
        std::printf("[feed] retransmission unavailable (too old) -> falling back to snapshot\n");
        close_recovery();
        rec_.kind = 'S';
        connect_recovery();
        return false;
      }
      if (rec_.kind == 'S') push_control(kReset);  // the books are rebuilt from scratch
    }
    const std::uint64_t tsc = lob::rdtsc();
    while (snap_len_ - off >= 2) {
      std::size_t len = itch::be16(&snap_buf_[off]);
      if (snap_len_ - off < 2 + len) break;
      const std::uint8_t* m = &snap_buf_[off + 2];
      off += 2 + len;
      const std::uint64_t seq = rec_.kind == 'R' ? snap_seq_ + snap_done_msgs_ : snap_seq_ - 1;
      ++snap_done_msgs_;
      if ((snap_done_msgs_ & 1023) == 0) drain_udp();  // keep the socket from overflowing meanwhile
      if (rec_.kind == 'R') {
        if (seq < expected_) continue;
        expected_ = seq + 1;
        ++st_.messages;
      }
      if (len == 0 || itch::kSizes.size[m[0]] != len) {
        ++st_.bad_messages;
        continue;
      }
      if (!book_relevant(m[0])) continue;
      Update& u = claim();
      decode_into(m, u);
      u.seq = seq;
      u.t_rx_tsc = tsc;
      u.wire_ns = 0;
      u.flags = rec_.kind == 'R' ? kReplayed : kFromSnapshot;
      u.t_push_tsc = tsc;
      q_.publish();
    }
    std::memmove(snap_buf_.data(), snap_buf_.data() + off, snap_len_ - off);
    snap_len_ -= off;
    return snap_done_msgs_ == snap_msgs_ && snap_len_ == 0;
  }

  void finish_recovery() {
    close_recovery();
    if (rec_.kind == 'S') {
      push_control(kSnapshotDone);
      expected_ = snap_seq_;
      st_.snapshot_msgs += snap_msgs_;
      ++st_.snapshots;
    } else {
      st_.retransmitted_msgs += snap_msgs_;
      ++st_.retransmissions;
    }
    recovering_ = false;  // process() now replays the pending buffer, then returns to the ring
    rec_.resume_seq = expected_;
    rec_.msgs = snap_msgs_;
    rec_.bytes = snap_bytes_;
    rec_.buffered_packets = pend_.count();
    rec_.total_ms = double(monotonic_ns() - rec_t0_) / 1e6;
    st_.recoveries.push_back(rec_);
    std::printf("[feed] RECOVERED by %s: %" PRIu64 " msgs (%.2f MB), resume at seq %" PRIu64 ", %.2f ms; replaying %" PRIu64
                " buffered packets\n",
                rec_.kind == 'R' ? "retransmission" : "snapshot", snap_msgs_, double(snap_bytes_) / 1e6, expected_,
                rec_.total_ms, rec_.buffered_packets);
    std::fflush(stdout);
  }

  void housekeeping() {
    std::int64_t now = monotonic_ns();
    if (recovering_ && tcp_ < 0 && now >= retry_at_) connect_recovery();
    if (tail_ > 0 && now - last_packet_ns_ > idle_exit_ns_) g_stop.store(true);  // only once the feed has started
  }

  SpscQueue<Update>& q_;
  const std::size_t ring_n_;
  const unsigned batch_;
  const bool busy_;
  const std::string snap_ip_;
  const int snap_port_;
  const double drop_prob_;
  const std::uint64_t drop_every_;
  const std::uint64_t retransmit_max_;
  std::mt19937_64 rng_;
  const std::int64_t idle_exit_ns_;

  net::Epoll ep_;
  int udp_ = -1;
  int rcvbuf_got_ = 0;
  std::unique_ptr<Slot[]> slots_;
  std::unique_ptr<mmsghdr[]> hdrs_;
  std::unique_ptr<iovec[]> iovs_;
  std::unique_ptr<Ctrl[]> ctrl_;
  std::uint64_t head_ = 0, tail_ = 0;  // free-running ring indices
  std::uint64_t rx_count_ = 0;
  bool ring_was_full_ = false;
  std::int64_t last_packet_ns_ = 0;

  std::uint64_t expected_ = 1;  // next sequence number we need
  bool recovering_ = false;
  PendingBuffer pend_;

  // snapshot client state
  int tcp_ = -1;
  bool connected_ = false, have_header_ = false;
  std::uint64_t want_seq_ = 0, snap_seq_ = 0, snap_msgs_ = 0, snap_bytes_ = 0, snap_done_msgs_ = 0;
  std::vector<std::uint8_t> snap_buf_;
  std::size_t snap_len_ = 0;
  std::int64_t retry_at_ = 0, rec_t0_ = 0;
  RecoveryRecord rec_{};

  mutable FeedStats st_;
};

// ---------------------------------------------------------------------------------------------
// Book thread
// ---------------------------------------------------------------------------------------------
struct StageHists {
  lob::LatencyHistogram wire, parse, queue, book, total, user_total;
  lob::LatencyHistogram by_type[6];  // A/F, E, C, X, D, U
};

int type_index(char t) {
  switch (t) {
    case 'A': case 'F': return 0;
    case 'E': return 1;
    case 'C': return 2;
    case 'X': return 3;
    case 'D': return 4;
    case 'U': return 5;
    default: return -1;
  }
}

class BookThread {
 public:
  BookThread(SpscQueue<Update>& q, double tsc_ghz, const itch::BuilderConfig& cfg, bool prefault)
      : q_(q), ns_per_tick_(1.0 / tsc_ghz), cfg_(cfg), prefault_(prefault) {
    reset();
  }

  void run(int cpu) {
    pin_this_thread(cpu, "book");
    for (;;) {
      Update* u = q_.front();
      if (!u) {
        __builtin_ia32_pause();
        continue;
      }
      const std::uint64_t t_pop = lob::rdtsc();
      const char type = u->type;
      if (type == kEndOfSession) {
        q_.pop();
        break;
      }
      if (type == kReset) {
        reset();
      } else if (type == kSnapshotDone) {
        ++snapshots_applied_;
      } else {
        apply(*b_, *u);
        ++applied_;
        const std::uint64_t t_done = lob::rdtsc();
        const int ti = type_index(type);
        if (ti >= 0 && !(u->flags & (kFromSnapshot | kReplayed))) record(*u, t_pop, t_done, ti);
        else if (u->flags & kReplayed) ++replayed_;
      }
      q_.pop();
    }
    digest_ = snap::digest(*b_);
    totals_ = b_->stats();
  }

  const StageHists& hists() const { return h_; }
  const itch::BuilderStats& totals() const { return totals_; }
  const snap::Digest& digest() const { return digest_; }
  std::uint64_t applied() const { return applied_; }
  std::uint64_t replayed() const { return replayed_; }
  std::uint64_t resets() const { return resets_; }
  std::uint64_t skewed() const { return skewed_; }

 private:
  void record(const Update& u, std::uint64_t t_pop, std::uint64_t t_done, int ti) {
    // Stamps taken on different cores can disagree by a few ns (TSC skew, worse under a
    // hypervisor), so b < a is possible for the queue hop. Clamp to 0 and count it.
    const double k = ns_per_tick_;
    const auto ns = [this, k](std::uint64_t a, std::uint64_t b) {
      if (b < a) {
        ++skewed_;
        return std::uint64_t(0);
      }
      return std::uint64_t(double(b - a) * k);
    };
    const std::uint64_t parse = ns(u.t_rx_tsc, u.t_push_tsc);
    const std::uint64_t queue = ns(u.t_push_tsc, t_pop);
    const std::uint64_t book = ns(t_pop, t_done);
    const std::uint64_t user = ns(u.t_rx_tsc, t_done);
    h_.parse.record(parse);
    h_.queue.record(queue);
    h_.book.record(book);
    h_.user_total.record(user);
    h_.by_type[ti].record(book);
    if (!(u.flags & kNoKernelTs)) {
      h_.wire.record(u.wire_ns);
      h_.total.record(u.wire_ns + user);
    }
  }

  // Before a snapshot: empty the books in place. Rebuilding the builder instead would unmap
  // and re-fault hundreds of MB, stalling this thread long enough to overflow the queue, the
  // packet ring and finally the kernel socket buffer: one gap would cause the next.
  void reset() {
    if (!b_) {
      b_ = std::make_unique<itch::BookBuilder>(cfg_);
      if (prefault_) b_->prefault();  // touch every page now, not on the hot path
      return;
    }
    ++resets_;
    b_->clear();
  }

  SpscQueue<Update>& q_;
  const double ns_per_tick_;
  const itch::BuilderConfig cfg_;
  const bool prefault_;
  std::unique_ptr<itch::BookBuilder> b_;
  StageHists h_;
  itch::BuilderStats totals_;
  snap::Digest digest_;
  std::uint64_t applied_ = 0, replayed_ = 0, resets_ = 0, snapshots_applied_ = 0, skewed_ = 0;
};

// ---------------------------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------------------------
struct Report {
  std::string text;
  void line(const char* fmt, ...) __attribute__((format(printf, 2, 3))) {
    char buf[512];
    va_list ap;
    va_start(ap, fmt);
    std::vsnprintf(buf, sizeof buf, fmt, ap);
    va_end(ap);
    text += buf;
    text += '\n';
  }
};

void hist_row(Report& r, const char* name, const lob::LatencyHistogram& h) {
  if (h.count() == 0) {
    r.line("  %-26s %12s", name, "(no samples)");
    return;
  }
  r.line("  %-26s %12" PRIu64 " %8" PRIu64 " %8" PRIu64 " %8" PRIu64 " %9" PRIu64 " %10" PRIu64 " %9.0f", name,
         h.count(), h.percentile(50), h.percentile(90), h.percentile(99), h.percentile(99.9), h.max(), h.mean());
}

}  // namespace

int main(int argc, char** argv) {
  Options o(argc, argv,
            {{"group", "239.1.1.1"}, {"port", "30001"}, {"iface", "127.0.0.1"}, {"snap-ip", "127.0.0.1"},
             {"snap-port", "31001"}, {"feed-cpu", "2"}, {"book-cpu", "4"}, {"queue", "65536"}, {"ring", "16384"},
             {"batch", "64"}, {"rcvbuf", "8388608"}, {"blocking", "false"}, {"drop-prob", "0"},
             {"drop-every", "0"}, {"seed", "7"}, {"retransmit-max", "50000"}, {"idle-exit", "10"}, {"no-digest", "false"}, {"report", ""},
             {"max-orders", "16777216"}, {"max-levels", "2097152"}, {"max-books", "12288"}, {"window", "4096"},
             {"prefault", "false"}},
            kUsage);
  signal(SIGINT, on_signal);
  signal(SIGTERM, on_signal);

  const double ghz = lob::calibrate_tsc_ghz();
  SpscQueue<Update> q(std::size_t(o.num("queue")));
  FeedHandler feed(o, q);
  itch::BuilderConfig cfg;
  cfg.max_orders = std::size_t(o.num("max-orders"));
  cfg.max_levels = std::size_t(o.num("max-levels"));
  cfg.max_books = std::size_t(o.num("max-books"));
  cfg.window = std::uint32_t(o.num("window"));
  if (cfg.window % 64) o.fail("--window must be a multiple of 64");
  BookThread book(q, ghz, cfg, o.flag("prefault"));
  std::printf("[handler] joined %s:%lld on %s (rcvbuf %d bytes), TSC %.3f GHz, %s; waiting for packets\n",
              o.str("group").c_str(), o.num("port"), o.str("iface").c_str(), feed.rcvbuf(), ghz,
              o.flag("blocking") ? "blocking epoll" : "busy-poll epoll");
  std::fflush(stdout);

  const std::int64_t t0 = monotonic_ns();
  std::thread book_thread([&] { book.run(int(o.num("book-cpu"))); });
  std::thread feed_thread([&] { feed.run(int(o.num("feed-cpu"))); });
  feed_thread.join();
  book_thread.join();
  const double secs = double(monotonic_ns() - t0) / 1e9;

  const FeedStats& fs = feed.stats();
  const itch::BuilderStats& bs = book.totals();
  Report r;
  r.line("==================== feed handler report ====================");
  r.line("packets %" PRIu64 "  bytes %" PRIu64 "  messages %" PRIu64 "  pushed to book %" PRIu64 "  (%.2f s)",
         fs.packets, fs.bytes, fs.messages, fs.pushed, secs);
  r.line("heartbeats %" PRIu64 "  duplicate/old packets %" PRIu64 "  bad packets %" PRIu64 "  bad messages %" PRIu64,
         fs.heartbeats, fs.duplicate_packets, fs.bad_packets, fs.bad_messages);
  r.line("loss: simulated drops %" PRIu64 "  kernel socket drops %" PRIu64 "  ring-full events %" PRIu64
         "  queue-full waits %" PRIu64,
         fs.sim_dropped, fs.kernel_drops, fs.ring_full, fs.queue_full_waits);
  r.line("gaps %" PRIu64 "  (messages missed on the wire %" PRIu64 ")  -> retransmissions %" PRIu64 " (%" PRIu64
         " msgs)  snapshots %" PRIu64 " (%" PRIu64 " msgs, book resets %" PRIu64 ")",
         fs.gaps, fs.missed_msgs, fs.retransmissions, fs.retransmitted_msgs, fs.snapshots, fs.snapshot_msgs,
         book.resets());
  r.line("recovery buffer: peak %.1f MB, packets lost to its cap %" PRIu64, double(fs.pending_peak_bytes) / 1e6,
         fs.pending_overflow);
  const std::size_t shown = std::min<std::size_t>(fs.recoveries.size(), 40);
  for (std::size_t i = 0; i < shown; ++i) {
    const RecoveryRecord& x = fs.recoveries[i];
    r.line("  #%-4zu %s expected %-10" PRIu64 " saw %-10" PRIu64 " resume@%-10" PRIu64 " %8" PRIu64
           " msgs %7.2f MB  first byte %7.2f ms  done %7.2f ms  buffered pkts %-7" PRIu64 " tries %d",
           i + 1, x.kind == 'R' ? "retransmit" : "snapshot  ", x.expected, x.seen, x.resume_seq, x.msgs,
           double(x.bytes) / 1e6, x.request_ms, x.total_ms, x.buffered_packets, x.attempts);
  }
  if (fs.recoveries.size() > shown) r.line("  ... %zu more", fs.recoveries.size() - shown);
  r.line("book: adds %" PRIu64 " execs %" PRIu64 " execs w/price %" PRIu64 " cancels %" PRIu64 " deletes %" PRIu64
         " replaces %" PRIu64,
         bs.adds, bs.executes, bs.executes_with_price, bs.cancels, bs.deletes, bs.replaces);
  r.line("book consistency: unknown order refs %" PRIu64 "  over-reductions %" PRIu64 "  add rejects %" PRIu64
         "  locate mismatches %" PRIu64 "   (all should be 0)",
         bs.unknown_refs, bs.over_reductions, bs.add_rejects, bs.locate_mismatches);
  r.line("%s", "");
  r.line("latency per stage, ns (live messages only; snapshot and replayed-after-recovery excluded: %" PRIu64
         " replayed)",
         book.replayed());
  if (book.skewed())
    r.line("  (%" PRIu64 " cross-core TSC readings went backwards and were clamped to 0)", book.skewed());
  r.line("  %-26s %12s %8s %8s %8s %9s %10s %9s", "stage", "count", "p50", "p90", "p99", "p99.9", "max", "mean");
  const StageHists& h = book.hists();
  hist_row(r, "1 wire -> user (kernel)", h.wire);
  hist_row(r, "2 user -> queued (parse)", h.parse);
  hist_row(r, "3 queue hop (feed->book)", h.queue);
  hist_row(r, "4 book apply", h.book);
  hist_row(r, "  user -> book (2+3+4)", h.user_total);
  hist_row(r, "= WIRE -> BOOK (1..4)", h.total);
  r.line("  book apply by message type:");
  const char* names[6] = {"    add (A/F)", "    execute (E)", "    execute w/ price (C)", "    cancel (X)",
                          "    delete (D)", "    replace (U)"};
  for (int i = 0; i < 6; ++i) hist_row(r, names[i], h.by_type[i]);

  const snap::Digest& d = book.digest();
  r.line("%s", "");
  r.line("final books: %" PRIu64 " instruments, %" PRIu64 " resting orders, digest %016" PRIx64, d.books, d.orders,
         d.value());
  int rc = 0;
  if (fs.end_of_session && !o.flag("no-digest")) {
    int fd = net::tcp_connect_blocking(o.str("snap-ip"), int(o.num("snap-port")));
    std::uint8_t req[snap::kRequestLen], resp[snap::kDigestLen];
    snap::encode_request(req, 'D', fs.final_seq);
    if (fd >= 0 && net::write_all(fd, req, sizeof req) && net::read_all(fd, resp, sizeof resp)) {
      std::uint64_t sseq = itch::be64(resp + 1), sd = itch::be64(resp + 9), so = itch::be64(resp + 17);
      bool ok = sseq == fs.final_seq && sd == d.value() && so == d.orders;
      r.line("server books: digest %016" PRIx64 " at seq<%" PRIu64 " (%" PRIu64 " orders)  ->  %s", sd, sseq, so,
             ok ? "MATCH: books identical to the publisher's" : "MISMATCH");
      rc = ok ? 0 : 1;
    } else {
      r.line("server digest: could not reach the snapshot server");
      rc = 1;
    }
    if (fd >= 0) ::close(fd);
  }
  std::printf("%s", r.text.c_str());
  if (!o.str("report").empty()) {
    if (FILE* f = std::fopen(o.str("report").c_str(), "w")) {
      std::fputs(r.text.c_str(), f);
      std::fclose(f);
    }
  }
  return rc;
}
