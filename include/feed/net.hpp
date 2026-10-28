#pragma once
// Thin, explicit wrappers over the BSD socket calls we need. Every socket is non-blocking:
// the threads that own them are event loops (epoll) and must never sleep inside a syscall.

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <string>

namespace feed::net {

[[noreturn]] inline void die(const std::string& what) {
  throw std::runtime_error(what + ": " + std::strerror(errno));
}

inline void set_nonblocking(int fd) {
  int fl = ::fcntl(fd, F_GETFL, 0);
  if (fl < 0 || ::fcntl(fd, F_SETFL, fl | O_NONBLOCK) < 0) die("fcntl(O_NONBLOCK)");
}

inline in_addr parse_ip(const std::string& s) {
  in_addr a{};
  if (::inet_pton(AF_INET, s.c_str(), &a) != 1) throw std::runtime_error("bad IPv4 address: " + s);
  return a;
}

inline sockaddr_in endpoint(const std::string& ip, int port) {
  sockaddr_in sa{};
  sa.sin_family = AF_INET;
  sa.sin_port = htons(std::uint16_t(port));
  sa.sin_addr = parse_ip(ip);
  return sa;
}

// Socket buffer sizes are capped by net.core.{r,w}mem_max unless we are root (the *FORCE
// variants need CAP_NET_ADMIN). Try FORCE first, fall back silently, report what we got.
inline int set_buffer(int fd, int opt, int force_opt, int bytes) {
  if (::setsockopt(fd, SOL_SOCKET, force_opt, &bytes, sizeof bytes) != 0)
    ::setsockopt(fd, SOL_SOCKET, opt, &bytes, sizeof bytes);
  int got = 0;
  socklen_t len = sizeof got;
  ::getsockopt(fd, SOL_SOCKET, opt, &got, &len);
  return got;  // the kernel doubles the requested value for bookkeeping overhead
}

// Multicast sender. `iface` picks the outgoing interface; loopback (TTL 0, LOOP on) keeps the
// traffic on this machine, which is what we want for a lab.
inline int multicast_sender(const std::string& iface, int ttl, int sndbuf) {
  int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
  if (fd < 0) die("socket(UDP)");
  in_addr ifa = parse_ip(iface);
  if (::setsockopt(fd, IPPROTO_IP, IP_MULTICAST_IF, &ifa, sizeof ifa) != 0) die("IP_MULTICAST_IF");
  unsigned char loop = 1, t = (unsigned char)ttl;
  ::setsockopt(fd, IPPROTO_IP, IP_MULTICAST_LOOP, &loop, sizeof loop);
  ::setsockopt(fd, IPPROTO_IP, IP_MULTICAST_TTL, &t, sizeof t);
  if (sndbuf > 0) set_buffer(fd, SO_SNDBUF, SO_SNDBUFFORCE, sndbuf);
  set_nonblocking(fd);
  return fd;
}

// Multicast receiver: bind to the group address (so we only see this group's traffic), join on
// the given interface, ask the kernel for a receive timestamp (SO_TIMESTAMPNS) and for a running
// count of datagrams it dropped because our buffer was full (SO_RXQ_OVFL).
inline int multicast_receiver(const std::string& group, int port, const std::string& iface, int rcvbuf,
                              int* rcvbuf_got = nullptr) {
  int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
  if (fd < 0) die("socket(UDP)");
  int one = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
  sockaddr_in sa = endpoint(group, port);
  if (::bind(fd, reinterpret_cast<sockaddr*>(&sa), sizeof sa) != 0) die("bind(" + group + ")");
  ip_mreq mreq{};
  mreq.imr_multiaddr = parse_ip(group);
  mreq.imr_interface = parse_ip(iface);
  if (::setsockopt(fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof mreq) != 0) die("IP_ADD_MEMBERSHIP");
  ::setsockopt(fd, SOL_SOCKET, SO_TIMESTAMPNS, &one, sizeof one);
  ::setsockopt(fd, SOL_SOCKET, SO_RXQ_OVFL, &one, sizeof one);
  int got = rcvbuf > 0 ? set_buffer(fd, SO_RCVBUF, SO_RCVBUFFORCE, rcvbuf) : 0;
  if (rcvbuf_got) *rcvbuf_got = got;
  set_nonblocking(fd);
  return fd;
}

inline void tcp_tune(int fd) {
  int one = 1;
  // Requests are tiny and latency-sensitive: do not let Nagle hold them back.
  ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
}

inline int tcp_listen(const std::string& ip, int port) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) die("socket(TCP)");
  int one = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
  sockaddr_in sa = endpoint(ip, port);
  if (::bind(fd, reinterpret_cast<sockaddr*>(&sa), sizeof sa) != 0) die("bind(tcp " + std::to_string(port) + ")");
  if (::listen(fd, 16) != 0) die("listen");
  set_nonblocking(fd);
  return fd;
}

// Non-blocking connect: returns the fd immediately; completion is signalled by EPOLLOUT, after
// which SO_ERROR tells whether it worked.
inline int tcp_connect_nb(const std::string& ip, int port) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) die("socket(TCP)");
  set_nonblocking(fd);
  tcp_tune(fd);
  sockaddr_in sa = endpoint(ip, port);
  if (::connect(fd, reinterpret_cast<sockaddr*>(&sa), sizeof sa) != 0 && errno != EINPROGRESS) {
    ::close(fd);
    return -1;
  }
  return fd;
}

inline int socket_error(int fd) {
  int err = 0;
  socklen_t len = sizeof err;
  ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
  return err;
}

// Blocking helpers, used only off the hot path (end-of-run digest check).
inline int tcp_connect_blocking(const std::string& ip, int port) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) die("socket(TCP)");
  tcp_tune(fd);
  sockaddr_in sa = endpoint(ip, port);
  if (::connect(fd, reinterpret_cast<sockaddr*>(&sa), sizeof sa) != 0) {
    ::close(fd);
    return -1;
  }
  return fd;
}
inline bool write_all(int fd, const void* p, std::size_t n) {
  const char* c = static_cast<const char*>(p);
  while (n) {
    ssize_t w = ::send(fd, c, n, MSG_NOSIGNAL);
    if (w < 0 && errno == EINTR) continue;
    if (w <= 0) return false;
    c += w;
    n -= std::size_t(w);
  }
  return true;
}
inline bool read_all(int fd, void* p, std::size_t n) {
  char* c = static_cast<char*>(p);
  while (n) {
    ssize_t r = ::recv(fd, c, n, 0);
    if (r < 0 && errno == EINTR) continue;
    if (r <= 0) return false;
    c += r;
    n -= std::size_t(r);
  }
  return true;
}

// RAII epoll instance.
class Epoll {
 public:
  Epoll() : fd_(::epoll_create1(EPOLL_CLOEXEC)) {
    if (fd_ < 0) die("epoll_create1");
  }
  ~Epoll() { ::close(fd_); }
  Epoll(const Epoll&) = delete;
  Epoll& operator=(const Epoll&) = delete;

  void add(int fd, std::uint32_t events) { ctl(EPOLL_CTL_ADD, fd, events); }
  void mod(int fd, std::uint32_t events) { ctl(EPOLL_CTL_MOD, fd, events); }
  void del(int fd) { ::epoll_ctl(fd_, EPOLL_CTL_DEL, fd, nullptr); }
  int wait(epoll_event* evs, int max, int timeout_ms) {
    int n = ::epoll_wait(fd_, evs, max, timeout_ms);
    return n < 0 ? (errno == EINTR ? 0 : -1) : n;
  }

 private:
  void ctl(int op, int fd, std::uint32_t events) {
    epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    if (::epoll_ctl(fd_, op, fd, &ev) != 0) die("epoll_ctl");
  }
  int fd_;
};

}  // namespace feed::net
