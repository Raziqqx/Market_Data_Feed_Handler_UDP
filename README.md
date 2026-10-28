# Market Data Feed Handler over UDP Multicast (C++17)

A publisher replays a full day of real NASDAQ TotalView-ITCH 5.0 data as **MoldUDP64 packets over UDP multicast**, at a controllable speed. A feed handler receives it with **epoll and non-blocking sockets**, parses it **zero-copy**, **detects gaps by sequence number** and **recovers over TCP** from a snapshot server. It passes every update through a **lock-free SPSC queue** to the order book on its own **pinned thread**, and reports **wire-to-book latency per stage**.

```
 feed_publisher                                          feed_handler
 ┌──────────────────────────────┐   UDP multicast      ┌───────────────────────────────────────┐
 │ publisher thread (cpu 6)     │   239.1.1.1:30001    │ feed thread (cpu 2)                   │
 │  mmap ITCH file              │   MoldUDP64 packets  │  epoll + recvmmsg into a packet ring  │
 │  seq numbers, pacing, drops ─┼─────────────────────►│  SO_TIMESTAMPNS kernel rx timestamps  │
 │  sendmmsg                    │                      │  gap detection by sequence number     │
 │          │ SPSC queue        │                      │  zero-copy decode into queue slots    │
 │          ▼ (ptrs into mmap)  │   TCP 31001          │  recovery client (non-blocking TCP)   │
 │ snapshot thread (cpu 8)      │◄────────────────────►│        │ SPSC queue (64-byte updates)│
 │  P3 order books = the truth  │  'R' retransmit gap  │        ▼                              │
 │  history ring for retransmit │  'S' full snapshot   │ book thread (cpu 4)                   │
 │  epoll TCP server            │  'D' book digest     │  P3 order books + latency histograms  │
 └──────────────────────────────┘                      └───────────────────────────────────────┘
```

- **P2** (SPSC queue): `include/feed/spsc_queue.hpp`. **P3** (order book + ITCH decoder): vendored in `external/lob/`.
- **Recovery is two-tier, as on real exchange feeds.** A small gap is filled by a *retransmission* of just the missing messages. A large gap, a late join, or history the server no longer holds is recovered from a full *snapshot* of every book. Snapshot-only mode is `--retransmit-max 0`.
- **Proof of correctness:** at the end of every run the handler asks the server for a digest of its books (every order, in exact queue priority) and compares it with its own.

See **[NOTES.txt](NOTES.txt)** for the full write-up: every design decision and the reason for it, the problems hit while building this (and how each was diagnosed), how to read the numbers, and interview Q&A.

## "Done when": deliberate drops, recovery, latency breakdown

Data: NASDAQ ITCH 5.0, 2019-12-30. Replayed at 500k msg/s on loopback multicast. Machine: Intel i5-14500HX, WSL2 (Ubuntu 22.04, kernel 6.6). Raw logs are in [`results/`](results/).

| Scenario (`scripts/run_demo.sh`) | Loss | Recovery | Final books vs publisher |
|---|---|---|---|
| baseline, 3M msgs | none | none | **MATCH** |
| publisher drops 3 packets every 250k, snapshot-only | 11 gaps | 11 snapshots, 1.8–8.8 ms each | **MATCH** |
| same drops, two-tier | 11 gaps | 11 retransmissions, ~0.35 ms each | **MATCH** |
| random receive loss 1/20k | 136 gaps | 136 retransmissions | **MATCH** |
| late join at ~10:00 (1.7M live orders) | 60M-msg gap | 66 MB snapshot in 0.82 s while buffering 366k live packets, then catch-up | **MATCH** |
| heavy loss 1/2k on that big book, two-tier | 1402 gaps | 1401 retransmissions + 1 snapshot, stays live | **MATCH** |
| heavy loss, snapshot-only | 15 gaps | each snapshot outlasts the time to the next loss: never live again | **MATCH** (at the end) |

Latency per stage, two-tier drop run (`results/drop_retransmit.handler.txt`), live messages only, ns:

| Stage | p50 | p90 | p99 | p99.9 |
|---|---|---|---|---|
| 1 wire → user (kernel rx timestamp → `recvmmsg` returned) | 1520 | 1744 | 12928 | 72704 |
| 2 user → queued (parse + decode into queue slot) | 134 | 186 | 2784 | 8320 |
| 3 queue hop (feed thread → book thread) | 142 | 242 | 35328 | 153600 |
| 4 book apply (P3 order book) | 105 | 182 | 476 | 1456 |
| **wire → book (1+2+3+4)** | **1904** | **2528** | **48640** | **186368** |

Load and polling-mode sweep (5M messages, no loss, all MATCH), wire → book in ns:

| Run | p50 | p99 | p99.9 |
|---|---|---|---|
| busy poll, 100k msg/s | 2032 | 40448 | 614400 |
| busy poll, 1M msg/s | 2032 | 84992 | 454656 |
| busy poll, 2M msg/s | 2272 | 2015232 | 4259840 |
| blocking epoll, 500k msg/s | 5056 | 137216 | 323584 |
| busy poll without `--prefault`, 500k msg/s | 1936 | 3964928 | 42467328 |

SPSC queue on its own (`queue_bench`): ~89 M updates/s, one-way hand-off p50 138 ns, p99 186 ns.

The median is dominated by the kernel's loopback/softirq path (stage 1); the user-space path (2+3+4) is ~380 ns at p50. Tails come from queueing behind bursts and from the VM (WSL2 vCPUs are scheduled by the Windows host). Per-stage tables for every run are in `results/`, and NOTES.txt section 8 explains how to read them.

## Build, test, run

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j
./build/feed_tests                                  # unit tests
scripts/run_demo.sh                                 # drops + recovery scenarios -> results/
scripts/run_latency.sh                              # load / polling-mode sweep  -> results/

# by hand, two terminals:
./build/feed_handler --prefault
./build/feed_publisher ~/itch-data/12302019.NASDAQ_ITCH50 --count 3000000 --rate 500000 --drop-every 250000 --drop-burst 3
```

The ITCH file comes from NASDAQ (<https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/>). The P3 repo's `scripts/get_data.sh` downloads it. Set `ITCH=/path/to/file` for the scripts. Every option is listed by `--help`.

## Layout

```
include/feed/spsc_queue.hpp      P2: lock-free SPSC ring, cache-line separated indices, in-place claim/publish
include/feed/moldudp64.hpp       MoldUDP64 packet framing (sequence numbers, heartbeat, end of session)
include/feed/update.hpp          64-byte queue element, zero-copy ITCH decode, apply-to-book
include/feed/snapshot.hpp        TCP recovery protocol, snapshot serialisation, book digest
include/feed/pending_buffer.hpp  chunked FIFO that holds live packets during a recovery
include/feed/net.hpp             socket helpers (multicast, timestamps, non-blocking TCP, epoll)
include/feed/util.hpp            thread pinning, clocks, option parsing
src/publisher.cpp                feed_publisher: replay + snapshot/retransmission server
src/subscriber.cpp               feed_handler: feed thread, book thread, report
src/queue_bench.cpp              SPSC queue throughput and hand-off latency on its own
external/lob/                    P3 order book and ITCH decoder (vendored, 3 small additions marked)
tests/test_main.cpp              unit tests
scripts/                         run_demo.sh, run_latency.sh
results/                         raw output of the runs above
```
