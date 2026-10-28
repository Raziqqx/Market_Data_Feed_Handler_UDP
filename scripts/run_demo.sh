#!/usr/bin/env bash
# The "done when" demo: deliberately drop packets and show the handler recovering. Every run
# ends with the subscriber asking the server for a digest of its books: MATCH means the
# subscriber's books are identical (every order, every queue position) to the publisher's.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

cmake -S "$ROOT" -B "$BIN" -DCMAKE_BUILD_TYPE=Release > /dev/null
cmake --build "$BIN" -j > /dev/null
"$BIN/feed_tests" | tail -1 | tee "$OUT/tests.txt"

# 1. Clean run: no loss. Baseline latency breakdown.
run_case baseline "--count 3000000 --rate 500000" "--prefault"

# 2. Publisher drops 3 consecutive packets every 250k packets. Snapshot-only recovery
#    (--retransmit-max 0): every gap rebuilds the books from a TCP snapshot.
run_case drop_snapshot "--count 3000000 --rate 500000 --drop-every 250000 --drop-burst 3" \
         "--prefault --retransmit-max 0"

# 3. Same drops, default two-tier recovery: small gaps are filled by retransmission.
run_case drop_retransmit "--count 3000000 --rate 500000 --drop-every 250000 --drop-burst 3" "--prefault"

# 4. Random receive-side loss (1 packet in 20k), as a flaky NIC or switch would cause.
run_case drop_random "--count 3000000 --rate 500000" "--prefault --drop-prob 0.00005"

# 5. Late join mid-morning: the publisher fast-forwards 60M messages (to ~10:00) into the
#    snapshot server, so the handler's first packet is a 60M-message gap: it bootstraps from a
#    snapshot of ~1.7M live orders while buffering the live feed, then catches up.
run_case late_join "--skip 60000000 --count 3000000 --rate 500000" "--prefault"

# 6. Heavy loss (1 packet in 2k) on that big book, two-tier recovery.
run_case drop_heavy "--skip 60000000 --count 3000000 --rate 500000 --drop-prob 0.0005" "--prefault"

# 7. The same heavy loss with snapshot-only recovery: each 66 MB snapshot takes longer than the
#    time between losses, so the handler never gets back to live (it still ends correct).
run_case drop_heavy_snapshot_only "--skip 60000000 --count 3000000 --rate 500000 --drop-prob 0.0005" \
         "--prefault --retransmit-max 0"

echo "logs in $OUT"
