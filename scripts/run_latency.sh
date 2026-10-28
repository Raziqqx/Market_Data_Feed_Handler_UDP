#!/usr/bin/env bash
# Latency breakdown under different loads and polling modes (no deliberate loss).
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

cmake -S "$ROOT" -B "$BIN" -DCMAKE_BUILD_TYPE=Release > /dev/null
cmake --build "$BIN" -j > /dev/null

for rate in 100000 500000 1000000 2000000; do
  run_case "latency_busy_${rate}" "--count 5000000 --rate $rate" "--prefault" || true
done
run_case "latency_blocking_500000" "--count 5000000 --rate 500000" "--prefault --blocking" || true
run_case "latency_busy_nopf_500000" "--count 5000000 --rate 500000" "" || true

"$BIN/queue_bench" | tee "$OUT/queue_bench.txt"
