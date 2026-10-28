#!/usr/bin/env bash
# Shared helpers: run one publisher/subscriber scenario and save both logs.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ITCH="${ITCH:-$HOME/itch-data/12302019.NASDAQ_ITCH50}"
OUT="${OUT:-$ROOT/results}"
BIN="$ROOT/build"
mkdir -p "$OUT"

[[ -f "$ITCH" ]] || { echo "ITCH file not found: $ITCH (set ITCH=/path/to/file)"; exit 1; }

# run_case NAME "publisher args" "subscriber args"
# Starts the subscriber, waits until it is ready, runs the publisher, waits for the report.
run_case() {
  local name="$1" pub_args="$2" sub_args="$3"
  local sub_log="$OUT/$name.handler.txt" pub_log="$OUT/$name.publisher.txt"
  echo "=== $name"
  echo "    publisher: $pub_args"
  echo "    handler:   $sub_args"
  # shellcheck disable=SC2086
  "$BIN/feed_handler" --idle-exit 5 $sub_args > "$sub_log" 2>&1 &
  local sub_pid=$!
  for _ in $(seq 1 300); do grep -q "waiting for packets" "$sub_log" 2>/dev/null && break; sleep 0.1; done
  # shellcheck disable=SC2086
  "$BIN/feed_publisher" "$ITCH" --linger 5 $pub_args > "$pub_log" 2>&1
  local rc=0
  wait "$sub_pid" || rc=$?
  grep -E "^gaps|^loss|MATCH|MISMATCH|^  = WIRE" "$sub_log" | sed 's/^/    /'
  return $rc
}
