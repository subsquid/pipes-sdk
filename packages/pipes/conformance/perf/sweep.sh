#!/usr/bin/env bash
# The CT-6 fork sweep: the knee per sink, the knee with a distant store, rollback cost by fork depth
# and by rows per block. Run from packages/pipes; JSON results go to $OUT (default ./perf-results).
set -uo pipefail

OUT=${OUT:-perf-results}
SECONDS_PER_POINT=${SECONDS_PER_POINT:-20}
mkdir -p "$OUT"

bench() {
  local name=$1
  shift
  echo "=== $name: $*"
  node --import tsx conformance/perf/run.ts --seconds "$SECONDS_PER_POINT" --json "$OUT/$name.json" "$@"
  echo
}

# A: the knee, store on loopback.
bench a-memory --sink memory --rows 10 --fork-every 20 --rates 1000,2000,5000 --stop-on-fail
for sink in postgres clickhouse; do
  bench "a-$sink" --sink $sink --rows 10 --fork-every 20 --rates 25,50,100,200,500,1000,2000 --stop-on-fail
done

# B: the knee with the store a few milliseconds away.
for rtt in 2 10; do
  for sink in postgres clickhouse; do
    bench "b-$sink-rtt$rtt" --sink $sink --rows 10 --fork-every 20 --db-rtt $rtt \
      --rates 5,10,25,50,100,200,500 --stop-on-fail
  done
done

# C: rollback cost by fork depth. A 409 carries enough blocks that every depth resolves in one round.
for depth in 1 4 16 30; do
  for sink in postgres clickhouse; do
    bench "c-$sink-depth$depth" --sink $sink --rows 10 --rates 20 --fork-every 40 --fork-depth $depth \
      --previous-blocks 64
  done
done

# D: rollback cost by rows per block.
for rows in 1 10 100 1000; do
  for sink in postgres clickhouse; do
    bench "d-$sink-rows$rows" --sink $sink --rows $rows --rates 20 --fork-every 20 --fork-depth 3
  done
done
