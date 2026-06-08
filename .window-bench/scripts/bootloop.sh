#!/usr/bin/env bash
# bootloop.sh -- the CORE_BOOT workload to be measured by the PMU top-down scripts (Test 1).
#
# Runs the bench-runsc spike in a tight create+start+delete loop, pinned to cores 0-3, with the
# Sentry's GOMAXPROCS forced to 4 by taskset -c 0-3. The PMU scripts wrap this under a fixed
# timeout and count events system-wide on the (idle) box, so essentially all attributed cycles
# belong to the boot work running on cores 0-3.
#
# This script is the measured subject only: it deliberately does NOT call perf/toplev itself.
# It self-bounds nothing except via the caller's `timeout`; the caller owns the wall-clock cap.
#
# Env knobs (all optional):
#   N         number of timed iterations (default 200)
#   WARMUP    warmup iterations excluded from bench-runsc stats (default 5)
#   PLATFORM  runsc platform (default systrap)
#   BIN       path to the bench-runsc binary (default ~/bench-runsc)
#   CORES     taskset cpu list (default 0-3)
set -uo pipefail

N="${N:-200}"
WARMUP="${WARMUP:-5}"
PLATFORM="${PLATFORM:-systrap}"
BIN="${BIN:-$HOME/bench-runsc}"
CORES="${CORES:-0-3}"

if [ ! -x "$BIN" ]; then
  echo "bootloop: bench-runsc not found or not executable at $BIN" >&2
  exit 127
fi

# Quiet per-iteration logs to keep the loop tight; the PMU counters, not stdout, are the signal.
# bench-runsc self-execs as runsc and runs create+start (CORE_BOOT) + delete per iteration.
exec taskset -c "$CORES" "$BIN" \
  -platform="$PLATFORM" \
  -iters "$N" \
  -warmup "$WARMUP" \
  -quiet
