#!/usr/bin/env bash
# inittrace.sh — E1/E2/E4 instrument: measure hostmm.init clock (ms) via
# GODEBUG=inittrace=1 in the bench *parent* process (whose import graph
# includes pkg/sentry/hostmm via maincli), at a set of GOMAXPROCS values,
# under a fixed RCU grace-period mode. Emits CSV to stdout.
#
# Usage: inittrace.sh <normal|expedited> <N_reps> <gp1 gp2 ...>
set -uo pipefail
BIN="$HOME/bench-runsc"
RCU="${1:?normal|expedited}"; N="${2:?reps}"; shift 2; GPS="$*"

if [ "$RCU" = expedited ]; then echo 1 | sudo tee /sys/kernel/rcu_expedited >/dev/null
else echo 0 | sudo tee /sys/kernel/rcu_expedited >/dev/null; fi
EFF=$(cat /sys/kernel/rcu_expedited 2>/dev/null)

echo "gp,rcu,rcu_eff,rep,hostmm_ms,threads,gomaxprocs"
for gp in $GPS; do
  for rep in $(seq 1 "$N"); do
    out=$(GODEBUG=inittrace=1 GOMAXPROCS="$gp" "$BIN" -probe 2>&1)
    hm=$(printf '%s\n' "$out" | grep 'sentry/hostmm @' | sed -E 's/.*ms, ([0-9.]+) ms clock.*/\1/' | head -1)
    th=$(printf '%s\n' "$out" | grep -oE 'Threads=[0-9]+' | head -1 | cut -d= -f2)
    gm=$(printf '%s\n' "$out" | grep -oE 'GOMAXPROCS=[0-9]+' | head -1 | cut -d= -f2)
    echo "$gp,$RCU,$EFF,$rep,${hm:-NA},${th:-NA},${gm:-NA}"
  done
done
