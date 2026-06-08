#!/usr/bin/env bash
# coreboot_sweep.sh — CORE_BOOT (runsc create+start) wall time across a sweep of
# the Sentry's GOMAXPROCS, under a fixed RCU mode. gVisor derives the Sentry's
# GOMAXPROCS from the visible-CPU count, so we drive GP by the taskset range
# (gp=1 -> cpu 0, gp=4 -> 0-3, gp=64 -> 0-63). nice -n -20 trims scheduler noise;
# we deliberately avoid chrt --fifo so the gp=1 (single-core) cell isn't starved
# by RT priority. Emits CSV: gp,pin,rcu,min,p50,mean,p95,max  (ms).
set -uo pipefail
BIN="${BENCH_BIN:-$HOME/bench-runsc}"
RCU="${1:?normal|expedited}"; ITERS="${2:?}"; WARM="${3:?}"; shift 3; GPS="$*"
if [ "$RCU" = expedited ]; then echo 1 | sudo tee /sys/kernel/rcu_expedited >/dev/null
else echo 0 | sudo tee /sys/kernel/rcu_expedited >/dev/null; fi
gp2pin() { if [ "$1" -le 1 ]; then echo 0; else echo "0-$(($1-1))"; fi; }
echo "gp,pin,rcu,min,p50,mean,p95,max"
for gp in $GPS; do
  pin=$(gp2pin "$gp")
  out=$(sudo env GOMAXPROCS="$gp" nice -n -20 taskset -c "$pin" \
        "$BIN" --iters "$ITERS" --warmup "$WARM" --platform systrap --quiet 2>&1)
  row=$(printf '%s\n' "$out" | awk '/^CORE_BOOT/{gsub("ms","",$0); print $2","$3","$4","$5","$6}')
  echo "${gp},${pin},${RCU},${row:-NA,NA,NA,NA,NA}"
done
