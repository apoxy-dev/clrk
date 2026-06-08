#!/usr/bin/env bash
# coreboot.sh — full runsc cold-boot (CORE_BOOT = runsc create + start) at a
# given GOMAXPROCS and RCU mode. Pins to 4 cores + RT priority as in the runbook
# so the only moving variable is the one under test. Prints the bench AGGREGATE
# table prefixed with the cell label.
#
# Usage: coreboot.sh <normal|expedited> <gp> <iters> <warmup> <pin cpus e.g. 0-3> <label>
set -uo pipefail
BIN="$HOME/bench-runsc"
RCU="${1:?}"; GP="${2:?}"; ITERS="${3:?}"; WARM="${4:?}"; PIN="${5:?}"; LABEL="${6:?}"

if [ "$RCU" = expedited ]; then echo 1 | sudo tee /sys/kernel/rcu_expedited >/dev/null
else echo 0 | sudo tee /sys/kernel/rcu_expedited >/dev/null; fi

# bench needs root (cgroup + mounts). Set GOMAXPROCS in the env so both the
# bench parent AND the forked runsc/Sentry (it passes os.Environ to children)
# inherit it. chrt/nice/taskset cut scheduler noise.
sudo env GOMAXPROCS="$GP" \
  nice -n -20 chrt --fifo 50 taskset -c "$PIN" \
  "$BIN" --iters "$ITERS" --warmup "$WARM" --platform systrap --quiet 2>&1 \
  | sed "s/^/[$LABEL gp=$GP rcu=$RCU] /"
