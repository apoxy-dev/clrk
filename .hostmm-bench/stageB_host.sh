#!/usr/bin/env bash
# stageB_host.sh "<gp sweep list>"
#   E2: decisive CORE_BOOT A/B at GP=4 (taskset 0-3), 50 iters, normal vs expedited.
#   E4: CORE_BOOT sweep across GP (taskset-driven), 20 iters, normal + expedited.
set -uo pipefail
SWEEP="${1:-1 2 4}"
mkdir -p ~/results
{
  echo "## E2 coreboot GP=4 normal"
  bash ~/coreboot_sweep.sh normal 50 5 4
  echo "## E2 coreboot GP=4 expedited"
  bash ~/coreboot_sweep.sh expedited 50 5 4
} > ~/results/e2_coreboot.csv 2>~/results/e2.err
{
  echo "## E4 coreboot sweep normal"
  bash ~/coreboot_sweep.sh normal 20 5 $SWEEP
  echo "## E4 coreboot sweep expedited"
  bash ~/coreboot_sweep.sh expedited 20 5 $SWEEP
} > ~/results/e4_coreboot.csv 2>~/results/e4.err
echo 0 | sudo tee /sys/kernel/rcu_expedited >/dev/null
echo "STAGE B DONE"
echo "=== e2_coreboot.csv ==="; cat ~/results/e2_coreboot.csv
echo "=== e4_coreboot.csv ==="; cat ~/results/e4_coreboot.csv
