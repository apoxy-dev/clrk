#!/usr/bin/env bash
# stageA_host.sh <N> "<gp list>"  — runs inittrace for both RCU modes, prints combined CSV.
set -uo pipefail
N="${1:-40}"; GPS="${2:-1 2 4 8 16 32 64}"
mkdir -p ~/results
bash ~/inittrace.sh normal    "$N" $GPS > ~/results/it_n.csv 2>~/results/it_n.err
bash ~/inittrace.sh expedited "$N" $GPS > ~/results/it_e.csv 2>~/results/it_e.err
echo 0 | sudo tee /sys/kernel/rcu_expedited >/dev/null
cat ~/results/it_n.csv
tail -n +2 ~/results/it_e.csv
