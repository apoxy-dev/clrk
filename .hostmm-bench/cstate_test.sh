#!/usr/bin/env bash
# Decisive C-state toggle on the amd64 metal: does deep idle cause the ~2ms?
set -u
cd "$HOME"
sumcsv() { python3 -c "
import statistics as st,sys
xs=sorted(float(l) for l in open(sys.argv[1]) if l.strip() and not l.startswith('#'))
print('%-26s p50=%.3f p5=%.3f p95=%.3f n=%d'%(sys.argv[2], st.median(xs), xs[len(xs)//20], xs[len(xs)*19//20], len(xs)))
" "$1" "$2"; }

echo "states:"; for s in /sys/devices/system/cpu/cpu0/cpuidle/state*; do echo "  $(basename $s) $(cat $s/name) lat=$(cat $s/latency)us"; done

# Baseline (all C-states enabled)
./rcubench global 500 2>/dev/null > results/cstate_all.csv

# Disable deep states C1E(2) + C6(3) on all CPUs
for s in 2 3; do for f in /sys/devices/system/cpu/cpu*/cpuidle/state$s/disable; do echo 1 | sudo tee "$f" >/dev/null; done; done
./rcubench global 500 2>/dev/null > results/cstate_noDeep.csv

# Also disable C1(1): only POLL left -> cores busy-spin in idle, never halt
for f in /sys/devices/system/cpu/cpu*/cpuidle/state1/disable; do echo 1 | sudo tee "$f" >/dev/null; done
./rcubench global 500 2>/dev/null > results/cstate_pollonly.csv

# Re-enable everything
for s in 1 2 3; do for f in /sys/devices/system/cpu/cpu*/cpuidle/state$s/disable; do echo 0 | sudo tee "$f" >/dev/null; done; done

echo "--- results ---"
sumcsv results/cstate_all.csv      "all_cstates(baseline)"
sumcsv results/cstate_noDeep.csv   "no_C1E_C6"
sumcsv results/cstate_pollonly.csv "POLL_only(no_halt)"
echo "CSTATE TEST DONE"
