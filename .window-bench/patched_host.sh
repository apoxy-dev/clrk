#!/usr/bin/env bash
# patched_host.sh -- build bench-runsc against the membarrier sync.Once patch, then
# re-measure CORE_BOOT with the RCU/membarrier tax removed. Runs on the host as ubuntu.
set -uo pipefail
export PATH=/usr/local/go/bin:$PATH GOFLAGS=-mod=mod
ARCH=$(dpkg --print-architecture)
mkdir -p ~/window/results

echo "### apply patch to a local gvisor + go.mod replace"
rm -rf ~/gvisor && mkdir -p ~/gvisor
tar -xzf ~/window/gvisor-src.tar.gz -C ~/gvisor
cp ~/window/membarrier_patched.go ~/gvisor/pkg/sentry/hostmm/membarrier.go
# clrk source (prewarm extracted to ~/clrk); point its gvisor replace at the local tree.
cd ~/clrk
if grep -q '^replace gvisor.dev/gvisor =>' go.mod; then
  sed -i 's|^replace gvisor.dev/gvisor => .*|replace gvisor.dev/gvisor => /home/ubuntu/gvisor|' go.mod
else
  printf '\nreplace gvisor.dev/gvisor => /home/ubuntu/gvisor\n' >> go.mod
fi
echo "replace line: $(grep '^replace gvisor.dev/gvisor =>' go.mod)"

echo "### build bench-runsc-patched (CGO)"
if CGO_ENABLED=1 go build -o ~/bench-runsc-patched ./cmd/bench-runsc 2>~/window/results/patched_build.err; then
  echo "PATCHED BUILD OK: $(stat -c%s ~/bench-runsc-patched) bytes"
else
  echo "PATCHED BUILD FAILED:"; tail -20 ~/window/results/patched_build.err; exit 1
fi

echo "### confirm hostmm.init is ~0 in the patched binary (-probe, GODEBUG=inittrace)"
GODEBUG=inittrace=1 GOMAXPROCS=4 ~/bench-runsc-patched -probe 2>&1 | grep -i 'sentry/hostmm @' | head -1 || echo "(no hostmm init line -- patched out, good)"

echo "### CORE_BOOT patched (normal RCU, taskset 0-3, 50 iters)"
echo 0 | sudo tee /sys/kernel/rcu_expedited >/dev/null 2>&1 || true
: > ~/window/results/coreboot_patched.jsonl
sudo taskset -c 0-3 nice -n -20 chrt --fifo 50 ~/bench-runsc-patched \
  -platform=systrap -iters 50 -warmup 5 -json ~/window/results/coreboot_patched.jsonl \
  > ~/window/results/coreboot_patched.stdout 2>&1
python3 - <<'PY'
import json
v=sorted(json.loads(l)["core_boot"]/1e6 for l in open("/home/ubuntu/window/results/coreboot_patched.jsonl")
         if l.strip() and not json.loads(l).get("warmup"))
if v:
    import statistics as st
    print("PATCHED CORE_BOOT p50=%.1fms p95=%.1fms n=%d" % (st.median(v), v[int(len(v)*0.95)-1], len(v)))
else:
    print("PATCHED CORE_BOOT: no rows")
PY

# --- Intel metal only: re-take the TMA on the patched boot loop ---
if [ "$ARCH" = amd64 ] && grep -qi GenuineIntel /proc/cpuinfo; then
  echo "### re-take toplev TMA + raw L3-miss-stall on the PATCHED boot (Intel metal)"
  TL=/root/pmu-tools/toplev.py; [ -f "$TL" ] || TL=~/pmu-tools/toplev.py
  sudo bash -c "cd ~ && timeout 120 python3 $TL -l3 -a --no-mux -x, -o /home/ubuntu/window/results/toplev_patched.csv -- taskset -c 0-3 /home/ubuntu/bench-runsc-patched -platform=systrap -iters 200 -warmup 5" >/dev/null 2>&1 || true
  sudo perf stat -a -e cycle_activity.stalls_l3_miss,cycle_activity.stalls_total,cycles,instructions \
    -- timeout 60 taskset -c 0-3 /home/ubuntu/bench-runsc-patched -platform=systrap -iters 200 -warmup 5 \
    2>/home/ubuntu/window/results/perfraw_patched.csv >/dev/null || true
  echo "--- patched toplev top-level ---"; grep -iE "^Frontend_Bound,|^Backend_Bound,|Core_Bound,|Memory_Bound" /home/ubuntu/window/results/toplev_patched.csv 2>/dev/null | head
  echo "--- patched raw ---"; grep -iE "stalls_l3_miss|insn per cycle|instructions" /home/ubuntu/window/results/perfraw_patched.csv 2>/dev/null | head
fi
echo "### PATCHED DONE ($ARCH)"
