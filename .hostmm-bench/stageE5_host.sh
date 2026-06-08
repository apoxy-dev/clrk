#!/usr/bin/env bash
# stageE5_host.sh "<coreboot gp cells>" — apply the hostmm lazy-init patch,
# rebuild bench-runsc-patched against a local patched gvisor, then re-measure:
#   (a) inittrace hostmm.init for the patched binary (should be ~0),
#   (b) CORE_BOOT at the given GP cells, patched vs baseline (normal rcu),
# to show the registration floor disappears on systrap with no boot breakage.
set -uo pipefail
export PATH=/usr/local/go/bin:$PATH GOFLAGS=-mod=mod
CELLS="${1:-4}"
mkdir -p ~/results
echo 0 | sudo tee /sys/kernel/rcu_expedited >/dev/null

echo "### extract gvisor + apply patch"
rm -rf ~/gvisor && mkdir -p ~/gvisor
tar -xzf ~/gvisor-src.tar.gz -C ~/gvisor
cp ~/membarrier_patched.go ~/gvisor/pkg/sentry/hostmm/membarrier.go

echo "### point clrk go.mod replace at local patched gvisor + rebuild"
cd ~/clrk
cp go.mod /tmp/go.mod.bak
sed -i 's|^replace gvisor.dev/gvisor => github.com/apoxy-dev/gvisor.*|replace gvisor.dev/gvisor => /home/ubuntu/gvisor|' go.mod
if CGO_ENABLED=1 go build -o ~/bench-runsc-patched ./cmd/bench-runsc 2>~/results/e5_build.err; then
  echo "E5 BUILD OK: $(stat -c %s ~/bench-runsc-patched) bytes"
else
  echo "E5 BUILD FAILED"; tail -25 ~/results/e5_build.err; cp /tmp/go.mod.bak go.mod; exit 1
fi
cp /tmp/go.mod.bak go.mod   # restore go.mod (keep baseline binary buildable)

echo "### (a) patched inittrace hostmm (GOMAXPROCS=4, N=20)"
echo "gp,rcu,rcu_eff,rep,hostmm_ms,threads,gomaxprocs" > ~/results/e5_inittrace.csv
for rep in $(seq 1 20); do
  out=$(GODEBUG=inittrace=1 GOMAXPROCS=4 ~/bench-runsc-patched -probe 2>&1)
  hm=$(printf '%s\n' "$out" | grep 'sentry/hostmm @' | sed -E 's/.*ms, ([0-9.]+) ms clock.*/\1/' | head -1)
  th=$(printf '%s\n' "$out" | grep -oE 'Threads=[0-9]+' | head -1 | cut -d= -f2)
  echo "4,patched,0,$rep,${hm:-NA},${th:-NA},4" >> ~/results/e5_inittrace.csv
done

echo "### (b) CORE_BOOT baseline vs patched (normal rcu), cells: $CELLS"
{
  echo "## baseline"
  BENCH_BIN=$HOME/bench-runsc         bash ~/coreboot_sweep.sh normal 30 5 $CELLS
  echo "## patched"
  BENCH_BIN=$HOME/bench-runsc-patched bash ~/coreboot_sweep.sh normal 30 5 $CELLS
} > ~/results/e5_coreboot.csv 2>~/results/e5_coreboot.err
echo 0 | sudo tee /sys/kernel/rcu_expedited >/dev/null
echo "STAGE E5 DONE"
echo "=== e5_inittrace.csv ==="; cat ~/results/e5_inittrace.csv
echo "=== e5_coreboot.csv ==="; cat ~/results/e5_coreboot.csv
