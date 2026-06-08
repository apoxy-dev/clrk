#!/usr/bin/env bash
# e3_kernel.sh — attribute the membarrier-register cost inside the kernel.
# Runs the bench parent (one REGISTER_PRIVATE_EXPEDITED per launch) in a loop
# while tracing: (a) membarrier syscall latency by cmd via the arch-independent
# syscalls:*_membarrier tracepoints, (b) synchronize_rcu latency, and
# (c) sync_runqueues_membarrier_state latency. If most of the register cost
# sits in synchronize_rcu, the RCU-grace-period mechanism is confirmed directly.
#
# Usage: e3_kernel.sh <normal|expedited> <gp> <launches>
set -uo pipefail
BIN="$HOME/bench-runsc"
RCU="${1:-normal}"; GP="${2:-4}"; LAUNCHES="${3:-60}"
if [ "$RCU" = expedited ]; then echo 1 | sudo tee /sys/kernel/rcu_expedited >/dev/null
else echo 0 | sudo tee /sys/kernel/rcu_expedited >/dev/null; fi
echo "## rcu_expedited=$(cat /sys/kernel/rcu_expedited) gp=$GP launches=$LAUNCHES"

# (a) membarrier syscall latency histogram by cmd (16 = REGISTER_PRIVATE_EXPEDITED).
sudo timeout 60 bpftrace -e '
tracepoint:syscalls:sys_enter_membarrier { @s[tid]=nsecs; @c[tid]=args.cmd; }
tracepoint:syscalls:sys_exit_membarrier /@s[tid]/ {
  @us_by_cmd[@c[tid]] = hist((nsecs-@s[tid])/1000);
  delete(@s[tid]); delete(@c[tid]);
}' >/tmp/e3_membarrier.txt 2>/tmp/e3_membarrier.err &
BPID=$!
sleep 2

# (b)+(c) kernel function latency (us) for the suspected dominant term + register body.
sudo timeout 60 funclatency-bpfcc -u -d 55 synchronize_rcu >/tmp/e3_syncrcu.txt 2>/tmp/e3_syncrcu.err &
sudo timeout 60 funclatency-bpfcc -u -d 55 sync_runqueues_membarrier_state >/tmp/e3_syncrq.txt 2>/tmp/e3_syncrq.err &

sleep 2
for i in $(seq 1 "$LAUNCHES"); do GOMAXPROCS="$GP" "$BIN" -probe >/dev/null 2>&1; done
wait
echo "=== (a) membarrier syscall latency by cmd (us) ==="; cat /tmp/e3_membarrier.txt
echo "=== (b) synchronize_rcu latency (us) ==="; cat /tmp/e3_syncrcu.txt
echo "=== (c) sync_runqueues_membarrier_state latency (us) ==="; cat /tmp/e3_syncrq.txt
echo "E3 DONE"
