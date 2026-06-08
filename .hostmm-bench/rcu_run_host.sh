#!/usr/bin/env bash
# Runs the full RCU experiment on one host. Writes ~/results/, drops ~/results/DONE.
set -u
cd "$HOME"; mkdir -p results
export DEBIAN_FRONTEND=noninteractive

echo "[install]"
sudo apt-get update -y >/dev/null 2>&1
sudo apt-get install -y build-essential bpfcc-tools bpftrace trace-cmd linux-headers-$(uname -r) >/dev/null 2>&1 || true

echo "[build]"
cc -O2 -o rcubench rcubench.c -lpthread 2>results/build.err && echo "BUILD OK" || { echo "BUILD FAIL"; cat results/build.err; exit 1; }

bash census.sh > results/census.txt 2>&1
grep -E 'rcu_gp_fqs|rcu_gp_init|force_quiescent_state|synchronize_rcu|rcu_gp_kthread' /proc/kallsyms 2>/dev/null | awk '{print $3}' | sort -u > results/rcu_syms.txt

NCPU=$(nproc)
echo "NCPU=$NCPU"

echo "[A: idle global synchronize_rcu, N=500]"
./rcubench global 500 > results/global_idle.csv 2>results/global_idle.err

echo "[B: funclatency synchronize_rcu (idle)]"
if command -v funclatency-bpfcc >/dev/null; then
  ( sudo funclatency-bpfcc -m -d 14 synchronize_rcu > results/funclat_sync_idle.txt 2>results/funclat.err ) &
  FPID=$!
  sleep 1; ./rcubench global 200 > /dev/null 2>&1
  wait $FPID 2>/dev/null || true
else echo "no funclatency-bpfcc" > results/funclat_sync_idle.txt; fi

echo "[C: FQS passes per grace period (idle)]"
SYMS=$(cat results/rcu_syms.txt 2>/dev/null)
echo "syms: $(echo $SYMS | tr '\n' ' ')" > results/fqs_idle.txt
# kprobe counts: grace periods (rcu_gp_init), FQS loops, synchronize_rcu calls.
if command -v bpftrace >/dev/null; then
  PROG='kprobe:rcu_gp_init{@gp=count();} kprobe:synchronize_rcu{@sync=count();}'
  echo "$SYMS" | grep -qx rcu_gp_fqs_loop && PROG="$PROG kprobe:rcu_gp_fqs_loop{@fqsloop=count();}"
  ( sudo bpftrace -e "$PROG" >> results/fqs_idle.txt 2>results/bpftrace.err ) &
  BPID=$!
  sleep 2; ./rcubench global 200 > /dev/null 2>&1; sleep 1
  sudo kill -INT "$BPID" 2>/dev/null; wait "$BPID" 2>/dev/null || true
fi
# tracepoint-based FQS-pass count: fqsstart events per grace period.
if [ -d /sys/kernel/tracing/events/rcu/rcu_grace_period ] && command -v trace-cmd >/dev/null; then
  sudo trace-cmd record -q -e rcu:rcu_grace_period -o results/rcu_tp.dat ./rcubench global 60 >/dev/null 2>results/tracecmd.err || true
  sudo trace-cmd report -i results/rcu_tp.dat 2>/dev/null | grep -oE 'rcu_grace_period:.*' > results/rcu_tp.txt || true
  sudo chown ubuntu results/rcu_tp.dat results/rcu_tp.txt 2>/dev/null || true
  echo "=== gpevent tally (fqsstart per gp = pass count) ===" >> results/fqs_idle.txt
  grep -oE '(fqsstart|fqsend|fqswait|reqwait|cpustart|cpuqs|cpuend|^| )(fqsstart)' results/rcu_tp.txt 2>/dev/null >/dev/null
  awk '{print $NF}' results/rcu_tp.txt 2>/dev/null | sort | uniq -c | sort -rn | head -20 >> results/fqs_idle.txt || true
  echo "tp_lines=$(wc -l < results/rcu_tp.txt 2>/dev/null)" >> results/fqs_idle.txt
else
  echo "(no rcu tracepoints)" >> results/fqs_idle.txt
fi

echo "[D: busy global synchronize_rcu, all cores loaded, N=500]"
PIDS=""
for i in $(seq 1 "$NCPU"); do taskset -c $((i-1)) yes >/dev/null & PIDS="$PIDS $!"; done
sleep 2
./rcubench global 500 > results/global_busy.csv 2>results/global_busy.err
kill $PIDS 2>/dev/null; pkill -x yes 2>/dev/null; sleep 1

echo "[E: register full-path (fork/sample), idle, N=100]"
./rcubench register 100 4 > results/register_idle.csv 2>results/register_idle.err

echo "[medians]"
python3 - <<'PY' > results/medians.txt 2>/dev/null
import statistics as st
def stats(f):
    try:
        xs=sorted(float(l) for l in open(f) if l.strip() and not l.startswith('#'))
        if not xs: return "empty"
        return f"n={len(xs)} p50={st.median(xs):.4f} p5={xs[len(xs)//20]:.4f} p95={xs[len(xs)*19//20]:.4f} max={xs[-1]:.4f}"
    except Exception as e: return f"ERR {e}"
for name,f in [("global_idle","results/global_idle.csv"),("global_busy","results/global_busy.csv"),("register_idle","results/register_idle.csv")]:
    print(name, stats(f))
PY
cat results/medians.txt

echo "RCU RUN DONE" > results/DONE
echo "ALL DONE"
