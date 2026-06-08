#!/usr/bin/env bash
# Config/topology/idle census. Same AMI within a pair => any diff here is the
# explanation; identical here => the delta is pure runtime/hardware.
CFG=/boot/config-$(uname -r)
echo "=== uname ==="; uname -r
echo "=== cpus ==="; echo "nproc=$(nproc) online=$(getconf _NPROCESSORS_ONLN)"
echo "=== cmdline ==="; cat /proc/cmdline
echo "=== kernel config (HZ/NOHZ/RCU/PREEMPT) ==="
grep -E '^CONFIG_HZ=|^CONFIG_HZ_|^CONFIG_NO_HZ|^CONFIG_PREEMPT|^CONFIG_RCU_FANOUT|^CONFIG_RCU_NOCB|^CONFIG_RCU_FAST_NO_HZ|^CONFIG_TREE_RCU|^CONFIG_RCU_BOOST|^CONFIG_HIGH_RES_TIMERS|^CONFIG_RCU_EXPERT' "$CFG" 2>/dev/null || echo "(no $CFG)"
echo "=== nohz_full / isolated ==="
echo "nohz_full=$(cat /sys/devices/system/cpu/nohz_full 2>/dev/null)"
echo "isolated=$(cat /sys/devices/system/cpu/isolated 2>/dev/null)"
echo "=== clocksource ==="
echo "current=$(cat /sys/devices/system/clocksource/clocksource0/current_clocksource 2>/dev/null)"
echo "available=$(cat /sys/devices/system/clocksource/clocksource0/available_clocksource 2>/dev/null)"
echo "=== rcutree params ==="
for f in /sys/module/rcutree/parameters/jiffies_till_first_fqs /sys/module/rcutree/parameters/jiffies_till_next_fqs /sys/module/rcutree/parameters/rcu_kick_kthreads /sys/module/rcutree/parameters/blimit; do
  [ -e "$f" ] && printf '%s = %s\n' "$(basename $f)" "$(cat $f)"
done
echo "rcu_expedited=$(cat /sys/kernel/rcu_expedited 2>/dev/null) rcu_normal=$(cat /sys/kernel/rcu_normal 2>/dev/null)"
echo "=== cpuidle driver + states (cpu0) ==="
echo "driver=$(cat /sys/devices/system/cpu/cpuidle/current_driver 2>/dev/null)"
for s in /sys/devices/system/cpu/cpu0/cpuidle/state*; do
  [ -e "$s" ] || continue
  printf '%s name=%s latency_us=%s residency_us=%s disable=%s\n' \
    "$(basename $s)" "$(cat $s/name 2>/dev/null)" "$(cat $s/latency 2>/dev/null)" \
    "$(cat $s/residency 2>/dev/null)" "$(cat $s/disable 2>/dev/null)"
done
echo "=== idle residency snapshot (cpu0, 1s) ==="
for s in /sys/devices/system/cpu/cpu0/cpuidle/state*; do
  [ -e "$s/time" ] && printf '%s usage=%s time_us=%s\n' "$(cat $s/name)" "$(cat $s/usage)" "$(cat $s/time)"
done
echo "=== governor ==="
echo "governor=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo none)"
echo "=== rcu kthreads ==="
echo "rcuo_count=$(ps -eL -o comm 2>/dev/null | grep -c '^rcuo')"
ps -eL -o pid,comm 2>/dev/null | grep -E 'rcu_preempt|rcu_sched|rcu_tasks|rcu_gp' | head
echo "=== steal/idle (vmstat-ish) ==="
grep -E '^cpu ' /proc/stat
