#!/usr/bin/env bash
D=/Users/dliyevsky/projects/clrk/.window-bench
HM=/Users/dliyevsky/projects/clrk/.hostmm-bench
one() {
  local label="$1" ip="$4"
  {
    echo "[$label] shipping gvisor-src + patch"
    bash "$D/push.sh" "$ip" "$HM/gvisor-src.tar.gz"        "window/gvisor-src.tar.gz"
    bash "$D/push.sh" "$ip" "$HM/membarrier_patched.go"    "window/membarrier_patched.go"
    bash "$D/push.sh" "$ip" "$D/patched_host.sh"           "window/patched_host.sh"
    echo "[$label] building + measuring patched"
    bash "$D/ssh.sh" "$ip" "bash ~/window/patched_host.sh"
    echo "[$label] PATCHED DONE"
  } > "$D/results/patched_${label}.log" 2>&1
}
while IFS=$'\t' read -r label type arch ip; do one "$label" "$type" "$arch" "$ip" & done < "$D/hosts.tsv"
wait
echo "ALL PATCHED COMPLETE"
