#!/usr/bin/env bash
D=/Users/dliyevsky/projects/clrk/.window-bench
HM=/Users/dliyevsky/projects/clrk/.hostmm-bench
prewarm_one() {
  local label="$1" ip="$2"
  {
    echo "[$label] mkdir + upload clrk-src + prewarm.sh"
    bash "$D/ssh.sh" "$ip" "mkdir -p ~/window/scripts ~/window/results"
    bash "$D/push.sh" "$ip" "$HM/clrk-src.tar.gz" "window/clrk-src.tar.gz"
    bash "$D/push.sh" "$ip" "$D/prewarm.sh" "window/prewarm.sh"
    echo "[$label] running prewarm"
    bash "$D/ssh.sh" "$ip" "bash ~/window/prewarm.sh"
    echo "[$label] PREWARM DONE"
  } > "$D/results/prewarm_${label}.log" 2>&1
}
while IFS=$'\t' read -r label type arch ip; do
  prewarm_one "$label" "$ip" &
done < "$D/hosts.tsv"
wait
echo "ALL PREWARM COMPLETE"
