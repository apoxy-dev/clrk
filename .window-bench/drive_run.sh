#!/usr/bin/env bash
D=/Users/dliyevsky/projects/clrk/.window-bench
run_host_full() {
  local label="$1" type="$2" arch="$3" ip="$4"
  {
    echo "[$label] pre-seed root go caches"
    bash "$D/ssh.sh" "$ip" '
      sudo mkdir -p /root/.cache
      [ -d /root/go ] || sudo cp -a /home/ubuntu/go /root/go 2>/dev/null
      [ -d /root/.cache/go-build ] || sudo cp -a /home/ubuntu/.cache/go-build /root/.cache/go-build 2>/dev/null
      echo seeded
    '
    bash "$D/run_one.sh" "$ip" "$label"
  } > "$D/results/full_${label}.log" 2>&1
}
while IFS=$'\t' read -r label type arch ip; do
  run_host_full "$label" "$type" "$arch" "$ip" &
done < "$D/hosts.tsv"
wait
echo "ALL HOSTS COMPLETE"
