#!/usr/bin/env bash
# push.sh <ip> <local> <remote>
ip="$1"; src="$2"; dst="$3"
exec scp -i "/Users/dliyevsky/projects/clrk/.hostmm-bench/hostmm-bench.pem" -o IdentitiesOnly=yes -o StrictHostKeyChecking=no \
  -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o LogLevel=ERROR \
  "$src" ubuntu@"$ip":"$dst"
