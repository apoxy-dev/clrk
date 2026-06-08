#!/usr/bin/env bash
# push.sh <ip> <local-path> <remote-path>
D=/Users/dliyevsky/projects/clrk/.window-bench
ip="$1"; src="$2"; dst="$3"
scp -i "$D/window-bench.pem" -o IdentitiesOnly=yes -o StrictHostKeyChecking=no \
    -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o LogLevel=ERROR \
    "$src" ubuntu@"$ip":"$dst"
