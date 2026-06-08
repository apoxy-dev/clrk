#!/usr/bin/env bash
# ssh.sh <ip> <remote-command...>
ip="$1"; shift
exec ssh -n -i "/Users/dliyevsky/projects/clrk/.hostmm-bench/hostmm-bench.pem" -o IdentitiesOnly=yes -o StrictHostKeyChecking=no \
  -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o LogLevel=ERROR -o BatchMode=yes \
  -o ServerAliveInterval=30 -o ServerAliveCountMax=120 ubuntu@"$ip" "$@"
