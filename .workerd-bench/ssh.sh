#!/usr/bin/env bash
# ssh.sh <ip> <remote-cmd...>
D=/Users/dliyevsky/projects/clrk/.workerd-bench
ip="$1"; shift
ssh -n -i "$D/workerd-bench.pem" -o IdentitiesOnly=yes -o StrictHostKeyChecking=no \
    -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o LogLevel=ERROR -o BatchMode=yes \
    ubuntu@"$ip" "$@"
