#!/usr/bin/env bash
# setup.sh — provision a single host for the hostmm/membarrier cold-boot re-test.
# Installs Go 1.25.x + tracing tools, unpacks clrk source, builds bench-runsc,
# preps /run/clrk, and records the kernel prerequisites the analysis depends on.
set -uo pipefail
GOVER=1.25.5
ARCH=$(dpkg --print-architecture)   # arm64 | amd64

echo "### apt deps"
sudo DEBIAN_FRONTEND=noninteractive apt-get update -y >/tmp/apt.log 2>&1
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
  build-essential git rsync jq numactl python3 \
  linux-tools-common "linux-tools-$(uname -r)" \
  bpfcc-tools bpftrace "linux-headers-$(uname -r)" \
  >>/tmp/apt.log 2>&1 || echo "WARN: some apt packages failed (see /tmp/apt.log)"

echo "### Go ${GOVER}"
if ! /usr/local/go/bin/go version 2>/dev/null | grep -q "go${GOVER} "; then
  curl -fsSL "https://go.dev/dl/go${GOVER}.linux-${ARCH}.tar.gz" -o /tmp/go.tgz
  sudo rm -rf /usr/local/go
  sudo tar -C /usr/local -xzf /tmp/go.tgz
fi
export PATH=/usr/local/go/bin:$PATH
export GOFLAGS=-mod=mod
go version

echo "### unpack clrk source"
rm -rf "$HOME/clrk" && mkdir -p "$HOME/clrk"
tar -xzf "$HOME/clrk-src.tar.gz" -C "$HOME/clrk"

echo "### build bench-runsc (CGO, linux/${ARCH})"
cd "$HOME/clrk"
if CGO_ENABLED=1 go build -o "$HOME/bench-runsc" ./cmd/bench-runsc 2>/tmp/build.log; then
  echo "BUILD OK: $(ls -la "$HOME/bench-runsc" | awk '{print $5}') bytes"
else
  echo "BUILD FAILED — tail of /tmp/build.log:"; tail -30 /tmp/build.log; exit 1
fi

echo "### runtime dirs"
sudo mkdir -p /run/clrk/runsc /run/clrk/images
sudo chmod 0755 /run/clrk

echo "### prerequisites"
PRE="$HOME/prereqs.txt"
{
  echo "host=$(hostname)"
  echo "arch=${ARCH}"
  echo "uname=$(uname -r)"
  echo "pagesize=$(getconf PAGESIZE)"
  echo "ncpu=$(nproc)"
  echo "model=$(grep -m1 -E 'model name|CPU implementer|Model' /proc/cpuinfo | cut -d: -f2- | sed 's/^ //')"
  echo "kallsyms_sync_runqueues=$(sudo grep -c sync_runqueues_membarrier_state /proc/kallsyms 2>/dev/null)"
  echo "kallsyms_register_priv=$(sudo grep -c membarrier_register_private_expedited /proc/kallsyms 2>/dev/null)"
  echo "kallsyms_synchronize_rcu=$(sudo grep -cw synchronize_rcu /proc/kallsyms 2>/dev/null)"
  echo "nohz_full=$(cat /sys/devices/system/cpu/nohz_full 2>/dev/null || echo none)"
  echo "rcu_expedited=$(cat /sys/kernel/rcu_expedited 2>/dev/null)"
  echo "rcu_normal=$(cat /sys/kernel/rcu_normal 2>/dev/null)"
  echo "thp_enabled=$(cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null)"
  echo "cmdline=$(cat /proc/cmdline)"
} | tee "$PRE"
echo "### setup complete"
