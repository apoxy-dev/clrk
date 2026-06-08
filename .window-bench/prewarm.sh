#!/usr/bin/env bash
# prewarm.sh -- install toolchain, build bench-runsc (CORE_BOOT binary), stage PMU tooling.
# Idempotent; safe to run before run_host.sh. Runs on the host as ubuntu (uses sudo).
set -uo pipefail
GOVER=1.25.5
ARCH=$(dpkg --print-architecture)   # arm64 | amd64
VENDOR=$(grep -m1 -E 'vendor_id|CPU implementer' /proc/cpuinfo | cut -d: -f2- | tr -d ' ')

echo "### apt deps ($ARCH)"
sudo DEBIAN_FRONTEND=noninteractive apt-get update -y >/tmp/apt.log 2>&1
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
  build-essential git rsync jq numactl python3 python3-pip cpufrequtils >>/tmp/apt.log 2>&1 \
  || echo "WARN: core apt deps failed (see /tmp/apt.log)"
# Optional/arch-specific, best-effort (must NOT abort the core install above):
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
  linux-tools-common "linux-tools-$(uname -r)" linux-tools-aws linux-tools-generic \
  "linux-headers-$(uname -r)" >>/tmp/apt.log 2>&1 || true
[ "$ARCH" = amd64 ] && { sudo DEBIAN_FRONTEND=noninteractive apt-get install -y msr-tools >>/tmp/apt.log 2>&1 || true; }

echo "### Go ${GOVER}"
if ! /usr/local/go/bin/go version 2>/dev/null | grep -q "go${GOVER} "; then
  curl -fsSL "https://go.dev/dl/go${GOVER}.linux-${ARCH}.tar.gz" -o /tmp/go.tgz
  sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf /tmp/go.tgz
fi
export PATH=/usr/local/go/bin:$PATH
export GOFLAGS=-mod=mod
go version

echo "### unpack clrk + build bench-runsc (CGO, linux/${ARCH})"
if [ ! -x "$HOME/bench-runsc" ]; then
  rm -rf "$HOME/clrk" && mkdir -p "$HOME/clrk"
  tar -xzf "$HOME/window/clrk-src.tar.gz" -C "$HOME/clrk"
  ( cd "$HOME/clrk" && CGO_ENABLED=1 go build -o "$HOME/bench-runsc" ./cmd/bench-runsc 2>/tmp/build.log ) \
    && echo "BUILD OK: $(stat -c%s "$HOME/bench-runsc") bytes" \
    || { echo "BUILD FAILED:"; tail -30 /tmp/build.log; }
else
  echo "bench-runsc already built"
fi

echo "### runtime dirs"
sudo mkdir -p /run/clrk/runsc /run/clrk/images && sudo chmod 0755 /run/clrk

echo "### PMU tooling"
if [ "$ARCH" = "amd64" ]; then
  [ -d "$HOME/pmu-tools" ] || git clone --depth 1 https://github.com/andikleen/pmu-tools "$HOME/pmu-tools" 2>/tmp/pmu.log \
    && echo "pmu-tools ready" || echo "WARN pmu-tools clone failed"
  sudo modprobe msr 2>/dev/null || true
else
  # Arm Telemetry Solution topdown-tool (best-effort; raw perf is the fallback in pmu_arm.sh).
  pip3 install --user --break-system-packages topdown-tool 2>/tmp/td.log && echo "topdown-tool ready" \
    || echo "WARN topdown-tool pip failed (pmu_arm.sh falls back to raw perf)"
fi

echo "### perf sanity"
echo "kernel.perf_event_paranoid=$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null)"
sudo sysctl -w kernel.perf_event_paranoid=-1 >/dev/null 2>&1 || true
echo "### prewarm complete ($(hostname) $ARCH vendor=$VENDOR)"
