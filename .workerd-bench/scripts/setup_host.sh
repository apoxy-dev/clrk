#!/usr/bin/env bash
# setup_host.sh -- provision one VM for the workerd exec-path bench: Go toolchain,
# build bench-runsc (CGO) + workerd-runner (static), download the workerd release
# binary, and stage /opt/workerd for the read-only bind mount.
set -uo pipefail
GOVER=1.25.5
WORKERD_TAG=${WORKERD_TAG:-v1.20260606.1}
ARCH=$(dpkg --print-architecture)   # arm64 | amd64
STAGE=/opt/workerd
WB="$HOME/workerd-bench"

echo "### apt deps"
sudo DEBIAN_FRONTEND=noninteractive apt-get update -y >/tmp/apt.log 2>&1
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
  build-essential git rsync curl ca-certificates file libstdc++6 \
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
tar -xzf "$WB/clrk-src.tar.gz" -C "$HOME/clrk"

echo "### build bench-runsc (CGO, linux/${ARCH}) + workerd-runner (static)"
cd "$HOME/clrk"
if CGO_ENABLED=1 go build -o "$HOME/bench-runsc" ./cmd/bench-runsc 2>/tmp/build.log; then
  echo "bench-runsc OK: $(stat -c%s "$HOME/bench-runsc") bytes"
else
  echo "BENCH BUILD FAILED — tail /tmp/build.log:"; tail -40 /tmp/build.log; exit 1
fi
if CGO_ENABLED=0 go build -o "$WB/workerd-runner" ./cmd/workerd-runner 2>/tmp/build2.log; then
  echo "workerd-runner OK: $(stat -c%s "$WB/workerd-runner") bytes"
else
  echo "RUNNER BUILD FAILED — tail /tmp/build2.log:"; tail -40 /tmp/build2.log; exit 1
fi

echo "### download workerd ${WORKERD_TAG} (${ARCH})"
case "$ARCH" in
  amd64) WK=workerd-linux-64 ;;
  arm64) WK=workerd-linux-arm64 ;;
  *) echo "unknown arch $ARCH"; exit 1 ;;
esac
URL="https://github.com/cloudflare/workerd/releases/download/${WORKERD_TAG}/${WK}.gz"
curl -fsSL "$URL" -o /tmp/workerd.gz || { echo "workerd download failed: $URL"; exit 1; }
gunzip -f /tmp/workerd.gz
chmod +x /tmp/workerd

echo "### stage ${STAGE}"
sudo rm -rf "$STAGE"; sudo mkdir -p "$STAGE/lib"
sudo cp /tmp/workerd "$STAGE/workerd"
sudo cp "$WB/workerd-runner" "$STAGE/workerd-runner"
sudo cp "$WB"/config*.capnp "$WB"/worker*.js "$STAGE/"
sudo chmod -R a+rX "$STAGE"
echo "  staged workers: $(cd "$STAGE" && ls config*.capnp worker*.js)"

echo "### workerd binary facts"
file "$STAGE/workerd" 2>/dev/null | sed 's/^/  /'
echo "  ldd:"; ldd "$STAGE/workerd" 2>&1 | sed 's/^/    /'
echo "  --version (host):"; ("$STAGE/workerd" --version 2>&1 | head -2 | sed 's/^/    /') || echo "    (host --version failed; may still run in the sandbox rootfs)"

echo "### runtime dirs"
sudo mkdir -p /run/clrk/runsc /run/clrk/images
sudo chmod 0755 /run/clrk

echo "### prereqs"
{ echo "host=$(hostname)"; echo "arch=$ARCH"; echo "uname=$(uname -r)"; echo "ncpu=$(nproc)";
  echo "mem=$(free -m | awk '/Mem:/{print $2"MB"}')";
  echo "model=$(grep -m1 -E 'model name|Model' /proc/cpuinfo | cut -d: -f2- | sed 's/^ //')";
  echo "workerd_tag=$WORKERD_TAG"; } | tee "$HOME/prereqs.txt"
echo "### setup complete"
