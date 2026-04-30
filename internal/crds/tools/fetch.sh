#!/usr/bin/env bash
# Re-vendor the upstream CRD bundles that pkg/crds embeds.
#
# Pulls the upstream YAML, strips everything except CRDs, and writes
# pinned files under pkg/crds/{gateway-api,envoy-gateway}/. Run after
# bumping the pinned versions below; commit the diff.
set -euo pipefail

GATEWAY_API_VERSION="${GATEWAY_API_VERSION:-v1.2.1}"
ENVOY_GATEWAY_VERSION="${ENVOY_GATEWAY_VERSION:-v1.1.3}"

cd "$(dirname "$0")/.."

# 1) Gateway API experimental install — CRD-only upstream. EG requires
#    the experimental channel (TLSRoute / TCPRoute / UDPRoute) on top of
#    the standard set; this bundle is a superset of standard-install.yaml.
curl -fsSLo "gateway-api/experimental-install-${GATEWAY_API_VERSION}.yaml" \
    "https://github.com/kubernetes-sigs/gateway-api/releases/download/${GATEWAY_API_VERSION}/experimental-install.yaml"

# 2) Envoy Gateway install.yaml ships the operator + CRDs + an older Gateway
#    API CRD set. Extract only gateway.envoyproxy.io CRDs.
TMP="$(mktemp)"
curl -fsSLo "$TMP" \
    "https://github.com/envoyproxy/gateway/releases/download/${ENVOY_GATEWAY_VERSION}/install.yaml"

# YAML splitter that keeps only documents whose kind=CustomResourceDefinition
# AND whose .spec.group=gateway.envoyproxy.io. Python because awk's RS-based
# multi-doc handling chokes on EG's mixed `---` placement.
python3 - "$TMP" > "envoy-gateway/crds-${ENVOY_GATEWAY_VERSION}.yaml" <<'PY'
import sys, re

src = open(sys.argv[1]).read()
# Split on lines that are exactly `---` (YAML doc separator).
docs = re.split(r'(?m)^---\s*$', src)
out = []
for d in docs:
    if not d.strip():
        continue
    if not re.search(r'(?m)^kind:\s*CustomResourceDefinition\s*$', d):
        continue
    if not re.search(r'(?m)^  group:\s*gateway\.envoyproxy\.io\s*$', d):
        continue
    out.append(d.strip('\n'))
sys.stdout.write('---\n' + '\n---\n'.join(out) + '\n')
PY

rm -f "$TMP"

echo "Vendored:"
ls -lh gateway-api/ envoy-gateway/
