#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

ROOT_DIR="$(git rev-parse --show-toplevel)"

# Configurable variables. Keep in sync with the k8s.io/apimachinery version in go.mod.
CODEGEN_VERSION=v0.32.3
BOILERPLATE_FILE="${ROOT_DIR}/codegen/boilerplate.go.txt"

echo "Generating deepcopy helpers..."

go run "k8s.io/code-generator/cmd/deepcopy-gen@${CODEGEN_VERSION}" \
  --output-file zz_generated.deepcopy.go \
  --go-header-file "${BOILERPLATE_FILE}" \
  ./api/clrk/v1alpha1 \
  ./api/metrics/v1alpha1 \
  ./internal/extproc/llmcall

echo "Generating register helpers..."

go run "k8s.io/code-generator/cmd/register-gen@${CODEGEN_VERSION}" \
  --output-file zz_generated.register.go \
  --go-header-file "${BOILERPLATE_FILE}" \
  ./api/clrk/v1alpha1 \
  ./api/metrics/v1alpha1

# Fix missing imports in generated register files (register-gen bug in v0.32.x).
echo "Fixing register imports..."
for f in $(find "${ROOT_DIR}" -name 'zz_generated.register.go'); do
  if ! grep -q '"k8s.io/apimachinery/pkg/runtime"' "$f"; then
    if grep -q 'v1 "k8s.io/apimachinery/pkg/apis/meta/v1"' "$f"; then
      sed -i.bak -e 's|v1 "k8s.io/apimachinery/pkg/apis/meta/v1"|v1 "k8s.io/apimachinery/pkg/apis/meta/v1"\'$'\n''\t"k8s.io/apimachinery/pkg/runtime"\'$'\n''\t"k8s.io/apimachinery/pkg/runtime/schema"|' "$f"
    elif grep -q 'metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"' "$f"; then
      sed -i.bak -e 's|metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"|metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"\'$'\n''\t"k8s.io/apimachinery/pkg/runtime"\'$'\n''\t"k8s.io/apimachinery/pkg/runtime/schema"|' "$f"
    fi
    rm -f "${f}.bak"
  fi
done

echo "Generating client code..."

go run "k8s.io/code-generator/cmd/client-gen@${CODEGEN_VERSION}" \
  --go-header-file "${BOILERPLATE_FILE}" \
  --output-dir "client/" \
  --output-pkg "github.com/apoxy-dev/clrk/client" \
  --input-base "github.com/apoxy-dev/clrk" \
  --clientset-name "versioned" \
  --input "./api/clrk/v1alpha1"

echo "Generating listers and informers..."

go run "k8s.io/code-generator/cmd/lister-gen@${CODEGEN_VERSION}" \
  --go-header-file "${BOILERPLATE_FILE}" \
  --output-dir "client/listers" \
  --output-pkg "github.com/apoxy-dev/clrk/client/listers" \
  ./api/clrk/v1alpha1

go run "k8s.io/code-generator/cmd/informer-gen@${CODEGEN_VERSION}" \
  --go-header-file "${BOILERPLATE_FILE}" \
  --output-dir "client/informers" \
  --output-pkg "github.com/apoxy-dev/clrk/client/informers" \
  --versioned-clientset-package "github.com/apoxy-dev/clrk/client/versioned" \
  --listers-package=github.com/apoxy-dev/clrk/client/listers \
  --single-directory \
  ./api/clrk/v1alpha1

echo "Generating OpenAPI schema..."

# kube-openapi has no published tags; the version is pinned in our go.mod
# to match apoxy-cloud's resolution. Use `go run` (no @version) so the
# pinned version is used and transitive deps (golang.org/x/tools etc.)
# resolve against the same module graph clrk compiles against — running
# `pkg@version` creates a fresh temp module and pulls incompatible
# transitive versions.
go run k8s.io/kube-openapi/cmd/openapi-gen \
  --go-header-file "${BOILERPLATE_FILE}" \
  --output-dir "api/generated" \
  --output-pkg "generated" \
  --output-file zz_generated.openapi.go \
  --report-filename /dev/null \
  k8s.io/api/coordination/v1 \
  k8s.io/api/core/v1 \
  k8s.io/api/events/v1 \
  k8s.io/apimachinery/pkg/api/resource \
  k8s.io/apimachinery/pkg/apis/meta/v1 \
  k8s.io/apimachinery/pkg/runtime \
  k8s.io/apimachinery/pkg/util/intstr \
  k8s.io/apimachinery/pkg/version \
  sigs.k8s.io/gateway-api/apis/v1 \
  sigs.k8s.io/gateway-api/apis/v1alpha2 \
  ./api/clrk/v1alpha1 \
  ./api/metrics/v1alpha1
