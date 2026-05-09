// Package eg pins the upstream Envoy Gateway release this clrk build
// is compatible with. Single source of truth on the clrk side; consumers
// of the matching binary + CRDs (embedded YAML, apoxy-cloud bazel
// image build) must stay in sync.
//
// See apoxy-cloud//docs/clrk-envoy-gateway.md for the full EG ↔ clrk
// contract: extension hooks, webhook surface, certgen flags, and the
// version-bump checklist.
package eg

// Version is the upstream Envoy Gateway release this clrk build pins.
// Bumping it is a four-step dance:
//
//  1. Update Version below.
//  2. Bump `github.com/envoyproxy/gateway` in clrk/go.mod to the same
//     vX.Y.Z so EG xDS extension-server protos line up.
//  3. Drop the matching upstream CRD bundle into
//     internal/crds/envoy-gateway/crds-<Version>.yaml; the embed in
//     internal/crds resolves the filename from this constant at
//     init time and panics on mismatch.
//  4. Bump apoxy-cloud//clrk/BUILD.bazel's EG_IMAGE_REF SHA to the
//     EG operator image carrying the matching `envoy-gateway`
//     binary (extracted into the controller-manager image).
//
// The four locations are NOT auto-synchronized — a forgotten step
// surfaces as either a CRD-not-found panic at startup (step 3) or an
// xDS translation drift between the embedded binary and clrk's
// extension-server protos (step 4).
const Version = "v1.4.0"
