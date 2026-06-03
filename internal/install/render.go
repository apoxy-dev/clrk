package install

import (
	"bytes"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/apoxy-dev/clrk/internal/crds"
)

// RenderInput parameterizes RenderManifests. Profile is the fully resolved
// install/upgrade profile (TLS posture, version stamp, NetworkPolicy CIDRs, and
// for the cert-manager path CertManagerCertRef / for self-signed CABundle all
// already set by PrepareTLS + the CIDR derivation). CertObjects are the
// serving-cert objects PrepareTLS produced for that posture (empty for the
// insecure/dev posture). CRDMode mirrors the apply path: ModeSkip emits no CRD
// documents, otherwise the embedded Gateway-API + Envoy-Gateway bundle is
// included.
type RenderInput struct {
	Profile     Profile
	CertObjects []client.Object
	CRDMode     crds.Mode
}

// RenderManifests serializes the full ordered control-plane manifest set to a
// multi-document YAML stream for `clrk install/upgrade --dry-run -o yaml`. It is
// the GitOps/audit counterpart to the live apply: the same objects the
// orchestration would lay down, in the same order — namespaces, serving-cert
// objects, the Gateway-API + Envoy-Gateway CRDs, the controller-manager set, and
// the WorkerPool set — emitted as `kubectl apply -f -`-able YAML.
//
// It is pure (no cluster access): every object is already built from the
// resolved Profile, so the output is deterministic except for the self-signed
// key material PrepareTLS minted (the CA + serving-cert Secrets). For that
// posture the stream is prefixed with a sensitive-material warning, since the
// emitted Secrets carry private keys.
func RenderManifests(in RenderInput) ([]byte, error) {
	p := in.Profile

	// Ordered to mirror Orchestration.Steps(): namespaces -> serving-cert ->
	// CRDs -> controller-manager -> WorkerPool. Apply ordering within each group
	// (SA before its binding, cm before workers) is preserved by the builders.
	var objs []client.Object
	objs = append(objs, namespaceObjects(p)...)
	objs = append(objs, in.CertObjects...)
	cmObjs := BuildControllerManager(p)
	wpObjs := BuildWorkerPool(p)

	var docs [][]byte
	render := func(group []client.Object) error {
		for _, o := range group {
			doc, err := renderObject(o)
			if err != nil {
				return fmt.Errorf("rendering %s %s: %w", kindLabel(o), nameOf(o), err)
			}
			docs = append(docs, doc)
		}
		return nil
	}

	if err := render(objs); err != nil {
		return nil, err
	}
	// CRDs are emitted from the embedded bundle (the upstream YAML, trimmed),
	// between the serving-cert objects and the control-plane objects, unless the
	// operator opted out with --crd-mode=skip. Only ModeSkip is special-cased:
	// under ModeIfMissing the render still emits the full bundle (whether a CRD is
	// already present is a cluster-runtime decision the pure render can't make,
	// and re-applying an existing CRD is idempotent). The buffer-wide
	// creationTimestamp:null strip below also runs over these docs — that drops a
	// handful of upstream metadata.creationTimestamp lines, harmless for apply and
	// consistent with the rest of the stream, so the bundle is near-verbatim, not
	// byte-verbatim.
	if in.CRDMode != crds.ModeSkip {
		crdDocs, err := crds.Manifests()
		if err != nil {
			return nil, fmt.Errorf("loading embedded CRDs: %w", err)
		}
		docs = append(docs, crdDocs...)
	}
	if err := render(cmObjs); err != nil {
		return nil, err
	}
	if err := render(wpObjs); err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if p.TLS == TLSSelfSigned {
		buf.WriteString(selfSignedSensitiveHeader)
	}
	for _, d := range docs {
		buf.WriteString("---\n")
		buf.Write(d)
		if !bytes.HasSuffix(d, []byte("\n")) {
			buf.WriteByte('\n')
		}
	}
	return stripNullCreationTimestamp(buf.Bytes()), nil
}

// selfSignedSensitiveHeader warns that the self-signed render embeds private
// keys (the CA + serving-cert Secrets), so the artifact must not be committed to
// a shared/public GitOps repo. cert-manager mints keys in-cluster and emits no
// key material, so it carries no such header.
const selfSignedSensitiveHeader = "# WARNING: this manifest set contains private key material - the self-signed\n" +
	"# CA and serving-certificate Secrets (tls.key). Treat it as sensitive: do not\n" +
	"# commit it to a shared or public GitOps repository. Use --tls=cert-manager to\n" +
	"# have cert-manager mint the keys in-cluster instead.\n"

// renderObject serializes a single object to apply-able YAML. The Unstructured
// cert-manager objects (which carry no status) are marshaled directly — this
// avoids both copying the caller's map and runtime.DeepCopyJSON, which would
// panic rather than error on a non-JSON value. Typed builder objects go through
// the unstructured converter (preserving the TypeMeta the builders set) with
// their always-empty status dropped, since an empty status block is pure noise
// in a manifest and the apiVersion/kind/metadata the apply path needs are kept.
func renderObject(obj client.Object) ([]byte, error) {
	if u, ok := obj.(*unstructured.Unstructured); ok {
		return yaml.Marshal(u.Object)
	}
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, err
	}
	delete(m, "status")
	return yaml.Marshal(m)
}

// stripNullCreationTimestamp removes the `creationTimestamp: null` lines the
// Kubernetes serializer emits at every metadata level (object metadata and
// nested pod-template metadata) for a built-but-never-persisted object. They are
// cosmetic noise — apply tolerates them — but a clean GitOps artifact shouldn't
// carry them. Matched on the trimmed line so any indentation is covered; no
// legitimate field carries this exact value.
func stripNullCreationTimestamp(b []byte) []byte {
	lines := strings.Split(string(b), "\n")
	out := lines[:0]
	for _, l := range lines {
		if strings.TrimSpace(l) == "creationTimestamp: null" {
			continue
		}
		out = append(out, l)
	}
	return []byte(strings.Join(out, "\n"))
}

// kindLabel returns an object's Kind for error messages, falling back to a
// generic label when TypeMeta is unset (shouldn't happen — every builder sets
// it — but keeps the error legible if one ever doesn't).
func kindLabel(obj client.Object) string {
	if k := obj.GetObjectKind().GroupVersionKind().Kind; k != "" {
		return k
	}
	return "object"
}
