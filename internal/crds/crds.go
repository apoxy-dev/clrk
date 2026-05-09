// Package crds embeds the upstream Gateway API and Envoy Gateway CRD
// bundles and applies them via server-side apply, replacing the
// `kubectl apply -f https://...` step that the dev driver used to run.
package crds

import (
	"context"
	_ "embed"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	memory "k8s.io/client-go/discovery/cached"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"sigs.k8s.io/yaml"

	"github.com/apoxy-dev/apoxy/pkg/cmd/resource"

	"github.com/apoxy-dev/clrk/internal/eg"
)

//go:embed gateway-api/experimental-install-v1.2.1.yaml
var gatewayAPIYAML []byte

// envoyGatewayCRDsFS embeds the entire envoy-gateway/ subdirectory; the
// active bundle filename is derived from eg.Version at init time so
// bumping the EG version requires editing only that constant + dropping
// the new YAML next to the old one(s). go:embed needs a literal path
// pattern, hence the dir-level embed.
//
//go:embed envoy-gateway/*.yaml
var envoyGatewayCRDsFS embed.FS

var envoyGatewayYAML []byte

func init() {
	name := "envoy-gateway/crds-" + eg.Version + ".yaml"
	b, err := envoyGatewayCRDsFS.ReadFile(name)
	if err != nil {
		// A bump that updated eg.Version without dropping the matching
		// CRD bundle next to it lands here; better to fail at process
		// start than to install a stale CRD set against a newer EG
		// binary.
		panic(fmt.Sprintf("clrk: embedded EG CRD bundle %q missing — bump internal/eg/version.go and drop the matching crds-<Version>.yaml under internal/crds/envoy-gateway/: %v", name, err))
	}
	envoyGatewayYAML = b
}

// envoyGatewayNamespaceYAML is the envoy-gateway-system namespace EG
// expects to exist (it stores its OIDC HMAC secret + provisions Envoy
// data-plane Deployments there). Upstream's install.yaml ships this as
// part of the operator bundle; we run the operator in-process so we
// create the namespace ourselves.
const envoyGatewayNamespaceYAML = `
apiVersion: v1
kind: Namespace
metadata:
  name: envoy-gateway-system
`

// Mode controls how Install handles CRDs already present in the cluster.
type Mode int

const (
	// ModeAlways force-applies every embedded CRD, taking field
	// ownership from any prior applier. Default for `clrk dev` where
	// the cluster is ours.
	ModeAlways Mode = iota
	// ModeIfMissing applies only CRDs that don't yet exist. Lets a
	// coexisting tenant own their copy of the same CRD.
	ModeIfMissing
	// ModeSkip is a no-op. Escape hatch for clusters where someone
	// else owns CRD lifecycle.
	ModeSkip
)

// InstallOptions controls Install.
type InstallOptions struct {
	// Mode selects the apply policy. Defaults to ModeAlways.
	Mode Mode
	// FieldManager identifies clrk's field-ownership in managed-fields.
	// Defaults to "clrk-controller-manager".
	FieldManager string
}

// Install applies the embedded Gateway API and Envoy Gateway CRD
// bundles against the cluster reachable via cfg. Idempotent. Must be
// called before any controller-runtime informer that watches these
// types starts its cache, otherwise the informer fails on missing
// REST mapping.
func Install(ctx context.Context, cfg *rest.Config, opts InstallOptions) error {
	if opts.Mode == ModeSkip {
		slog.Info("CRD install skipped by mode=skip")
		return nil
	}
	if opts.FieldManager == "" {
		opts.FieldManager = "clrk-controller-manager"
	}

	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return fmt.Errorf("discovery client: %w", err)
	}
	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("dynamic client: %w", err)
	}
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(dc))

	bundles := []struct {
		name string
		data []byte
	}{
		{"gateway-api", gatewayAPIYAML},
		{"envoy-gateway", envoyGatewayYAML},
	}

	applyOpts := resource.ApplyOptions{
		FieldManager: opts.FieldManager,
		Force:        opts.Mode == ModeAlways,
	}

	// Apply CRDs concurrently. ~12 docs across both bundles; the
	// apiserver serializes admission internally but the network /
	// JSON-decode hops parallelize well, cutting install time roughly
	// in half.
	var applied, skipped atomic.Int32
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for _, b := range bundles {
		bundleName := b.name
		for _, doc := range resource.SplitYAMLDocuments(b.data) {
			doc := doc
			g.Go(func() error {
				crdName, err := extractCRDName(doc)
				if err != nil {
					return fmt.Errorf("%s: %w", bundleName, err)
				}
				if crdName == "" {
					return nil
				}
				if opts.Mode == ModeIfMissing {
					exists, err := crdExists(gctx, dynClient, crdName)
					if err != nil {
						return fmt.Errorf("checking CRD %s: %w", crdName, err)
					}
					if exists {
						skipped.Add(1)
						return nil
					}
				}
				if _, _, err := resource.Apply(gctx, dynClient, mapper, doc, applyOpts); err != nil {
					return fmt.Errorf("%s: %w", bundleName, err)
				}
				applied.Add(1)
				return nil
			})
		}
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// Apply non-CRD bootstrap (envoy-gateway-system namespace). SSA so
	// re-applies are idempotent and don't fight any user-applied labels.
	if _, _, err := resource.Apply(ctx, dynClient, mapper, []byte(envoyGatewayNamespaceYAML), applyOpts); err != nil {
		return fmt.Errorf("applying envoy-gateway-system namespace: %w", err)
	}
	slog.Info("CRD install complete", "applied", applied.Load(), "skipped", skipped.Load(), "mode", modeString(opts.Mode))
	return nil
}

// extractCRDName parses just enough of the doc to assert kind=CRD and
// return metadata.name. Returns ("", nil) for comment-only or empty
// documents (the upstream Gateway API bundle leads with a copyright
// header that splits as its own doc) so the caller can skip them.
func extractCRDName(doc []byte) (string, error) {
	var meta struct {
		Kind     string `json:"kind"`
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
	}
	if err := yaml.Unmarshal(doc, &meta); err != nil {
		return "", fmt.Errorf("decoding embedded YAML: %w", err)
	}
	if meta.Kind == "" {
		return "", nil
	}
	if meta.Kind != "CustomResourceDefinition" {
		return "", fmt.Errorf("embedded doc has kind=%q, want CustomResourceDefinition", meta.Kind)
	}
	if meta.Metadata.Name == "" {
		return "", errors.New("embedded CRD missing metadata.name")
	}
	return meta.Metadata.Name, nil
}

func crdExists(ctx context.Context, dynClient dynamic.Interface, name string) (bool, error) {
	gvr := schema.GroupVersionResource{
		Group:    "apiextensions.k8s.io",
		Version:  "v1",
		Resource: "customresourcedefinitions",
	}
	_, err := dynClient.Resource(gvr).Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		return true, nil
	}
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	return false, err
}

func modeString(m Mode) string {
	switch m {
	case ModeAlways:
		return "always"
	case ModeIfMissing:
		return "if-missing"
	case ModeSkip:
		return "skip"
	default:
		return "unknown"
	}
}
