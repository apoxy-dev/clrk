package cmd

import (
	"context"
	"fmt"
	"log/slog"

	"k8s.io/client-go/discovery"
	memory "k8s.io/client-go/discovery/cached"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/apoxy-dev/apoxy/pkg/cmd/resource"
)

// applyManifests server-side-applies every YAML/JSON document found under
// paths against the cluster pointed at by kubeconfig. Used by `clrk dev
// --apply` to bootstrap CRDs into the embedded apiserver as soon as the
// aggregated APIService is registered and /readyz returns 200.
func applyManifests(ctx context.Context, kubeconfig string, paths []string, recursive bool) error {
	if len(paths) == 0 {
		return nil
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return fmt.Errorf("loading kubeconfig %s: %w", kubeconfig, err)
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

	docs, err := resource.ReadInputs(paths, recursive)
	if err != nil {
		return err
	}
	opts := resource.ApplyOptions{FieldManager: "clrk-dev", Force: true}
	var applied, failed int
	for _, data := range docs {
		for _, doc := range resource.SplitYAMLDocuments(data) {
			name, kind, err := resource.Apply(ctx, dynClient, mapper, doc, opts)
			if err != nil {
				slog.Error("Apply failed", "err", err)
				failed++
				continue
			}
			slog.Info("Applied", "kind", kind, "name", name)
			applied++
		}
	}
	if failed > 0 {
		return fmt.Errorf("apply: %d/%d documents failed", failed, applied+failed)
	}
	return nil
}
