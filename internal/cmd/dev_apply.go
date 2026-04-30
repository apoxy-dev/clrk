package cmd

import (
	"context"
	"fmt"
	"log/slog"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/discovery"
	memory "k8s.io/client-go/discovery/cached"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/yaml"

	"github.com/apoxy-dev/apoxy/pkg/cmd/resource"
)

// applyManifests server-side-applies every YAML/JSON document found under
// paths against the cluster pointed at by kubeconfig. Defaults the
// namespace on namespaced resources from the kubeconfig context (or
// "default") so manifests that omit metadata.namespace work the same way
// `kubectl apply` handles them. Used by `clrk dev --apply` and
// `clrk apply --local`.
func applyManifests(ctx context.Context, kubeconfig string, paths []string, recursive bool) error {
	if len(paths) == 0 {
		return nil
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return fmt.Errorf("loading kubeconfig %s: %w", kubeconfig, err)
	}
	defaultNS, err := contextNamespace(kubeconfig)
	if err != nil {
		return err
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
			doc, err := defaultNamespace(doc, mapper, defaultNS)
			if err != nil {
				slog.Error("Apply failed", "err", err)
				failed++
				continue
			}
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

// contextNamespace returns the kubeconfig's current-context namespace,
// falling back to "default" — the same defaulting kubectl applies when
// neither -n nor a context namespace is set.
func contextNamespace(kubeconfig string) (string, error) {
	raw, err := clientcmd.LoadFromFile(kubeconfig)
	if err != nil {
		return "", fmt.Errorf("loading kubeconfig %s: %w", kubeconfig, err)
	}
	if ctx, ok := raw.Contexts[raw.CurrentContext]; ok && ctx.Namespace != "" {
		return ctx.Namespace, nil
	}
	return "default", nil
}

// defaultNamespace patches doc to set metadata.namespace = ns when the
// resource is namespaced and the field is empty. Cluster-scoped resources
// pass through untouched. Without this, the dynamic client builds a
// cluster-scoped URL for what should be a namespaced PATCH and the
// apiserver returns 404.
func defaultNamespace(doc []byte, mapper *restmapper.DeferredDiscoveryRESTMapper, ns string) ([]byte, error) {
	obj := &unstructured.Unstructured{}
	if err := yaml.Unmarshal(doc, obj); err != nil {
		return nil, fmt.Errorf("decoding manifest: %w", err)
	}
	gvk := obj.GroupVersionKind()
	if gvk.Empty() {
		return doc, nil
	}
	if obj.GetNamespace() != "" {
		return doc, nil
	}
	mapping, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return nil, fmt.Errorf("REST mapping for %s: %w", gvk, err)
	}
	if mapping.Scope.Name() != meta.RESTScopeNameNamespace {
		return doc, nil
	}
	obj.SetNamespace(ns)
	return yaml.Marshal(obj.Object)
}
