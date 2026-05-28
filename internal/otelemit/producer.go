package otelemit

import (
	"context"
	"fmt"
	"os"
	"strings"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// DevOTLPEndpointEnv overrides CMOTLPEndpointEnv when set, routing
// signals to the in-process devotel receiver inside `clrk dev`. The
// env is read on every ProducerEndpoint() call so tests can swap the
// endpoint via t.Setenv after init.
const DevOTLPEndpointEnv = "CLRK_DEV_OTEL_ENDPOINT"

// CMOTLPEndpointEnv is the OTLP/HTTP URL of the controller-manager's
// receiver. Deploy manifests set this on every producer pod
// (worker, ingress ext_proc, egress ext_proc); cm persists every
// signal to its embedded ClickHouse and re-exports per-EG to
// customer endpoints.
const CMOTLPEndpointEnv = "CLRK_CM_OTLP_ENDPOINT"

// ProducerEndpoint picks the URL producers POST OTLP signals to.
// Dev wins because `clrk dev` co-locates the TUI receiver; prod
// uses the cm receiver. Returns "" when neither is configured;
// callers fall back to a noop / slog sink in that case.
func ProducerEndpoint() string {
	if dev := strings.TrimSpace(os.Getenv(DevOTLPEndpointEnv)); dev != "" {
		return dev
	}
	return strings.TrimSpace(os.Getenv(CMOTLPEndpointEnv))
}

// ResolveEgressGateway returns the EgressGateway a producer should
// stamp on every OTLP signal as the clrk.egress_gateway resource
// attribute. envVar is the producer-specific env var holding
// "namespace/name"; when unset, falls back to "the only EG in ns"
// so `clrk dev` works without explicit identity.
//
// Callers pass either Manager.GetAPIReader() (uncached, for code
// running before the cache syncs) or Manager.GetClient() (cached,
// once the cache is warm).
func ResolveEgressGateway(ctx context.Context, api client.Reader, ns, envVar string) (*clrkv1alpha1.EgressGateway, error) {
	if raw := strings.TrimSpace(os.Getenv(envVar)); raw != "" {
		egNS, name, ok := strings.Cut(raw, "/")
		if !ok || egNS == "" || name == "" {
			return nil, fmt.Errorf("%s=%q is not in namespace/name format", envVar, raw)
		}
		var eg clrkv1alpha1.EgressGateway
		if err := api.Get(ctx, types.NamespacedName{Namespace: egNS, Name: name}, &eg); err != nil {
			return nil, fmt.Errorf("get EgressGateway %s/%s: %w", egNS, name, err)
		}
		return &eg, nil
	}

	var egs clrkv1alpha1.EgressGatewayList
	if err := api.List(ctx, &egs, client.InNamespace(ns)); err != nil {
		return nil, fmt.Errorf("list EgressGateways in %s: %w", ns, err)
	}
	switch len(egs.Items) {
	case 0:
		return nil, fmt.Errorf("no EgressGateway in namespace %s and %s unset", ns, envVar)
	case 1:
		return &egs.Items[0], nil
	default:
		names := make([]string, 0, len(egs.Items))
		for _, eg := range egs.Items {
			names = append(names, eg.Name)
		}
		return nil, fmt.Errorf("multiple EgressGateways in %s (%s); set %s to disambiguate",
			ns, strings.Join(names, ","), envVar)
	}
}
