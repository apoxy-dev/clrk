package extproc

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/egidentity"
)

// DevOTLPEndpointEnv is set by `clrk dev` on the controller-manager
// container. When per-EG OTLP endpoint is empty and this env is set,
// the registry routes records to the dev receiver instead of slogSink.
// Production deployments don't set this env, so behaviour is unchanged.
const DevOTLPEndpointEnv = "CLRK_DEV_OTEL_ENDPOINT"

// devOTLPEndpoint is captured once at process start. The env doesn't
// change at runtime; reading it on every per-stream sinkRegistry.get
// would be needless work.
var devOTLPEndpoint = strings.TrimSpace(os.Getenv(DevOTLPEndpointEnv))

// sinkShutdownTimeout bounds how long we wait for a replaced sink's
// background workers to flush during cache rebuild. OTLP exporters use
// it as a hard deadline.
const sinkShutdownTimeout = 5 * time.Second

// defaultIncludedContentTypes is the body-capture content-type allow-list
// applied when OTLP.CaptureBody.IncludeContentTypes is empty. Matched by
// case-insensitive prefix on the request/response content-type header.
var defaultIncludedContentTypes = []string{
	"application/json",
	"application/x-ndjson",
	"text/event-stream",
}

// egSink is a per-EgressGateway capture configuration: the resolved Sink
// (OTLP or fallback slog), the body-capture cap, the content-type
// allow-list, the AIProviderRoute matcher built from APRs attached to
// this EG, the credential-injection table built from CIPs attached to
// this EG (directly or via APR), plus a snapshot of the spec + APR list-
// version + CIP list-version we use to decide whether to rebuild on
// subsequent stream starts.
type egSink struct {
	sink            Sink
	maxCaptureBytes int
	includedTypes   []string
	specSnapshot    *clrkv1alpha1.OTLPLogsSinkSpec
	shutdown        func(context.Context) error

	routes        *routeTable
	routesVersion string

	mcpRoutes        *mcpRouteTable
	mcpRoutesVersion string

	creds        *credTable
	credsVersion string
}

// sinkRegistry caches one egSink per EgressGateway, keyed by ns/name.
// Process() calls get(ctx) once at stream start; the registry resolves
// the calling Envoy back to its EG, fetches the EG from the
// controller-runtime cache, and returns the cached sink (rebuilding it
// if EG.ResourceVersion has changed).
type sinkRegistry struct {
	client client.Client

	mu sync.Mutex
	by map[types.NamespacedName]*egSink
}

func newSinkRegistry(c client.Client) *sinkRegistry {
	return &sinkRegistry{
		client: c,
		by:     make(map[types.NamespacedName]*egSink),
	}
}

// get resolves the EgressGateway from ctx (stamped by the egidentity
// gRPC interceptor off the incoming HTTP/2 :authority header) and
// returns the (possibly newly-built) cached egSink. Returns an error
// when no EG identity was attached or the controller-runtime client
// isn't wired — callers fall back to slogSink in that case.
//
// Cache invalidation is keyed on the OTLP spec snapshot, not
// EG.ResourceVersion. The EG controller writes status frequently
// (cert rotation, condition flips); rebuilding the OTLP exporter pair
// on every status write would churn batcher goroutines for no signal
// change.
func (r *sinkRegistry) get(ctx context.Context) (*egSink, error) {
	if r.client == nil {
		return nil, fmt.Errorf("no kube client configured")
	}
	key, err := egidentity.MustFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve EgressGateway: %w", err)
	}

	var eg clrkv1alpha1.EgressGateway
	if err := r.client.Get(ctx, key, &eg); err != nil {
		return nil, fmt.Errorf("get EgressGateway %s: %w", key, err)
	}

	// List APRs cluster-wide; a route in any namespace can attach to
	// this EG via parentRef. The cached client backs this with an
	// informer so the call is cheap. Pass routes to buildRouteTable
	// only when the spec or APR set changed; otherwise reuse the
	// existing cached entry.
	var aprs clrkv1alpha1.AIProviderRouteList
	if err := r.client.List(ctx, &aprs); err != nil {
		return nil, fmt.Errorf("list AIProviderRoutes: %w", err)
	}
	aprVersion := aiproviderRoutesVersion(aprs.Items, key)

	// List MCPRoutes cluster-wide for the same reason as APRs: an
	// MCPRoute in any namespace can attach via parentRef. The version
	// fingerprint participates in the cache-equality check below so a
	// rule edit invalidates the snapshot.
	var mcprs clrkv1alpha1.MCPRouteList
	if err := r.client.List(ctx, &mcprs); err != nil {
		return nil, fmt.Errorf("list MCPRoutes: %w", err)
	}
	mcpVersion := mcpRoutesVersion(mcprs.Items, key)

	// Same shape for CredentialInjectionPolicies. Cluster-wide list
	// because policies can attach across namespaces via parentRefs.
	var cips clrkv1alpha1.CredentialInjectionPolicyList
	if err := r.client.List(ctx, &cips); err != nil {
		return nil, fmt.Errorf("list CredentialInjectionPolicies: %w", err)
	}
	credVersion := CredPoliciesVersion(ctx, r.client, cips.Items, aprs.Items, key)

	r.mu.Lock()
	if hit, ok := r.by[key]; ok &&
		reflect.DeepEqual(hit.specSnapshot, eg.Spec.OTLP) &&
		hit.routesVersion == aprVersion &&
		hit.mcpRoutesVersion == mcpVersion &&
		hit.credsVersion == credVersion {
		r.mu.Unlock()
		return hit, nil
	}
	prev := r.by[key]
	r.mu.Unlock()

	// Build the new sink before tearing down the old one — if the rebuild
	// errors (bad endpoint, etc.) the previous sink stays live so we
	// keep emitting somewhere instead of dropping records on the floor.
	built, err := buildEgSink(ctx, &eg)
	if err != nil {
		return nil, fmt.Errorf("build sink: %w", err)
	}
	built.routes = buildRouteTable(key.Namespace, key.Name, aprs.Items)
	built.routesVersion = aprVersion
	built.mcpRoutes = buildMCPRouteTable(ctx, key.Namespace, key.Name, mcprs.Items)
	built.mcpRoutesVersion = mcpVersion
	built.creds = buildCredTable(ctx, r.client, key.Namespace, key.Name, aprs.Items, cips.Items)
	built.credsVersion = credVersion

	r.mu.Lock()
	r.by[key] = built
	r.mu.Unlock()

	// Shutdown the predecessor in the background — it owns an OTLP
	// exporter goroutine that needs to flush. Caller doesn't wait.
	if prev != nil && prev.shutdown != nil {
		go func(sd func(context.Context) error) {
			shutCtx, cancel := context.WithTimeout(context.Background(), sinkShutdownTimeout)
			defer cancel()
			_ = sd(shutCtx)
		}(prev.shutdown)
	}
	return built, nil
}

// aiproviderRoutesVersion fingerprints the list of APRs that attach to
// the given EG. We only consider routes that pass the parentRef check
// (irrelevant routes shouldn't trigger a rebuild). The fingerprint is
// just sorted (name, resourceVersion) pairs; correctness only requires
// that any meaningful change to the attached set produces a different
// string.
func aiproviderRoutesVersion(routes []clrkv1alpha1.AIProviderRoute, eg types.NamespacedName) string {
	var parts []string
	for _, r := range routes {
		if !routeAttachesTo(r, eg.Namespace, eg.Name) {
			continue
		}
		parts = append(parts, r.Namespace+"/"+r.Name+"@"+r.ResourceVersion)
	}
	if len(parts) == 0 {
		return ""
	}
	// Sort for determinism — list order from the cached client isn't
	// guaranteed stable across calls.
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// mcpRoutesVersion mirrors aiproviderRoutesVersion for MCPRoutes. Kept
// separate so a change to one route family doesn't invalidate the
// other's fingerprint and force an unrelated rebuild.
func mcpRoutesVersion(routes []clrkv1alpha1.MCPRoute, eg types.NamespacedName) string {
	var parts []string
	for _, r := range routes {
		if !mcpRouteAttachesTo(r, eg.Namespace, eg.Name) {
			continue
		}
		parts = append(parts, r.Namespace+"/"+r.Name+"@"+r.ResourceVersion)
	}
	if len(parts) == 0 {
		return ""
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// shutdownAll tears down every cached sink. Best-effort: errors are
// logged via slog and don't block teardown of the rest.
func (r *sinkRegistry) shutdownAll(ctx context.Context) {
	r.mu.Lock()
	sinks := r.by
	r.by = make(map[types.NamespacedName]*egSink)
	r.mu.Unlock()
	for _, s := range sinks {
		if s.shutdown == nil {
			continue
		}
		_ = s.shutdown(ctx)
	}
}

// buildEgSink builds a fresh egSink for the given EG spec. Picks OTLP
// when an endpoint is configured (either on the EG or via the dev
// CLRK_DEV_OTEL_ENDPOINT env), falls back to slogSink otherwise.
func buildEgSink(ctx context.Context, eg *clrkv1alpha1.EgressGateway) (*egSink, error) {
	maxBytes := captureMaxBytesDefault
	var included []string
	if eg.Spec.OTLP != nil && eg.Spec.OTLP.CaptureBody != nil {
		cb := eg.Spec.OTLP.CaptureBody
		if cb.MaxBytes != nil && *cb.MaxBytes > 0 {
			maxBytes = int(*cb.MaxBytes)
		}
		if len(cb.IncludeContentTypes) > 0 {
			included = lowerAll(cb.IncludeContentTypes)
		}
	}
	if len(included) == 0 {
		included = defaultIncludedContentTypes
	}

	out := &egSink{
		maxCaptureBytes: maxBytes,
		includedTypes:   included,
		specSnapshot:    eg.Spec.OTLP.DeepCopy(),
	}

	if endpoint := effectiveOTLPEndpoint(eg.Spec.OTLP); endpoint != "" {
		exportSpec := clrkv1alpha1.OTLPLogsSinkSpec{Endpoint: endpoint}
		if eg.Spec.OTLP != nil {
			exportSpec.Headers = eg.Spec.OTLP.Headers
		}
		sink, shutdown, err := newOTLPSink(ctx, exportSpec)
		if err != nil {
			return nil, fmt.Errorf("otlp sink: %w", err)
		}
		out.sink = sink
		out.shutdown = shutdown
		return out, nil
	}

	out.sink = slogSink{}
	return out, nil
}

// effectiveOTLPEndpoint returns the URL to dial. Per-EG config always
// wins. When the EG didn't set an endpoint and clrk dev is running
// (devOTLPEndpoint is set), the dev receiver is used so capture lands
// in the TUI's otel panes instead of the controller-manager pane.
func effectiveOTLPEndpoint(spec *clrkv1alpha1.OTLPLogsSinkSpec) string {
	if spec != nil && spec.Endpoint != "" {
		return spec.Endpoint
	}
	return devOTLPEndpoint
}

func lowerAll(in []string) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = strings.ToLower(strings.TrimSpace(s))
	}
	return out
}

// contentTypeIncluded reports whether the given Content-Type header
// matches any prefix in includedTypes. An empty includedTypes slice
// means "capture everything" — used when EG resolution fell back to
// slogSink and we never derived a per-EG allow-list.
func contentTypeIncluded(contentType string, includedTypes []string) bool {
	if len(includedTypes) == 0 {
		return true
	}
	ct := strings.ToLower(contentType)
	for _, t := range includedTypes {
		if strings.HasPrefix(ct, t) {
			return true
		}
	}
	return false
}
