package worker

import (
	"context"
	"fmt"
	"os"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/otelemit"
)

// EgressGatewayEnv pins the worker to a specific EgressGateway for
// telemetry. Format: "namespace/name". When unset, the worker falls
// back to "if exactly one EG exists in my own namespace, use it" so
// `clrk dev` works without explicit identity.
const EgressGatewayEnv = "CLRK_EGRESS_GATEWAY"

// buildEmitter always returns a non-nil Emitter; any discovery /
// construction failure logs the reason and falls back to Noop so
// telemetry never blocks the data plane.
func buildEmitter(ctx context.Context, r *Runtime) otelemit.Emitter {
	log := ctrl.LoggerFrom(ctx).WithName("worker-otel")

	eg, err := resolveEgressGateway(ctx, r)
	if err != nil {
		log.Info("EgressGateway not resolved; OTLP emitter is noop", "reason", err.Error())
		return otelemit.Noop()
	}
	if eg.Spec.OTLP == nil || strings.TrimSpace(eg.Spec.OTLP.Endpoint) == "" {
		log.Info("EgressGateway has no OTLP endpoint; OTLP emitter is noop")
		return otelemit.Noop()
	}

	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName("clrk-worker"),
		semconv.K8SPodName(r.PodName),
		semconv.K8SNamespaceName(r.Namespace),
		attribute.String(otelemit.AttrComponent, "worker"),
		attribute.String(otelemit.AttrWorkerPool, r.PoolName),
	)

	em, err := otelemit.New(ctx, *eg.Spec.OTLP, "github.com/apoxy-dev/clrk/internal/worker", res)
	if err != nil {
		log.Error(err, "Building OTLP emitter; continuing with noop",
			"egress_gateway", eg.Namespace+"/"+eg.Name,
			"endpoint", eg.Spec.OTLP.Endpoint)
		return otelemit.Noop()
	}
	log.Info("OTLP emitter wired",
		"egress_gateway", eg.Namespace+"/"+eg.Name,
		"endpoint", eg.Spec.OTLP.Endpoint)
	return em
}

// resolveEgressGateway uses the manager's uncached API reader because
// Runtime.Start runs before the informer cache is synced.
func resolveEgressGateway(ctx context.Context, r *Runtime) (*clrkv1alpha1.EgressGateway, error) {
	api := r.Manager.GetAPIReader()

	if raw := strings.TrimSpace(os.Getenv(EgressGatewayEnv)); raw != "" {
		ns, name, ok := strings.Cut(raw, "/")
		if !ok || ns == "" || name == "" {
			return nil, fmt.Errorf("%s=%q is not in namespace/name format", EgressGatewayEnv, raw)
		}
		var eg clrkv1alpha1.EgressGateway
		if err := api.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &eg); err != nil {
			return nil, fmt.Errorf("get EgressGateway %s/%s: %w", ns, name, err)
		}
		return &eg, nil
	}

	// Scoping the list to r.Namespace keeps cluster-wide RBAC unnecessary.
	var egs clrkv1alpha1.EgressGatewayList
	if err := api.List(ctx, &egs, client.InNamespace(r.Namespace)); err != nil {
		return nil, fmt.Errorf("list EgressGateways in %s: %w", r.Namespace, err)
	}
	switch len(egs.Items) {
	case 0:
		return nil, fmt.Errorf("no EgressGateway in namespace %s and %s unset", r.Namespace, EgressGatewayEnv)
	case 1:
		return &egs.Items[0], nil
	default:
		names := make([]string, 0, len(egs.Items))
		for _, eg := range egs.Items {
			names = append(names, eg.Name)
		}
		return nil, fmt.Errorf("multiple EgressGateways in %s (%s); set %s to disambiguate",
			r.Namespace, strings.Join(names, ","), EgressGatewayEnv)
	}
}
