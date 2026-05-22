package ingress

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

// EgressGatewayEnv pins the ingress ext_proc to a specific
// EgressGateway for OTLP. Format: "namespace/name". When unset, the
// server falls back to "if exactly one EG exists in my own namespace,
// use it" so `clrk dev` works without explicit identity.
const EgressGatewayEnv = "CLRK_INGRESS_OTLP_GATEWAY"

// BuildEmitter always returns a non-nil Emitter; any discovery /
// construction failure logs the reason and falls back to Noop so
// ingress dispatch never blocks on telemetry.
func BuildEmitter(ctx context.Context, api client.Reader, ns string) otelemit.Emitter {
	log := ctrl.LoggerFrom(ctx).WithName("ingress-otel")

	eg, err := resolveEgressGateway(ctx, api, ns)
	if err != nil {
		log.Info("EgressGateway not resolved; ingress OTLP emitter is noop", "reason", err.Error())
		return otelemit.Noop()
	}
	if eg.Spec.OTLP == nil || strings.TrimSpace(eg.Spec.OTLP.Endpoint) == "" {
		log.Info("EgressGateway has no OTLP endpoint; ingress OTLP emitter is noop")
		return otelemit.Noop()
	}

	// POD_NAME / POD_NAMESPACE come from the CM Deployment via Downward
	// API. Same empty-safe shape as the egress sink resource.
	podName := os.Getenv("POD_NAME")
	attrs := []attribute.KeyValue{
		semconv.ServiceName("clrk"),
		semconv.K8SNamespaceName(ns),
		attribute.String(otelemit.AttrComponent, "ingress"),
	}
	if podName != "" {
		attrs = append(attrs, semconv.ServiceInstanceID(podName), semconv.K8SPodName(podName))
	}
	res := resource.NewWithAttributes(semconv.SchemaURL, attrs...)

	em, err := otelemit.New(ctx, *eg.Spec.OTLP, "github.com/apoxy-dev/clrk/internal/extproc/ingress", res)
	if err != nil {
		log.Error(err, "Building ingress OTLP emitter; continuing with noop",
			"egress_gateway", eg.Namespace+"/"+eg.Name,
			"endpoint", eg.Spec.OTLP.Endpoint)
		return otelemit.Noop()
	}
	log.Info("Ingress OTLP emitter wired",
		"egress_gateway", eg.Namespace+"/"+eg.Name,
		"endpoint", eg.Spec.OTLP.Endpoint)
	return em
}

func resolveEgressGateway(ctx context.Context, api client.Reader, ns string) (*clrkv1alpha1.EgressGateway, error) {
	if raw := strings.TrimSpace(os.Getenv(EgressGatewayEnv)); raw != "" {
		egNS, name, ok := strings.Cut(raw, "/")
		if !ok || egNS == "" || name == "" {
			return nil, fmt.Errorf("%s=%q is not in namespace/name format", EgressGatewayEnv, raw)
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
		return nil, fmt.Errorf("no EgressGateway in namespace %s and %s unset", ns, EgressGatewayEnv)
	case 1:
		return &egs.Items[0], nil
	default:
		names := make([]string, 0, len(egs.Items))
		for _, eg := range egs.Items {
			names = append(names, eg.Name)
		}
		return nil, fmt.Errorf("multiple EgressGateways in %s (%s); set %s to disambiguate",
			ns, strings.Join(names, ","), EgressGatewayEnv)
	}
}
