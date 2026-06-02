package ingress

import (
	"context"
	"os"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/otelemit"
)

// EgressGatewayEnv pins the ingress ext_proc to a specific
// EgressGateway for telemetry. Format: "namespace/name". When unset,
// the server falls back to "if exactly one EG exists in my own
// namespace, use it". The EG identity is stamped as the
// clrk.egress_gateway resource attribute on every signal so cm's
// receiver can pick the right per-EG forwarder.
const EgressGatewayEnv = "CLRK_INGRESS_OTLP_GATEWAY"

// BuildEmitter always returns a non-nil Emitter; any discovery /
// construction failure logs the reason and falls back to Noop so
// ingress dispatch never blocks on telemetry.
func BuildEmitter(ctx context.Context, api client.Reader, ns string) otelemit.Emitter {
	log := ctrl.LoggerFrom(ctx).WithName("ingress-otel")

	endpoint := otelemit.ProducerEndpoint()
	if endpoint == "" {
		log.Info("Ingress OTLP emitter is noop: no CLRK_CM_OTLP_ENDPOINT or CLRK_DEV_OTEL_ENDPOINT")
		return otelemit.Noop()
	}

	egRef := ""
	if eg, err := otelemit.ResolveEgressGateway(ctx, api, ns, EgressGatewayEnv); err != nil {
		log.Info("EgressGateway not resolved; OTLP records will lack EGRef attribute",
			"reason", err.Error())
	} else {
		egRef = eg.Namespace + "/" + eg.Name
	}

	podName := os.Getenv("POD_NAME")
	attrs := []attribute.KeyValue{
		semconv.ServiceName("clrk"),
		semconv.K8SNamespaceName(ns),
		attribute.String(otelemit.AttrComponent, otelemit.ComponentIngressExtproc),
	}
	if podName != "" {
		attrs = append(attrs, semconv.ServiceInstanceID(podName), semconv.K8SPodName(podName))
	}
	if egRef != "" {
		attrs = append(attrs, attribute.String(otelemit.AttrEgressGateway, egRef))
	}
	res := resource.NewWithAttributes(semconv.SchemaURL, attrs...)

	spec := clrkv1alpha1.OTLPLogsSinkSpec{Endpoint: endpoint}
	em, err := otelemit.New(ctx, spec, "github.com/apoxy-dev/clrk/internal/extproc/ingress", res)
	if err != nil {
		log.Error(err, "Building ingress OTLP emitter; continuing with noop",
			"endpoint", endpoint, "egress_gateway", egRef)
		return otelemit.Noop()
	}
	log.Info("Ingress OTLP emitter wired", "endpoint", endpoint, "egress_gateway", egRef)
	return em
}
