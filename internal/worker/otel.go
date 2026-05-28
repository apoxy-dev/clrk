package worker

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	ctrl "sigs.k8s.io/controller-runtime"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/otelemit"
)

// EgressGatewayEnv pins the worker to a specific EgressGateway for
// telemetry. Format: "namespace/name". When unset, the worker falls
// back to "if exactly one EG exists in my own namespace, use it" so
// `clrk dev` works without explicit identity. The EG identity is
// stamped as the clrk.egress_gateway resource attribute on every
// OTLP record so cm's receiver can pick the right per-EG forwarder.
const EgressGatewayEnv = "CLRK_EGRESS_GATEWAY"

// buildEmitter always returns a non-nil Emitter; any discovery /
// construction failure logs the reason and falls back to Noop so
// telemetry never blocks the data plane.
func buildEmitter(ctx context.Context, r *Runtime) otelemit.Emitter {
	log := ctrl.LoggerFrom(ctx).WithName("worker-otel")

	endpoint := otelemit.ProducerEndpoint()
	if endpoint == "" {
		log.Info("OTLP emitter is noop: no CLRK_CM_OTLP_ENDPOINT or CLRK_DEV_OTEL_ENDPOINT")
		return otelemit.Noop()
	}

	egRef := ""
	if eg, err := otelemit.ResolveEgressGateway(ctx, r.Manager.GetAPIReader(), r.Namespace, EgressGatewayEnv); err != nil {
		log.Info("EgressGateway not resolved; OTLP records will lack EGRef attribute",
			"reason", err.Error())
	} else {
		egRef = eg.Namespace + "/" + eg.Name
	}

	attrs := []attribute.KeyValue{
		semconv.ServiceName("clrk"),
		semconv.ServiceInstanceID(r.PodName),
		semconv.K8SPodName(r.PodName),
		semconv.K8SNamespaceName(r.Namespace),
		attribute.String(otelemit.AttrComponent, "worker"),
		attribute.String(otelemit.AttrWorkerPool, r.PoolName),
	}
	if egRef != "" {
		attrs = append(attrs, attribute.String(otelemit.AttrEgressGateway, egRef))
	}
	res := resource.NewWithAttributes(semconv.SchemaURL, attrs...)

	spec := clrkv1alpha1.OTLPLogsSinkSpec{Endpoint: endpoint}
	em, err := otelemit.New(ctx, spec, "github.com/apoxy-dev/clrk/internal/worker", res)
	if err != nil {
		log.Error(err, "Building OTLP emitter; continuing with noop",
			"endpoint", endpoint, "egress_gateway", egRef)
		return otelemit.Noop()
	}
	log.Info("OTLP emitter wired", "endpoint", endpoint, "egress_gateway", egRef)
	return em
}
