package controller

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/otelforward"
)

// EgressGatewayOTLPForwardReconciler watches EgressGateway objects
// and keeps internal/otelforward.Registry's per-EG forwarders in
// sync with each EG's Spec.OTLP. The cm OTLP receiver consults the
// registry per inbound request to decide whether to ship a copy of
// the captured signal to the customer's endpoint.
//
// Cm always persists captured signals to the embedded ClickHouse
// via internal/chwriter regardless of forwarder state, so a missing
// or wedged forwarder never costs us the durable copy.
//
// `clrk dev` doesn't go through this reconciler — the cm OTLP
// receiver is wired with an unconditional dev sink (see
// cmd/controller-manager/main.go, --dev-otlp-fallback-endpoint) that
// fans every inbound payload to the TUI receiver regardless of EG
// state. That way the TUI lights up even when no EgressGateway is
// applied.
type EgressGatewayOTLPForwardReconciler struct {
	client.Client

	// Registry is the per-EG forwarder map the receiver reads from.
	Registry *otelforward.Registry
}

// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=egressgateways,verbs=get;list;watch

// Reconcile applies the EG's Spec.OTLP to the registry: install or
// swap when the endpoint changes, remove when the EG is deleted or
// the endpoint is cleared. Spec-equality short-circuiting lives
// inside Registry.Apply.
func (r *EgressGatewayOTLPForwardReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("eg-otlp-forward")
	egRef := req.Namespace + "/" + req.Name

	var eg clrkv1alpha1.EgressGateway
	if err := r.Get(ctx, req.NamespacedName, &eg); err != nil {
		if apierrors.IsNotFound(err) {
			r.Registry.Remove(egRef)
			logger.Info("EgressGateway deleted; OTLP forwarder removed", "eg", egRef)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	spec := clrkv1alpha1.OTLPLogsSinkSpec{}
	if eg.Spec.OTLP != nil {
		spec = *eg.Spec.OTLP
	}
	r.Registry.Apply(egRef, spec)
	return ctrl.Result{}, nil
}

// SetupWithManager wires the reconciler onto EgressGateway events.
// No predicate: change detection happens inside Registry.Apply via
// the spec-DeepEqual short-circuit, so EG status churn is filtered
// without predicate boilerplate.
func (r *EgressGatewayOTLPForwardReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&clrkv1alpha1.EgressGateway{}).
		Named("egressgateway-otlp-forward").
		Complete(r)
}
