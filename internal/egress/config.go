//go:build linux

package egress

import (
	"context"
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// ConfigWatcher watches egress CRDs and rebuilds the Router's route table
// on any change.
type ConfigWatcher struct {
	client.Client
	router    *Router
	namespace string
}

// NewConfigWatcher creates a watcher that rebuilds the given router when
// egress CRDs change.
func NewConfigWatcher(c client.Client, router *Router, namespace string) *ConfigWatcher {
	return &ConfigWatcher{
		Client:    c,
		router:    router,
		namespace: namespace,
	}
}

// SetupWithManager registers informer-based watches for all egress CRDs.
func (w *ConfigWatcher) SetupWithManager(mgr manager.Manager) error {
	// Watch EgressGateway changes.
	if err := ctrl.NewControllerManagedBy(mgr).
		For(&clrkv1alpha1.EgressGateway{}).
		Named("egress-gateway-watcher").
		Complete(reconcile.Func(w.reconcile)); err != nil {
		return fmt.Errorf("setting up EgressGateway watcher: %w", err)
	}

	// Watch EgressL4Route changes.
	if err := ctrl.NewControllerManagedBy(mgr).
		For(&clrkv1alpha1.EgressL4Route{}).
		Named("egress-l4route-watcher").
		Complete(reconcile.Func(w.reconcile)); err != nil {
		return fmt.Errorf("setting up EgressL4Route watcher: %w", err)
	}

	// Watch LoggingPolicy. The router doesn't read its fields yet; the
	// watcher exists so applying a LoggingPolicy CR doesn't blow up the
	// worker on an unknown-CRD informer error and so that future field
	// consumption picks up changes via the same reconcile path.
	if err := ctrl.NewControllerManagedBy(mgr).
		For(&clrkv1alpha1.LoggingPolicy{}).
		Named("logging-policy-watcher").
		Complete(reconcile.Func(w.reconcile)); err != nil {
		return fmt.Errorf("setting up LoggingPolicy watcher: %w", err)
	}

	return nil
}

// reconcile is called when any watched CRD changes. It re-lists all egress
// resources and rebuilds the router's routing table atomically.
func (w *ConfigWatcher) reconcile(ctx context.Context, _ ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx).WithName("egress-config")

	var gateways clrkv1alpha1.EgressGatewayList
	if err := w.List(ctx, &gateways, client.InNamespace(w.namespace)); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing EgressGateways: %w", err)
	}

	var l4Routes clrkv1alpha1.EgressL4RouteList
	if err := w.List(ctx, &l4Routes, client.InNamespace(w.namespace)); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing EgressL4Routes: %w", err)
	}

	log.Info("Rebuilding egress route table",
		"gateways", len(gateways.Items),
		"l4routes", len(l4Routes.Items),
	)

	w.router.Rebuild(gateways.Items, l4Routes.Items)

	return ctrl.Result{}, nil
}
