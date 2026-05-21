//go:build linux

package egress

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"
)

// ReconcileForTest exposes ConfigWatcher.reconcile to cross-module
// black-box tests under apoxy-cloud//clrk/egress.
func (w *ConfigWatcher) ReconcileForTest(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	return w.reconcile(ctx, req)
}
