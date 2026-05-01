package controller

import (
	"context"

	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// createOrUpdateWithRetry wraps ctrl.CreateOrUpdate with retry-on-conflict.
//
// `ctrl.CreateOrUpdate` does Get→mutate→Update under the hood; that
// Get→Update window races with sibling controllers (envoy-gateway, the
// gateway-api conformance writer, our own status reconcilers) that
// patch the same object on every event. The Update then 409s and the
// reconciler error bubbles up as noise.
//
// Retrying on conflict refetches the latest resourceVersion, replays
// the mutate closure on the fresh object, and re-Updates. Because the
// closure is idempotent (it sets desired-state fields), replays
// converge instead of compounding.
//
// See APO-567.
func createOrUpdateWithRetry(ctx context.Context, c client.Client, obj client.Object, mutate func() error) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		_, err := ctrl.CreateOrUpdate(ctx, c, obj, mutate)
		return err
	})
}
