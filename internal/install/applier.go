// Package install holds the shared control-plane bootstrap engine used by
// both `clrk dev` (against a local k3d cluster) and `clrk install`/`clrk
// upgrade` (against a customer's existing cluster). It owns the typed-object
// manifest builders, the cluster-agnostic apply/verify helpers, the cluster
// preflight, the plan/diff/confirm flow, the progress orchestration, the
// readiness gate, cert wiring, and the upgrade version gate.
//
// The package is deliberately k3d-free: it imports only controller-runtime and
// the leaf constant packages (internal/clickhouse, internal/nats,
// internal/ports, internal/otelemit, internal/eg, internal/crds,
// api/clrk/v1alpha1). The dependency edge points one way — internal/drivers
// (which pulls the heavy k3d v5 tree) imports internal/install, never the
// reverse — so the install/upgrade code path links and tests without any
// docker/k3d toolchain.
package install

import (
	"context"
	"fmt"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// Applier is the cluster-agnostic surface the bootstrap engine needs to apply
// and wait on Kubernetes objects. It is satisfied by both
// *drivers.ClusterDriver (dev/k3d) and *install.RemoteCluster (customer
// cluster), so the same builders + orchestration run against either. The
// implementation owns the server-side-apply field manager (dev uses
// "clrk-dev", install uses "clrk-install"), so callers never thread it
// through.
type Applier interface {
	// ApplyObjects server-side-applies one or more typed objects with
	// ForceOwnership under the implementation's field manager.
	ApplyObjects(ctx context.Context, objs ...client.Object) error
	// KubeClient returns the underlying controller-runtime client (and its
	// scheme) for Get/List/Watch outside the SSA-only path.
	KubeClient(ctx context.Context) (client.Client, error)
	// EnsureNamespace idempotently applies a Namespace.
	EnsureNamespace(ctx context.Context, ns string) error
	// WaitDeploymentAvailable blocks until ns/name reports
	// DeploymentAvailable=True or timeout elapses.
	WaitDeploymentAvailable(ctx context.Context, ns, name string, timeout time.Duration) error
}

// ApplyAndVerify SSAs obj through the Applier (so it inherits the
// implementation's field manager), then re-GETs the stored object and runs
// verify against it. Returns nil iff the apply succeeded *and* verify saw the
// field it cares about. This catches the "SSA returned 200 but the spec didn't
// change" case that left a stale WorkerPool image in kine across multiple
// applies — the embedded apiserver (kine) is the failure surface, which is why
// the WorkerPool apply goes through here.
//
// A free function with a type parameter (Go forbids generic methods) so the
// verify callback is typed at the call site. The freshly-zeroed verify target
// is built via the registered scheme rather than reusing obj, because SSA
// mutates obj (clears TypeMeta) in ways that confuse a subsequent GET.
func ApplyAndVerify[T client.Object](ctx context.Context, a Applier, obj T, verify func(got T) error) error {
	c, err := a.KubeClient(ctx)
	if err != nil {
		return err
	}
	gvk, err := apiutil.GVKForObject(obj, c.Scheme())
	if err != nil {
		return fmt.Errorf("resolving GVK for %T: %w", obj, err)
	}
	key := client.ObjectKeyFromObject(obj)
	if err := a.ApplyObjects(ctx, obj); err != nil {
		return err
	}
	raw, err := c.Scheme().New(gvk)
	if err != nil {
		return fmt.Errorf("instantiating verify object for %s: %w", gvk, err)
	}
	got, ok := raw.(T)
	if !ok {
		return fmt.Errorf("scheme returned %T for %s, expected %T", raw, gvk, obj)
	}
	if err := c.Get(ctx, key, got); err != nil {
		return fmt.Errorf("re-reading %s/%s after apply: %w", key.Namespace, key.Name, err)
	}
	if verify != nil {
		if err := verify(got); err != nil {
			return fmt.Errorf("apply of %s/%s did not persist: %w", key.Namespace, key.Name, err)
		}
	}
	return nil
}
