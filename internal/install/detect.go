package install

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// DetectInstall reports whether a clrk control plane already exists in
// namespace ns and, if so, the version stamped on its controller-manager
// Deployment (empty if unstamped). Used by preflight (install-vs-upgrade hint)
// and by the upgrade version gate.
func DetectInstall(ctx context.Context, c client.Client, ns string) (exists bool, version string, err error) {
	var dep appsv1.Deployment
	key := client.ObjectKey{Namespace: ns, Name: ControllerManagerName}
	if err := c.Get(ctx, key, &dep); err != nil {
		if apierrors.IsNotFound(err) {
			return false, "", nil
		}
		return false, "", err
	}
	return true, dep.Annotations[clrkv1alpha1.InstalledVersionAnnotation], nil
}

// CurrentWorkerCount returns the replica count of the existing default WorkerPool
// in ns so `clrk upgrade` can carry it forward instead of resetting the fleet to
// the --workers flag default. ForceOwnership SSA would otherwise overwrite
// spec.replicas back to the default, silently scaling down an operator's fleet
// (set at install or scaled later). ok is false when the WorkerPool is absent or
// carries no explicit replica count, in which case the caller keeps its default.
func CurrentWorkerCount(ctx context.Context, c client.Client, ns, name string) (count int, ok bool, err error) {
	var wp clrkv1alpha1.WorkerPool
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &wp); err != nil {
		if apierrors.IsNotFound(err) {
			return 0, false, nil
		}
		return 0, false, err
	}
	if wp.Spec.Replicas == nil {
		return 0, false, nil
	}
	return int(*wp.Spec.Replicas), true, nil
}
