package install

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// installedVersionAnnotation is the annotation the installer stamps the clrk
// version onto (cm Deployment + namespace). Kept here as a literal until M5
// promotes it to api/clrk/v1alpha1 alongside the other annotation constants.
const installedVersionAnnotation = "clrk.apoxy.dev/installed-version"

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
	return true, dep.Annotations[installedVersionAnnotation], nil
}
