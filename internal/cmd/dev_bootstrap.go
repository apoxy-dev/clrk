package cmd

import (
	"context"

	"github.com/apoxy-dev/clrk/internal/drivers"
	"github.com/apoxy-dev/clrk/internal/install"
)

// The k3d ClusterDriver is the dev implementation of the shared installer's
// Applier; this assertion fails the build if their surfaces drift.
var _ install.Applier = (*drivers.ClusterDriver)(nil)

// cmAccountName is the controller-manager object name (ServiceAccount /
// Deployment / Service). Owned by the installer package now; aliased here so
// the dev log streamers, status, reload, and the bringUp wait keep referencing
// one name.
const cmAccountName = install.ControllerManagerName

// devProfile is the dev/k3d parameterization of the shared control-plane
// builders. Dev:true plants the host.docker.internal HostAlias, the
// --dev-otlp-fallback-endpoint, the --enable-invocation-test-writes door, and
// the InsecureSkipTLSVerify APIService — exactly the shortcuts the previous
// in-line dev bootstrap used. The customer-facing `clrk install`/`upgrade`
// commands drive the same builders with a cluster-shaped Profile instead.
func devProfile() install.Profile {
	return install.Profile{
		Dev:              true,
		Namespace:        devClrkNamespace,
		WorkerNamespace:  "default",
		Replicas:         1,
		EnableTestWrites: true,
		TLS:              install.TLSInsecureSkipVerify,
	}
}

// bootstrapControllerManager applies the dev control plane (cm SA + cluster-admin
// ClusterRoleBinding + Deployment + Services + APIService + PVCs) via the shared
// installer engine. Thin adapter so bringUp's call site is unchanged; the object
// construction now lives in internal/install. Idempotent.
func bootstrapControllerManager(ctx context.Context, cluster *drivers.ClusterDriver, image, pullPolicy, otelEndpoint, gatewayIP string) error {
	p := devProfile()
	p.ControllerImage = image
	p.PullPolicy = pullPolicy
	p.OTLPFallbackEndpoint = otelEndpoint
	p.HostAliasIP = gatewayIP
	return install.ApplyControllerManager(ctx, cluster, p)
}

// bootstrapDefaultWorkerPool applies the worker SA + cluster-admin
// ClusterRoleBinding and the `default` WorkerPool (the dev-shaped privileged
// Pod) via the shared installer engine. WorkerPoolDeploymentReconciler turns the
// CR into the worker Deployment + Service. Idempotent.
func bootstrapDefaultWorkerPool(ctx context.Context, cluster *drivers.ClusterDriver, image string, replicas int32, pullPolicy string) error {
	p := devProfile()
	p.WorkerImage = image
	p.PullPolicy = pullPolicy
	p.Workers = replicas
	return install.ApplyWorkerPool(ctx, cluster, p)
}
