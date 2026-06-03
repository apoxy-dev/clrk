package install

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/discovery"
	memory "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// installFieldManager is the server-side-apply field owner for the customer
// installer. Distinct from the dev driver's "clrk-dev" so SSA managedFields
// make the install/upgrade ownership legible — and so a hypothetical cluster
// that was ever both never co-owns a field. ForceOwnership resolves takeover
// either way.
const installFieldManager = "clrk-install"

// newScheme builds the controller-runtime scheme the installer applies through:
// client-go's combined scheme (Namespace/SA/Service/Deployment/RBAC/PVC/...),
// the aggregation API (APIService), and clrk.apoxy.dev (WorkerPool). Mirrors
// drivers.kubeClientFor so the dev and customer paths register the same types.
func newScheme() (*runtime.Scheme, error) {
	sch := runtime.NewScheme()
	if err := scheme.AddToScheme(sch); err != nil {
		return nil, fmt.Errorf("registering core scheme: %w", err)
	}
	if err := apiregistrationv1.AddToScheme(sch); err != nil {
		return nil, fmt.Errorf("registering apiregistration scheme: %w", err)
	}
	if err := clrkv1alpha1.Install(sch); err != nil {
		return nil, fmt.Errorf("registering clrk scheme: %w", err)
	}
	return sch, nil
}

// RemoteCluster is the customer-cluster implementation of Applier: it server-
// side-applies and waits on objects through a controller-runtime client built
// from an operator-supplied kubeconfig + context. It holds no k3d state, so the
// install/upgrade code path links and tests without the k3d toolchain.
type RemoteCluster struct {
	cfg     *rest.Config
	context string
	scheme  *runtime.Scheme
	cl      client.Client
}

// NewRemoteCluster resolves the kubeconfig + context and builds the typed
// client. The returned RemoteCluster satisfies Applier.
func NewRemoteCluster(kubeconfigPath, contextName string) (*RemoteCluster, error) {
	cfg, resolvedContext, err := LoadRESTConfig(kubeconfigPath, contextName)
	if err != nil {
		return nil, err
	}
	sch, err := newScheme()
	if err != nil {
		return nil, err
	}
	cl, err := client.New(cfg, client.Options{Scheme: sch})
	if err != nil {
		return nil, fmt.Errorf("building client for context %q: %w", resolvedContext, err)
	}
	return &RemoteCluster{cfg: cfg, context: resolvedContext, scheme: sch, cl: cl}, nil
}

// Context returns the resolved kubeconfig context name (for operator-facing
// "about to install into <context>" messaging).
func (r *RemoteCluster) Context() string { return r.context }

// RESTConfig returns the resolved rest.Config (e.g. for crds.Install, discovery,
// or SelfSubjectAccessReview clients).
func (r *RemoteCluster) RESTConfig() *rest.Config { return r.cfg }

// KubeClient returns the controller-runtime client.
func (r *RemoteCluster) KubeClient(ctx context.Context) (client.Client, error) {
	return r.cl, nil
}

// Discovery returns a memory-cached discovery client for the target cluster,
// used by preflight to detect API groups, CRDs, and the aggregation layer.
func (r *RemoteCluster) Discovery() (discovery.CachedDiscoveryInterface, error) {
	dc, err := discovery.NewDiscoveryClientForConfig(r.cfg)
	if err != nil {
		return nil, fmt.Errorf("building discovery client: %w", err)
	}
	return memory.NewMemCacheClient(dc), nil
}

// RESTMapper returns a deferred discovery REST mapper, used by the plan engine
// to resolve REST mappings for arbitrary objects.
func (r *RemoteCluster) RESTMapper() (*restmapper.DeferredDiscoveryRESTMapper, error) {
	dc, err := r.Discovery()
	if err != nil {
		return nil, err
	}
	return restmapper.NewDeferredDiscoveryRESTMapper(dc), nil
}

// ApplyObjects server-side-applies one or more typed objects with ForceOwnership
// under the install field manager. Same semantics as the dev driver, so the
// shared builders + ApplyAndVerify behave identically against either.
func (r *RemoteCluster) ApplyObjects(ctx context.Context, objs ...client.Object) error {
	for _, o := range objs {
		if err := r.cl.Patch(ctx, o, client.Apply, client.ForceOwnership, client.FieldOwner(installFieldManager)); err != nil {
			return fmt.Errorf("applying %s/%s: %w", o.GetNamespace(), o.GetName(), err)
		}
	}
	return nil
}

// EnsureNamespace idempotently applies a Namespace.
func (r *RemoteCluster) EnsureNamespace(ctx context.Context, ns string) error {
	return r.ApplyObjects(ctx, &corev1.Namespace{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
		ObjectMeta: metav1.ObjectMeta{Name: ns},
	})
}

// WaitDeploymentAvailable blocks until ns/name reports DeploymentAvailable=True
// or timeout elapses.
func (r *RemoteCluster) WaitDeploymentAvailable(ctx context.Context, ns, name string, timeout time.Duration) error {
	pollErr := wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, timeout, true, func(ctx context.Context) (bool, error) {
		var dep appsv1.Deployment
		if err := r.cl.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &dep); err != nil {
			return false, nil
		}
		for _, cond := range dep.Status.Conditions {
			if cond.Type == appsv1.DeploymentAvailable && cond.Status == corev1.ConditionTrue {
				return true, nil
			}
		}
		return false, nil
	})
	if wait.Interrupted(pollErr) {
		return fmt.Errorf("deployment %s/%s not available within %s", ns, name, timeout)
	}
	return pollErr
}

// Compile-time assurance RemoteCluster satisfies the shared Applier.
var _ Applier = (*RemoteCluster)(nil)
