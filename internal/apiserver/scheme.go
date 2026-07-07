package apiserver

import (
	egv1alpha1 "github.com/envoyproxy/gateway/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	eventsv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// Scheme is the controller-runtime scheme used by the embedded apiserver and
// the reconcilers attached to it. It is populated at init() time so that the
// apiserver and ctrl.Manager both see the same type registry.
var Scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clrkv1alpha1.Install(Scheme))
	utilruntime.Must(coordinationv1.AddToScheme(Scheme))
	// The TaskAgent and WorkerPool reconcilers Own() Service/Deployment and
	// HTTPRoute/Gateway. They must be registered or controller-runtime fails
	// to start the watch sources for those owned types.
	utilruntime.Must(corev1.AddToScheme(Scheme))
	// events.k8s.io/v1: the notifications recorder (client-go events
	// broadcaster) and the reporter's Event watch resolve Event types through
	// this shared scheme; regarding-object GVKs resolve via the clrk types
	// already installed above.
	utilruntime.Must(eventsv1.AddToScheme(Scheme))
	utilruntime.Must(appsv1.AddToScheme(Scheme))
	// EndpointSlices: the cron HTTPInvoker lists them to find the
	// WorkerPool dispatch endpoint without going through cluster DNS
	// (controller-manager runs outside the cluster in clrk dev).
	utilruntime.Must(discoveryv1.AddToScheme(Scheme))
	utilruntime.Must(gwapiv1.Install(Scheme))
	// Envoy Gateway EnvoyProxy is optional at runtime (the controller
	// tolerates NoMatchError when the CRD is missing), but we still need
	// the type registered so the controller can construct typed objects
	// even on the happy path.
	utilruntime.Must(egv1alpha1.AddToScheme(Scheme))
}
