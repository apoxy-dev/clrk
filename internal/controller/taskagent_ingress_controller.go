package controller

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// TaskAgentIngressReconciler owns the k8s-side half of TaskAgent: it
// creates a Gateway + HTTPRoute to front the agent's HTTP trigger. It is
// only useful when Gateway API is available in the cluster; wire it in
// cluster mode only.
type TaskAgentIngressReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=gateways,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=httproutes,verbs=get;list;watch;create;update;patch

func (r *TaskAgentIngressReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var ta clrkv1alpha1.TaskAgent
	if err := r.Get(ctx, req.NamespacedName, &ta); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	taStatusBase := ta.DeepCopy()

	gw := &gwapiv1.Gateway{ObjectMeta: metav1.ObjectMeta{Name: ta.Name, Namespace: ta.Namespace}}
	if err := createOrUpdateWithRetry(ctx, r.Client, gw, func() error {
		desired := desiredGateway(&ta)
		gw.Labels = desired.Labels
		gw.Spec = desired.Spec
		return ctrl.SetControllerReference(&ta, gw, r.Scheme)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling Gateway: %w", err)
	}

	gatewayReady := metav1.Condition{
		Type:               condGatewayReady,
		ObservedGeneration: ta.Generation,
		LastTransitionTime: metav1.Now(),
	}
	if gatewayProgrammed(gw) {
		gatewayReady.Status = metav1.ConditionTrue
		gatewayReady.Reason = "GatewayProgrammed"
		gatewayReady.Message = "Gateway has Programmed=True"
	} else {
		gatewayReady.Status = metav1.ConditionFalse
		gatewayReady.Reason = "GatewayNotProgrammed"
		gatewayReady.Message = "Gateway exists but Programmed != True"
	}
	meta.SetStatusCondition(&ta.Status.Conditions, gatewayReady)

	hr := &gwapiv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Name: ta.Name, Namespace: ta.Namespace}}
	if err := createOrUpdateWithRetry(ctx, r.Client, hr, func() error {
		desired := desiredHTTPRoute(&ta)
		hr.Labels = desired.Labels
		hr.Spec = desired.Spec
		return ctrl.SetControllerReference(&ta, hr, r.Scheme)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling HTTPRoute: %w", err)
	}

	if err := r.Status().Patch(ctx, &ta, client.MergeFrom(taStatusBase)); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}

	return ctrl.Result{}, nil
}

func gatewayProgrammed(gw *gwapiv1.Gateway) bool {
	for _, cond := range gw.Status.Conditions {
		if cond.Type == string(gwapiv1.GatewayConditionProgrammed) && cond.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

func (r *TaskAgentIngressReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("taskagent-ingress").
		For(&clrkv1alpha1.TaskAgent{}).
		Owns(&gwapiv1.Gateway{}).
		Owns(&gwapiv1.HTTPRoute{}).
		Complete(r)
}

func desiredGateway(ta *clrkv1alpha1.TaskAgent) *gwapiv1.Gateway {
	return &gwapiv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ta.Name,
			Namespace: ta.Namespace,
			Labels: map[string]string{
				labelAgent:     ta.Name,
				labelComponent: "ingress",
			},
		},
		Spec: gwapiv1.GatewaySpec{
			GatewayClassName: gwapiv1.ObjectName(defaultGatewayClassName),
			Listeners: []gwapiv1.Listener{
				{
					Name:     "http",
					Port:     80,
					Protocol: gwapiv1.HTTPProtocolType,
				},
			},
		},
	}
}

func desiredHTTPRoute(ta *clrkv1alpha1.TaskAgent) *gwapiv1.HTTPRoute {
	pathPrefix := gwapiv1.PathMatchPathPrefix

	// Backend is the WorkerPool's Service, port `dispatch`. EG resolves
	// the Service to live Endpoints (default routingType=Endpoint) and
	// load-balances across worker pods. Per-request muxing to the right
	// TaskAgent happens at the worker via the X-Clrk-TaskAgent header
	// injected by the RequestHeaderModifier filter below.
	wpSvcName := gwapiv1.ObjectName(ta.Spec.WorkerPoolRef + "-workers")
	wpSvcPort := gwapiv1.PortNumber(DispatchPort)

	return &gwapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ta.Name,
			Namespace: ta.Namespace,
			Labels: map[string]string{
				labelAgent:     ta.Name,
				labelComponent: "ingress",
			},
		},
		Spec: gwapiv1.HTTPRouteSpec{
			CommonRouteSpec: gwapiv1.CommonRouteSpec{
				ParentRefs: []gwapiv1.ParentReference{
					{
						Name: gwapiv1.ObjectName(ta.Name),
					},
				},
			},
			Rules: []gwapiv1.HTTPRouteRule{
				{
					Matches: []gwapiv1.HTTPRouteMatch{
						{
							Path: &gwapiv1.HTTPPathMatch{
								Type:  &pathPrefix,
								Value: ptr.To("/"),
							},
						},
					},
					Filters: []gwapiv1.HTTPRouteFilter{
						{
							Type: gwapiv1.HTTPRouteFilterRequestHeaderModifier,
							RequestHeaderModifier: &gwapiv1.HTTPHeaderFilter{
								Set: []gwapiv1.HTTPHeader{
									{
										Name:  "X-Clrk-TaskAgent",
										Value: ta.Namespace + "/" + ta.Name,
									},
									{
										Name:  "X-Clrk-Trigger",
										Value: "http",
									},
								},
							},
						},
					},
					BackendRefs: []gwapiv1.HTTPBackendRef{
						{
							BackendRef: gwapiv1.BackendRef{
								BackendObjectReference: gwapiv1.BackendObjectReference{
									Name: wpSvcName,
									Port: &wpSvcPort,
								},
							},
						},
					},
				},
			},
		},
	}
}
