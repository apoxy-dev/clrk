package controller

import (
	"context"
	"fmt"
	"reflect"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
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
	logger := log.FromContext(ctx)

	var ta clrkv1alpha1.TaskAgent
	if err := r.Get(ctx, req.NamespacedName, &ta); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	now := metav1.Now()

	// 1. Create or update Gateway.
	desiredGW := desiredGateway(&ta)
	if err := ctrl.SetControllerReference(&ta, desiredGW, r.Scheme); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting Gateway owner reference: %w", err)
	}
	var existingGW gwapiv1.Gateway
	gwKey := types.NamespacedName{Name: ta.Name, Namespace: ta.Namespace}
	if err := r.Get(ctx, gwKey, &existingGW); apierrors.IsNotFound(err) {
		logger.Info("Creating Gateway", "name", desiredGW.Name)
		if err := r.Create(ctx, desiredGW); err != nil {
			return ctrl.Result{}, fmt.Errorf("creating Gateway: %w", err)
		}
		existingGW = *desiredGW
	} else if err != nil {
		return ctrl.Result{}, fmt.Errorf("getting Gateway: %w", err)
	} else if !reflect.DeepEqual(existingGW.Spec, desiredGW.Spec) {
		existingGW.Spec = desiredGW.Spec
		if err := r.Update(ctx, &existingGW); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating Gateway: %w", err)
		}
	}

	// 2. Set GatewayReady condition from the Gateway's own Programmed
	// condition. Multiple TaskAgent reconcilers write distinct condition
	// types on the same ta.Status.Conditions slice; meta.SetStatusCondition
	// matches by Type so this is safe.
	gatewayReady := metav1.Condition{
		Type:               "GatewayReady",
		ObservedGeneration: ta.Generation,
		LastTransitionTime: now,
	}
	programmed := false
	for _, cond := range existingGW.Status.Conditions {
		if cond.Type == string(gwapiv1.GatewayConditionProgrammed) && cond.Status == metav1.ConditionTrue {
			programmed = true
			break
		}
	}
	if programmed {
		gatewayReady.Status = metav1.ConditionTrue
		gatewayReady.Reason = "GatewayProgrammed"
		gatewayReady.Message = "Gateway has Programmed=True"
	} else {
		gatewayReady.Status = metav1.ConditionFalse
		gatewayReady.Reason = "GatewayNotProgrammed"
		gatewayReady.Message = "Gateway exists but Programmed != True"
	}
	meta.SetStatusCondition(&ta.Status.Conditions, gatewayReady)

	// 3. Create or update HTTPRoute.
	desiredHR := desiredHTTPRoute(&ta)
	if err := ctrl.SetControllerReference(&ta, desiredHR, r.Scheme); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting HTTPRoute owner reference: %w", err)
	}
	var existingHR gwapiv1.HTTPRoute
	hrKey := types.NamespacedName{Name: ta.Name, Namespace: ta.Namespace}
	if err := r.Get(ctx, hrKey, &existingHR); apierrors.IsNotFound(err) {
		logger.Info("Creating HTTPRoute", "name", desiredHR.Name)
		if err := r.Create(ctx, desiredHR); err != nil {
			return ctrl.Result{}, fmt.Errorf("creating HTTPRoute: %w", err)
		}
	} else if err != nil {
		return ctrl.Result{}, fmt.Errorf("getting HTTPRoute: %w", err)
	} else if !reflect.DeepEqual(existingHR.Spec, desiredHR.Spec) {
		existingHR.Spec = desiredHR.Spec
		if err := r.Update(ctx, &existingHR); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating HTTPRoute: %w", err)
		}
	}

	// Only patch Status if we actually changed something on it. The
	// revision reconciler owns ObservedGeneration, so leave it alone.
	if err := r.Status().Update(ctx, &ta); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}

	return ctrl.Result{}, nil
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
	extProcGroup := gwapiv1.Group(envoyGatewayGroup)
	extProcKind := gwapiv1.Kind("ExternalProcessor")
	extProcName := gwapiv1.ObjectName(ta.Name + "-ext-proc")

	resolverGroup := gwapiv1.Group(envoyGatewayGroup)
	resolverKind := gwapiv1.Kind("DynamicResolver")
	resolverName := gwapiv1.ObjectName(ta.Name + "-resolver")

	pathPrefix := gwapiv1.PathMatchPathPrefix

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
							Type: gwapiv1.HTTPRouteFilterExtensionRef,
							ExtensionRef: &gwapiv1.LocalObjectReference{
								Group: extProcGroup,
								Kind:  extProcKind,
								Name:  extProcName,
							},
						},
					},
					BackendRefs: []gwapiv1.HTTPBackendRef{
						{
							BackendRef: gwapiv1.BackendRef{
								BackendObjectReference: gwapiv1.BackendObjectReference{
									Group: (*gwapiv1.Group)(&resolverGroup),
									Kind:  (*gwapiv1.Kind)(&resolverKind),
									Name:  resolverName,
								},
							},
						},
					},
				},
			},
		},
	}
}
