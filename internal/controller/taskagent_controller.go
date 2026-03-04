package controller

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

const (
	defaultGatewayClassName = "envoy"
	envoyGatewayGroup       = "gateway.envoyproxy.io"
)

// TaskAgentReconciler reconciles TaskAgent objects.
// It validates WorkerPool and EgressGateway refs and sets status conditions.
type TaskAgentReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=taskagents,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=taskagents/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=workerpools,verbs=get;list;watch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=egressgateways,verbs=get;list;watch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=sandboxstates,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=sandboxstates/status,verbs=get
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=gateways,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=httproutes,verbs=get;list;watch;create;update;patch

func (r *TaskAgentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var ta clrkv1alpha1.TaskAgent
	if err := r.Get(ctx, req.NamespacedName, &ta); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	now := metav1.Now()

	// Validate WorkerPool ref.
	wpReady := metav1.Condition{
		Type:               "WorkerPoolReady",
		ObservedGeneration: ta.Generation,
		LastTransitionTime: now,
	}
	var wp clrkv1alpha1.WorkerPool
	wpKey := types.NamespacedName{Name: ta.Spec.WorkerPoolRef, Namespace: ta.Namespace}
	if err := r.Get(ctx, wpKey, &wp); err != nil {
		if apierrors.IsNotFound(err) {
			wpReady.Status = metav1.ConditionFalse
			wpReady.Reason = "WorkerPoolNotFound"
			wpReady.Message = fmt.Sprintf("WorkerPool %q not found", ta.Spec.WorkerPoolRef)
		} else {
			return ctrl.Result{}, fmt.Errorf("looking up WorkerPool: %w", err)
		}
	} else {
		wpReady.Status = metav1.ConditionTrue
		wpReady.Reason = "WorkerPoolFound"
		wpReady.Message = fmt.Sprintf("WorkerPool %q exists", ta.Spec.WorkerPoolRef)
	}
	setCondition(&ta.Status.Conditions, wpReady)

	// Validate egress refs.
	egressConfigured := metav1.Condition{
		Type:               "EgressConfigured",
		ObservedGeneration: ta.Generation,
		LastTransitionTime: now,
	}
	if len(ta.Spec.EgressRefs) == 0 {
		egressConfigured.Status = metav1.ConditionTrue
		egressConfigured.Reason = "NoEgressRefs"
		egressConfigured.Message = "no egress refs configured"
	} else {
		allFound := true
		var missing []string
		for _, ref := range ta.Spec.EgressRefs {
			var egw clrkv1alpha1.EgressGateway
			key := types.NamespacedName{Name: ref.GatewayRef, Namespace: ta.Namespace}
			if err := r.Get(ctx, key, &egw); err != nil {
				if apierrors.IsNotFound(err) {
					allFound = false
					missing = append(missing, ref.GatewayRef)
				} else {
					return ctrl.Result{}, fmt.Errorf("looking up EgressGateway %q: %w", ref.GatewayRef, err)
				}
			}
		}
		if allFound {
			egressConfigured.Status = metav1.ConditionTrue
			egressConfigured.Reason = "AllEgressRefsFound"
			egressConfigured.Message = "all egress gateway refs resolved"
		} else {
			egressConfigured.Status = metav1.ConditionFalse
			egressConfigured.Reason = "EgressRefsNotFound"
			egressConfigured.Message = fmt.Sprintf("missing EgressGateway(s): %v", missing)
		}
	}
	setCondition(&ta.Status.Conditions, egressConfigured)

	// Create or update SandboxState.
	logger := log.FromContext(ctx)
	ss := &clrkv1alpha1.SandboxState{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ta.Name,
			Namespace: ta.Namespace,
		},
	}
	var existingSS clrkv1alpha1.SandboxState
	ssKey := types.NamespacedName{Name: ta.Name, Namespace: ta.Namespace}
	if err := r.Get(ctx, ssKey, &existingSS); apierrors.IsNotFound(err) {
		ss.Spec = clrkv1alpha1.SandboxStateSpec{
			AgentRef:  ta.Name,
			PoolRef:   ta.Spec.WorkerPoolRef,
			Sandbox:   ta.Spec.Sandbox,
			Resources: ta.Spec.Resources,
		}
		if err := ctrl.SetControllerReference(&ta, ss, r.Scheme); err != nil {
			return ctrl.Result{}, fmt.Errorf("setting SandboxState owner reference: %w", err)
		}
		logger.Info("Creating SandboxState", "name", ss.Name)
		if err := r.Create(ctx, ss); err != nil {
			return ctrl.Result{}, fmt.Errorf("creating SandboxState: %w", err)
		}
		existingSS = *ss
	} else if err != nil {
		return ctrl.Result{}, fmt.Errorf("getting SandboxState: %w", err)
	} else {
		existingSS.Spec.AgentRef = ta.Name
		existingSS.Spec.PoolRef = ta.Spec.WorkerPoolRef
		existingSS.Spec.Sandbox = ta.Spec.Sandbox
		existingSS.Spec.Resources = ta.Spec.Resources
		if err := r.Update(ctx, &existingSS); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating SandboxState: %w", err)
		}
	}

	// Set SandboxReady condition based on SandboxState status.
	sandboxReady := metav1.Condition{
		Type:               "SandboxReady",
		ObservedGeneration: ta.Generation,
		LastTransitionTime: now,
	}
	if existingSS.Status.ReadyWorkers >= 1 {
		sandboxReady.Status = metav1.ConditionTrue
		sandboxReady.Reason = "WorkersReady"
		sandboxReady.Message = fmt.Sprintf("%d worker(s) have sandbox ready", existingSS.Status.ReadyWorkers)
	} else {
		sandboxReady.Status = metav1.ConditionFalse
		sandboxReady.Reason = "NoWorkersReady"
		sandboxReady.Message = "no workers have sandbox ready"
	}
	setCondition(&ta.Status.Conditions, sandboxReady)

	// Create or update Gateway.
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
	} else {
		existingGW.Spec = desiredGW.Spec
		if err := r.Update(ctx, &existingGW); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating Gateway: %w", err)
		}
	}

	// Set GatewayReady condition based on Gateway status.
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
	} else if existingGW.CreationTimestamp.IsZero() {
		gatewayReady.Status = metav1.ConditionFalse
		gatewayReady.Reason = "GatewayCreated"
		gatewayReady.Message = "Gateway created, not yet provisioned"
	} else {
		gatewayReady.Status = metav1.ConditionFalse
		gatewayReady.Reason = "GatewayNotProgrammed"
		gatewayReady.Message = "Gateway exists but Programmed != True"
	}
	setCondition(&ta.Status.Conditions, gatewayReady)

	// Create or update HTTPRoute.
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
	} else {
		existingHR.Spec = desiredHR.Spec
		if err := r.Update(ctx, &existingHR); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating HTTPRoute: %w", err)
		}
	}

	// Accepted condition — spec is structurally valid.
	accepted := metav1.Condition{
		Type:               "Accepted",
		Status:             metav1.ConditionTrue,
		ObservedGeneration: ta.Generation,
		LastTransitionTime: now,
		Reason:             "SpecValid",
		Message:            "spec is structurally valid",
	}
	setCondition(&ta.Status.Conditions, accepted)

	// TODO: CronJob management for spec.schedule will be added once the
	// ingress/execution path is implemented.

	if err := r.Status().Update(ctx, &ta); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}

	return ctrl.Result{}, nil
}

func (r *TaskAgentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&clrkv1alpha1.TaskAgent{}).
		Owns(&clrkv1alpha1.SandboxState{}).
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
				"clrk.apoxy.dev/taskagent":  ta.Name,
				"clrk.apoxy.dev/component": "ingress",
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
				"clrk.apoxy.dev/taskagent":  ta.Name,
				"clrk.apoxy.dev/component": "ingress",
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
								Value: ptrTo("/"),
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

func ptrTo(s string) *string {
	return &s
}
