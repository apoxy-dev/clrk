package controller

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"reflect"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

const (
	defaultGatewayClassName = "envoy"
	envoyGatewayGroup       = "gateway.envoyproxy.io"

	labelAgent       = clrkv1alpha1.LabelAgent
	labelAgentKind   = clrkv1alpha1.LabelAgentKind
	labelGeneration  = clrkv1alpha1.LabelGeneration
	labelWorkerPool  = clrkv1alpha1.LabelWorkerPool
	labelComponent   = "clrk.apoxy.dev/component"

	maxRevisionHistory = 10
)

// TaskAgentReconciler reconciles TaskAgent objects.
type TaskAgentReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=taskagents,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=taskagents/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=workerpools,verbs=get;list;watch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=egressgateways,verbs=get;list;watch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=agentsandboxrevisions,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=agentsandboxrevisions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=gateways,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=httproutes,verbs=get;list;watch;create;update;patch

func (r *TaskAgentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var ta clrkv1alpha1.TaskAgent
	if err := r.Get(ctx, req.NamespacedName, &ta); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	now := metav1.Now()

	// 1. Validate WorkerPool ref.
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
	meta.SetStatusCondition(&ta.Status.Conditions, wpReady)

	// 2. Validate egress refs.
	egressConfigured := metav1.Condition{
		Type:               "EgressConfigured",
		ObservedGeneration: ta.Generation,
		LastTransitionTime: now,
	}
	if len(ta.Spec.EgressRefs) == 0 {
		egressConfigured.Status = metav1.ConditionTrue
		egressConfigured.Reason = "NoEgressRefs"
		egressConfigured.Message = "No egress refs configured"
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
			egressConfigured.Message = "All egress gateway refs resolved"
		} else {
			egressConfigured.Status = metav1.ConditionFalse
			egressConfigured.Reason = "EgressRefsNotFound"
			egressConfigured.Message = fmt.Sprintf("Missing EgressGateway(s): %v", missing)
		}
	}
	meta.SetStatusCondition(&ta.Status.Conditions, egressConfigured)

	// 3. Create or get AgentSandboxRevision.
	revisionName := fmt.Sprintf("%s-%05d", ta.Name, ta.Generation)
	var rev clrkv1alpha1.AgentSandboxRevision
	revKey := types.NamespacedName{Name: revisionName, Namespace: ta.Namespace}
	if err := r.Get(ctx, revKey, &rev); apierrors.IsNotFound(err) {
		rev = clrkv1alpha1.AgentSandboxRevision{
			ObjectMeta: metav1.ObjectMeta{
				Name:      revisionName,
				Namespace: ta.Namespace,
				Labels: map[string]string{
					labelAgent:      ta.Name,
					labelAgentKind:  "TaskAgent",
					labelGeneration: strconv.FormatInt(ta.Generation, 10),
					labelWorkerPool: ta.Spec.WorkerPoolRef,
				},
				Annotations: ta.Spec.Template.Annotations,
			},
			Spec: ta.Spec.Template.Spec,
		}
		if err := ctrl.SetControllerReference(&ta, &rev, r.Scheme); err != nil {
			return ctrl.Result{}, fmt.Errorf("setting AgentSandboxRevision owner reference: %w", err)
		}
		logger.Info("Creating AgentSandboxRevision", "name", revisionName)
		if err := r.Create(ctx, &rev); err != nil {
			return ctrl.Result{}, fmt.Errorf("creating AgentSandboxRevision: %w", err)
		}
	} else if err != nil {
		return ctrl.Result{}, fmt.Errorf("getting AgentSandboxRevision: %w", err)
	}
	ta.Status.LatestCreatedRevisionName = revisionName

	// 4. Set RevisionReady condition and update LatestReadyRevisionName.
	revisionReady := metav1.Condition{
		Type:               "RevisionReady",
		ObservedGeneration: ta.Generation,
		LastTransitionTime: now,
	}
	if rev.Status.ReadyWorkers >= 1 {
		ta.Status.LatestReadyRevisionName = revisionName
		revisionReady.Status = metav1.ConditionTrue
		revisionReady.Reason = "RevisionReady"
		revisionReady.Message = fmt.Sprintf("Revision %q has %d ready worker(s)", revisionName, rev.Status.ReadyWorkers)
	} else {
		// Keep previous latestReadyRevisionName if the new revision isn't ready yet.
		revisionReady.Status = metav1.ConditionFalse
		revisionReady.Reason = "NoWorkersReady"
		revisionReady.Message = fmt.Sprintf("Revision %q has no ready workers", revisionName)
	}
	meta.SetStatusCondition(&ta.Status.Conditions, revisionReady)

	// 5. Create or update Gateway.
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

	// Set GatewayReady condition.
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

	// 6. Create or update HTTPRoute.
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

	// 7. Accepted condition.
	accepted := metav1.Condition{
		Type:               "Accepted",
		Status:             metav1.ConditionTrue,
		ObservedGeneration: ta.Generation,
		LastTransitionTime: now,
		Reason:             "SpecValid",
		Message:            "Spec is structurally valid",
	}
	meta.SetStatusCondition(&ta.Status.Conditions, accepted)

	// 8. Revision GC.
	if ta.Generation > maxRevisionHistory {
		if err := r.gcRevisions(ctx, &ta); err != nil {
			logger.Error(err, "Revision GC failed")
		}
	}

	ta.Status.ObservedGeneration = ta.Generation
	if err := r.Status().Update(ctx, &ta); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}

	return ctrl.Result{}, nil
}

// gcRevisions deletes old AgentSandboxRevisions beyond the history limit,
// keeping the latest created and latest ready revisions.
func (r *TaskAgentReconciler) gcRevisions(ctx context.Context, ta *clrkv1alpha1.TaskAgent) error {
	var revList clrkv1alpha1.AgentSandboxRevisionList
	if err := r.List(ctx, &revList, &client.ListOptions{
		Namespace:     ta.Namespace,
		LabelSelector: labels.SelectorFromSet(map[string]string{labelAgent: ta.Name}),
	}); err != nil {
		return fmt.Errorf("listing revisions: %w", err)
	}

	keep := map[string]bool{
		ta.Status.LatestCreatedRevisionName: true,
		ta.Status.LatestReadyRevisionName:   true,
	}

	// Sort oldest first by creation timestamp.
	sort.Slice(revList.Items, func(i, j int) bool {
		return revList.Items[i].CreationTimestamp.Before(&revList.Items[j].CreationTimestamp)
	})

	// Delete oldest revisions beyond the history limit that aren't in the keep set.
	excess := len(revList.Items) - maxRevisionHistory
	if excess <= 0 {
		return nil
	}

	logger := log.FromContext(ctx)
	deleted := 0
	for i := range revList.Items {
		if deleted >= excess {
			break
		}
		name := revList.Items[i].Name
		if keep[name] {
			continue
		}
		logger.Info("Deleting old AgentSandboxRevision", "name", name)
		if err := r.Delete(ctx, &revList.Items[i]); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("deleting revision %q: %w", name, err)
		}
		deleted++
	}
	return nil
}

func (r *TaskAgentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&clrkv1alpha1.TaskAgent{}).
		Owns(&clrkv1alpha1.AgentSandboxRevision{}).
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
				labelAgent:  ta.Name,
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
				labelAgent:  ta.Name,
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


