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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// clrk parentRef / backendRef group + kinds and the controller name this
// reconciler stamps on RouteParentStatus. Duplicated from the extproc
// route table's unexported predicates to avoid importing that package
// into the controller (and to keep the controller's view of the contract
// explicit).
const (
	routeStatusGroup          = "clrk.apoxy.dev"
	routeStatusEGKind         = "EgressGateway"
	routeStatusBackendKind    = "Backend"
	aiProviderRouteController = "clrk.apoxy.dev/aiproviderroute-controller"
)

// AIProviderRouteStatusReconciler writes the previously-unwritten
// AIProviderRoute Status.Parents: an Accepted condition per clrk
// EgressGateway parentRef (does the gateway exist?) and a ResolvedRefs
// condition (do all clrk Backend backendRefs resolve?). It is status-only
// — it never mutates spec or the data plane — so Backend/BackendSelector
// references become discoverable and a typo'd ref surfaces as
// ResolvedRefs=False instead of silently degrading to pass-through.
type AIProviderRouteStatusReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=aiproviderroutes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=aiproviderroutes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=egressgateways,verbs=get;list;watch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=backends,verbs=get;list;watch

func (r *AIProviderRouteStatusReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var apr clrkv1alpha1.AIProviderRoute
	if err := r.Get(ctx, req.NamespacedName, &apr); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	statusBase := apr.DeepCopy()

	// Seed each parent's conditions from the existing status so
	// meta.SetStatusCondition preserves LastTransitionTime when a
	// condition's Status is unchanged. Parents are rebuilt from scratch
	// every reconcile; without seeding, SetStatusCondition would see an
	// empty slice, treat every condition as new, and stamp a fresh
	// LastTransitionTime — defeating the DeepEqual guard below, churning
	// the APR's resourceVersion (which the ext_proc sink cache keys on),
	// and re-triggering this controller's own For() watch.
	existing := make(map[string][]metav1.Condition, len(statusBase.Status.Parents))
	for i := range statusBase.Status.Parents {
		p := &statusBase.Status.Parents[i]
		existing[parentRefKey(p.ParentRef)] = p.Conditions
	}

	// ResolvedRefs is route-wide (it inspects every rule's backendRefs),
	// so it is computed once and stamped onto every parent.
	resolved, err := r.resolvedRefsCondition(ctx, &apr)
	if err != nil {
		return ctrl.Result{}, err
	}

	var parents []gwapiv1.RouteParentStatus
	for _, ref := range apr.Spec.ParentRefs {
		if !parentRefIsClrkEG(ref) {
			// Not a clrk EgressGateway parent — not ours to status.
			continue
		}
		ps := gwapiv1.RouteParentStatus{
			ParentRef:      ref,
			ControllerName: gwapiv1.GatewayController(aiProviderRouteController),
			Conditions:     append([]metav1.Condition(nil), existing[parentRefKey(ref)]...),
		}

		egNS := apr.Namespace
		if ref.Namespace != nil && *ref.Namespace != "" {
			egNS = string(*ref.Namespace)
		}
		var eg clrkv1alpha1.EgressGateway
		egErr := r.Get(ctx, types.NamespacedName{Namespace: egNS, Name: string(ref.Name)}, &eg)
		// LastTransitionTime is intentionally left zero: SetStatusCondition
		// stamps it only when the condition is new or its Status changes,
		// otherwise it keeps the seeded value.
		accepted := metav1.Condition{
			Type:               string(gwapiv1.RouteConditionAccepted),
			ObservedGeneration: apr.Generation,
		}
		switch {
		case egErr == nil:
			accepted.Status = metav1.ConditionTrue
			accepted.Reason = string(gwapiv1.RouteReasonAccepted)
			accepted.Message = fmt.Sprintf("Attached to EgressGateway %s/%s", egNS, ref.Name)
		case apierrors.IsNotFound(egErr):
			accepted.Status = metav1.ConditionFalse
			accepted.Reason = string(gwapiv1.RouteReasonNoMatchingParent)
			accepted.Message = fmt.Sprintf("EgressGateway %s/%s not found", egNS, ref.Name)
		default:
			return ctrl.Result{}, fmt.Errorf("getting parent EgressGateway %s/%s: %w", egNS, ref.Name, egErr)
		}
		meta.SetStatusCondition(&ps.Conditions, accepted)

		resolved.ObservedGeneration = apr.Generation
		meta.SetStatusCondition(&ps.Conditions, resolved)

		parents = append(parents, ps)
	}
	apr.Status.Parents = parents

	if reflect.DeepEqual(statusBase.Status, apr.Status) {
		return ctrl.Result{}, nil
	}
	if err := r.Status().Patch(ctx, &apr, client.MergeFrom(statusBase)); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating AIProviderRoute status: %w", err)
	}
	return ctrl.Result{}, nil
}

// resolvedRefsCondition evaluates every rule's backendRefs. A non-clrk
// backendRef (e.g. a plain Service) yields InvalidKind; a clrk Backend
// ref that doesn't resolve yields BackendNotFound; otherwise ResolvedRefs.
// An InferencePool Backend resolves (it exists) and does NOT fail this
// condition — the data plane refuses it at runtime and richer per-backend
// status is a follow-up.
func (r *AIProviderRouteStatusReconciler) resolvedRefsCondition(ctx context.Context, apr *clrkv1alpha1.AIProviderRoute) (metav1.Condition, error) {
	cond := metav1.Condition{Type: string(gwapiv1.RouteConditionResolvedRefs)}
	for _, rule := range apr.Spec.Rules {
		for _, ref := range rule.BackendRefs {
			group, kind := backendRefGroupKind(ref)
			if group != routeStatusGroup || kind != routeStatusBackendKind {
				cond.Status = metav1.ConditionFalse
				cond.Reason = string(gwapiv1.RouteReasonInvalidKind)
				cond.Message = fmt.Sprintf("backendRef %q is not a %s/%s", ref.Name, routeStatusGroup, routeStatusBackendKind)
				return cond, nil
			}
			ns := apr.Namespace
			if ref.Namespace != nil && *ref.Namespace != "" {
				ns = string(*ref.Namespace)
			}
			var be clrkv1alpha1.Backend
			err := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: string(ref.Name)}, &be)
			if apierrors.IsNotFound(err) {
				cond.Status = metav1.ConditionFalse
				cond.Reason = string(gwapiv1.RouteReasonBackendNotFound)
				cond.Message = fmt.Sprintf("Backend %s/%s not found", ns, ref.Name)
				return cond, nil
			}
			if err != nil {
				return cond, fmt.Errorf("getting Backend %s/%s: %w", ns, ref.Name, err)
			}
		}
	}
	cond.Status = metav1.ConditionTrue
	cond.Reason = string(gwapiv1.RouteReasonResolvedRefs)
	cond.Message = "All backendRefs resolved"
	return cond, nil
}

func (r *AIProviderRouteStatusReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("aiproviderroute-status").
		For(&clrkv1alpha1.AIProviderRoute{}).
		Watches(&clrkv1alpha1.EgressGateway{}, handler.EnqueueRequestsFromMapFunc(r.routesForEgressGateway)).
		Watches(&clrkv1alpha1.Backend{}, handler.EnqueueRequestsFromMapFunc(r.routesForBackend)).
		Complete(r)
}

// routesForEgressGateway enqueues every AIProviderRoute that parent-refs
// the changed EgressGateway, so Accepted refreshes when an EG appears or
// is deleted.
func (r *AIProviderRouteStatusReconciler) routesForEgressGateway(ctx context.Context, obj client.Object) []reconcile.Request {
	eg, ok := obj.(*clrkv1alpha1.EgressGateway)
	if !ok {
		return nil
	}
	return r.enqueueMatching(ctx, func(apr clrkv1alpha1.AIProviderRoute) bool {
		return aprAttachesToEG(apr, eg.Namespace, eg.Name)
	})
}

// routesForBackend enqueues every AIProviderRoute that references the
// changed Backend, so ResolvedRefs refreshes when a Backend appears or is
// deleted.
func (r *AIProviderRouteStatusReconciler) routesForBackend(ctx context.Context, obj client.Object) []reconcile.Request {
	be, ok := obj.(*clrkv1alpha1.Backend)
	if !ok {
		return nil
	}
	return r.enqueueMatching(ctx, func(apr clrkv1alpha1.AIProviderRoute) bool {
		return aprReferencesBackend(apr, be.Namespace, be.Name)
	})
}

func (r *AIProviderRouteStatusReconciler) enqueueMatching(ctx context.Context, pred func(clrkv1alpha1.AIProviderRoute) bool) []reconcile.Request {
	var aprs clrkv1alpha1.AIProviderRouteList
	if err := r.List(ctx, &aprs); err != nil {
		return nil
	}
	var reqs []reconcile.Request
	for _, apr := range aprs.Items {
		if pred(apr) {
			reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: apr.Namespace, Name: apr.Name}})
		}
	}
	return reqs
}

// parentRefIsClrkEG reports whether ref points at a clrk EgressGateway.
// Empty group/kind defaults to the Gateway API Gateway and is rejected.
func parentRefIsClrkEG(ref gwapiv1.ParentReference) bool {
	group := ""
	if ref.Group != nil {
		group = string(*ref.Group)
	}
	kind := ""
	if ref.Kind != nil {
		kind = string(*ref.Kind)
	}
	return group == routeStatusGroup && kind == routeStatusEGKind
}

// aprAttachesToEG reports whether any parentRef of apr points at the EG.
func aprAttachesToEG(apr clrkv1alpha1.AIProviderRoute, egNamespace, egName string) bool {
	for _, ref := range apr.Spec.ParentRefs {
		if !parentRefIsClrkEG(ref) {
			continue
		}
		if string(ref.Name) != egName {
			continue
		}
		ns := apr.Namespace
		if ref.Namespace != nil && *ref.Namespace != "" {
			ns = string(*ref.Namespace)
		}
		if ns == egNamespace {
			return true
		}
	}
	return false
}

// aprReferencesBackend reports whether any rule's backendRefs name the
// given clrk Backend.
func aprReferencesBackend(apr clrkv1alpha1.AIProviderRoute, beNamespace, beName string) bool {
	for _, rule := range apr.Spec.Rules {
		for _, ref := range rule.BackendRefs {
			group, kind := backendRefGroupKind(ref)
			if group != routeStatusGroup || kind != routeStatusBackendKind {
				continue
			}
			if string(ref.Name) != beName {
				continue
			}
			ns := apr.Namespace
			if ref.Namespace != nil && *ref.Namespace != "" {
				ns = string(*ref.Namespace)
			}
			if ns == beNamespace {
				return true
			}
		}
	}
	return false
}

func backendRefGroupKind(ref gwapiv1.BackendRef) (group, kind string) {
	if ref.Group != nil {
		group = string(*ref.Group)
	}
	if ref.Kind != nil {
		kind = string(*ref.Kind)
	}
	return
}

// parentRefKey identifies a parentRef for matching a freshly-built
// RouteParentStatus against the existing one (to carry LastTransitionTime
// forward). Namespace + name + sectionName are the fields that
// distinguish parents for clrk EgressGateway refs.
func parentRefKey(ref gwapiv1.ParentReference) string {
	ns := ""
	if ref.Namespace != nil {
		ns = string(*ref.Namespace)
	}
	section := ""
	if ref.SectionName != nil {
		section = string(*ref.SectionName)
	}
	return ns + "/" + string(ref.Name) + "/" + section
}
