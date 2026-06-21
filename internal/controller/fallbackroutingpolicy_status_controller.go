package controller

import (
	"context"
	"fmt"
	"reflect"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gwapiv1a2 "sigs.k8s.io/gateway-api/apis/v1alpha2"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/llmroute"
)

// fallbackRoutingPolicyController is the controller name stamped on the
// FallbackRoutingPolicy ancestor rows this reconciler owns.
const fallbackRoutingPolicyController = "clrk.apoxy.dev/fallbackroutingpolicy-controller"

// FallbackRoutingPolicyStatusReconciler writes the GEP-2649 PolicyStatus of a
// FallbackRoutingPolicy: one ancestor per targetRef with an Accepted condition
// reporting whether the (same-namespace) AIProviderRoute target exists. It is
// status-only — the egextension reads the policy off its spec when synthesizing
// the per-rule clusters — so a dangling targetRef surfaces as
// Accepted=False/TargetNotFound instead of silently never applying fallback.
type FallbackRoutingPolicyStatusReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=fallbackroutingpolicies,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=fallbackroutingpolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=aiproviderroutes,verbs=get;list;watch

func (r *FallbackRoutingPolicyStatusReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var frp clrkv1alpha1.FallbackRoutingPolicy
	if err := r.Get(ctx, req.NamespacedName, &frp); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	statusBase := frp.DeepCopy()

	existing := make(map[string][]metav1.Condition, len(statusBase.Status.Ancestors))
	for i := range statusBase.Status.Ancestors {
		a := &statusBase.Status.Ancestors[i]
		existing[ancestorRefKey(a.AncestorRef)] = a.Conditions
	}

	// Conflict resolution: only the FallbackFor winner (lowest namespace, name)
	// applies to a given route; the rest are accepted-but-overridden. List every
	// FRP once so a losing policy reports Accepted=False/Conflicted naming the
	// winner instead of a misleading Accepted=True for config that never runs.
	var allFRPs clrkv1alpha1.FallbackRoutingPolicyList
	if err := r.List(ctx, &allFRPs); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing FallbackRoutingPolicies: %w", err)
	}
	resolve := func(ctx context.Context, ref gwapiv1a2.LocalPolicyTargetReferenceWithSectionName) (ancestorCondition, error) {
		cond, err := baseTargetCondition(ctx, r.Client, frp.Namespace, ref, nil)
		if err != nil || cond.status != metav1.ConditionTrue {
			return cond, err
		}
		// Reuse the data-plane winner selection so status can't drift from which
		// policy actually applies. targetRefs are same-namespace, so the route
		// lives in frp.Namespace.
		if winner := llmroute.FallbackFor(allFRPs.Items, frp.Namespace, string(ref.Name)); winner != nil &&
			!(winner.Namespace == frp.Namespace && winner.Name == frp.Name) {
			return ancestorCondition{
				status:  metav1.ConditionFalse,
				reason:  string(gwapiv1a2.PolicyReasonConflicted),
				message: fmt.Sprintf("Overridden by FallbackRoutingPolicy %s/%s", winner.Namespace, winner.Name),
			}, nil
		}
		return cond, nil
	}
	ancestors, err := policyAncestors(ctx, frp.Namespace, frp.Spec.TargetRefs,
		frp.Generation, fallbackRoutingPolicyController, existing, resolve)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("building FallbackRoutingPolicy ancestors: %w", err)
	}
	frp.Status.Ancestors = ancestors

	if reflect.DeepEqual(statusBase.Status, frp.Status) {
		return ctrl.Result{}, nil
	}
	if err := r.Status().Patch(ctx, &frp, client.MergeFrom(statusBase)); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating FallbackRoutingPolicy status: %w", err)
	}
	return ctrl.Result{}, nil
}

func (r *FallbackRoutingPolicyStatusReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("fallbackroutingpolicy-status").
		For(&clrkv1alpha1.FallbackRoutingPolicy{}).
		Watches(&clrkv1alpha1.AIProviderRoute{}, handler.EnqueueRequestsFromMapFunc(r.policiesForRoute)).
		Complete(r)
}

// policiesForRoute enqueues every FRP whose targetRefs name the changed
// AIProviderRoute, so Accepted refreshes when a route appears or is deleted.
func (r *FallbackRoutingPolicyStatusReconciler) policiesForRoute(ctx context.Context, obj client.Object) []reconcile.Request {
	var frps clrkv1alpha1.FallbackRoutingPolicyList
	if err := r.List(ctx, &frps); err != nil {
		return nil
	}
	var reqs []reconcile.Request
	for i := range frps.Items {
		frp := &frps.Items[i]
		if policyTargetsObject(frp.Namespace, frp.Spec.TargetRefs, "AIProviderRoute", obj.GetNamespace(), obj.GetName()) {
			reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: frp.Namespace, Name: frp.Name}})
		}
	}
	return reqs
}
