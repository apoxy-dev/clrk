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
)

// credentialInjectionPolicyController is the controller name stamped on the
// CredentialInjectionPolicy ancestor rows this reconciler owns.
const credentialInjectionPolicyController = "clrk.apoxy.dev/credentialinjectionpolicy-controller"

// cipPendingKinds are target kinds a CredentialInjectionPolicy may attach to
// at admission but whose credential-injection data path is not yet wired:
// MCPRoute injection is a follow-up (buildCredTable no-ops the MCPRoute arm).
// Such an attachment reports Accepted=False/Pending so the status does not
// claim a credential is injected when none is.
var cipPendingKinds = map[string]bool{"MCPRoute": true}

// CredentialInjectionPolicyStatusReconciler writes the GEP-2649 PolicyStatus
// of a CredentialInjectionPolicy: one ancestor per targetRef with an Accepted
// condition reporting whether the (same-namespace) EgressGateway / AIProviderRoute
// / MCPRoute target exists. It is status-only — the worker resolves and applies
// the credential off the policy spec — so a typo'd or dangling targetRef
// surfaces as Accepted=False/TargetNotFound instead of silently never injecting.
type CredentialInjectionPolicyStatusReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=credentialinjectionpolicies,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=credentialinjectionpolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=egressgateways,verbs=get;list;watch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=aiproviderroutes,verbs=get;list;watch
// +kubebuilder:rbac:groups=clrk.apoxy.dev,resources=mcproutes,verbs=get;list;watch

func (r *CredentialInjectionPolicyStatusReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var cip clrkv1alpha1.CredentialInjectionPolicy
	if err := r.Get(ctx, req.NamespacedName, &cip); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	statusBase := cip.DeepCopy()

	// Seed each ancestor's conditions from the existing status so
	// SetStatusCondition preserves LastTransitionTime when nothing changed;
	// the DeepEqual guard below then avoids churning resourceVersion (which
	// the ext_proc sink cache keys on) and re-triggering this For() watch.
	existing := make(map[string][]metav1.Condition, len(statusBase.Status.Ancestors))
	for i := range statusBase.Status.Ancestors {
		a := &statusBase.Status.Ancestors[i]
		existing[ancestorRefKey(a.AncestorRef)] = a.Conditions
	}

	resolve := func(ctx context.Context, ref gwapiv1a2.LocalPolicyTargetReferenceWithSectionName) (ancestorCondition, error) {
		return baseTargetCondition(ctx, r.Client, cip.Namespace, ref, cipPendingKinds)
	}
	ancestors, err := policyAncestors(ctx, cip.Namespace, cip.Spec.TargetRefs,
		cip.Generation, credentialInjectionPolicyController, existing, resolve)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("building CredentialInjectionPolicy ancestors: %w", err)
	}
	cip.Status.Ancestors = ancestors

	if reflect.DeepEqual(statusBase.Status, cip.Status) {
		return ctrl.Result{}, nil
	}
	if err := r.Status().Patch(ctx, &cip, client.MergeFrom(statusBase)); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating CredentialInjectionPolicy status: %w", err)
	}
	return ctrl.Result{}, nil
}

func (r *CredentialInjectionPolicyStatusReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("credentialinjectionpolicy-status").
		For(&clrkv1alpha1.CredentialInjectionPolicy{}).
		Watches(&clrkv1alpha1.EgressGateway{}, handler.EnqueueRequestsFromMapFunc(r.policiesForTarget("EgressGateway"))).
		Watches(&clrkv1alpha1.AIProviderRoute{}, handler.EnqueueRequestsFromMapFunc(r.policiesForTarget("AIProviderRoute"))).
		Watches(&clrkv1alpha1.MCPRoute{}, handler.EnqueueRequestsFromMapFunc(r.policiesForTarget("MCPRoute"))).
		Complete(r)
}

// policiesForTarget returns a map func that enqueues every CIP whose targetRefs
// name the changed object of the given kind, so Accepted refreshes when a
// target appears or is deleted.
func (r *CredentialInjectionPolicyStatusReconciler) policiesForTarget(kind string) handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		var cips clrkv1alpha1.CredentialInjectionPolicyList
		if err := r.List(ctx, &cips); err != nil {
			return nil
		}
		var reqs []reconcile.Request
		for i := range cips.Items {
			cip := &cips.Items[i]
			if policyTargetsObject(cip.Namespace, cip.Spec.TargetRefs, kind, obj.GetNamespace(), obj.GetName()) {
				reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: cip.Namespace, Name: cip.Name}})
			}
		}
		return reqs
	}
}
