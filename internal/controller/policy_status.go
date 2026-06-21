package controller

import (
	"context"
	"fmt"
	"sort"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"
	gwapiv1a2 "sigs.k8s.io/gateway-api/apis/v1alpha2"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/policyattach"
)

// maxPolicyAncestors is the GEP-2649 cap on PolicyStatus.Ancestors. Beyond it
// the policy is unimplementable for the surplus targets; rather than silently
// drop them we keep maxPolicyAncestors-1 real rows and turn the last slot into
// an overflow marker that names how many targets are unreconciled.
const maxPolicyAncestors = 16

// reasonPending marks an attachment the controller recognizes but whose data
// path is not yet wired (e.g. CredentialInjectionPolicy -> MCPRoute). It is not
// a GEP-2649 well-known reason, but the well-known set has no "valid target,
// feature not implemented" state and Accepted=False/Pending is the honest
// signal that the policy is inert today.
const reasonPending = "Pending"

// The Direct Policy Attachment (GEP-2648) policies share one status shape —
// GEP-2649 PolicyStatus.Ancestors — and one acceptance skeleton: an ancestor
// per targetRef whose Accepted condition reports whether the (same-namespace)
// target object exists. The per-policy reconcilers (CIP, FRP) differ only in
// the per-ref verdict (CIP defers MCPRoute and resolves a sectionName against
// the route's backends; FRP marks the conflict loser Overridden), which they
// supply as an ancestorResolver. Dedup, deterministic ordering, the 16-cap, and
// LastTransitionTime carry-forward below are common to all of them.

// ancestorCondition is the Accepted verdict an ancestorResolver computes for one
// targetRef. policyAncestors stamps it onto a metav1.Condition with the policy's
// generation and merges it into the carried-forward conditions.
type ancestorCondition struct {
	status  metav1.ConditionStatus
	reason  string
	message string
}

// ancestorResolver computes the Accepted verdict for a single targetRef. The
// closure captures the client and policy namespace; policyAncestors owns the
// surrounding status plumbing.
type ancestorResolver func(ctx context.Context, ref gwapiv1a2.LocalPolicyTargetReferenceWithSectionName) (ancestorCondition, error)

// policyAncestors builds PolicyStatus.Ancestors for a policy living in
// policyNamespace: one PolicyAncestorStatus per unique targetRef, each carrying
// the Accepted condition resolve returns. controllerName scopes the rows so each
// clrk policy controller owns only its own ancestors. existing carries the prior
// per-ancestor conditions so meta.SetStatusCondition preserves LastTransitionTime
// when nothing changed — the policy's resourceVersion feeds the ext_proc /
// egextension caches, so churning it would re-trigger translation for no signal
// change. Ancestors are emitted in a deterministic (key-sorted) order so a pure
// targetRefs reorder produces an identical status and does not churn.
func policyAncestors(
	ctx context.Context,
	policyNamespace string,
	refs []gwapiv1a2.LocalPolicyTargetReferenceWithSectionName,
	generation int64,
	controllerName string,
	existing map[string][]metav1.Condition,
	resolve ancestorResolver,
) ([]gwapiv1a2.PolicyAncestorStatus, error) {
	type entry struct {
		key       string
		ancestRef gwapiv1.ParentReference
		cond      metav1.Condition
	}

	// GEP-2649 treats Ancestors as a set keyed by (AncestorRef, ControllerName),
	// so duplicate targetRefs collapse to one row; admission does not reject
	// duplicates.
	seen := map[string]bool{}
	var entries []entry
	for i := range refs {
		ref := refs[i]
		ancestorRef := targetRefToParentRef(policyNamespace, ref)
		key := ancestorRefKey(ancestorRef)
		if seen[key] {
			continue
		}
		seen[key] = true
		ac, err := resolve(ctx, ref)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry{
			key:       key,
			ancestRef: ancestorRef,
			cond: metav1.Condition{
				Type:               string(gwapiv1a2.PolicyConditionAccepted),
				ObservedGeneration: generation,
				Status:             ac.status,
				Reason:             ac.reason,
				Message:            ac.message,
			},
		})
	}

	// Sort by ancestor key so the emitted order is a function of the target set,
	// not the spec list order. GEP-2649 Ancestors is unordered, so this is free
	// of semantic meaning and makes both the DeepEqual guard and the 16-cap
	// deterministic.
	sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })

	// When the policy targets more than the cap, the surplus cannot be
	// represented. Keep maxPolicyAncestors-1 real rows and repurpose the last
	// slot as an overflow marker so the operator sees the policy is partially
	// unimplemented instead of a clean status hiding dropped targets.
	overflowAt := -1
	if len(entries) > maxPolicyAncestors {
		overflowAt = maxPolicyAncestors - 1
	}

	var ancestors []gwapiv1a2.PolicyAncestorStatus
	for i := range entries {
		if overflowAt >= 0 && i >= maxPolicyAncestors {
			break
		}
		e := entries[i]
		cond := e.cond
		if i == overflowAt {
			unreconciled := len(entries) - overflowAt
			cond = metav1.Condition{
				Type:               string(gwapiv1a2.PolicyConditionAccepted),
				ObservedGeneration: generation,
				Status:             metav1.ConditionFalse,
				Reason:             string(gwapiv1a2.PolicyReasonInvalid),
				Message: fmt.Sprintf(
					"policy targets %d refs; the last %d exceed the GEP-2649 16-ancestor status cap and are not reconciled here — reduce targetRefs",
					len(entries), unreconciled),
			}
		}
		as := gwapiv1a2.PolicyAncestorStatus{
			AncestorRef:    e.ancestRef,
			ControllerName: gwapiv1.GatewayController(controllerName),
			// LastTransitionTime is intentionally left zero on cond:
			// SetStatusCondition stamps it only when the condition is new or its
			// Status changes, otherwise it keeps the seeded value from existing.
			Conditions: append([]metav1.Condition(nil), existing[e.key]...),
		}
		meta.SetStatusCondition(&as.Conditions, cond)
		ancestors = append(ancestors, as)
	}
	return ancestors, nil
}

// baseTargetCondition is the per-targetRef verdict shared by every Direct Policy
// Attachment policy: Invalid for an unsupported kind, TargetNotFound for an
// absent object, Invalid for a sectionName that names no backend, Pending for a
// recognized-but-unwired kind, else Accepted. pendingKinds lets a policy mark a
// kind whose data path is not yet implemented (CIP passes {MCPRoute}); pass nil
// when every supported kind is enforced (FRP).
func baseTargetCondition(
	ctx context.Context,
	c client.Client,
	policyNamespace string,
	ref gwapiv1a2.LocalPolicyTargetReferenceWithSectionName,
	pendingKinds map[string]bool,
) (ancestorCondition, error) {
	group, kind, name := string(ref.Group), string(ref.Kind), string(ref.Name)
	section := policyattach.Section(ref)
	found, supported, sectionOK, err := clrkTargetExists(ctx, c, group, kind, policyNamespace, name, section)
	if err != nil {
		return ancestorCondition{}, err
	}
	switch {
	case !supported:
		return ancestorCondition{
			status:  metav1.ConditionFalse,
			reason:  string(gwapiv1a2.PolicyReasonInvalid),
			message: fmt.Sprintf("Unsupported target %s/%s", group, kind),
		}, nil
	case !found:
		return ancestorCondition{
			status:  metav1.ConditionFalse,
			reason:  string(gwapiv1a2.PolicyReasonTargetNotFound),
			message: fmt.Sprintf("%s %s/%s not found", kind, policyNamespace, name),
		}, nil
	case !sectionOK:
		return ancestorCondition{
			status:  metav1.ConditionFalse,
			reason:  string(gwapiv1a2.PolicyReasonInvalid),
			message: fmt.Sprintf("%s %s/%s has no backend named %q", kind, policyNamespace, name, section),
		}, nil
	case pendingKinds[kind]:
		return ancestorCondition{
			status:  metav1.ConditionFalse,
			reason:  reasonPending,
			message: fmt.Sprintf("%s credential injection is not yet implemented; attachment to %s/%s recorded but inactive", kind, policyNamespace, name),
		}, nil
	default:
		msg := fmt.Sprintf("Attached to %s %s/%s", kind, policyNamespace, name)
		if section != "" {
			msg = fmt.Sprintf("Attached to %s %s/%s backend %q", kind, policyNamespace, name, section)
		}
		return ancestorCondition{
			status:  metav1.ConditionTrue,
			reason:  string(gwapiv1a2.PolicyReasonAccepted),
			message: msg,
		}, nil
	}
}

// targetRefToParentRef renders a targetRef as the ParentReference shape
// PolicyAncestorStatus.AncestorRef requires. The namespace is the policy's own
// (targetRefs are same-namespace by construction); a non-empty sectionName is
// carried through so a backend-scoped CIP reports a distinct ancestor.
func targetRefToParentRef(policyNamespace string, ref gwapiv1a2.LocalPolicyTargetReferenceWithSectionName) gwapiv1.ParentReference {
	pr := gwapiv1.ParentReference{
		Group:     ptr.To(gwapiv1.Group(ref.Group)),
		Kind:      ptr.To(gwapiv1.Kind(ref.Kind)),
		Namespace: ptr.To(gwapiv1.Namespace(policyNamespace)),
		Name:      gwapiv1.ObjectName(ref.Name),
	}
	if ref.SectionName != nil && *ref.SectionName != "" {
		pr.SectionName = ref.SectionName
	}
	return pr
}

// clrkTargetExists reports whether the clrk object named by a policy targetRef
// exists and, when the ref carries a sectionName, whether that section resolves.
// supported is false for a non-clrk group or a kind this resolver does not know
// (admission validation prevents both; the status path degrades to
// Accepted=False/Invalid rather than crashing, and stays consistent with the
// data plane, which also gates on the clrk group). sectionOK is true when the
// ref names no section, or names a BackendRef that exists on the route — the
// same key (BackendRef.Name == resolvedBackend.name) the data plane gates
// injection on; an unresolvable section never injects, so it must not report
// Accepted. Same-namespace lookup mirrors the attachment rule.
func clrkTargetExists(ctx context.Context, c client.Client, group, kind, namespace, name, section string) (found, supported, sectionOK bool, err error) {
	if group != routeStatusGroup {
		return false, false, false, nil
	}
	nn := types.NamespacedName{Namespace: namespace, Name: name}
	switch kind {
	case "AIProviderRoute":
		var apr clrkv1alpha1.AIProviderRoute
		if getErr := c.Get(ctx, nn, &apr); getErr != nil {
			if apierrors.IsNotFound(getErr) {
				return false, true, false, nil
			}
			return false, true, false, getErr
		}
		return true, true, section == "" || aprHasBackend(&apr, section), nil
	case "EgressGateway", "MCPRoute", "EgressL4Route":
		var obj client.Object
		switch kind {
		case "EgressGateway":
			obj = &clrkv1alpha1.EgressGateway{}
		case "MCPRoute":
			obj = &clrkv1alpha1.MCPRoute{}
		case "EgressL4Route":
			obj = &clrkv1alpha1.EgressL4Route{}
		}
		if getErr := c.Get(ctx, nn, obj); getErr != nil {
			if apierrors.IsNotFound(getErr) {
				return false, true, false, nil
			}
			return false, true, false, getErr
		}
		// A sectionName has no BackendRef to bind on a non-AIProviderRoute
		// target; admission rejects it, so a set section here is treated as
		// unresolved rather than silently accepted.
		return true, true, section == "", nil
	default:
		return false, false, false, nil
	}
}

// aprHasBackend reports whether any rule on the route declares a BackendRef
// named section — the sectionName key a CredentialInjectionPolicy targets.
func aprHasBackend(apr *clrkv1alpha1.AIProviderRoute, section string) bool {
	for i := range apr.Spec.Rules {
		for _, br := range apr.Spec.Rules[i].BackendRefs {
			if string(br.Name) == section {
				return true
			}
		}
	}
	return false
}

// ancestorRefKey identifies an ancestor for carrying its conditions forward
// across reconciles and for the deterministic emit order. Group/kind/namespace/
// name/section are the fields that distinguish ancestors of a Direct Policy
// Attachment policy.
func ancestorRefKey(ref gwapiv1.ParentReference) string {
	group := ""
	if ref.Group != nil {
		group = string(*ref.Group)
	}
	kind := ""
	if ref.Kind != nil {
		kind = string(*ref.Kind)
	}
	ns := ""
	if ref.Namespace != nil {
		ns = string(*ref.Namespace)
	}
	section := ""
	if ref.SectionName != nil {
		section = string(*ref.SectionName)
	}
	return group + "/" + kind + "/" + ns + "/" + string(ref.Name) + "/" + section
}

// policyTargetsObject reports whether any of a policy's targetRefs (declared in
// policyNamespace) names the given clrk object — the predicate the status
// reconcilers use to enqueue the policies a changed target affects. It reuses
// policyattach.RefMatches so the watch-enqueue rule can't drift from the
// attachment rule the worker resolvers apply.
func policyTargetsObject(policyNamespace string, refs []gwapiv1a2.LocalPolicyTargetReferenceWithSectionName, kind, ns, name string) bool {
	target := policyattach.Target{Group: routeStatusGroup, Kind: kind, Namespace: ns, Name: name}
	for i := range refs {
		if policyattach.RefMatches(policyNamespace, refs[i], target) {
			return true
		}
	}
	return false
}
