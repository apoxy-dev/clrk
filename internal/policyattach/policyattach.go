// Package policyattach implements the Direct Policy Attachment (GEP-713 /
// GEP-2648) matching rule shared by every clrk policy that attaches DOWN
// to a target via spec.targetRefs — CredentialInjectionPolicy and
// FallbackRoutingPolicy today, EgressDenyPolicy next.
//
// A LocalPolicyTargetReferenceWithSectionName is same-namespace by
// construction (it carries no namespace field), so a policy attaches only
// to targets in the policy's own namespace; cross-namespace attachment is
// deferred to an explicit ReferenceGrant (not yet implemented). Centralizing
// the rule keeps the worker resolvers (extproc credtable, llmroute) and the
// controller-manager status reconcilers from drifting on the group/kind/name
// spelling or the same-namespace gate.
package policyattach

import (
	gwapiv1a2 "sigs.k8s.io/gateway-api/apis/v1alpha2"
)

// Target identifies a concrete object a policy may attach to. Namespace is
// the target's own namespace; RefMatches enforces it equals the policy's
// namespace.
type Target struct {
	Group     string
	Kind      string
	Namespace string
	Name      string
}

// RefMatches reports whether ref — declared by a policy living in
// policyNamespace — attaches to target. Group, Kind, and Name must match
// exactly, and target.Namespace must equal policyNamespace (same-namespace
// by construction; cross-namespace requires a ReferenceGrant).
func RefMatches(policyNamespace string, ref gwapiv1a2.LocalPolicyTargetReferenceWithSectionName, target Target) bool {
	if target.Namespace != policyNamespace {
		return false
	}
	return string(ref.Group) == target.Group &&
		string(ref.Kind) == target.Kind &&
		string(ref.Name) == target.Name
}

// Section returns ref's sectionName as a plain string, or "" when unset.
// For a CredentialInjectionPolicy targeting an AIProviderRoute the section
// names a specific BackendRef; an empty section means whole-target.
func Section(ref gwapiv1a2.LocalPolicyTargetReferenceWithSectionName) string {
	if ref.SectionName != nil {
		return string(*ref.SectionName)
	}
	return ""
}

// Match reports whether any of refs (declared by a policy in
// policyNamespace) attaches to target, returning the first matching ref's
// sectionName ("" when whole-target or unset). First match wins; callers
// that need every matching section iterate refs with RefMatches/Section
// directly (the CredentialInjectionPolicy resolver does, to stack one
// injection per backend-scoped ref).
func Match(policyNamespace string, refs []gwapiv1a2.LocalPolicyTargetReferenceWithSectionName, target Target) (bool, string) {
	for i := range refs {
		if RefMatches(policyNamespace, refs[i], target) {
			return true, Section(refs[i])
		}
	}
	return false, ""
}
