package extproc

import (
	"path"
	"strings"

	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// clrkAPIGroup is the apiGroup name AIProviderRoute parentRefs must
// reference for the route to attach to a clrk EgressGateway.
const clrkAPIGroup = "clrk.apoxy.dev"

// egressGatewayKind is the parentRef kind clrk EgressGateways use.
const egressGatewayKind = "EgressGateway"

// routeTable holds a flattened view of the AIProviderRoutes attached to
// one EgressGateway. Each rule on a route becomes one routeRule entry;
// match() walks the slice in spec order and returns the first hit.
//
// The table is rebuilt by sinkRegistry whenever the per-EG snapshot
// changes (see sinks.go). Its shape is read-only after construction so
// match() needs no synchronization.
type routeTable struct {
	rules []routeRule
}

// routeRule is one flattened (route, rule, match) tuple. The route's
// namespace+name are stamped on the captured Record for diagnostics;
// tokenBudget (if set on the rule) is read by the budget enforcer at
// pre-flight (allow/deny) and post-flight (increment) time.
type routeRule struct {
	routeNamespace string
	routeName      string

	// provider is the canonical provider name from the match clause.
	// "" when the rule had no provider clause (skipped by match()).
	provider string

	// endpoints are :path globs in path.Match shape (e.g. "/v1/*").
	// Empty means any path.
	endpoints []string

	// models are gen_ai.request.model globs in path.Match shape.
	// Empty means any model.
	models []string

	// tokenBudget is the first TokenBudget filter found on the
	// containing rule. nil when the rule has no TokenBudget. Filters
	// are scoped to the rule, not the match clause, so we copy it
	// onto every routeRule produced from one rule.
	tokenBudget *clrkv1alpha1.TokenBudgetFilter
}

// buildRouteTable filters routes by parentRef matching the given
// EgressGateway and flattens each (route, rule, match) tuple into a
// routeRule. Routes whose parentRefs don't reference this EG are
// silently skipped. Match clauses within a rule are OR'd per Gateway
// API conventions (any match satisfies the rule), so each match
// becomes its own routeRule entry; first-hit-wins at match time.
func buildRouteTable(egNamespace, egName string, routes []clrkv1alpha1.AIProviderRoute) *routeTable {
	t := &routeTable{}
	for _, r := range routes {
		if !routeAttachesTo(r, egNamespace, egName) {
			continue
		}
		for _, rule := range r.Spec.Rules {
			budget := firstTokenBudget(rule.Filters)
			for _, m := range rule.Matches {
				t.rules = append(t.rules, routeRule{
					routeNamespace: r.Namespace,
					routeName:      r.Name,
					provider:       strings.ToLower(m.Provider),
					endpoints:      append([]string(nil), m.Endpoints...),
					models:         append([]string(nil), m.Models...),
					tokenBudget:    budget,
				})
			}
		}
	}
	return t
}

// firstTokenBudget returns the first TokenBudget filter on a rule, or
// nil. Multiple TokenBudgets per rule are allowed by the schema but
// have no MVP semantics (which one wins?); we honor the first.
func firstTokenBudget(filters []clrkv1alpha1.AIProviderRouteFilter) *clrkv1alpha1.TokenBudgetFilter {
	for i := range filters {
		f := &filters[i]
		if f.Type == clrkv1alpha1.AIProviderFilterTokenBudget && f.TokenBudget != nil {
			return f.TokenBudget
		}
	}
	return nil
}

// routeAttachesTo reports whether any of route's parentRefs point at
// the EgressGateway (egNamespace, egName). Cross-namespace refs are
// allowed today (no ReferenceGrant gate); revisit when we add deny
// semantics.
func routeAttachesTo(route clrkv1alpha1.AIProviderRoute, egNamespace, egName string) bool {
	for _, ref := range route.Spec.ParentRefs {
		if !parentRefIsClrkEgressGateway(ref) {
			continue
		}
		if string(ref.Name) != egName {
			continue
		}
		ns := route.Namespace
		if ref.Namespace != nil && *ref.Namespace != "" {
			ns = string(*ref.Namespace)
		}
		if ns == egNamespace {
			return true
		}
	}
	return false
}

// parentRefIsClrkEgressGateway returns true when the ref's group+kind
// resolves to clrk.apoxy.dev/EgressGateway. Empty group/kind defaults
// to gateway.networking.k8s.io/Gateway per Gateway API conventions and
// must be rejected.
func parentRefIsClrkEgressGateway(ref gwapiv1.ParentReference) bool {
	group := ""
	if ref.Group != nil {
		group = string(*ref.Group)
	}
	kind := ""
	if ref.Kind != nil {
		kind = string(*ref.Kind)
	}
	return group == clrkAPIGroup && kind == egressGatewayKind
}

// match returns a pointer to the first rule that accepts (provider,
// reqPath, model). Returns nil when nothing matches.
//
// provider is the canonical name from the parsers package (e.g.
// "anthropic"). When the rule's provider is "custom", we accept any
// host (the route author opted into endpoint-only matching). When the
// rule's provider is "", the rule is skipped — a misconfigured route
// without a provider clause shouldn't silently match everything.
//
// Callers that don't yet know the model (pre-flight, before body
// buffering) pass model="" and rely on matchesGlobAny treating an
// empty value against a non-empty model glob as no-match. Today
// path.Match("claude-3-*", "") returns false, so a model-scoped rule
// won't be used for pre-flight enforcement — that's intentional: a
// model gate can't fire until we've parsed the body.
func (t *routeTable) match(provider, reqPath, model string) *routeRule {
	for i := range t.rules {
		rr := &t.rules[i]
		if rr.provider == "" {
			continue
		}
		if rr.provider != "custom" && rr.provider != provider {
			continue
		}
		if !matchesGlobAny(rr.endpoints, reqPath) {
			continue
		}
		if len(rr.models) > 0 && model == "" {
			continue
		}
		if !matchesGlobAny(rr.models, model) {
			continue
		}
		return rr
	}
	return nil
}

// matchesGlobAny returns true when patterns is empty (no constraint)
// or value matches at least one pattern via path.Match. Match errors
// are treated as no-match (path.Match only errors on malformed
// patterns; defending here keeps a typo from blackholing a route).
func matchesGlobAny(patterns []string, value string) bool {
	if len(patterns) == 0 {
		return true
	}
	for _, p := range patterns {
		ok, err := path.Match(p, value)
		if err == nil && ok {
			return true
		}
	}
	return false
}
