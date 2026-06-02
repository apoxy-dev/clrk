package extproc

import (
	"path"
	"strings"

	"k8s.io/apimachinery/pkg/types"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/extproc/parsers"
)

// backendKind is the BackendRef kind that resolves to a clrk Backend.
const backendKind = "Backend"

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

	// backends is the resolved candidate set for this rule, in spec
	// order, built from rule.BackendRefs that point at clrk Backends.
	// Empty when the rule declares no (resolvable) backendRefs — that
	// rule keeps today's pass-through-to-original-host behavior.
	// Pre-resolved at build time so selection is allocation-free.
	backends []resolvedBackend
}

// resolvedBackend is a clrk Backend resolved to the facts the data plane
// needs at RequestBody EOS: where to send the request, what wire schema
// to parse the response against, and any per-backend rewrites. Resolved
// once at route-table build time from a (rule, BackendRef, Backend)
// tuple.
type resolvedBackend struct {
	// name is the BackendRef name (== the Backend's metadata.name). It
	// doubles as the CredentialInjectionPolicy parentRef sectionName key
	// for post-selection credential injection.
	name      string
	namespace string

	// host:port the egress proxy re-points :authority to. Set only when
	// refuse is false.
	host string
	port int

	// system is the canonical gen_ai.system (e.g. "anthropic",
	// "google_genai") the response parser is keyed to. Derived from the
	// Backend's schema.name via parsers.Canonical.
	system string

	// weight is the standard Gateway API BackendRef.Weight (defaulting
	// to 1 when unset) — the relative share for weighted selection when
	// the rule lists more than one candidate.
	weight int

	// modelRewrites / bodyMutation are the per-backend request rewrites
	// applied at RequestBody EOS when this backend is selected.
	modelRewrites []clrkv1alpha1.ModelRewrite
	bodyMutation  *clrkv1alpha1.BackendBodyMutation

	// refuse is true for a resolved-but-unservable backend (an
	// InferencePool backend, which the data plane does not yet support).
	// Selecting it produces a clean 501 rather than a silent
	// mis-route. unsupportedReason explains why, for the deny body.
	refuse            bool
	unsupportedReason string
}

// rewriteModel returns the literal this backend remaps model to, using
// the first ModelRewrite whose From glob matches. Returns ("", false)
// when no rewrite applies (leave the model untouched).
func (b resolvedBackend) rewriteModel(model string) (string, bool) {
	if model == "" {
		return "", false
	}
	for _, r := range b.modelRewrites {
		if ok, err := path.Match(r.From, model); err == nil && ok {
			return r.To, true
		}
	}
	return "", false
}

// buildRouteTable filters routes by parentRef matching the given
// EgressGateway and flattens each (route, rule, match) tuple into a
// routeRule. Routes whose parentRefs don't reference this EG are
// silently skipped. Match clauses within a rule are OR'd per Gateway
// API conventions (any match satisfies the rule), so each match
// becomes its own routeRule entry; first-hit-wins at match time.
func buildRouteTable(egNamespace, egName string, routes []clrkv1alpha1.AIProviderRoute, backends []clrkv1alpha1.Backend) *routeTable {
	byKey := indexBackends(backends)
	t := &routeTable{}
	for _, r := range routes {
		if !routeAttachesTo(r, egNamespace, egName) {
			continue
		}
		for _, rule := range r.Spec.Rules {
			budget := firstTokenBudget(rule.Filters)
			resolved := resolveBackends(rule.BackendRefs, r.Namespace, byKey)
			for _, m := range rule.Matches {
				prov := parsers.Canonical(strings.ToLower(m.Provider))
				t.rules = append(t.rules, routeRule{
					routeNamespace: r.Namespace,
					routeName:      r.Name,
					provider:       prov,
					endpoints:      append([]string(nil), m.Endpoints...),
					models:         append([]string(nil), m.Models...),
					tokenBudget:    budget,
					backends:       sameSchemaBackends(resolved, prov),
				})
			}
		}
	}
	return t
}

// indexBackends keys the Backend list by namespace/name for O(1) lookup
// during BackendRef resolution.
func indexBackends(backends []clrkv1alpha1.Backend) map[types.NamespacedName]*clrkv1alpha1.Backend {
	m := make(map[types.NamespacedName]*clrkv1alpha1.Backend, len(backends))
	for i := range backends {
		b := &backends[i]
		m[types.NamespacedName{Namespace: b.Namespace, Name: b.Name}] = b
	}
	return m
}

// resolveBackends turns a rule's BackendRefs into the candidate set the
// selector picks from. Refs that don't name a clrk Backend, or name one
// that doesn't exist, are dropped (the status controller surfaces those
// as unresolved); the rule degrades to its remaining candidates, or to
// pass-through when none resolve. An InferencePool Backend resolves to a
// refuse=true candidate so selecting it produces a clean 501 instead of
// a silent mis-route. routeNamespace defaults the ref namespace.
func resolveBackends(refs []gwapiv1.BackendRef, routeNamespace string, byKey map[types.NamespacedName]*clrkv1alpha1.Backend) []resolvedBackend {
	var out []resolvedBackend
	for _, ref := range refs {
		if !backendRefIsClrkBackend(ref) {
			continue
		}
		ns := routeNamespace
		if ref.Namespace != nil && *ref.Namespace != "" {
			ns = string(*ref.Namespace)
		}
		be, ok := byKey[types.NamespacedName{Namespace: ns, Name: string(ref.Name)}]
		if !ok {
			continue
		}
		// Standard Gateway API weight semantics: nil defaults to 1.
		weight := 1
		if ref.Weight != nil {
			weight = int(*ref.Weight)
		}
		rb := resolvedBackend{
			name:          be.Name,
			namespace:     be.Namespace,
			weight:        weight,
			system:        parsers.Canonical(strings.ToLower(string(be.Spec.Schema.Name))),
			modelRewrites: be.Spec.ModelRewrites,
			bodyMutation:  be.Spec.BodyMutation,
		}
		switch {
		case be.Spec.Type == clrkv1alpha1.BackendTypeUpstream && be.Spec.Upstream != nil && be.Spec.Upstream.Host != "":
			rb.host = be.Spec.Upstream.Host
			rb.port = int(be.Spec.Upstream.Port)
			if rb.port == 0 {
				rb.port = 443
			}
		case be.Spec.Type == clrkv1alpha1.BackendTypeInferencePool:
			rb.refuse = true
			rb.unsupportedReason = "InferencePool backends are not yet supported by the egress data plane"
		default:
			// Malformed Upstream (no host). Drop so the rule falls back
			// instead of routing nowhere; the status controller flags it.
			continue
		}
		out = append(out, rb)
	}
	return out
}

// sameSchemaBackends restricts a rule's resolved candidate set to backends
// whose wire schema matches the rule's provider. APO-689 supports
// same-schema reselection only: a backend speaking a different wire format
// than the route's provider would need request/response translation
// (APO-572), so it is dropped from the candidate set rather than silently
// sent a mismatched body. The rule then degrades to its same-schema
// candidates, or to pass-through to the original host when none remain — an
// empty set also clears the re-selectable latch (deferReselect) in
// server.go, so such a rule keeps today's header-time path verbatim.
//
// A "custom" provider does endpoint-only matching and clrk has no parser
// for its wire format, so it cannot verify schema parity; those rules keep
// every candidate and trust the operator's backendRefs (e.g. an
// OpenAI-compatible gateway reselecting to api.openai.com). refuse
// (InferencePool) candidates are always kept so selecting one yields a
// clean 501 rather than a silent fall-through to the original host.
func sameSchemaBackends(candidates []resolvedBackend, provider string) []resolvedBackend {
	if provider == "" || provider == "custom" {
		return candidates
	}
	out := make([]resolvedBackend, 0, len(candidates))
	for _, c := range candidates {
		if c.refuse || c.system == provider {
			out = append(out, c)
		}
	}
	return out
}

// backendRefIsClrkBackend reports whether ref names a clrk
// (clrk.apoxy.dev/Backend) resource. Empty group/kind defaults to the
// Gateway API Service backend and must be rejected.
func backendRefIsClrkBackend(ref gwapiv1.BackendRef) bool {
	group := ""
	if ref.Group != nil {
		group = string(*ref.Group)
	}
	kind := ""
	if ref.Kind != nil {
		kind = string(*ref.Kind)
	}
	return group == clrkAPIGroup && kind == backendKind
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
// reqPath is the HTTP/2 :path pseudo-header, which can include a query
// string. The endpoint globs are documented as path-only patterns, so
// we strip "?..." before matching; otherwise a literal endpoint like
// "/v1/messages" silently fails to match SDK-emitted paths like
// "/v1/messages?beta=true" (Claude Code, OpenAI SDKs).
//
// Callers that don't yet know the model (pre-flight, before body
// buffering) pass model="" and rely on matchesGlobAny treating an
// empty value against a non-empty model glob as no-match. Today
// path.Match("claude-3-*", "") returns false, so a model-scoped rule
// won't be used for pre-flight enforcement — that's intentional: a
// model gate can't fire until we've parsed the body.
func (t *routeTable) match(provider, reqPath, model string) *routeRule {
	if i := strings.IndexByte(reqPath, '?'); i >= 0 {
		reqPath = reqPath[:i]
	}
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
