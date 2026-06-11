// Package llmroute holds the LLM-rule identity and candidate-resolution
// logic shared between the EG extension server (which synthesizes the
// per-rule Envoy routes and multi-endpoint clusters) and the egress
// ext_proc (which pins matched requests onto those routes and adapts
// each upstream attempt). Both sides MUST derive rule identity and
// cluster membership from this package: drift between what a
// synthesized cluster contains and what the pin-time candidate set
// assumed surfaces as per-attempt fail-closed 502s.
package llmroute

import (
	"crypto/sha256"
	"encoding/hex"
	"path"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/types"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/extproc/llmcall"
	"github.com/apoxy-dev/clrk/internal/extproc/parsers"
)

// PinHeader is the request header the downstream ext_proc sets at
// RequestBody EOS (together with ClearRouteCache) to steer a matched
// LLM transaction onto its rule's synthesized route. The value is the
// RuleKey; the synthesized route removes the header before the request
// leaves for the provider.
const PinHeader = "x-clrk-llm-rule"

// ClusterPrefix prefixes every synthesized per-rule cluster name.
const ClusterPrefix = "clrk-llm-"

// Group/kind spellings for ref checks. The API group doubles as the
// dynamic-metadata namespace carrying clrk endpoint identity.
const (
	clrkAPIGroup      = "clrk.apoxy.dev"
	backendKind       = "Backend"
	egressGatewayKind = "EgressGateway"
)

// RuleKey derives the stable identifier of one AIProviderRoute rule on
// one EgressGateway, scoped to the rule's match provider. The provider
// is part of the identity because TranslatableCandidates filters per
// provider — two match clauses on one rule with different providers can
// yield different candidate sets and therefore need distinct clusters.
// It keys the synthesized route's PinHeader match and the cluster name,
// so it must be deterministic across processes and short enough for an
// xDS resource name: 12 hex chars of a sha256 over the fully-qualified
// tuple.
func RuleKey(eg types.NamespacedName, route types.NamespacedName, ruleIdx int, provider string) string {
	h := sha256.Sum256([]byte(eg.Namespace + "/" + eg.Name + "|" + route.Namespace + "/" + route.Name + "|" + strconv.Itoa(ruleIdx) + "|" + provider))
	return hex.EncodeToString(h[:6])
}

// ClusterName returns the synthesized cluster name for a rule key.
func ClusterName(ruleKey string) string { return ClusterPrefix + ruleKey }

// SubsetKeyBackend is the envoy.lb endpoint-metadata key (and the
// cluster subset-selector key) carrying the backend name. The
// downstream ext_proc pins a request onto a subset of a rule's
// endpoints by setting this key in envoy.lb dynamic metadata; with no
// pin the subset LB is inert and the full endpoint set serves.
const SubsetKeyBackend = "clrk_backend_name"

// EndpointMetaNamespace is the endpoint-metadata filter namespace
// carrying clrk identity facts. The upstream ext_proc reads it from
// the xds.upstream_host_metadata attribute each attempt to learn which
// Backend Envoy picked.
const EndpointMetaNamespace = clrkAPIGroup

// Endpoint-metadata keys under EndpointMetaNamespace.
const (
	EndpointMetaBackendNamespace = "backend_namespace"
	EndpointMetaBackendName      = "backend_name"
	EndpointMetaBackendSchema    = "schema"
	EndpointMetaBackendHost      = "host"
	EndpointMetaBackendPort      = "port"
)

// CanonicalProvider normalizes an AIProviderRoute match provider to
// the canonical parser name both sides key on (e.g. "Anthropic" ->
// "anthropic"). Centralized so RuleKey inputs can't drift between the
// egextension synthesis and the extproc pin path.
func CanonicalProvider(provider string) string {
	return parsers.Canonical(strings.ToLower(provider))
}

// Candidate is a clrk Backend resolved to the facts the data plane
// needs: where the endpoint dials, what wire schema it speaks, and the
// per-backend rewrites. Candidate order mirrors BackendRefs spec order
// — under an attached FallbackRoutingPolicy the slice index IS the
// fallback priority.
type Candidate struct {
	// Name is the BackendRef name (== the Backend's metadata.name). It
	// doubles as the CredentialInjectionPolicy parentRef sectionName
	// key for per-backend credential injection, the endpoint's
	// transport-socket-match key, and the envoy.lb subset key.
	Name      string
	Namespace string

	// Host:Port the endpoint dials. Set only when Refuse is false.
	Host string
	Port int

	// System is the canonical gen_ai.system (e.g. "anthropic",
	// "google_genai") the backend's wire schema parses against.
	System string

	// Weight is the standard Gateway API BackendRef.Weight (defaulting
	// to 1 when unset) — the relative share for distribution when no
	// FallbackRoutingPolicy attaches.
	Weight int

	// ModelRewrites / BodyMutation are the per-backend request
	// rewrites applied when an attempt targets this backend.
	ModelRewrites []clrkv1alpha1.ModelRewrite
	BodyMutation  *clrkv1alpha1.BackendBodyMutation

	// Refuse is true for a resolved-but-unservable backend (an
	// InferencePool backend, which the data plane does not yet
	// support). Selecting it produces a clean 501 rather than a
	// silent mis-route. UnsupportedReason explains why.
	Refuse            bool
	UnsupportedReason string
}

// RewriteModel returns the literal this backend remaps model to, using
// the first ModelRewrite whose From glob matches. Returns ("", false)
// when no rewrite applies (leave the model untouched).
func (c Candidate) RewriteModel(model string) (string, bool) {
	if model == "" {
		return "", false
	}
	for _, r := range c.ModelRewrites {
		if ok, err := path.Match(r.From, model); err == nil && ok {
			return r.To, true
		}
	}
	return "", false
}

// IndexBackends keys the Backend list by namespace/name for O(1)
// lookup during BackendRef resolution.
func IndexBackends(backends []clrkv1alpha1.Backend) map[types.NamespacedName]*clrkv1alpha1.Backend {
	m := make(map[types.NamespacedName]*clrkv1alpha1.Backend, len(backends))
	for i := range backends {
		b := &backends[i]
		m[types.NamespacedName{Namespace: b.Namespace, Name: b.Name}] = b
	}
	return m
}

// ResolveCandidates turns a rule's BackendRefs into the candidate set,
// preserving spec order. Refs that don't name a clrk Backend, or name
// one that doesn't exist, are dropped (the status controller surfaces
// those as unresolved); the rule degrades to its remaining candidates,
// or to pass-through when none resolve. An InferencePool Backend
// resolves to a Refuse=true candidate so selecting it produces a clean
// 501 instead of a silent mis-route. routeNamespace defaults the ref
// namespace.
func ResolveCandidates(refs []gwapiv1.BackendRef, routeNamespace string, byKey map[types.NamespacedName]*clrkv1alpha1.Backend) []Candidate {
	var out []Candidate
	for _, ref := range refs {
		if !BackendRefIsClrkBackend(ref) {
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
		c := Candidate{
			Name:          be.Name,
			Namespace:     be.Namespace,
			Weight:        weight,
			System:        parsers.Canonical(strings.ToLower(string(be.Spec.Schema.Name))),
			ModelRewrites: be.Spec.ModelRewrites,
			BodyMutation:  be.Spec.BodyMutation,
		}
		switch {
		case be.Spec.Type == clrkv1alpha1.BackendTypeUpstream && be.Spec.Upstream != nil && be.Spec.Upstream.Host != "":
			c.Host = be.Spec.Upstream.Host
			c.Port = int(be.Spec.Upstream.Port)
			if c.Port == 0 {
				c.Port = 443
			}
		case be.Spec.Type == clrkv1alpha1.BackendTypeInferencePool:
			c.Refuse = true
			c.UnsupportedReason = "InferencePool backends are not yet supported by the egress data plane"
		default:
			// Malformed Upstream (no host). Drop so the rule falls back
			// instead of routing nowhere; the status controller flags it.
			continue
		}
		out = append(out, c)
	}
	return out
}

// TranslatableCandidates restricts a rule's resolved candidate set to
// backends the data plane can actually serve: same-schema candidates
// as before (APO-689), plus cross-schema candidates the llmcall
// registry can translate to (APO-742). A cross-schema candidate
// survives when both the rule's provider and the candidate's schema
// carry a registered Codec AND the Backend declares modelRewrites —
// model IDs don't transfer across providers, so a backend with no
// rewrite table can never serve and is dropped here rather than
// dead-weighting selection. This filtering is static-only; per-request
// gates (streaming translatability, capability misses, whether a
// rewrite actually covers the request's model) run at RequestBody EOS.
//
// A "custom" provider does endpoint-only matching and clrk has no
// parser for its wire format, so it can neither verify schema parity
// nor translate; those rules keep every candidate and trust the
// operator's backendRefs (e.g. an OpenAI-compatible gateway routing to
// api.openai.com). Refuse (InferencePool) candidates are always kept
// so selecting one yields a clean 501 rather than a silent
// fall-through to the original host.
func TranslatableCandidates(candidates []Candidate, provider string) []Candidate {
	if provider == "" || provider == "custom" {
		return candidates
	}
	src := llmcall.ByName(provider)
	out := make([]Candidate, 0, len(candidates))
	for _, c := range candidates {
		if c.Refuse || c.System == provider {
			out = append(out, c)
			continue
		}
		if src == nil || src.Codec == nil {
			continue
		}
		if tgt := llmcall.ByName(c.System); tgt == nil || tgt.Codec == nil {
			continue
		}
		if len(c.ModelRewrites) == 0 {
			continue
		}
		out = append(out, c)
	}
	return out
}

// BackendRefIsClrkBackend reports whether ref names a clrk
// (clrk.apoxy.dev/Backend) resource. Empty group/kind defaults to the
// Gateway API Service backend and must be rejected.
func BackendRefIsClrkBackend(ref gwapiv1.BackendRef) bool {
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

// RouteAttachesTo reports whether any of route's parentRefs point at
// the EgressGateway (egNamespace, egName). Cross-namespace refs are
// allowed today (no ReferenceGrant gate); revisit when we add deny
// semantics.
func RouteAttachesTo(route clrkv1alpha1.AIProviderRoute, egNamespace, egName string) bool {
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

// PolicyAttachesTo reports whether any of policy's parentRefs point at
// the AIProviderRoute (routeNamespace, routeName). Used to join
// FallbackRoutingPolicies onto the routes they modify; CIP-style
// semantics (group+kind must spell out clrk.apoxy.dev/AIProviderRoute,
// namespace defaults to the policy's own).
func PolicyAttachesTo(policy clrkv1alpha1.FallbackRoutingPolicy, routeNamespace, routeName string) bool {
	for _, ref := range policy.Spec.ParentRefs {
		group := ""
		if ref.Group != nil {
			group = string(*ref.Group)
		}
		kind := ""
		if ref.Kind != nil {
			kind = string(*ref.Kind)
		}
		if group != clrkAPIGroup || kind != "AIProviderRoute" {
			continue
		}
		if string(ref.Name) != routeName {
			continue
		}
		ns := policy.Namespace
		if ref.Namespace != nil && *ref.Namespace != "" {
			ns = string(*ref.Namespace)
		}
		if ns == routeNamespace {
			return true
		}
	}
	return false
}

// FallbackFor returns the first FallbackRoutingPolicy attached to the
// given AIProviderRoute, or nil. Multiple attached policies have no
// defined precedence; first (list order) wins, matching the credTable
// convention for duplicate CIPs.
func FallbackFor(policies []clrkv1alpha1.FallbackRoutingPolicy, routeNamespace, routeName string) *clrkv1alpha1.FallbackRoutingPolicy {
	for i := range policies {
		if PolicyAttachesTo(policies[i], routeNamespace, routeName) {
			return &policies[i]
		}
	}
	return nil
}
