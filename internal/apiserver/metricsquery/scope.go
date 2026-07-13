package metricsquery

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// EntityKind selects which path entity a per-entity metrics Connecter scopes
// to. It is the metrics analogue of the telemetry agentKind pin: the scope is
// derived from the request path and server-enforced, so a caller can only read
// the entity its path names.
type EntityKind string

const (
	// EntityTaskAgent scopes to the named TaskAgent
	// (taskagentmetrics/{name}/series).
	EntityTaskAgent EntityKind = "taskagent"
	// EntityDaemonAgent scopes to the named DaemonAgent
	// (daemonagentmetrics/{name}/series).
	EntityDaemonAgent EntityKind = "daemonagent"
	// EntityEgressGateway scopes to the named EgressGateway
	// (egressgatewaymetrics/{name}/series).
	EntityEgressGateway EntityKind = "egressgateway"
)

// scope is the server-enforced read scope: a set of WHERE pins narrowing the
// aggregation to one namespace, agent, and/or egress gateway. Every field is
// derived from the request (path for per-entity, namespace + ?scope= for the
// fleet surface) and escaped when rendered, never trusted raw.
type scope struct {
	// namespace pins agent.namespace (the project bound for the fleet surface
	// and the per-agent surface). Empty for a pure gateway scope, whose EGRef
	// already encodes the namespace.
	namespace string
	// agentKind pins agent.kind ("TaskAgent" / "DaemonAgent"), or "".
	agentKind string
	// agent pins the materialized Agent column (agent.name), or "".
	agent string
	// egRef pins the EGRef column ("<ns>/<name>" of the EgressGateway), or "".
	egRef string
}

// entityScope builds the fixed scope for a per-entity series Connecter from the
// path namespace + entity name. An agent scope pins namespace + kind + name; a
// gateway scope pins EGRef only (it already encodes the namespace, and an
// agent.namespace predicate would drop egress spans that never recorded it --
// same reasoning as refineFleetScope's gateway case).
func entityScope(kind EntityKind, namespace, name string) scope {
	switch kind {
	case EntityTaskAgent:
		return scope{namespace: namespace, agentKind: clrkv1alpha1.AgentKindTask, agent: name}
	case EntityDaemonAgent:
		return scope{namespace: namespace, agentKind: clrkv1alpha1.AgentKindDaemon, agent: name}
	case EntityEgressGateway:
		return scope{egRef: namespace + "/" + name}
	default:
		// Fail closed: an unhandled kind (a wiring bug) pins an impossible Agent
		// so it matches nothing, rather than broadening to the whole namespace.
		return scope{namespace: namespace, agent: "\x00"}
	}
}

// refineFleetScope narrows a fleet (namespace-only) scope by the optional typed
// scopeKind / scopeName fields ("TaskAgent" / "DaemonAgent" / "EgressGateway" +
// the entity name). The namespace bound is retained for agent scopes, so a
// refinement can only narrow within the path's namespace, never escape it. Both
// empty leaves the fleet scope unchanged; one set without the other, or an
// unknown kind, is an error.
func refineFleetScope(base scope, kind, name string) (scope, error) {
	kind = strings.TrimSpace(kind)
	name = strings.TrimSpace(name)
	if kind == "" && name == "" {
		return base, nil
	}
	if kind == "" || name == "" {
		return base, fmt.Errorf("scopeKind and scopeName must be set together")
	}
	// The name reaches the hand-built SQL through chsql.String, which escapes
	// quotes but does not constrain shape (chsql.go documents that its callers
	// must keep values to apiserver-validated names). The path-derived surfaces
	// get that validation for free; apply the same DNS-1123 check here.
	if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 {
		return base, fmt.Errorf("invalid scopeName %q: %s", name, errs[0])
	}
	switch kind {
	case scopeKindEgressGateway:
		// Match the per-entity gateway scope exactly -- pin EGRef only. EGRef
		// already encodes the gateway's namespace, and retaining the
		// agent.namespace predicate would drop egress spans that never recorded
		// it, under-counting the gateway.
		base.egRef = base.namespace + "/" + name
		base.namespace = ""
	case clrkv1alpha1.AgentKindTask:
		base.agentKind = clrkv1alpha1.AgentKindTask
		base.agent = name
	case clrkv1alpha1.AgentKindDaemon:
		base.agentKind = clrkv1alpha1.AgentKindDaemon
		base.agent = name
	default:
		return base, fmt.Errorf("invalid scopeKind %q (want TaskAgent, DaemonAgent, or EgressGateway)", kind)
	}
	return base, nil
}

// scopeKindEgressGateway is the scopeKind value selecting a gateway scope. The
// agent kinds reuse the clrkv1alpha1.AgentKind* constants ("TaskAgent" /
// "DaemonAgent"); EgressGateway has no such constant, so it is named here.
const scopeKindEgressGateway = "EgressGateway"

// clauses renders the scope as WHERE predicates against the given attribute-map
// column (SpanAttributes for traces, LogAttributes for logs). The Agent and
// EGRef columns are materialized on both tables, so they are referenced
// directly; namespace and kind index the per-record attribute map.
func (s scope) clauses(attrCol string) []string {
	var cl []string
	if s.egRef != "" {
		cl = append(cl, "EGRef = "+sqlString(s.egRef))
	}
	if s.namespace != "" {
		cl = append(cl, attrCol+"['agent.namespace'] = "+sqlString(s.namespace))
	}
	if s.agentKind != "" {
		cl = append(cl, attrCol+"['agent.kind'] = "+sqlString(s.agentKind))
	}
	if s.agent != "" {
		cl = append(cl, "Agent = "+sqlString(s.agent))
	}
	return cl
}
