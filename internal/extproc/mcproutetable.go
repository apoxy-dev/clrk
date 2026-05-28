package extproc

import (
	"context"
	"path"
	"regexp"
	"strings"

	"github.com/go-logr/logr"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/extproc/parsers"
)

// mcpRouteTable holds a flattened view of the MCPRoutes attached to one
// EgressGateway. Each (rule, match) tuple becomes one mcpRouteRule;
// match() walks the slice in spec order and returns the first hit.
//
// Lifecycle mirrors routeTable: rebuilt by sinkRegistry when the per-EG
// snapshot changes; read-only after construction so match() needs no
// synchronization.
type mcpRouteTable struct {
	// hostnames is the union of every scoped route's Hostnames, used
	// by hostMatches() as a cheap gate before any body parse.
	hostnames []string
	// acceptAnyHost is true when at least one attached route declared
	// no Hostnames. Such a route accepts all hosts, so the cheap gate
	// must always return true regardless of the hostnames union (which
	// would otherwise be populated from sibling scoped routes and
	// reject hosts the unscoped route should accept).
	acceptAnyHost bool
	rules         []mcpRouteRule
}

// mcpRouteRule is one flattened (route, rule, match) tuple. routeHosts
// captures the Hostnames declared on the parent MCPRoute (one rule may
// inherit from a route that lists "*.mcp.example.com" plus "foo.bar")
// so match() can scope its decision to the right route's hostnames
// without re-walking the source spec.
type mcpRouteRule struct {
	routeNamespace string
	routeName      string

	// routeHosts is the parent MCPRoute's Spec.Hostnames, lowered. Empty
	// means the route is host-unscoped (matches any host).
	routeHosts []string

	// servers, tools, resources, methods are globs in path.Match shape.
	// Empty means "any" for that field.
	servers   []string
	tools     []string
	resources []string
	methods   []string

	// toolsRE is the compiled MCPRouteMatch.ToolsRegex slice. nil when
	// the match clause had no regex patterns or all of them failed to
	// compile (invalid patterns drop the whole rule at build time).
	toolsRE []*regexp.Regexp

	// toolPolicy is the first ToolPolicy filter on the containing rule,
	// pre-compiled. nil when the rule has no ToolPolicy filter.
	toolPolicy *compiledToolPolicy
}

// compiledToolPolicy wraps a ToolPolicyFilter with its regex patterns
// pre-compiled so the request-time evaluation doesn't pay the compile
// cost on every call. raw is kept around so the enforcement code can
// read the AllowedTools/DeniedTools glob lists directly.
type compiledToolPolicy struct {
	raw          *clrkv1alpha1.ToolPolicyFilter
	allowedRegex []*regexp.Regexp
	deniedRegex  []*regexp.Regexp
}

// buildMCPRouteTable filters routes by parentRef matching the given
// EgressGateway and flattens each (route, rule, match) tuple into an
// mcpRouteRule. Returns nil when no rules survive, so callers can use
// `mcpRoutes != nil` as the cheap gate that short-circuits MCP body
// parsing for traffic with no policy.
//
// Match clauses within a rule are OR'd per Gateway API conventions, so
// each match becomes its own mcpRouteRule entry; first-hit-wins at
// match time.
//
// Invalid regex patterns are logged once via the context's logr.Logger
// and drop the whole rule — partial enforcement of a policy is worse
// than no enforcement because the operator may not notice the gap.
func buildMCPRouteTable(ctx context.Context, egNamespace, egName string, routes []clrkv1alpha1.MCPRoute) *mcpRouteTable {
	log := logr.FromContextOrDiscard(ctx).WithName("mcproutetable")
	t := &mcpRouteTable{}
	for _, r := range routes {
		if !mcpRouteAttachesTo(r, egNamespace, egName) {
			continue
		}
		hosts := lowerHostnames(r.Spec.Hostnames)
		for _, rule := range r.Spec.Rules {
			tp, tpOK := compileToolPolicy(firstMCPToolPolicy(rule.Filters), r.Namespace, r.Name, log)
			if !tpOK {
				continue
			}
			for _, m := range rule.Matches {
				toolsRE, ok := compileRegexList(m.ToolsRegex, r.Namespace, r.Name, "MCPRouteMatch.ToolsRegex", log)
				if !ok {
					continue
				}
				t.rules = append(t.rules, mcpRouteRule{
					routeNamespace: r.Namespace,
					routeName:      r.Name,
					routeHosts:     hosts,
					servers:        append([]string(nil), m.Servers...),
					tools:          append([]string(nil), m.Tools...),
					resources:      append([]string(nil), m.Resources...),
					methods:        append([]string(nil), m.Methods...),
					toolsRE:        toolsRE,
					toolPolicy:     tp,
				})
			}
		}
		if len(hosts) == 0 {
			t.acceptAnyHost = true
		} else {
			t.hostnames = appendUniqueStrings(t.hostnames, hosts...)
		}
	}
	if len(t.rules) == 0 {
		return nil
	}
	return t
}

// firstMCPToolPolicy returns the first ToolPolicy filter on a rule, or
// nil. Multiple ToolPolicy filters per rule have no MVP semantics (last
// wins? merged?); honoring the first matches firstTokenBudget.
func firstMCPToolPolicy(filters []clrkv1alpha1.MCPRouteFilter) *clrkv1alpha1.ToolPolicyFilter {
	for i := range filters {
		f := &filters[i]
		if f.Type == clrkv1alpha1.MCPFilterToolPolicy && f.ToolPolicy != nil {
			return f.ToolPolicy
		}
	}
	return nil
}

func compileToolPolicy(tp *clrkv1alpha1.ToolPolicyFilter, ns, name string, log logr.Logger) (*compiledToolPolicy, bool) {
	if tp == nil {
		return nil, true
	}
	allowed, ok := compileRegexList(tp.AllowedToolsRegex, ns, name, "ToolPolicyFilter.AllowedToolsRegex", log)
	if !ok {
		return nil, false
	}
	denied, ok := compileRegexList(tp.DeniedToolsRegex, ns, name, "ToolPolicyFilter.DeniedToolsRegex", log)
	if !ok {
		return nil, false
	}
	return &compiledToolPolicy{raw: tp, allowedRegex: allowed, deniedRegex: denied}, true
}

// compileRegexList compiles each pattern; on the first compile error
// the whole list is rejected and the caller is expected to drop the
// enclosing rule. The route ns/name and field name are logged so the
// operator can find the offending CRD without grepping logs against
// the raw regex syntax.
func compileRegexList(patterns []string, ns, name, field string, log logr.Logger) ([]*regexp.Regexp, bool) {
	if len(patterns) == 0 {
		return nil, true
	}
	out := make([]*regexp.Regexp, 0, len(patterns))
	for _, p := range patterns {
		re, err := regexp.Compile(p)
		if err != nil {
			log.Error(err, "Invalid regex in MCPRoute, dropping rule",
				"namespace", ns, "name", name, "field", field, "pattern", p)
			return nil, false
		}
		out = append(out, re)
	}
	return out, true
}

// mcpRouteAttachesTo reports whether any of route's parentRefs point at
// the EgressGateway (egNamespace, egName). Mirrors routeAttachesTo —
// parametrizing on the source type would require interface ceremony
// for two callers.
func mcpRouteAttachesTo(route clrkv1alpha1.MCPRoute, egNamespace, egName string) bool {
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

// hostMatches reports whether host falls under any attached route's
// Spec.Hostnames. Used as the cheap pre-parse gate so non-MCP egress
// traffic pays nothing for the JSON-RPC parse. Gateway-API semantics:
// a pattern starting with "*." matches exactly one extra label
// (RFC 4592); plain patterns match exactly. Case-insensitive.
//
// Caller (mcpCandidate) guards on t != nil so we don't repeat that
// check here.
func (t *mcpRouteTable) hostMatches(host string) bool {
	if t.acceptAnyHost {
		return true
	}
	h := strings.ToLower(host)
	for _, pattern := range t.hostnames {
		if hostnameMatches(pattern, h) {
			return true
		}
	}
	return false
}

// match returns a pointer to the first rule that accepts (host, info).
// host is the request :authority with port stripped, already lowered.
// info carries the parsed MCP method + tool + resource. Caller
// (evaluate) guards on info != nil; only invoked after a successful
// ParseRequest.
//
// Per Gateway API conventions, fields within one MCPRouteMatch are
// AND-combined and empty lists mean "any". Tools is only consulted on
// tools/call; Resources only on resources/read.
func (t *mcpRouteTable) match(host string, info *parsers.MCPInfo) *mcpRouteRule {
	h := strings.ToLower(host)
	for i := range t.rules {
		rr := &t.rules[i]
		if !rr.acceptsHost(h) {
			continue
		}
		if !matchesGlobAny(rr.servers, h) {
			continue
		}
		if !matchesGlobAny(rr.methods, info.Method) {
			continue
		}
		if info.Method == parsers.MCPMethodToolsCall {
			if !matchesToolPattern(rr.tools, rr.toolsRE, info.ToolName) {
				continue
			}
		}
		if info.Method == parsers.MCPMethodResourcesRead {
			if !matchesGlobAny(rr.resources, info.ResourceURI) {
				continue
			}
		}
		return rr
	}
	return nil
}

// mcpCandidate is the cheap pre-parse gate: an MCPRoute must be
// attached to the EG, the host must fall under its Hostnames, and
// the request content-type must be JSON. Returns false fast so
// non-MCP traffic skips the JSON-RPC parse entirely.
func mcpCandidate(t *mcpRouteTable, host, contentType string) bool {
	if t == nil || !t.hostMatches(host) {
		return false
	}
	return strings.HasPrefix(strings.ToLower(contentType), "application/json")
}

// mcpEvalResult captures everything an MCP enforcement pass produces:
// the parsed envelope, the route that matched (if any), and the
// ToolPolicy decision (if any). All fields are independently
// nil/empty so callers can stamp partial state on the Record.
type mcpEvalResult struct {
	info          *parsers.MCPInfo
	matchedRoute  *mcpRouteRule
	decision      mcpDecision
	denyDetail    string
}

// evaluate runs the parse → match → ToolPolicy chain for one buffered
// request body. Caller has already gated on hostMatches + content
// type, so this always at least attempts the parse. Returns a zero
// result with all fields nil/empty when the body wasn't a valid
// single JSON-RPC request — caller should fall through to the
// continue path in that case.
func (t *mcpRouteTable) evaluate(host string, body []byte, truncated bool) mcpEvalResult {
	info := parsers.ParseRequest(body, truncated)
	if info == nil {
		return mcpEvalResult{}
	}
	rr := t.match(host, info)
	if rr == nil {
		return mcpEvalResult{info: info}
	}
	decision, detail := evaluateToolPolicy(rr.toolPolicy, info, rr.routeNamespace, rr.routeName)
	return mcpEvalResult{
		info:         info,
		matchedRoute: rr,
		decision:     decision,
		denyDetail:   detail,
	}
}

// stampMCPResult writes the evaluation outcome onto the Record.
// Split out so server.go's Process() loop stays a sequence of guard
// clauses rather than a five-deep if-cascade.
func stampMCPResult(rec *Record, res mcpEvalResult) {
	if res.info == nil {
		return
	}
	rec.MCP = res.info
	if res.matchedRoute != nil {
		rec.MatchedMCPRouteNamespace = res.matchedRoute.routeNamespace
		rec.MatchedMCPRouteName = res.matchedRoute.routeName
	}
	if res.decision != "" {
		rec.MCPToolPolicyDecision = string(res.decision)
	}
}

// acceptsHost reports whether host falls under this rule's parent
// route's Hostnames. Empty routeHosts means the parent route declared
// no Hostnames, which accepts any host.
func (rr *mcpRouteRule) acceptsHost(host string) bool {
	if len(rr.routeHosts) == 0 {
		return true
	}
	for _, pattern := range rr.routeHosts {
		if hostnameMatches(pattern, host) {
			return true
		}
	}
	return false
}

// matchesToolPattern is the glob-∪-regex evaluator used for Tools (in
// the route matcher) and for AllowedTools/DeniedTools (in the policy
// filter — the caller passes the appropriate field pairs). Returns
// true when either list is empty (no constraint) or the value matches
// at least one entry across both.
func matchesToolPattern(globs []string, regexes []*regexp.Regexp, value string) bool {
	if len(globs) == 0 && len(regexes) == 0 {
		return true
	}
	if matchesGlobList(globs, value) {
		return true
	}
	for _, re := range regexes {
		if re.MatchString(value) {
			return true
		}
	}
	return false
}

// matchesGlobList is matchesGlobAny without the "empty means any"
// short-circuit. Used by the union evaluator that needs to ask "did
// the glob list match?" without lumping in the empty-list case.
func matchesGlobList(patterns []string, value string) bool {
	for _, p := range patterns {
		ok, err := path.Match(p, value)
		if err == nil && ok {
			return true
		}
	}
	return false
}

// matchesAnyRegex reports whether any regex in rs matches value.
// Empty rs returns false (no implicit "any").
func matchesAnyRegex(rs []*regexp.Regexp, value string) bool {
	for _, r := range rs {
		if r.MatchString(value) {
			return true
		}
	}
	return false
}

// mcpDecision is the outcome of a ToolPolicy evaluation. The empty
// value means "no decision applies" (e.g. non-tools/call method).
type mcpDecision string

const (
	mcpDecisionAllow mcpDecision = "allow"
	mcpDecisionDeny  mcpDecision = "deny"
)

// evaluateToolPolicy applies a compiled ToolPolicyFilter to a parsed
// MCP request. Returns ("", "") when there is no decision to make
// (policy nil, request isn't tools/call); (deny, reason) when the
// call must be rejected; (allow, "") when the policy explicitly
// approved the call.
//
// Deny wins: a tool that matches DeniedTools/DeniedToolsRegex is
// rejected even if it also matches AllowedTools. When the allowlist
// (in either form) is non-empty, the tool MUST match it; an empty
// allowlist means "allow anything that wasn't denied".
//
// RateLimits / MaxCallsPerExecution / RequireConfirmation are
// deliberately not consulted here — they require state (counters,
// async confirmation) that ext_proc doesn't own in the APO-556 MVP.
func evaluateToolPolicy(tp *compiledToolPolicy, info *parsers.MCPInfo, routeNS, routeName string) (mcpDecision, string) {
	if tp == nil || info == nil || info.Method != parsers.MCPMethodToolsCall {
		return "", ""
	}
	name := info.ToolName
	raw := tp.raw
	if matchesGlobList(raw.DeniedTools, name) || matchesAnyRegex(tp.deniedRegex, name) {
		return mcpDecisionDeny, "tool '" + name + "' denied by MCPRoute " + routeNS + "/" + routeName
	}
	if len(raw.AllowedTools) > 0 || len(tp.allowedRegex) > 0 {
		if !matchesGlobList(raw.AllowedTools, name) && !matchesAnyRegex(tp.allowedRegex, name) {
			return mcpDecisionDeny, "tool '" + name + "' not in allowlist for MCPRoute " + routeNS + "/" + routeName
		}
	}
	return mcpDecisionAllow, ""
}

// hostnameMatches implements Gateway API hostname matching per
// RFC 4592: "*.example.com" matches "foo.example.com" (one wildcard
// label) but not "example.com" (zero labels) and not
// "foo.bar.example.com" (two labels). Plain patterns match exactly.
// Case-insensitive; caller pre-lowers host. Mirrors the equivalent
// helper in internal/egress/route_table.go but stays separate to
// avoid pulling the egress package into the extproc dep graph.
func hostnameMatches(pattern, host string) bool {
	pattern = strings.ToLower(pattern)
	if strings.HasPrefix(pattern, "*.") {
		suffix := pattern[1:]
		if !strings.HasSuffix(host, suffix) {
			return false
		}
		return strings.Count(host, ".") == strings.Count(pattern, ".")
	}
	return pattern == host
}

func lowerHostnames(hs []gwapiv1.Hostname) []string {
	if len(hs) == 0 {
		return nil
	}
	out := make([]string, 0, len(hs))
	for _, h := range hs {
		out = append(out, strings.ToLower(string(h)))
	}
	return out
}

func appendUniqueStrings(dst []string, src ...string) []string {
	for _, s := range src {
		found := false
		for _, existing := range dst {
			if existing == s {
				found = true
				break
			}
		}
		if !found {
			dst = append(dst, s)
		}
	}
	return dst
}
