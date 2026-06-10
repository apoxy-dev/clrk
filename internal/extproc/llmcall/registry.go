package llmcall

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

// Provider is one registered wire schema: identity, traffic matching,
// telemetry extraction, and (when implemented) IR conversion. Provider
// packages under providers/ construct one and call Register from init.
type Provider struct {
	// Name is the canonical gen_ai.system value ("anthropic", "openai",
	// "google_genai"). It is the key the route table, backend schema
	// resolution, and telemetry all share.
	Name string

	// Aliases are CRD short names that resolve to Name via Canonical
	// (e.g. AIProviderRouteMatch.Provider "google" -> "google_genai").
	Aliases []string

	// Hosts are the exact :authority hosts (lowercase, port already
	// stripped by the caller) this provider serves.
	Hosts []string

	// MatchHost, when set, replaces the exact Hosts match — for
	// wildcard surfaces like *.openai.azure.com. It receives a
	// lowercased host.
	MatchHost func(host string) bool

	// Telemetry extracts ProviderInfo from captured transactions.
	// Required for built-in providers.
	Telemetry Parser

	// Codec converts this provider's wire format to and from the IR.
	// May be nil: the provider is then registered (telemetry, routing)
	// but not translatable.
	Codec Codec

	// Capabilities declare what the schema can express; consumed by
	// cross-schema backend selection.
	Capabilities Capabilities
}

// pendingAliases maps CRD short names to canonical gen_ai.system values
// for schemas that have no provider package yet. The host table is
// registration-driven, but these CRD spellings predate their plugins —
// each entry migrates into its provider package when that plugin lands
// (Register tolerates a plugin re-claiming its pending alias).
var pendingAliases = map[string]string{
	"azure-openai": "azure_openai",
	"bedrock":      "aws_bedrock",
}

var (
	providersByName = map[string]*Provider{}
	aliasToName     = map[string]string{}
	hostOrder       []*Provider
)

func init() {
	for alias, name := range pendingAliases {
		aliasToName[alias] = name
	}
}

// Register adds a provider to the registry. It must be called from
// package init only — lookups are unsynchronized reads after process
// start. It panics on identity conflicts (duplicate name, alias, or
// host) because a double registration is a link-time bug, not a
// runtime condition.
func Register(p Provider) {
	if p.Name == "" {
		panic("llmcall: Register with empty Name")
	}
	if _, dup := providersByName[p.Name]; dup {
		panic(fmt.Sprintf("llmcall: provider %q registered twice", p.Name))
	}
	for _, a := range p.Aliases {
		if owner, ok := aliasToName[a]; ok && owner != p.Name {
			panic(fmt.Sprintf("llmcall: alias %q already maps to %q", a, owner))
		}
	}
	for _, h := range p.Hosts {
		if h != strings.ToLower(h) {
			panic(fmt.Sprintf("llmcall: host %q must be lowercase", h))
		}
		if other := byHost(h); other != nil {
			panic(fmt.Sprintf("llmcall: host %q already served by %q", h, other.Name))
		}
	}
	stored := p
	providersByName[p.Name] = &stored
	for _, a := range p.Aliases {
		aliasToName[a] = p.Name
	}
	hostOrder = append(hostOrder, &stored)
}

// ByHost returns the provider serving host (the request's :authority
// host portion, port stripped). Returns nil when no provider matches.
func ByHost(host string) *Provider {
	if host == "" {
		return nil
	}
	return byHost(strings.ToLower(host))
}

func byHost(host string) *Provider {
	for _, p := range hostOrder {
		if p.MatchHost != nil {
			if p.MatchHost(host) {
				return p
			}
			continue
		}
		for _, h := range p.Hosts {
			if h == host {
				return p
			}
		}
	}
	return nil
}

// ByName returns the provider registered under a canonical
// gen_ai.system value. It deliberately does NOT resolve aliases —
// callers hold canonical names (backend schema resolution runs
// Canonical first), and an alias-only system ("azure_openai" before
// its plugin exists) must stay unresolvable so downstream parsing is
// skipped, the same as for any unrecognized host.
func ByName(system string) *Provider {
	if system == "" {
		return nil
	}
	return providersByName[system]
}

// Canonical normalizes a CRD-supplied provider name to the canonical
// gen_ai.system value used everywhere downstream (route matching,
// span attributes). Unknown names pass through verbatim — `custom`
// has special semantics in the route matcher and a typo is better
// surfaced as a non-matching rule than silently rewritten. No case
// folding: callers lowercase CRD input before calling.
func Canonical(name string) string {
	if canonical, ok := aliasToName[name]; ok {
		return canonical
	}
	return name
}

// KnownName reports whether name (a lowercased CRD-supplied provider
// or schema spelling) is recognized: a registered canonical name, a
// registered alias, or a pending alias whose plugin hasn't landed yet.
// Pending aliases count so admission validation never tightens beyond
// the historical enum ("azure-openai"/"bedrock" were always legal).
// "custom" is not a registry concept — callers that accept it (route
// matching, admission validation) special-case it before asking.
func KnownName(name string) bool {
	if name == "" {
		return false
	}
	if _, ok := aliasToName[name]; ok {
		return true
	}
	_, ok := providersByName[name]
	return ok
}

// KnownNames returns every spelling KnownName accepts — registered
// canonical names plus (pending) aliases — sorted and deduplicated.
// Admission validation uses it for actionable NotSupported details.
func KnownNames() []string {
	seen := make(map[string]bool, len(providersByName)+len(aliasToName))
	for name := range providersByName {
		seen[name] = true
	}
	for alias := range aliasToName {
		seen[alias] = true
	}
	return slices.Sorted(maps.Keys(seen))
}
