package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// MCPRouteFilterType identifies the type of filter in an MCPRouteFilter.
// +kubebuilder:validation:Enum=ToolPolicy;ExtensionRef
type MCPRouteFilterType string

const (
	MCPFilterToolPolicy   MCPRouteFilterType = "ToolPolicy"
	MCPFilterExtensionRef MCPRouteFilterType = "ExtensionRef"
)

// ToolPolicyFilter controls tool-call-level policy for MCP traffic.
type ToolPolicyFilter struct {
	// AllowedTools is an explicit allowlist. Supports glob.
	// If set, only matching tools may be invoked.
	// +optional
	AllowedTools []string `json:"allowedTools,omitempty"`

	// AllowedToolsRegex is an explicit allowlist of Go regexp patterns.
	// Evaluated as a union with AllowedTools: when either list is
	// non-empty, the tool must match at least one entry across both.
	// +optional
	AllowedToolsRegex []string `json:"allowedToolsRegex,omitempty"`

	// DeniedTools blocks specific tools. Evaluated after AllowedTools.
	// +optional
	DeniedTools []string `json:"deniedTools,omitempty"`

	// DeniedToolsRegex blocks tools by Go regexp pattern. Evaluated as
	// a union with DeniedTools; a match in either list denies the call.
	// +optional
	DeniedToolsRegex []string `json:"deniedToolsRegex,omitempty"`

	// RequireConfirmation lists tools that need out-of-band human
	// confirmation before execution proceeds.
	// +optional
	RequireConfirmation []string `json:"requireConfirmation,omitempty"`

	// MaxCallsPerExecution caps total tool invocations per agent run.
	// +optional
	MaxCallsPerExecution *int32 `json:"maxCallsPerExecution,omitempty"`

	// RateLimits enforces per-tool rate limits using the same windowing
	// vocabulary as RateLimitPolicy. Each entry is scoped independently.
	// +optional
	RateLimits []ToolRateLimit `json:"rateLimits,omitempty"`
}

// ToolRateLimit caps invocations for a subset of tools matched by the
// enclosing rule.
type ToolRateLimit struct {
	// Tools selects which tools this limit applies to. Supports glob.
	// Empty means all tools matched by the enclosing rule.
	// +optional
	Tools []string `json:"tools,omitempty"`

	// ToolsRegex restricts the limit to tools matching any Go regexp
	// pattern here. Evaluated as a union with Tools; empty Tools and
	// empty ToolsRegex means "any tool matched by the enclosing rule".
	// +optional
	ToolsRegex []string `json:"toolsRegex,omitempty"`

	// Requests is the maximum number of invocations permitted within
	// Window before further calls are rejected.
	Requests int32 `json:"requests"`

	// Window is a Go duration string (e.g. "1m", "1h", "24h") over which
	// Requests are counted.
	Window string `json:"window"`

	// Scope chooses the counter dimension. Defaults to PerAgent.
	// +kubebuilder:default=PerAgent
	Scope RateLimitScope `json:"scope,omitempty"`
}

// MCPRouteFilter defines a filter for MCPRoute rules.
type MCPRouteFilter struct {
	// Type selects the filter.
	Type MCPRouteFilterType `json:"type"`

	// ToolPolicy — inline, MCP-specific.
	// +optional
	ToolPolicy *ToolPolicyFilter `json:"toolPolicy,omitempty"`

	// ExtensionRef for cross-cutting policies.
	// +optional
	ExtensionRef *gwapiv1.LocalObjectReference `json:"extensionRef,omitempty"`
}

// MCPRouteMatch defines match criteria for MCP traffic.
type MCPRouteMatch struct {
	// Servers matches by MCP server identity (URL pattern).
	// +optional
	Servers []string `json:"servers,omitempty"`

	// Tools matches by tool name. Supports glob: "github_*".
	// +optional
	Tools []string `json:"tools,omitempty"`

	// ToolsRegex matches by tool name via Go regexp. Evaluated as a
	// union with Tools; a match on either selects the rule.
	// +optional
	ToolsRegex []string `json:"toolsRegex,omitempty"`

	// Resources matches by MCP resource URI pattern.
	// +optional
	Resources []string `json:"resources,omitempty"`

	// Methods matches MCP JSON-RPC methods.
	// +optional
	Methods []string `json:"methods,omitempty"`
}

// MCPRouteRule defines a rule within an MCPRoute.
type MCPRouteRule struct {
	Matches []MCPRouteMatch `json:"matches,omitempty"`

	// Filters apply policy to matched MCP traffic.
	// +optional
	Filters []MCPRouteFilter `json:"filters,omitempty"`

	// BackendRefs optionally routes through an explicit proxy.
	// Empty = passthrough to original destination.
	// +optional
	BackendRefs []gwapiv1.BackendRef `json:"backendRefs,omitempty"`
}

// MCPRouteSpec defines the desired state of an MCPRoute.
type MCPRouteSpec struct {
	// ParentRefs attaches this route to EgressGateway listeners.
	ParentRefs []gwapiv1.ParentReference `json:"parentRefs"`

	// Hostnames scopes to MCP server endpoints by Host header.
	// +optional
	Hostnames []gwapiv1.Hostname `json:"hostnames,omitempty"`

	Rules []MCPRouteRule `json:"rules"`
}

// MCPRouteStatus describes the observed state of an MCPRoute.
type MCPRouteStatus struct {
	Parents []gwapiv1.RouteParentStatus `json:"parents,omitempty"`
}

// MCPRoute enables fine-grained routing and policy for outbound MCP
// (Model Context Protocol) traffic.
//
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=mcpr
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type MCPRoute struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              MCPRouteSpec   `json:"spec"`
	Status            MCPRouteStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

// MCPRouteList contains a list of MCPRoute resources.
type MCPRouteList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MCPRoute `json:"items"`
}
