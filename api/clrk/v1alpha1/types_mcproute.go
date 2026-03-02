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

	// DeniedTools blocks specific tools. Evaluated after AllowedTools.
	// +optional
	DeniedTools []string `json:"deniedTools,omitempty"`

	// RequireConfirmation lists tools that need out-of-band human
	// confirmation before execution proceeds.
	// +optional
	RequireConfirmation []string `json:"requireConfirmation,omitempty"`

	// MaxCallsPerExecution caps total tool invocations per agent run.
	// +optional
	MaxCallsPerExecution *int32 `json:"maxCallsPerExecution,omitempty"`
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

// +kubebuilder:object:root=true

// MCPRouteList contains a list of MCPRoute resources.
type MCPRouteList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MCPRoute `json:"items"`
}

func init() {
	SchemeBuilder.Register(&MCPRoute{}, &MCPRouteList{})
}
