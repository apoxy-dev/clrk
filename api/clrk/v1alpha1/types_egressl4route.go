package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// L4Protocol selects TCP or UDP.
// +kubebuilder:validation:Enum=TCP;UDP
type L4Protocol string

const (
	L4ProtocolTCP L4Protocol = "TCP"
	L4ProtocolUDP L4Protocol = "UDP"
)

// L4RouteFilterType identifies the type of filter in an L4RouteFilter.
// +kubebuilder:validation:Enum=ExtensionRef
type L4RouteFilterType string

const (
	L4FilterExtensionRef L4RouteFilterType = "ExtensionRef"
)

// L4PortMatch matches a single port or an inclusive port range.
type L4PortMatch struct {
	// Port matches a single port.
	// +optional
	Port *int32 `json:"port,omitempty"`

	// StartPort + EndPort define an inclusive range.
	// Both must be set together. Mutually exclusive with Port.
	// +optional
	StartPort *int32 `json:"startPort,omitempty"`
	// +optional
	EndPort *int32 `json:"endPort,omitempty"`
}

// L4RouteMatch defines match criteria for L4 traffic.
type L4RouteMatch struct {
	// DestinationCIDRs matches by IP range. Single IPs as /32 or /128.
	// IPv4 and IPv6 CIDRs are both honored.
	// +optional
	// +listType=set
	// +kubebuilder:validation:items:Format=cidr
	DestinationCIDRs []string `json:"destinationCIDRs,omitempty"`

	// DestinationHostnames matches by hostname. Exact
	// (`api.openai.com`) and wildcard (`*.openai.com`) forms are
	// accepted (gwapiv1.Hostname semantics — wildcard matches exactly
	// one prefix label).
	//
	// On TLS-terminated listeners (protocol=TLS or HTTPS with
	// mode=Terminate) the match runs against the SNI value the
	// tls_inspector recorded on the connection. On plain TCP
	// listeners the match runs against the DNS-bound destination
	// name: the worker snoops UDP/53 responses, caches each
	// `(resolved IP) → name` binding, and emits the hostname for the
	// connection's destination IP via PROXY v2 TLVDstName so Envoy
	// and L4 ext_proc records pick it up.
	//
	// Limitation: encrypted resolvers (DoT on TCP/853, DoH on
	// TCP/443) bypass the snoop. Connections from agents that use
	// those resolvers fall back to CIDR-only matching — no error,
	// just no name binding. Use the kernel resolver (default) to
	// guarantee hostname rules apply.
	// +optional
	// +listType=set
	DestinationHostnames []gwapiv1.Hostname `json:"destinationHostnames,omitempty"`

	// Ports restricts to specific destination ports or port ranges.
	// +optional
	Ports []L4PortMatch `json:"ports,omitempty"`

	// Protocol selects TCP or UDP. Must be compatible with the parent
	// listener's protocol. If unset, inherits from the listener.
	// +optional
	Protocol *L4Protocol `json:"protocol,omitempty"`

	// SourceAgents narrows the match to traffic from specific Agent CRs.
	// +optional
	SourceAgents *metav1.LabelSelector `json:"sourceAgents,omitempty"`
}

// L4RouteFilter defines a filter for EgressL4Route rules.
type L4RouteFilter struct {
	// +kubebuilder:validation:Enum=ExtensionRef
	Type         L4RouteFilterType             `json:"type"`
	ExtensionRef *gwapiv1.LocalObjectReference `json:"extensionRef,omitempty"`
}

// EgressL4RouteRule defines a rule within an EgressL4Route.
type EgressL4RouteRule struct {
	Matches []L4RouteMatch `json:"matches,omitempty"`

	// Filters — ExtensionRef to LoggingPolicy, RateLimitPolicy, etc.
	// +optional
	Filters []L4RouteFilter `json:"filters,omitempty"`

	// BackendRefs optionally routes through an explicit proxy.
	// Empty = passthrough to original destination.
	// +optional
	BackendRefs []gwapiv1.BackendRef `json:"backendRefs,omitempty"`
}

// EgressL4RouteSpec defines the desired state of an EgressL4Route.
type EgressL4RouteSpec struct {
	// ParentRefs attaches to EgressGateway TCP or UDP listeners.
	ParentRefs []gwapiv1.ParentReference `json:"parentRefs"`

	Rules []EgressL4RouteRule `json:"rules"`
}

// EgressL4RouteStatus describes the observed state of an EgressL4Route.
type EgressL4RouteStatus struct {
	Parents []gwapiv1.RouteParentStatus `json:"parents,omitempty"`
}

// EgressL4Route provides destination CIDR, port, and protocol matching
// for raw L4 egress traffic.
//
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=el4r
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type EgressL4Route struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              EgressL4RouteSpec   `json:"spec"`
	Status            EgressL4RouteStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

// EgressL4RouteList contains a list of EgressL4Route resources.
type EgressL4RouteList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EgressL4Route `json:"items"`
}
