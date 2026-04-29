package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// EgressPolicy controls the default action for traffic matching no route.
// +kubebuilder:validation:Enum=allow-all;deny-all
type EgressPolicy string

const (
	EgressPolicyAllowAll EgressPolicy = "allow-all"
	EgressPolicyDenyAll  EgressPolicy = "deny-all"
)

// EgressListenerProtocol selects the interception layer.
// +kubebuilder:validation:Enum=TCP;TLS;HTTP;UDP
type EgressListenerProtocol string

const (
	EgressProtocolTCP  EgressListenerProtocol = "TCP"
	EgressProtocolTLS  EgressListenerProtocol = "TLS"
	EgressProtocolHTTP EgressListenerProtocol = "HTTP"
	EgressProtocolUDP  EgressListenerProtocol = "UDP"
)

// EgressTLSMode controls TLS handling on a listener.
// +kubebuilder:validation:Enum=Passthrough;Terminate
type EgressTLSMode string

const (
	EgressTLSPassthrough EgressTLSMode = "Passthrough"
	EgressTLSTerminate   EgressTLSMode = "Terminate"
)

// EgressListenerTLS configures TLS handling for a listener.
type EgressListenerTLS struct {
	// Mode controls TLS handling.
	//   Passthrough: SNI-route only, no termination.
	//   Terminate:   MITM decrypt for L7 inspection, re-encrypt to upstream.
	// +kubebuilder:default=Passthrough
	Mode EgressTLSMode `json:"mode"`

	// CACertRef references a Secret with the CA cert+key for on-the-fly
	// cert generation. Required when mode=Terminate.
	// +optional
	CACertRef *gwapiv1.SecretObjectReference `json:"caCertRef,omitempty"`
}

// EgressListener declares an interception capability by protocol layer.
type EgressListener struct {
	// Name identifies this listener. Routes reference it via
	// parentRef.sectionName (standard GW API field).
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`

	// Protocol selects the interception layer.
	Protocol EgressListenerProtocol `json:"protocol"`

	// Port constrains interception to a single destination port.
	// If unset, all ports are intercepted at this protocol layer.
	// +optional
	Port *int32 `json:"port,omitempty"`

	// TLS configures TLS handling. Required when protocol=TLS,
	// optional for HTTP (enables HTTPS MITM). Invalid for TCP/UDP.
	// +optional
	TLS *EgressListenerTLS `json:"tls,omitempty"`

	// AllowedRoutes constrains which route kinds and namespaces may
	// attach. Reuses the standard GW API AllowedRoutes type.
	// +optional
	AllowedRoutes *gwapiv1.AllowedRoutes `json:"allowedRoutes,omitempty"`
}

// EgressGatewaySpec defines the desired state of an EgressGateway.
type EgressGatewaySpec struct {
	// DefaultPolicy applies to traffic that matches no attached route.
	// +kubebuilder:default=deny-all
	DefaultPolicy EgressPolicy `json:"defaultPolicy"`

	// Listeners declare interception capabilities by protocol layer.
	// Routes attach to a specific listener by name via parentRef.sectionName.
	// +kubebuilder:validation:MinItems=1
	Listeners []EgressListener `json:"listeners"`

	// ExtProc configures the L7 HTTP body-capture plugin wired into the
	// Envoy HCM filter chain. Disabled by default — the MITM path still
	// works without body capture, traffic just doesn't get logged.
	// +optional
	ExtProc *ExtProcSpec `json:"extProc,omitempty"`

	// OTLP is the OTLP/HTTP logs sink for captured request/response pairs.
	// Required when ExtProc.Enabled is true.
	// +optional
	OTLP *OTLPLogsSinkSpec `json:"otlp,omitempty"`
}

// ExtProcSpec configures the ext_proc body-capture plugin.
type ExtProcSpec struct {
	// Enabled toggles ext_proc for this gateway.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// CaptureBody bounds how much of the request/response body is captured
	// and emitted to the OTLP sink.
	// +optional
	CaptureBody *BodyCaptureSpec `json:"captureBody,omitempty"`
}

// BodyCaptureSpec governs request/response body capture bounds.
type BodyCaptureSpec struct {
	// MaxBytes caps the captured body size per direction. Bodies larger
	// than this are truncated; the log record carries a truncated marker.
	// +kubebuilder:default=65536
	// +optional
	MaxBytes *int32 `json:"maxBytes,omitempty"`

	// IncludeContentTypes limits body capture to these Content-Type
	// prefixes. Empty means the default set (application/json,
	// application/x-ndjson, text/event-stream).
	// +optional
	IncludeContentTypes []string `json:"includeContentTypes,omitempty"`
}

// OTLPLogsSinkSpec configures the OTLP/HTTP endpoint receiving captured
// request/response records from ext_proc.
type OTLPLogsSinkSpec struct {
	// Endpoint is the OTLP/HTTP base URL (e.g. "https://otel.example.com").
	Endpoint string `json:"endpoint"`

	// Headers are added to every OTLP export — typically used to carry
	// authentication tokens.
	// +optional
	Headers map[string]string `json:"headers,omitempty"`
}

// EgressListenerStatus describes the status of a single listener.
type EgressListenerStatus struct {
	Name           string             `json:"name"`
	AttachedRoutes int32              `json:"attachedRoutes"`
	Conditions     []metav1.Condition `json:"conditions,omitempty"`
}

// EgressGatewayStatus describes the observed state of an EgressGateway.
type EgressGatewayStatus struct {
	Conditions    []metav1.Condition     `json:"conditions,omitempty"`
	Listeners     []EgressListenerStatus `json:"listeners,omitempty"`
	ListenerCount int32                  `json:"listenerCount,omitempty"`

	// EgressBackendAddress is the host:port workers dial to reach this
	// EgressGateway's Envoy data-plane. Populated by the EgressGateway
	// controller after the Envoy-Gateway-managed Service is provisioned.
	// In-cluster this is the EG Service's cluster DNS name; in `clrk dev`
	// it's a NodePort on the k3s container hostname (workers and k3s share
	// a docker network but ClusterIPs aren't routable across them).
	// +optional
	EgressBackendAddress string `json:"egressBackendAddress,omitempty"`
}

// EgressGateway defines a transparent egress proxy that intercepts outbound
// traffic from agent sandboxes.
//
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=egw
// +kubebuilder:printcolumn:name="Policy",type=string,JSONPath=`.spec.defaultPolicy`
// +kubebuilder:printcolumn:name="Listeners",type=integer,JSONPath=`.status.listenerCount`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type EgressGateway struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              EgressGatewaySpec   `json:"spec"`
	Status            EgressGatewayStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

// EgressGatewayList contains a list of EgressGateway resources.
type EgressGatewayList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EgressGateway `json:"items"`
}
