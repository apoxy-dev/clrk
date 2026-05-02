package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
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
// +kubebuilder:validation:Enum=TCP;TLS;HTTP;HTTPS;UDP
type EgressListenerProtocol string

const (
	EgressProtocolTCP   EgressListenerProtocol = "TCP"
	EgressProtocolTLS   EgressListenerProtocol = "TLS"
	EgressProtocolHTTP  EgressListenerProtocol = "HTTP"
	EgressProtocolHTTPS EgressListenerProtocol = "HTTPS"
	EgressProtocolUDP   EgressListenerProtocol = "UDP"
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

	// OTLP configures L7 capture and the OTLP/HTTP logs sink for
	// captured request/response pairs. L7 capture is always on for HTTP
	// and MITM-terminated TLS listeners; OTLP defines where the records
	// go and how much body is captured. When unset, records are emitted
	// to the controller-manager's structured log instead.
	// +optional
	OTLP *OTLPLogsSinkSpec `json:"otlp,omitempty"`

	// UpstreamTLS adjusts how the EG-managed Envoy validates TLS when
	// re-encrypting traffic to upstreams (the egress dial after MITM).
	// +optional
	UpstreamTLS *EgressUpstreamTLSSpec `json:"upstreamTLS,omitempty"`
}

// EgressUpstreamTLSSpec configures the EG-managed Envoy's upstream
// connection behavior. The default trust source is the system bundle
// at /etc/ssl/certs/ca-certificates.crt baked into the Envoy image
// and the default DNS resolver is the cluster DNS — this spec lets
// operators override either when reaching upstreams that aren't on
// the public Internet (internal services, integration-test stubs).
type EgressUpstreamTLSSpec struct {
	// AdditionalTrustBundleSecretRef references a Secret carrying one
	// or more CA certificates (PEM, under any data key) that Envoy
	// should trust in addition to the system bundle. Used for private
	// upstreams whose certs aren't anchored in a public CA.
	//
	// All non-empty values in the Secret's `data` map are concatenated
	// and appended to the system bundle inside the Envoy pod via an
	// init container; the main Envoy container reads the merged bundle
	// from the same well-known path it always has.
	// +optional
	AdditionalTrustBundleSecretRef *gwapiv1.SecretObjectReference `json:"additionalTrustBundleSecretRef,omitempty"`

	// HostAliases programs the EG-managed Envoy Pod's
	// spec.hostAliases. Each entry maps an IP to one or more hostnames;
	// the kubelet writes them into the pod's /etc/hosts ahead of the
	// cluster DNS lookup chain. Use this to point Envoy's
	// dynamic_forward_proxy resolver at a specific IP for a given
	// upstream hostname, e.g. an in-cluster stub pretending to be
	// api.openai.com without globally hijacking the cluster's CoreDNS.
	// +optional
	HostAliases []corev1.HostAlias `json:"hostAliases,omitempty"`
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
// request/response records from ext_proc, plus the bounds on what gets
// captured.
type OTLPLogsSinkSpec struct {
	// Endpoint is the OTLP/HTTP base URL (e.g. "https://otel.example.com").
	// Optional — when empty in dev (`clrk dev` sets the
	// CLRK_DEV_OTEL_ENDPOINT env on the controller-manager) records flow
	// to the in-process dev receiver and surface in the TUI's
	// otel-logs/otel-traces panes; in prod with no env override, capture
	// falls back to the controller-manager's structured log.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// Headers are added to every OTLP export — typically used to carry
	// authentication tokens.
	// +optional
	Headers map[string]string `json:"headers,omitempty"`

	// CaptureBody bounds the request/response body bytes captured and
	// emitted as OTLP log records. Defaults: 64KiB per direction;
	// capture application/json, application/x-ndjson, text/event-stream.
	// +optional
	CaptureBody *BodyCaptureSpec `json:"captureBody,omitempty"`
}

// EgressGatewayConditionReady is set on EgressGateway.Status.Conditions
// to mirror the EG-managed Gateway's Programmed condition. Workers should
// only dial Status.EgressBackendAddress once Ready=True.
const EgressGatewayConditionReady = "Ready"

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
