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

	// OTLP configures L7 capture for this EgressGateway. The
	// controller-manager always persists captured request/response
	// pairs to its embedded ClickHouse and additionally re-exports
	// them to OTLP.Endpoint when set. L7 capture is always on for
	// HTTP and MITM-terminated TLS listeners; this field shapes the
	// re-export destination and the body-capture bounds.
	// +optional
	OTLP *OTLPLogsSinkSpec `json:"otlp,omitempty"`

	// UpstreamTLS adjusts how the EG-managed Envoy validates TLS when
	// re-encrypting traffic to upstreams (the egress dial after MITM).
	// +optional
	UpstreamTLS *EgressUpstreamTLSSpec `json:"upstreamTLS,omitempty"`

	// DNS configures the Egress Gateway's DNS resolver.
	// Defaults to LookupFamily=V4Preferred.
	// +optional
	DNS *EgressDNSSpec `json:"dns,omitempty"`

	// RequestTimeout caps the duration of a single outbound request
	// transiting this gateway. Applied to the catch-all HTTPRoute's
	// Request and BackendRequest timeouts. Defaults to 5m when unset —
	// chosen to cover AI-provider streaming completions (Anthropic /
	// OpenAI SSE responses routinely run 30-60s; reasoning models can
	// stream several minutes). Envoy Gateway's bare default of 15s 504's
	// those mid-stream, so a generous clrk-side default keeps the
	// AI-runtime case working out of the box. Operators wanting a
	// tighter cap (e.g. test environments) can override; per-route
	// overrides are not supported today — every flow through this EG
	// shares the cap.
	// +optional
	RequestTimeout *metav1.Duration `json:"requestTimeout,omitempty"`
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

// EgressDNSLookupFamily selects which IP families the EG-managed Envoy's
// dynamic_forward_proxy DNS cache considers when resolving upstream
// hostnames.
//
// V4Preferred is the clrk default because the most common clrk deployment
// environments (containerized dev clusters running in Docker, plain Linux
// VMs without an IPv6 transit) expose an IPv6 default route on the pod
// network but have no path to globally-routable v6 destinations. With
// Envoy's stock Auto behavior the EG sends every upstream SYN to the
// AAAA answer first and waits the full connect timeout for it to fail
// before retrying on A — blowing the agent's request budget. V4Preferred
// flips the order so the working family is tried first.
//
// +kubebuilder:validation:Enum=V4Preferred;V4Only;V6Only;Auto;All
type EgressDNSLookupFamily string

const (
	// EgressDNSLookupV4Preferred resolves both A and AAAA; the connect
	// tries A first and falls back to AAAA only if no A record exists.
	// The clrk default.
	EgressDNSLookupV4Preferred EgressDNSLookupFamily = "V4Preferred"

	// EgressDNSLookupV4Only resolves only A records. AAAA-only hostnames
	// fail to resolve.
	EgressDNSLookupV4Only EgressDNSLookupFamily = "V4Only"

	// EgressDNSLookupV6Only resolves only AAAA records. A-only hostnames
	// fail to resolve.
	EgressDNSLookupV6Only EgressDNSLookupFamily = "V6Only"

	// EgressDNSLookupAuto returns AAAA when any AAAA record exists,
	// otherwise A. This is Envoy's stock default and prefers v6 even when
	// the v6 path is broken — appropriate only when the operator has
	// verified end-to-end IPv6 transit on this gateway.
	EgressDNSLookupAuto EgressDNSLookupFamily = "Auto"

	// EgressDNSLookupAll resolves both A and AAAA and load-balances
	// across the union. Useful when the operator wants Happy Eyeballs-
	// style v4/v6 racing rather than a strict preference order.
	EgressDNSLookupAll EgressDNSLookupFamily = "All"
)

// EgressDNSSpec configures the EG-managed Envoy's upstream DNS behavior
// for the dynamic_forward_proxy resolver shared by every shape that
// re-resolves upstream by hostname (tls-terminate, https, http,
// tls-passthrough). Does not affect the tcp shape, which routes by
// destination IP via ORIGINAL_DST and never consults DNS.
type EgressDNSSpec struct {
	// LookupFamily selects which IP families the resolver considers when
	// resolving upstream hostnames. Defaults to V4Preferred.
	// +kubebuilder:default=V4Preferred
	// +optional
	LookupFamily EgressDNSLookupFamily `json:"lookupFamily,omitempty"`
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

// OTLPLogsSinkSpec configures where the controller-manager re-exports
// captured signals to and how much of each request/response body is
// captured. The controller-manager always persists captured signals
// to its embedded ClickHouse regardless of this field; Endpoint only
// adds a best-effort fan-out copy to an external collector.
type OTLPLogsSinkSpec struct {
	// Endpoint is the OTLP/HTTP base URL the controller-manager
	// forwards captured signals to (e.g. "https://otel.example.com").
	// Empty disables the customer fan-out; signals are still kept in
	// the embedded ClickHouse.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// Headers are added to every forwarded OTLP export — typically
	// used to carry authentication tokens for the customer endpoint.
	// +optional
	Headers map[string]string `json:"headers,omitempty"`

	// CaptureBody bounds the request/response body bytes captured by
	// ext_proc and emitted as OTLP log records. Defaults: 64KiB per
	// direction; capture application/json, application/x-ndjson,
	// text/event-stream.
	// +optional
	CaptureBody *BodyCaptureSpec `json:"captureBody,omitempty"`
}

// EgressGatewayConditionReady is set on EgressGateway.Status.Conditions
// to mirror the EG-managed Gateway's Programmed condition. Workers should
// only dial Status.Listeners[*].BackendAddress once Ready=True.
const EgressGatewayConditionReady = "Ready"

// EgressListenerStatus describes the status of a single listener.
type EgressListenerStatus struct {
	Name string `json:"name"`

	// Port is the TCP port the EG-managed Envoy listens on for this
	// EgressListener. Workers dial Address:Port for connections that
	// match this listener's protocol/port selectors. Distinct
	// listener types (TLS-terminate, plain TCP, etc.) each get their
	// own port — protocols can't share a listener.
	// +optional
	Port int32 `json:"port,omitempty"`

	// BackendAddress is the host:port workers dial to reach this
	// listener's filter chain. In-cluster it's the EG Service's
	// cluster DNS name; in `clrk dev` it's a NodePort on the k3s
	// container hostname.
	// +optional
	BackendAddress string `json:"backendAddress,omitempty"`

	AttachedRoutes int32              `json:"attachedRoutes"`
	Conditions     []metav1.Condition `json:"conditions,omitempty"`
}

// EgressGatewayStatus describes the observed state of an EgressGateway.
type EgressGatewayStatus struct {
	Conditions    []metav1.Condition     `json:"conditions,omitempty"`
	Listeners     []EgressListenerStatus `json:"listeners,omitempty"`
	ListenerCount int32                  `json:"listenerCount,omitempty"`
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
