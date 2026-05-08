package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"
	gwapiv1a2 "sigs.k8s.io/gateway-api/apis/v1alpha2"
)

// ============================================================================
// CredentialInjectionPolicy
// ============================================================================

// CredentialTarget defines where to inject the credential.
// +kubebuilder:validation:Enum=Header;QueryParam;ProviderAuth
type CredentialTarget string

const (
	CredentialTargetHeader       CredentialTarget = "Header"
	CredentialTargetQueryParam   CredentialTarget = "QueryParam"
	CredentialTargetProviderAuth CredentialTarget = "ProviderAuth"
)

// ProviderAuthConfig configures provider-specific authentication.
type ProviderAuthConfig struct {
	// +kubebuilder:validation:Enum=AWSv4;GCPServiceAccount
	Type    string  `json:"type"`
	Region  *string `json:"region,omitempty"`
	Service *string `json:"service,omitempty"`
}

// CredentialInjectionSpec defines the desired state of a CredentialInjectionPolicy.
type CredentialInjectionSpec struct {
	// ParentRefs attaches this policy to AIProviderRoute, MCPRoute, or
	// EgressGateway listeners. The proxy applies the credential to
	// traffic matching the referenced parent.
	//
	// Match semantics by parent kind:
	//   - AIProviderRoute: applies when the request matches that APR's
	//     rules (provider + endpoint + model gates).
	//   - MCPRoute: applies when the request matches that route
	//     (no-op until MCPRoute consumption ships).
	//   - EgressGateway: catch-all for any traffic on the gateway that
	//     no narrower policy claimed.
	ParentRefs []gwapiv1.ParentReference `json:"parentRefs"`

	// SecretRef points at a K8s Secret containing the credential. The
	// `namespace` field is ignored: the Secret must live in the same
	// namespace as the CredentialInjectionPolicy. Cross-namespace refs
	// will gate on ReferenceGrant post-MVP.
	SecretRef gwapiv1.SecretObjectReference `json:"secretRef"`

	// SecretKey selects a key within the Secret. Defaults to "token".
	// +kubebuilder:default=token
	SecretKey string `json:"secretKey,omitempty"`

	// Target defines where to inject the credential.
	Target CredentialTarget `json:"target"`

	// HeaderName — required when target=Header.
	// +optional
	HeaderName *string `json:"headerName,omitempty"`

	// QueryParamName — required when target=QueryParam.
	// +optional
	QueryParamName *string `json:"queryParamName,omitempty"`

	// ProviderAuth — required when target=ProviderAuth.
	// +optional
	ProviderAuth *ProviderAuthConfig `json:"providerAuth,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=cip
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.spec.target`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type CredentialInjectionPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              CredentialInjectionSpec `json:"spec"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

// CredentialInjectionPolicyList contains a list of CredentialInjectionPolicy resources.
type CredentialInjectionPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CredentialInjectionPolicy `json:"items"`
}

// ============================================================================
// RateLimitPolicy
// ============================================================================

// RateLimitScope defines the scope of rate limiting.
// +kubebuilder:validation:Enum=PerAgent;PerExecution;PerRoute
type RateLimitScope string

const (
	RateLimitScopePerAgent     RateLimitScope = "PerAgent"
	RateLimitScopePerExecution RateLimitScope = "PerExecution"
	RateLimitScopePerRoute     RateLimitScope = "PerRoute"
)

// RateLimitSpec defines the desired state of a RateLimitPolicy.
type RateLimitSpec struct {
	Requests int32  `json:"requests"`
	Window   string `json:"window"`

	// +kubebuilder:default=PerAgent
	Scope RateLimitScope `json:"scope,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=rlp
// +kubebuilder:printcolumn:name="Requests",type=integer,JSONPath=`.spec.requests`
// +kubebuilder:printcolumn:name="Window",type=string,JSONPath=`.spec.window`
// +kubebuilder:printcolumn:name="Scope",type=string,JSONPath=`.spec.scope`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type RateLimitPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              RateLimitSpec `json:"spec"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

// RateLimitPolicyList contains a list of RateLimitPolicy resources.
type RateLimitPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RateLimitPolicy `json:"items"`
}

// ============================================================================
// LoggingPolicy
// ============================================================================

// LoggingSpec defines the desired state of a LoggingPolicy.
type LoggingSpec struct {
	CaptureRequest  bool     `json:"captureRequest,omitempty"`
	CaptureResponse bool     `json:"captureResponse,omitempty"`
	RedactHeaders   []string `json:"redactHeaders,omitempty"`
	SinkRef         *string  `json:"sinkRef,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=lp
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type LoggingPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              LoggingSpec `json:"spec"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

// LoggingPolicyList contains a list of LoggingPolicy resources.
type LoggingPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []LoggingPolicy `json:"items"`
}

// ============================================================================
// EgressDenyPolicy — GEP-713 Direct Policy Attachment
// ============================================================================

// DenyResponseConfig configures the rejection returned to the caller.
type DenyResponseConfig struct {
	// +kubebuilder:default=403
	StatusCode int32   `json:"statusCode,omitempty"`
	Message    *string `json:"message,omitempty"`
}

// EgressDenyPolicySpec defines the desired state of an EgressDenyPolicy.
type EgressDenyPolicySpec struct {
	// TargetRef identifies the route this policy attaches to.
	TargetRef gwapiv1a2.LocalPolicyTargetReference `json:"targetRef"`

	// DenyResponse configures the rejection returned to the caller.
	// +optional
	DenyResponse *DenyResponseConfig `json:"denyResponse,omitempty"`
}

// EgressDenyPolicy attaches to any route via targetRef to invert it from
// "allow" to "deny".
//
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=edp
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type EgressDenyPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              EgressDenyPolicySpec `json:"spec"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

// EgressDenyPolicyList contains a list of EgressDenyPolicy resources.
type EgressDenyPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EgressDenyPolicy `json:"items"`
}
