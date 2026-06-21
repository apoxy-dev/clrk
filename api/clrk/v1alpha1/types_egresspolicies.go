/*
Copyright 2026 Apoxy, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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

// ProviderAuth type values.
const (
	// ProviderAuthTypeAWSv4 signs requests with AWS Signature Version 4.
	// The referenced Secret carries `access_key_id` and
	// `secret_access_key` (required) plus an optional `session_token`.
	ProviderAuthTypeAWSv4 = "AWSv4"
	// ProviderAuthTypeGCPServiceAccount is reserved; not implemented.
	ProviderAuthTypeGCPServiceAccount = "GCPServiceAccount"
)

// ProviderAuthConfig configures provider-specific authentication.
type ProviderAuthConfig struct {
	// +kubebuilder:validation:Enum=AWSv4;GCPServiceAccount
	Type string `json:"type"`
	// Region scopes the AWSv4 signature. When unset, the region is
	// derived from the target hostname at signing time
	// (bedrock-runtime.<region>.amazonaws.com) — the derived value is
	// the only one guaranteed to agree with the endpoint actually hit
	// when one policy covers Backends in multiple regions.
	Region *string `json:"region,omitempty"`
	// Service is the AWSv4 credential-scope service name. Defaults to
	// "bedrock".
	Service *string `json:"service,omitempty"`
}

// CredentialInjectionSpec defines the desired state of a CredentialInjectionPolicy.
type CredentialInjectionSpec struct {
	// TargetRefs attaches this policy DOWN onto AIProviderRoute, MCPRoute,
	// or EgressGateway targets (GEP-2648 Direct Policy Attachment). The
	// proxy applies the credential to traffic matching the referenced
	// target. Plural so one policy can cover several targets;
	// LocalPolicyTargetReferenceWithSectionName is same-namespace by
	// construction (cross-namespace attachment requires a ReferenceGrant).
	//
	// Match semantics by target kind:
	//   - AIProviderRoute: applies when the request matches that APR's
	//     rules (provider + endpoint + model gates). A sectionName names a
	//     specific BackendRef and gates injection to post-selection of that
	//     backend; an empty sectionName injects route-wide.
	//   - MCPRoute: applies when the request matches that route
	//     (no-op until MCPRoute consumption ships).
	//   - EgressGateway: catch-all for any traffic on the gateway that
	//     no narrower policy claimed.
	TargetRefs []gwapiv1a2.LocalPolicyTargetReferenceWithSectionName `json:"targetRefs"`

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

// CredentialInjectionPolicyStatus reports attachment acceptance for a
// CredentialInjectionPolicy. It embeds the Gateway API GEP-2649
// PolicyStatus so every clrk policy reports through one uniform shape:
// one PolicyAncestorStatus per resolved attachment, carrying Accepted /
// Conflicted conditions.
type CredentialInjectionPolicyStatus struct {
	gwapiv1a2.PolicyStatus `json:",inline"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=cip
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.spec.target`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type CredentialInjectionPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              CredentialInjectionSpec         `json:"spec"`
	Status            CredentialInjectionPolicyStatus `json:"status,omitempty"`
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
// FallbackRoutingPolicy
// ============================================================================

// FallbackRetry tunes the per-attempt retry behavior of a
// FallbackRoutingPolicy. All fields are optional; nil fields take the
// defaults documented per field.
type FallbackRetry struct {
	// NumRetries caps the number of retry attempts after the first.
	// Defaults to one fewer than the rule's viable backend count,
	// capped at 5.
	// +optional
	NumRetries *int32 `json:"numRetries,omitempty"`

	// RetriableStatusCodes lists upstream HTTP status codes that
	// trigger a retry, in addition to connection failures and stream
	// resets (always retried). Defaults to [429, 503].
	// +optional
	RetriableStatusCodes []int32 `json:"retriableStatusCodes,omitempty"`

	// PerTryTimeout bounds each individual attempt. Unset means no
	// per-attempt bound — streaming LLM responses routinely outlive any
	// reasonable fixed timeout, and a retry can only fire before
	// response headers arrive, so leaving this unset never strands a
	// stream.
	// +optional
	PerTryTimeout *metav1.Duration `json:"perTryTimeout,omitempty"`
}

// FallbackEjection tunes passive outlier ejection on the synthesized
// per-rule cluster: a backend that fails repeatedly (consecutive 5xx
// or connection failures) is ejected from load balancing for a
// cooldown, so a dead primary stops eating the first attempt of every
// request.
type FallbackEjection struct {
	// MaxEjectionTime caps how long a failing backend stays ejected.
	// Ejection durations grow linearly with the backend's ejection
	// count (30s, 60s, 90s, ...) and the count never decays, so without
	// a cap a backend that flapped for a while keeps serving 5-minute
	// (Envoy's default cap) blackout windows even after it recovers —
	// there is no active probing, the expiry of this timer is what puts
	// the backend back in rotation. Values below the 30s base ejection
	// time also lower the base, so the very first ejection honors the
	// cap too. Defaults to Envoy's 300s.
	// +optional
	MaxEjectionTime *metav1.Duration `json:"maxEjectionTime,omitempty"`
}

// FallbackRoutingPolicySpec defines the desired state of a
// FallbackRoutingPolicy.
type FallbackRoutingPolicySpec struct {
	// TargetRefs attaches this policy DOWN onto AIProviderRoutes (GEP-2648
	// Direct Policy Attachment, same-namespace by construction).
	// Attachment changes how the referenced routes' rules distribute
	// traffic across their BackendRefs: without a policy, BackendRef.Weight
	// splits traffic and each request gets a single attempt; with one,
	// BackendRefs list ORDER becomes the fallback priority — the first
	// viable backend serves all traffic while healthy, and a failed
	// attempt retries against the next, walking the list in order
	// (weights are ignored). Whole-route attachment only: AIProviderRoute
	// rules are unnamed, so a sectionName has no section to bind to and is
	// rejected at admission until per-rule scoping is designed.
	TargetRefs []gwapiv1a2.LocalPolicyTargetReferenceWithSectionName `json:"targetRefs"`

	// Retry tunes the retry behavior. Nil applies the per-field
	// defaults documented on FallbackRetry.
	// +optional
	Retry *FallbackRetry `json:"retry,omitempty"`

	// Ejection tunes passive outlier ejection of failing backends. Nil
	// applies the per-field defaults documented on FallbackEjection.
	// +optional
	Ejection *FallbackEjection `json:"ejection,omitempty"`
}

// FallbackRoutingPolicyStatus reports attachment acceptance for a
// FallbackRoutingPolicy via the GEP-2649 PolicyStatus shape shared by
// every clrk policy.
type FallbackRoutingPolicyStatus struct {
	gwapiv1a2.PolicyStatus `json:",inline"`
}

// FallbackRoutingPolicy opts AIProviderRoutes into ordered inter-backend
// fallback: a request whose attempt fails (connect failure, reset, or a
// retriable status) is retried against the next backend in the rule's
// BackendRefs list, with per-attempt schema translation and credential
// injection. It is the first of the routing-policy family; siblings with
// other selection strategies (cost- or latency-based) can follow the
// same attachment pattern.
//
// Fallback only applies before the upstream response starts: once
// response headers are forwarded to the agent there are no further
// attempts (a mid-stream provider failure surfaces through the
// translation error-frame convention instead).
//
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=frp
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type FallbackRoutingPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              FallbackRoutingPolicySpec   `json:"spec"`
	Status            FallbackRoutingPolicyStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

// FallbackRoutingPolicyList contains a list of FallbackRoutingPolicy resources.
type FallbackRoutingPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []FallbackRoutingPolicy `json:"items"`
}

// ============================================================================
// RateLimitPolicy
// ============================================================================

// RateLimitScope defines the scope of rate limiting. The kubebuilder Enum
// marker is intentionally omitted: this group is served by the aggregated
// apiserver, which does not honor CRD validation markers (see defaults.go),
// so scope validity is enforced in the Validate hooks via
// validateRateLimitScope instead.
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
	// TargetRefs identifies the routes this policy attaches to. Plural
	// (GEP-2648 Direct Policy Attachment) so one policy can deny several
	// routes; LocalPolicyTargetReferenceWithSectionName is same-namespace
	// by construction and section-scopable (an empty sectionName denies
	// the whole route).
	TargetRefs []gwapiv1a2.LocalPolicyTargetReferenceWithSectionName `json:"targetRefs"`

	// DenyResponse configures the rejection returned to the caller.
	// +optional
	DenyResponse *DenyResponseConfig `json:"denyResponse,omitempty"`
}

// EgressDenyPolicyStatus reports attachment acceptance for an
// EgressDenyPolicy via the GEP-2649 PolicyStatus shape shared by every
// clrk policy.
type EgressDenyPolicyStatus struct {
	gwapiv1a2.PolicyStatus `json:",inline"`
}

// EgressDenyPolicy attaches to routes via targetRefs to invert them from
// "allow" to "deny".
//
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=edp
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type EgressDenyPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              EgressDenyPolicySpec   `json:"spec"`
	Status            EgressDenyPolicyStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

// EgressDenyPolicyList contains a list of EgressDenyPolicy resources.
type EgressDenyPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EgressDenyPolicy `json:"items"`
}
