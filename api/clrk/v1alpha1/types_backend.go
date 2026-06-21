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
)

// BackendSchemaName is the wire schema an upstream speaks: the
// request/response body shape, endpoint layout, and credential scheme.
// The short names deliberately match AIProviderRouteMatch.Provider so the
// ext_proc data plane can key its response parser to a selected backend's
// schema through the same parsers.Canonical mapping it already uses for
// host-derived providers.
//
// Not an enum: Backend.Validate checks the value against the llmcall
// provider registry at admission, so a new provider plugin extends what
// is accepted without an API change. The constants below are the
// built-in spellings.
type BackendSchemaName string

const (
	BackendSchemaOpenAI      BackendSchemaName = "openai"
	BackendSchemaAnthropic   BackendSchemaName = "anthropic"
	BackendSchemaGoogle      BackendSchemaName = "google"
	BackendSchemaAzureOpenAI BackendSchemaName = "azure-openai"
	BackendSchemaBedrock     BackendSchemaName = "bedrock"
	BackendSchemaCustom      BackendSchemaName = "custom"
)

// BackendType selects the destination kind. Exactly one of the matching
// destination fields must be set: Upstream for an external DNS host,
// InferencePoolRef for an in-cluster pool of model-serving pods.
//
// +kubebuilder:validation:Enum=Upstream;InferencePool
type BackendType string

const (
	// BackendTypeUpstream routes to an external host:port resolved via
	// the egress dynamic_forward_proxy. This is the only type with data-
	// plane support today.
	BackendTypeUpstream BackendType = "Upstream"
	// BackendTypeInferencePool routes to a Gateway API Inference
	// Extension InferencePool. Accepted by the API but not yet wired in
	// the data plane; selecting such a backend is refused at runtime
	// until the in-cluster-pool follow-up lands.
	BackendTypeInferencePool BackendType = "InferencePool"
)

// BackendSchema declares the wire schema of the upstream. It is a struct
// (not a bare enum field) so vendor-specific schema details — e.g. a
// Bedrock region or an Azure deployment name — can be added later without
// reshaping the discriminator.
type BackendSchema struct {
	// Name is the wire schema the upstream speaks.
	Name BackendSchemaName `json:"name"`
}

// BackendUpstream is an external host:port destination. The host is
// DNS-resolved per request by the egress forward proxy; no static cluster
// is provisioned per backend.
type BackendUpstream struct {
	// Host is the upstream DNS name (or IP literal) the egress proxy
	// dials. SNI and certificate SAN validation are derived from it.
	// +kubebuilder:validation:MinLength=1
	Host string `json:"host"`

	// Port is the upstream TCP port. Defaults to 443 (the egress data
	// plane is TLS-terminating MITM, so backends are HTTPS).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +kubebuilder:default=443
	Port int32 `json:"port,omitempty"`
}

// ModelRewrite remaps a request's model ID before it reaches the selected
// backend. Rewrites are an ordered list; the first entry whose From glob
// matches the request model wins (mirrors the glob-and-first-hit
// conventions used by AIProviderRoute matching).
type ModelRewrite struct {
	// From is a glob (path.Match shape) over the request's model ID.
	// +kubebuilder:validation:MinLength=1
	From string `json:"from"`

	// To is the literal model ID substituted on the wire.
	// +kubebuilder:validation:MinLength=1
	To string `json:"to"`
}

// BackendBodyMutation configures request-body rewrites applied at
// RequestBody end-of-stream when this backend is selected. Today the only
// mutation is forcing OpenAI-shaped streaming usage; the struct exists so
// further per-backend body rewrites can be added without a schema break.
type BackendBodyMutation struct {
	// EnsureStreamUsage forces stream_options.include_usage=true on
	// OpenAI-shaped streaming requests so streamed responses always emit
	// terminal token usage. No-op for non-OpenAI schemas.
	// +optional
	EnsureStreamUsage *bool `json:"ensureStreamUsage,omitempty"`
}

// BackendSpec defines the desired state of a Backend.
//
// Exactly one destination is set per the Type discriminator: Upstream when
// Type=Upstream, InferencePoolRef when Type=InferencePool. The API server
// does not enforce this exclusivity (clrk does not use CEL validation
// markers); the AIProviderRoute status controller surfaces a violation as
// ResolvedRefs=False and ext_proc refuses an under-specified backend.
//
// Credentials are NOT declared here. A Backend names no secret and holds
// no key: the credential injected when this backend is selected comes from
// a CredentialInjectionPolicy whose targetRef names the route with a
// sectionName equal to this Backend's ref name. This keeps the
// architectural invariant that keys live only in policy + Secret, never in
// a routing object.
type BackendSpec struct {
	// Type selects the destination kind.
	// +kubebuilder:default=Upstream
	Type BackendType `json:"type"`

	// Schema declares the wire schema the upstream speaks.
	Schema BackendSchema `json:"schema"`

	// Upstream is the external host:port destination. Required when
	// Type=Upstream.
	// +optional
	Upstream *BackendUpstream `json:"upstream,omitempty"`

	// InferencePoolRef references a Gateway API Inference Extension
	// InferencePool in the same namespace. Required when
	// Type=InferencePool. Accepted but not yet served by the data plane.
	// +optional
	InferencePoolRef *gwapiv1.LocalObjectReference `json:"inferencePoolRef,omitempty"`

	// ModelRewrites remap the request model ID before it reaches the
	// upstream; the first entry whose From glob matches wins.
	// +optional
	ModelRewrites []ModelRewrite `json:"modelRewrites,omitempty"`

	// BodyMutation configures request-body rewrites applied at
	// RequestBody end-of-stream when this backend is selected.
	// +optional
	BodyMutation *BackendBodyMutation `json:"bodyMutation,omitempty"`
}

// BackendStatus describes the observed state of a Backend.
type BackendStatus struct {
	// Conditions report whether the Backend is well-formed and its
	// destination resolvable (Accepted, ResolvedRefs).
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Backend is an addressable AI-provider upstream that an AIProviderRoute
// rule can route to via BackendRefs. It declares the wire schema, the
// destination, and any per-backend model/body rewrites; credentials attach
// separately via CredentialInjectionPolicy (sectionName-targeted).
//
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=be
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
// +kubebuilder:printcolumn:name="Schema",type=string,JSONPath=`.spec.schema.name`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type Backend struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              BackendSpec   `json:"spec"`
	Status            BackendStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

// BackendList contains a list of Backend resources.
type BackendList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Backend `json:"items"`
}
