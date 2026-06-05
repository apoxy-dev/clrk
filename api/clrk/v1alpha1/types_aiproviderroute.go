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

// AIProviderRouteFilterType identifies the type of filter in an AIProviderRouteFilter.
// +kubebuilder:validation:Enum=TokenBudget;ExtensionRef
type AIProviderRouteFilterType string

const (
	AIProviderFilterTokenBudget  AIProviderRouteFilterType = "TokenBudget"
	AIProviderFilterExtensionRef AIProviderRouteFilterType = "ExtensionRef"
)

// TokenBudgetFilter enforces token consumption limits on AI provider traffic.
type TokenBudgetFilter struct {
	// MaxTokensPerExecution caps total tokens (input+output) per agent run.
	// +optional
	MaxTokensPerExecution *int64 `json:"maxTokensPerExecution,omitempty"`

	// MaxTokensPerDay caps daily token usage across all runs for this route.
	// +optional
	MaxTokensPerDay *int64 `json:"maxTokensPerDay,omitempty"`

	// MaxOutputTokensPerRequest caps output tokens on a single API call.
	// +optional
	MaxOutputTokensPerRequest *int64 `json:"maxOutputTokensPerRequest,omitempty"`

	// BudgetRef references a shared TokenBudget resource for cross-agent
	// or cross-route budgets.
	// +optional
	BudgetRef *string `json:"budgetRef,omitempty"`
}

// AIProviderRouteFilter defines a filter for AIProviderRoute rules.
type AIProviderRouteFilter struct {
	// Type selects the filter.
	Type AIProviderRouteFilterType `json:"type"`

	// TokenBudget — inline, AI-provider-specific.
	// +optional
	TokenBudget *TokenBudgetFilter `json:"tokenBudget,omitempty"`

	// ExtensionRef references a cross-cutting policy. It is also the seam
	// for classifier-driven backend selection: an ExtensionRef resolving
	// to a classifier kind picks among the rule's BackendRefs at
	// RequestBody end-of-stream (the classifier protocol is APO-480). When
	// absent, backend selection is the standard weighted pick over
	// BackendRefs (BackendRef.Weight).
	// +optional
	ExtensionRef *gwapiv1.LocalObjectReference `json:"extensionRef,omitempty"`
}

// AIProviderRouteMatch defines match criteria for AI provider traffic.
type AIProviderRouteMatch struct {
	// Provider selects the AI API provider.
	// +kubebuilder:validation:Enum=openai;anthropic;google;azure-openai;bedrock;custom
	Provider string `json:"provider"`

	// Models restricts to specific model IDs. Supports glob: "claude-*".
	// +optional
	Models []string `json:"models,omitempty"`

	// Endpoints restricts to API paths.
	// +optional
	Endpoints []string `json:"endpoints,omitempty"`
}

// AIProviderRouteRule defines a rule within an AIProviderRoute.
type AIProviderRouteRule struct {
	Matches []AIProviderRouteMatch `json:"matches,omitempty"`

	// Filters apply policy to matched AI provider traffic.
	// +optional
	Filters []AIProviderRouteFilter `json:"filters,omitempty"`

	// BackendRefs is the candidate set of clrk Backends
	// (clrk.apoxy.dev/Backend) this rule may route to, selected at
	// RequestBody end-of-stream. With a single ref the request is
	// re-pointed to that backend. With two or more, the request is
	// distributed by the standard Gateway API BackendRef.Weight
	// (deterministically per request so retries are stable), unless an
	// ExtensionRef classifier filter on this rule picks one instead
	// (APO-480). Refs that are not clrk Backends are reported as
	// unresolved by the status controller and ignored at selection time.
	// +optional
	BackendRefs []gwapiv1.BackendRef `json:"backendRefs,omitempty"`
}

// AIProviderRouteSpec defines the desired state of an AIProviderRoute.
type AIProviderRouteSpec struct {
	ParentRefs []gwapiv1.ParentReference `json:"parentRefs"`
	Rules      []AIProviderRouteRule     `json:"rules"`
}

// AIProviderRouteStatus describes the observed state of an AIProviderRoute.
type AIProviderRouteStatus struct {
	Parents []gwapiv1.RouteParentStatus `json:"parents,omitempty"`
}

// AIProviderRoute matches outbound requests to AI provider APIs and
// applies provider-aware policy.
//
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=aipr
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type AIProviderRoute struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              AIProviderRouteSpec   `json:"spec"`
	Status            AIProviderRouteStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

// AIProviderRouteList contains a list of AIProviderRoute resources.
type AIProviderRouteList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AIProviderRoute `json:"items"`
}
