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
)

// CLRKConfigSingletonName is the only permitted name for the CLRKConfig
// singleton. Validation pins metadata.name to it so at most one object can
// exist per namespace.
const CLRKConfigSingletonName = "default"

// CLRKConfig is the install-wide singleton configuration object. Exactly one
// object, named "default", lives in the clrk system namespace. It is the home
// for cross-cutting settings; the Notifications feature is one section
// (spec.notifications), and future settings live beside it. The spec is
// browser-writable (the console's signup gate sets spec.notifications.email);
// the status is controller-written (phone-home registration health). The
// registration token itself is never here -- it lives in a core/v1 Secret the
// browser cannot reach, referenced from status.notifications.
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=cc
// +kubebuilder:printcolumn:name="Email",type=string,JSONPath=`.spec.notifications.email`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type CLRKConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              CLRKConfigSpec   `json:"spec"`
	Status            CLRKConfigStatus `json:"status,omitempty"`
}

// CLRKConfigSpec holds the install-wide configuration sections.
type CLRKConfigSpec struct {
	// Notifications configures the Notification Center + phone-home feature.
	// +optional
	Notifications NotificationsConfig `json:"notifications,omitempty"`
}

// NotificationsConfig is the browser-writable notifications settings.
type NotificationsConfig struct {
	// Email captured by the console signup gate. Empty => the feature is inert
	// (no notifications recorded to phone home, no registration).
	// +optional
	Email string `json:"email,omitempty"`

	// SignedUpAt is stamped by the defaulter when Email is first set.
	// +optional
	SignedUpAt *metav1.Time `json:"signedUpAt,omitempty"`

	// AdvisoryPollEnabled opts into INBOUND security-advisory polling from
	// api.apoxy.dev. Defaults true on signup.
	// +optional
	AdvisoryPollEnabled *bool `json:"advisoryPollEnabled,omitempty"`

	// EventRetention is the retention window for events.k8s.io/v1 notification
	// Events in the embedded apiserver; older Events are pruned. Unset falls
	// back to the controller-manager's --notifications-event-retention flag.
	// +optional
	EventRetention *metav1.Duration `json:"eventRetention,omitempty"`
}

// CLRKConfigStatus is the controller-written status.
type CLRKConfigStatus struct {
	// Notifications carries the phone-home registration + health.
	// +optional
	Notifications NotificationsStatus `json:"notifications,omitempty"`
}

// NotificationsStatus reflects the phone-home lifecycle. The registration token
// is NOT here (it lives in a core/v1 Secret); only the browser-safe reference
// and non-secret correlation fields are.
type NotificationsStatus struct {
	// Conditions: Registered, PhoneHomeHealthy, AdvisorySyncHealthy.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// DeploymentID is the non-secret correlation id api.apoxy.dev returns at
	// registration. Safe to surface in the console.
	// +optional
	DeploymentID string `json:"deploymentID,omitempty"`

	// RegisteredEmail is the spec.notifications.email that produced the current
	// registration. The controller compares it to the live spec email to detect a
	// changed signup and re-register (metadata.generation is unreliable in this
	// apiserver). Not secret -- it is already the browser-writable spec email.
	// +optional
	RegisteredEmail string `json:"registeredEmail,omitempty"`

	// AdvisoryPollIntervalSeconds is the server-driven advisory poll cadence
	// returned at registration. Persisted so it survives restarts (the poller
	// reverts to the flag default if unset).
	// +optional
	AdvisoryPollIntervalSeconds int64 `json:"advisoryPollIntervalSeconds,omitempty"`

	// RegistrationTokenSecretRef points at the Secret holding the phone-home
	// bearer token. The console sees the reference but cannot fetch the Secret
	// (core/v1 is not proxied by the embedded apiserver).
	// +optional
	RegistrationTokenSecretRef *SecretKeyReference `json:"registrationTokenSecretRef,omitempty"`

	// RegisteredAt / LastAdvisorySync / LastReportAt for the health panel.
	// +optional
	RegisteredAt *metav1.Time `json:"registeredAt,omitempty"`
	// +optional
	LastAdvisorySync *metav1.Time `json:"lastAdvisorySync,omitempty"`
	// +optional
	LastReportAt *metav1.Time `json:"lastReportAt,omitempty"`

	// ReportsDropped counts security reports dropped under queue backpressure.
	// +optional
	ReportsDropped int64 `json:"reportsDropped,omitempty"`

	// ObservedGeneration is the spec generation this status reflects.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// SecretKeyReference names a key within a Secret in a namespace.
type SecretKeyReference struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

// CLRKConfigList contains a list of CLRKConfig resources.
type CLRKConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CLRKConfig `json:"items"`
}
