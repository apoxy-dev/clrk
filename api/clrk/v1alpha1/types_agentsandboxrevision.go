package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AgentSandboxRevisionTemplate is the template embedded in agent specs.
// When the template content changes, the agent controller creates a new
// AgentSandboxRevision snapshot.
type AgentSandboxRevisionTemplate struct {
	// Metadata allows setting labels and annotations on the created revision.
	// Name and GenerateName are ignored; the controller generates the name.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec is the desired state of the revision.
	Spec AgentSandboxRevisionSpec `json:"spec"`
}

// AgentSandboxRevisionSpec captures the immutable deployable artifact —
// the OCI image and process configuration.
type AgentSandboxRevisionSpec struct {
	AgentSandbox `json:",inline"`
}

// AgentSandboxRevisionStatus describes the observed state of a revision.
type AgentSandboxRevisionStatus struct {
	// Conditions represent the latest available observations.
	// Known condition types: "Ready", "Active".
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Workers tracks the sandbox status for each worker pod.
	// +optional
	Workers []WorkerSandboxStatus `json:"workers,omitempty"`

	// ReadyWorkers is the count of workers that have this revision's
	// sandbox ready (image pulled, warm instances available).
	// +optional
	ReadyWorkers int32 `json:"readyWorkers,omitempty"`
}

// WorkerSandboxStatus tracks sandbox readiness on a single worker pod.
type WorkerSandboxStatus struct {
	// PodName is the name of the worker pod.
	PodName string `json:"podName"`
	// ImagePulled indicates whether the sandbox image has been pulled.
	ImagePulled bool `json:"imagePulled"`
	// WarmCount is the number of warm sandbox instances on this worker.
	// +optional
	WarmCount int32 `json:"warmCount,omitempty"`
	// LastHeartbeat is the last time this worker reported status.
	// +optional
	LastHeartbeat metav1.Time `json:"lastHeartbeat,omitempty"`
}

// AgentSandboxRevision is an immutable snapshot of an agent's sandbox
// configuration. Created automatically when a TaskAgent or DaemonAgent
// template changes. Named "{agent-name}-{5-digit-generation}".
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=asr
// +kubebuilder:printcolumn:name="Image",type=string,JSONPath=`.spec.image`
// +kubebuilder:printcolumn:name="Ready Workers",type=integer,JSONPath=`.status.readyWorkers`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type AgentSandboxRevision struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              AgentSandboxRevisionSpec   `json:"spec"`
	Status            AgentSandboxRevisionStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// AgentSandboxRevisionList contains a list of AgentSandboxRevision resources.
type AgentSandboxRevisionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AgentSandboxRevision `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AgentSandboxRevision{}, &AgentSandboxRevisionList{})
}
