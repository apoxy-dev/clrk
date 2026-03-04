package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SandboxState tracks per-worker sandbox readiness for a TaskAgent.
// There is a 1:1 relationship between SandboxState and TaskAgent.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=ss
// +kubebuilder:printcolumn:name="Agent",type=string,JSONPath=`.spec.agentRef`
// +kubebuilder:printcolumn:name="Pool",type=string,JSONPath=`.spec.poolRef`
// +kubebuilder:printcolumn:name="Ready Workers",type=integer,JSONPath=`.status.readyWorkers`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type SandboxState struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              SandboxStateSpec   `json:"spec"`
	Status            SandboxStateStatus `json:"status,omitempty"`
}

type SandboxStateSpec struct {
	// AgentRef is the name of the TaskAgent this SandboxState tracks.
	AgentRef string `json:"agentRef"`
	// PoolRef is the name of the WorkerPool.
	PoolRef string `json:"poolRef"`
	// Sandbox is copied from TaskAgent.Spec.Sandbox.
	Sandbox AgentSandbox `json:"sandbox"`
	// Resources is copied from TaskAgent.Spec.Resources.
	// +optional
	Resources ExecutionResources `json:"resources,omitempty"`
}

type SandboxStateStatus struct {
	// Workers tracks the sandbox status for each worker pod.
	// +optional
	Workers []WorkerSandboxStatus `json:"workers,omitempty"`
	// ReadyWorkers is the count of workers with sandbox ready.
	// +optional
	ReadyWorkers int32 `json:"readyWorkers,omitempty"`
	// Conditions represent the latest available observations of the SandboxState.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

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

// +kubebuilder:object:root=true

// SandboxStateList contains a list of SandboxState resources.
type SandboxStateList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SandboxState `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SandboxState{}, &SandboxStateList{})
}
