package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RestartPolicy controls what happens when the daemon process exits.
// +kubebuilder:validation:Enum=Always;OnFailure;Never
type RestartPolicy string

const (
	RestartPolicyAlways    RestartPolicy = "Always"
	RestartPolicyOnFailure RestartPolicy = "OnFailure"
	RestartPolicyNever     RestartPolicy = "Never"
)

// DaemonPhase describes the observed lifecycle phase of a DaemonAgent.
type DaemonPhase string

const (
	DaemonPhaseRunning          DaemonPhase = "Running"
	DaemonPhaseStopped          DaemonPhase = "Stopped"
	DaemonPhaseCrashLoopBackOff DaemonPhase = "CrashLoopBackOff"
)

// DaemonAgent defines a long-lived autonomous agent process — outbound-only,
// stays alive. NOT a server — does not accept incoming requests. Health is
// determined by process liveness. Single-instance by definition.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=da
// +kubebuilder:printcolumn:name="Image",type=string,JSONPath=`.spec.sandbox.image`
// +kubebuilder:printcolumn:name="Pool",type=string,JSONPath=`.spec.workerPoolRef`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Restarts",type=integer,JSONPath=`.status.restartCount`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type DaemonAgent struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              DaemonAgentSpec   `json:"spec"`
	Status            DaemonAgentStatus `json:"status,omitempty"`
}

type DaemonAgentSpec struct {
	// Sandbox defines the OCI image and process configuration.
	Sandbox AgentSandbox `json:"sandbox"`

	// WorkerPoolRef references a WorkerPool resource by name in the same namespace.
	WorkerPoolRef string `json:"workerPoolRef"`

	// Resources defines per-execution CPU/memory limits.
	// +optional
	Resources ExecutionResources `json:"resources,omitempty"`

	// RestartPolicy controls what happens when the process exits.
	// +kubebuilder:default=Always
	// +optional
	RestartPolicy RestartPolicy `json:"restartPolicy,omitempty"`

	// MaxRestarts caps total restart attempts. 0 = unlimited.
	// +optional
	MaxRestarts *int32 `json:"maxRestarts,omitempty"`

	// MaxLifetimeSeconds caps how long the daemon can run before
	// being terminated and optionally restarted. 0 = forever.
	// +optional
	MaxLifetimeSeconds *int32 `json:"maxLifetimeSeconds,omitempty"`

	// EgressRefs references EgressGateway objects for outbound access.
	// Token budgets are configured on AIProviderRoute, not here.
	// +optional
	EgressRefs []AgentEgressRef `json:"egressRefs,omitempty"`
}

type DaemonAgentStatus struct {
	// Conditions represent the latest available observations of the DaemonAgent's state.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// Phase is the observed lifecycle phase.
	// +optional
	Phase DaemonPhase `json:"phase,omitempty"`
	// UpSince is when the current daemon instance started.
	// +optional
	UpSince *metav1.Time `json:"upSince,omitempty"`
	// RestartCount is the number of times the daemon has been restarted.
	// +optional
	RestartCount int32 `json:"restartCount,omitempty"`
}

// +kubebuilder:object:root=true

// DaemonAgentList contains a list of DaemonAgent resources.
type DaemonAgentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DaemonAgent `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DaemonAgent{}, &DaemonAgentList{})
}
