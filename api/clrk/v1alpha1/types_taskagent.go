package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
)

// TaskAgent defines a triggered agent workload — HTTP or cron, runs to
// completion, exits. Executions are multiplexed across shared worker pods
// managed by WorkerPool.
//
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=ta
// +kubebuilder:printcolumn:name="Image",type=string,JSONPath=`.spec.template.spec.image`
// +kubebuilder:printcolumn:name="Pool",type=string,JSONPath=`.spec.workerPoolRef`
// +kubebuilder:printcolumn:name="Latest Ready",type=string,JSONPath=`.status.latestReadyRevisionName`
// +kubebuilder:printcolumn:name="Active",type=integer,JSONPath=`.status.activeExecutions`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type TaskAgent struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              TaskAgentSpec   `json:"spec"`
	Status            TaskAgentStatus `json:"status,omitempty"`
}

type TaskAgentSpec struct {
	// Template defines the sandbox revision template. Changes to this
	// field trigger creation of a new AgentSandboxRevision.
	Template AgentSandboxRevisionTemplate `json:"template"`

	// WorkerPoolRef references a WorkerPool resource by name in the same namespace.
	WorkerPoolRef string `json:"workerPoolRef"`

	// Resources defines per-execution CPU/memory limits.
	// +optional
	Resources ExecutionResources `json:"resources,omitempty"`

	// TimeoutSeconds caps how long a single execution can run.
	// +kubebuilder:default=300
	// +optional
	TimeoutSeconds *int32 `json:"timeoutSeconds,omitempty"`

	// MaxConcurrent caps how many executions of this agent can run
	// simultaneously across all workers. 0 = unlimited.
	// +optional
	MaxConcurrent *int32 `json:"maxConcurrent,omitempty"`

	// Schedule adds a cron trigger. Does NOT disable HTTP triggering.
	// +optional
	Schedule *string `json:"schedule,omitempty"`

	// ScheduleInput is the JSON body sent to the agent on cron triggers.
	// +optional
	ScheduleInput *apiextensionsv1.JSON `json:"scheduleInput,omitempty"`

	// EgressRefs references EgressGateway objects for outbound access.
	// Token budgets are configured on AIProviderRoute, not here.
	// +optional
	EgressRefs []AgentEgressRef `json:"egressRefs,omitempty"`

	// Identity configures user identity extraction from incoming requests.
	// +optional
	Identity *AgentIdentity `json:"identity,omitempty"`

	// Streaming configures stdout/stderr streaming behavior.
	// +optional
	Streaming *AgentStreaming `json:"streaming,omitempty"`

	// State configures persistent state across executions.
	// +optional
	State *AgentState `json:"state,omitempty"`
}

type TaskAgentStatus struct {
	// Conditions represent the latest available observations of the TaskAgent's state.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// ActiveExecutions is the number of currently running executions.
	// +optional
	ActiveExecutions int32 `json:"activeExecutions,omitempty"`
	// LatestCreatedRevisionName is the name of the last created AgentSandboxRevision.
	// +optional
	LatestCreatedRevisionName string `json:"latestCreatedRevisionName,omitempty"`
	// LatestReadyRevisionName is the name of the last revision that became ready.
	// +optional
	LatestReadyRevisionName string `json:"latestReadyRevisionName,omitempty"`
	// ObservedGeneration is the generation most recently observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

// TaskAgentList contains a list of TaskAgent resources.
type TaskAgentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TaskAgent `json:"items"`
}
