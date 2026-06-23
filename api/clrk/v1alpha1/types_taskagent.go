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
	"k8s.io/apimachinery/pkg/runtime"
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
// +kubebuilder:printcolumn:name="Warm",type=integer,JSONPath=`.status.warmSandboxes`
// +kubebuilder:printcolumn:name="Schedule",type=string,JSONPath=`.spec.schedule`
// +kubebuilder:printcolumn:name="Last Run",type=date,JSONPath=`.status.lastScheduleTime`
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

	// Timeout caps how long a single execution can run. Accepts a Go
	// duration string ("100s", "5m", "1h30m"). Defaults to 100s.
	// +kubebuilder:default="100s"
	// +optional
	Timeout *metav1.Duration `json:"timeout,omitempty"`

	// MaxConcurrent caps how many executions of this agent can run
	// simultaneously across all workers. 0 = unlimited.
	// +optional
	MaxConcurrent *int32 `json:"maxConcurrent,omitempty"`

	// WarmPoolSize is the per-worker target for pre-Created sandboxes
	// kept Ready (rootfs mounted, TAP+netns provisioned, libcontainer
	// container created but agent process not yet started). A warm
	// sandbox shaves cold-start off the request hot path. Defaults to
	// 0 (no warm pool). Capped by the worker at MaxExecutionsPerWorker/2.
	// +optional
	// +kubebuilder:default=0
	WarmPoolSize *int32 `json:"warmPoolSize,omitempty"`

	// WarmPoolIdleTTL bounds how long a pre-warmed sandbox can sit
	// in the pool before it is evicted and replaced. Defaults to
	// 10 minutes when unset. Bounds resource-leak accumulation in
	// long-idle agent processes (file descriptors, anon memory,
	// language-runtime caches) at the cost of a cold-fill every
	// IdleTTL when traffic is sparse.
	// +optional
	WarmPoolIdleTTL *metav1.Duration `json:"warmPoolIdleTTL,omitempty"`

	// Schedule adds a cron trigger. Does NOT disable HTTP triggering.
	// +optional
	Schedule *string `json:"schedule,omitempty"`

	// ScheduleInput is the JSON body sent to the agent on cron triggers.
	// +optional
	// +kubebuilder:pruning:PreserveUnknownFields
	ScheduleInput *runtime.RawExtension `json:"scheduleInput,omitempty"`

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

	// Delivery selects how the request payload reaches the agent.
	// Stdin (default) writes a CloudEvents structured-mode JSON
	// envelope to the agent's stdin. Metadata closes stdin and
	// exposes an IMDS-style HTTP server inside the sandbox; the
	// agent fetches via $CLRK_METADATA_URL.
	// +optional
	Delivery *AgentDelivery `json:"delivery,omitempty"`
}

// AgentDelivery selects the wire format used to hand a request off
// to the agent process.
type AgentDelivery struct {
	// Mode is Stdin (default) or Metadata.
	// +kubebuilder:validation:Enum=Stdin;Metadata
	// +kubebuilder:default=Stdin
	// +optional
	Mode AgentDeliveryMode `json:"mode,omitempty"`
}

// AgentDeliveryMode enumerates request delivery transports.
type AgentDeliveryMode string

const (
	// AgentDeliveryStdin writes a structured-mode CloudEvents JSON
	// envelope to the agent's stdin.
	AgentDeliveryStdin AgentDeliveryMode = "Stdin"

	// AgentDeliveryMetadata exposes an IMDS-style HTTP server in
	// the sandbox; the agent fetches the request via
	// $CLRK_METADATA_URL/event and posts the response body to
	// $CLRK_METADATA_URL/response.
	AgentDeliveryMetadata AgentDeliveryMode = "Metadata"
)

type TaskAgentStatus struct {
	// Conditions represent the latest available observations of the TaskAgent's state.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// ActiveExecutions is the count of this agent's in-flight Invocations
	// — those in a non-terminal phase (Pending, Dispatched, or Running).
	// It is sourced from the invocation read model (APO-620), falling back
	// to the per-worker WorkerStatus sum when that model is unavailable.
	// "Execution" here means an in-flight invocation, not a separate
	// resource; see Invocation.
	// +optional
	ActiveExecutions int32 `json:"activeExecutions,omitempty"`
	// WarmSandboxes is the current count of pre-warmed sandboxes held Ready
	// for this agent across its workers — Created sandboxes (rootfs mounted,
	// TAP+netns provisioned, libcontainer container created) whose agent
	// process has not started, ready to absorb a request without cold-start.
	// It is the per-worker WarmCount summed over the latest-ready revision's
	// WorkerStatus stream, the actual realization of spec.warmPoolSize. The
	// metrics snapshot's `warm` gauge reads this verbatim.
	// +optional
	WarmSandboxes int32 `json:"warmSandboxes,omitempty"`
	// LatestCreatedRevisionName is the name of the last created AgentSandboxRevision.
	// +optional
	LatestCreatedRevisionName string `json:"latestCreatedRevisionName,omitempty"`
	// LatestReadyRevisionName is the name of the last revision that became ready.
	// +optional
	LatestReadyRevisionName string `json:"latestReadyRevisionName,omitempty"`
	// LastScheduleTime is the last time the cron schedule fired. Unset
	// when spec.schedule is empty or no fire has occurred yet.
	// +optional
	LastScheduleTime *metav1.Time `json:"lastScheduleTime,omitempty"`
	// NextScheduleTime is the next time the cron schedule will fire on
	// the current leader. Computed lazily from spec.schedule; expect skew
	// of up to one slot across leader failover.
	// +optional
	NextScheduleTime *metav1.Time `json:"nextScheduleTime,omitempty"`
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
