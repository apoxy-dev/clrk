package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// InvocationPhase enumerates the observable lifecycle states of an
// Invocation. Transitions are monotonic: Pending -> Dispatched ->
// Running -> (Succeeded | Failed | Timeout); Rejected is a terminal
// state from Pending when ingress admission denies the request before
// any worker assignment. Failed and Timeout are post-dispatch only.
// +kubebuilder:validation:Enum=Pending;Dispatched;Running;Succeeded;Failed;Timeout;Rejected
type InvocationPhase string

const (
	InvocationPhasePending    InvocationPhase = "Pending"
	InvocationPhaseDispatched InvocationPhase = "Dispatched"
	InvocationPhaseRunning    InvocationPhase = "Running"
	InvocationPhaseSucceeded  InvocationPhase = "Succeeded"
	InvocationPhaseFailed     InvocationPhase = "Failed"
	InvocationPhaseTimeout    InvocationPhase = "Timeout"
	InvocationPhaseRejected   InvocationPhase = "Rejected"
)

// InvocationTriggerType selects how an invocation was initiated.
// +kubebuilder:validation:Enum=HTTP;Cron;CLI
type InvocationTriggerType string

const (
	InvocationTriggerHTTP InvocationTriggerType = "HTTP"
	InvocationTriggerCron InvocationTriggerType = "Cron"
	InvocationTriggerCLI  InvocationTriggerType = "CLI"
)

// InvocationTrigger describes the source that initiated this
// invocation. Source is a free-form descriptor: for HTTP the gateway
// route name, for Cron the parent's spec.schedule expression, for CLI
// the operator principal. Empty when unavailable.
type InvocationTrigger struct {
	Type InvocationTriggerType `json:"type"`
	// +optional
	Source string `json:"source,omitempty"`
}

// InvocationIdentity is the resolved identity asserted at ingress.
// Audiences are the validated outputs of the parent's spec.identity
// extractors. Stays empty until AgentIdentity grows an Audiences
// field in a follow-up ticket.
type InvocationIdentity struct {
	// +optional
	Audiences []string `json:"audiences,omitempty"`
}

// InvocationParentKind enumerates the kinds that can own an Invocation.
// +kubebuilder:validation:Enum=TaskAgent;DaemonAgent
type InvocationParentKind string

const (
	InvocationParentTaskAgent   InvocationParentKind = "TaskAgent"
	InvocationParentDaemonAgent InvocationParentKind = "DaemonAgent"
)

// InvocationParentRef names the TaskAgent or DaemonAgent this
// Invocation belongs to. The parent must live in the same namespace
// as the Invocation. This is the canonical API surface for parent
// identity; the Create strategy synthesises a matching
// metadata.ownerReferences entry (controller=true,
// blockOwnerDeletion=false) so Kubernetes garbage collection cascades
// parent deletion.
type InvocationParentRef struct {
	Kind InvocationParentKind `json:"kind"`
	Name string               `json:"name"`
}

// InvocationSpec carries the ingress-known descriptors. Set once on
// Create and immutable thereafter. The canonical invocation ID lives
// on metadata.name (also mirrored into metadata.uid by the Create
// strategy); ce-id, metadata.name, metadata.uid, and the JetStream
// stream key are all the same UUID end-to-end.
type InvocationSpec struct {
	// ParentRef identifies the TaskAgent or DaemonAgent that owns
	// this Invocation. Required.
	ParentRef InvocationParentRef `json:"parentRef"`

	// Trigger describes how this invocation was initiated.
	Trigger InvocationTrigger `json:"trigger"`

	// Identity is the resolved identity asserted at ingress.
	// +optional
	Identity *InvocationIdentity `json:"identity,omitempty"`
}

// InvocationStatus intentionally starts minimal: phase plus Conditions.
// Lifecycle timestamps, exit code, response size, worker assignment,
// revision selection, and failure detail are all expressed as
// Conditions (LastTransitionTime / Reason / Message) until a concrete
// consumer demonstrates need for a first-class status field.
// CloudEvent envelope attributes are not duplicated here; recover them
// from the OTel span the ingress emits, keyed by metadata.name.
type InvocationStatus struct {
	// +optional
	Phase InvocationPhase `json:"phase,omitempty"`
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Invocation is the durable lifecycle record of one request to a
// TaskAgent or DaemonAgent — the logical unit a customer trigger maps
// to, tracked from admission through a terminal phase (see
// InvocationPhase). It is deliberately distinct from an "execution",
// the act of running the agent in a worker sandbox, and the two are not
// 1:1: a Rejected invocation never executes, a Pending or Dispatched
// one has not started yet, and a retried request could execute more
// than once under a single invocation. The ActiveExecutions counters on
// TaskAgent, WorkerPool, and AgentSandboxRevision status are derived
// from this type and count a parent's non-terminal invocations
// (Pending, Dispatched, Running) — its in-flight executions — not all
// requests ever made. This Invocation/ActiveExecutions split mirrors AWS
// Lambda's Invocations vs. ConcurrentExecutions metrics.
//
// Parent identity lives on spec.parentRef; the Create strategy
// synthesises a matching metadata.ownerReferences entry
// (controller=true, blockOwnerDeletion=false) so Kubernetes GC nukes
// Invocations when the parent is deleted. Invocations are
// system-written and immutable from clients: the stub backend returns
// 405 on POST; the eventual JetStream-backed storage will accept POST
// only from the ingress service principal.
//
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=inv
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Trigger",type=string,JSONPath=`.spec.trigger.type`
// +kubebuilder:printcolumn:name="Parent",type=string,JSONPath=`.spec.parentRef.kind`
// +kubebuilder:printcolumn:name="ParentName",type=string,JSONPath=`.spec.parentRef.name`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type Invocation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              InvocationSpec   `json:"spec"`
	Status            InvocationStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

// InvocationList contains a list of Invocation resources.
type InvocationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Invocation `json:"items"`
}
