package v1alpha1

// Labels projected onto AgentSandboxRevision and other derived resources by
// the controllers. Workers depend on these to filter and look up parent
// agent metadata without walking owner references on every reconcile.
const (
	// LabelAgent is the name of the parent agent (TaskAgent or DaemonAgent)
	// that owns this revision.
	LabelAgent = "clrk.apoxy.dev/agent"
	// LabelAgentKind is the kind of the parent agent ("TaskAgent" or
	// "DaemonAgent").
	LabelAgentKind = "clrk.apoxy.dev/agent-kind"
	// LabelGeneration is the parent agent's metadata.generation that the
	// revision was minted from.
	LabelGeneration = "clrk.apoxy.dev/generation"
	// LabelWorkerPool is the name of the WorkerPool the revision is
	// targeted at. Workers filter the watch by this label.
	LabelWorkerPool = "clrk.apoxy.dev/worker-pool"
)

// AgentKind values written into LabelAgentKind. Workers branch on this to
// pick the right sandbox lifecycle (TaskAgent: per-trigger, DaemonAgent:
// long-lived with restart policy).
const (
	AgentKindTask   = "TaskAgent"
	AgentKindDaemon = "DaemonAgent"
)
