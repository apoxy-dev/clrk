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
	// LabelExpose opts a Service into the `clrk dev` host-port
	// auto-forwarder. Set on TaskAgent ingress data-plane Services by
	// the ingress controller; users can also stamp it on their own
	// Services to expose them to the host without a manual
	// `kubectl port-forward`.
	LabelExpose = "clrk.apoxy.dev/expose"
	// LabelExposePort is an optional per-Service host-port hint for the
	// `clrk dev` auto-forwarder. When set to a parsable port number and
	// the port is free, the forwarder binds that exact port; otherwise
	// it falls back to the next free port in its range. The ingress
	// controller does not set this; it's user-facing only.
	LabelExposePort = "clrk.apoxy.dev/expose-port"
)

// Annotations bumped to trigger pod rollouts (the same mechanism as
// `kubectl rollout restart`).
const (
	// RestartedAtAnnotation is stamped with an RFC3339 timestamp on a pod
	// template's annotations to force a rolling restart. For a controller-
	// owned Deployment (a WorkerPool's), it must be set on
	// WorkerPool.spec.template.metadata.annotations, not the Deployment: the
	// WorkerPoolDeploymentReconciler rebuilds the Deployment's pod template
	// from the WorkerPool on every reconcile, so an annotation patched onto
	// the Deployment is wiped on the next pass and the new ReplicaSet is
	// scaled back to zero. Set on the WorkerPool, the controller propagates
	// it into the Deployment template itself.
	RestartedAtAnnotation = "clrk.apoxy.dev/restartedAt"
)

// AgentKind values written into LabelAgentKind. Workers branch on this to
// pick the right sandbox lifecycle (TaskAgent: per-trigger, DaemonAgent:
// long-lived with restart policy).
const (
	AgentKindTask   = "TaskAgent"
	AgentKindDaemon = "DaemonAgent"
)
