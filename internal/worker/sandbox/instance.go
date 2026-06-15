package sandbox

import (
	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
	sandboxcore "github.com/apoxy-dev/clrk/pkg/sandbox"
)

// CreateRequest bundles the per-call inputs of Manager.Create. The
// previous positional 10-arg signature was easy to mis-order and inflated
// every call site; CreateRequest keeps each field's role visible.
//
// AgentRef is the parent agent's K8s name (TaskAgent.Name or
// DaemonAgent.Name). It is distinct from Identity.Name on TaskAgents,
// where the latter holds the per-invocation name — AgentRef drives the
// per-(ns,agent) state dir path and the warm-pool ownership lookup, and
// must stay parent-scoped.
//
// Lives in this cross-platform file so cross-package callers in agents/ —
// Dispatcher / WarmPool / DaemonLifecycle — can name the type on darwin
// too; their fake-runtime test doubles need to construct CreateRequest
// values, and pinning the type behind //go:build linux would force every
// test target to be linux-only.
type CreateRequest struct {
	ID        SandboxID
	AgentRef  string
	Identity  proxyproto.AgentIdentity
	CAPEM     []byte
	Sandbox   clrkv1alpha1.AgentSandbox
	Resources clrkv1alpha1.ExecutionResources
	// State opts into a per-(ns,agent) persistent state bind-mount. Nil for
	// stateless callers (DaemonAgent, stateless TaskAgent).
	State *clrkv1alpha1.AgentState
	// Stdio requests caller-facing stdio pipes on the resulting Instance.
	// Daemons set false; stdout/stderr still flow into the per-agent log
	// file via the line-splitter sink in either mode.
	Stdio bool
	// Attempt is the restart-attempt counter (daemons); 0 for TaskAgent
	// invocations.
	Attempt int32
}

// Instance is clrk's tenant view of a live sandbox: the neutral core
// [sandboxcore.Instance] (ID, Phase, RootFS, addressing, stdio pipes,
// CreatedAt — all promoted via the embedded pointer) plus the agent
// lineage + identity the egress bridge, status publisher, and logs
// pipeline attribute traffic and output by.
type Instance struct {
	*sandboxcore.Instance

	// AgentRef is the parent agent's K8s name (see CreateRequest.AgentRef).
	AgentRef string
	// Namespace is the agent's namespace (== Identity.Namespace), kept
	// directly so status/warm-pool code doesn't reach through Identity.
	Namespace string

	// Sandbox / Resources are the CRD snapshots the sandbox was created
	// from, retained for status reporting and warm-pool revision matching.
	Sandbox   clrkv1alpha1.AgentSandbox
	Resources clrkv1alpha1.ExecutionResources

	// Identity is stamped into PROXY v2 TLVs on every egress connection
	// dialed through this sandbox so the Envoy MITM gateway can attribute
	// traffic back to its parent agent.
	Identity proxyproto.AgentIdentity
}
