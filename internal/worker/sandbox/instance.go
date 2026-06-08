package sandbox

import (
	"io"
	"net/netip"
	"os"
	"time"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
)

// CreateRequest bundles the per-call inputs of Manager.Create. The
// previous positional 10-arg signature was easy to mis-order and
// inflated every call site; CreateRequest keeps each field's role
// visible at the call site.
//
// AgentRef is the parent agent's K8s name (TaskAgent.Name or
// DaemonAgent.Name). It is distinct from Identity.Name on TaskAgents,
// where the latter holds the per-invocation name — AgentRef drives
// the per-(ns,agent) state dir path and the warm-pool ownership
// lookup, and must stay parent-scoped.
//
// Lives in this cross-platform file (vs manager.go) so cross-package
// callers in agents/ — Dispatcher / WarmPool / DaemonLifecycle — can
// name the type on darwin too; their fake-runtime test doubles need
// to construct CreateRequest values, and pinning the type behind
// //go:build linux would force every test target to be linux-only.
type CreateRequest struct {
	ID        SandboxID
	AgentRef  string
	Identity  proxyproto.AgentIdentity
	CAPEM     []byte
	Sandbox   clrkv1alpha1.AgentSandbox
	Resources clrkv1alpha1.ExecutionResources
	// State opts into a per-(ns,agent) persistent state bind-mount.
	// Nil for stateless callers (DaemonAgent, stateless TaskAgent).
	State *clrkv1alpha1.AgentState
	// Stdio requests dispatcher-facing stdio pipes on the resulting
	// Instance. Daemons set false; stdout/stderr still flow into the
	// per-agent log file via the line-splitter sink in either mode.
	Stdio bool
	// Attempt is the restart-attempt counter (daemons); 0 for
	// TaskAgent invocations.
	Attempt int32
}

// Instance tracks the state of a single sandbox container.
type Instance struct {
	ID        SandboxID
	AgentRef  string
	Namespace string
	Phase     SandboxPhase
	RootFS    string     // Extracted rootfs path.
	SandboxIP netip.Addr // Per-sandbox container IP, written into the in-Sentry PluginStack eth0 via initStr.
	GatewayIP netip.Addr // Per-sandbox /30 gateway IP. Cosmetic with sentrystack — the in-Sentry forwarder never delivers frames to it — but exposed so `ip route` inside the sandbox shows a sane default route, and so the sandbox's /etc/resolv.conf has a destination that triggers the in-Sentry UDP/DNS forwarder.

	// Stdin/Stdout/Stderr are populated when the sandbox was Created with
	// stdio=true. The dispatcher uses these to stream the HTTP request
	// body in and the response body out. Nil on daemon (non-stdio)
	// sandboxes — stdout/stderr still flow to the per-agent log file
	// via the line-splitter sink in either mode.
	Stdin  io.WriteCloser
	Stdout io.ReadCloser
	Stderr io.ReadCloser

	// stdioPipes groups the six worker-side stdio plumbing FDs that
	// used to dangle on the struct. Embedded so existing field
	// references (sb.stdinChild, sb.stdoutToDispatcher, etc.) keep
	// working via promotion — the grouping is for the struct
	// definition, not the call sites.
	stdioPipes

	Sandbox   clrkv1alpha1.AgentSandbox
	Resources clrkv1alpha1.ExecutionResources

	// Identity is stamped into PROXY v2 TLVs on every egress connection
	// dialed through this sandbox's netstack so the Envoy MITM gateway can
	// attribute traffic back to its parent agent.
	Identity proxyproto.AgentIdentity

	// initStr is the per-sandbox sentrystack init payload computed at
	// Create. Retained on the instance so runscStart can re-pass it
	// through the runsc-start subprocess's env — gVisor calls
	// PluginStack.PreInit from inside `runsc start`
	// (https://github.com/apoxy-dev/gvisor/blob/5d6cfb0c0960/runsc/sandbox/network.go#L350),
	// not from `runsc create`, so the create-time env doesn't reach
	// where PreInit reads it.
	initStr string

	// InboundListenAddr, when set, enables the ingress path: the in-sandbox
	// "ip:port" a resident server listens on (e.g. "127.0.0.1:8080"). At
	// Start the worker opens a host AF_UNIX listening socket, hands its fd to
	// the Sentry (which installs the inbound forwarder), and records the
	// socket path in InboundSockPath. Empty keeps the sandbox egress-only.
	//
	// NOTE (spike): set directly on the Instance to drive the APO-694
	// mechanism. The customer-facing surface (a Worker/AgentSandbox API field
	// or CreateRequest option) is M1 — see docs/workerd-runtime-mvp.md.
	InboundListenAddr string

	// InboundSockPath is the host filesystem path of the AF_UNIX listening
	// socket that fronts the resident server. Callers on the host (the Envoy
	// MITM, the backplane bridge, or a spike test) dial this path to reach
	// the in-sandbox listener. Set at Start; empty when inbound is disabled.
	InboundSockPath string

	CreatedAt time.Time
}

// stdioPipes groups the six worker-side stdio plumbing FDs of a
// sandbox.Instance. Embedded into Instance so call sites that reach
// for sb.stdinChild / sb.stdoutToDispatcher / etc. keep working via
// field promotion — keeping the struct definition tidy without
// forcing every reference to add a `.pipes.` indirection.
//
// stdinChild / stdoutChild / stderrChild are the Sentry-facing ends:
// passed to the runsc-create subprocess as its cmd.Stdin/Stdout/Stderr
// so the subprocess donates them to the Sentry boot child.
//
// stdoutInternalR / stderrInternalR are the worker-side read ends of
// the Sentry's stdout/stderr pipes. drainSentryStdio reads from these
// and fans bytes out to the slog sink and (in stdio mode) the
// dispatcher-facing outer pipe.
//
// stdoutToDispatcher / stderrToDispatcher are the write ends of the
// dispatcher-facing outer pipes (paired with sb.Stdout / sb.Stderr).
// Only allocated when stdio=true; nil otherwise so the drain
// goroutine knows to skip the dispatcher fan-out.
type stdioPipes struct {
	stdinChild         *os.File
	stdoutChild        *os.File
	stderrChild        *os.File
	stdoutInternalR    *os.File
	stderrInternalR    *os.File
	stdoutToDispatcher *os.File
	stderrToDispatcher *os.File
}
