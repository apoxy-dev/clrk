package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"time"

	"github.com/go-logr/logr"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/egress"
	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
)

// Sandbox-ID prefixes. The prefix distinguishes the spawn path so
// stray state directories from a previous worker incarnation can be
// triaged at a glance and so warm vs. cold dispatches are
// differentiable in libcontainer state listings.
const (
	sandboxIDPrefixTask = "ta"
	sandboxIDPrefixWarm = "warm"
)

// WarmKey identifies a warm-pool slot: same (ns, agent) at different
// revisions are different keys, so a revision bump cleanly evicts.
// Lives in the platform-agnostic types file so the (also platform-
// agnostic) Dispatcher can construct keys without depending on the
// linux-only WarmPool implementation.
type WarmKey struct {
	Namespace string
	Agent     string
	Revision  string
}

func (k WarmKey) String() string {
	return fmt.Sprintf("%s/%s@%s", k.Namespace, k.Agent, k.Revision)
}

// newSandboxID builds a sandbox ID of the form "<prefix>-<ns>-<name>-<hex>"
// with 8 hex chars of entropy. Used by both the dispatcher (cold path)
// and the warm pool to disambiguate IDs across concurrent fires.
func newSandboxID(prefix, ns, name string) (SandboxID, error) {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("random suffix: %w", err)
	}
	return SandboxID(fmt.Sprintf("%s-%s-%s-%s", prefix, ns, name, hex.EncodeToString(b))), nil
}

// newAgentIdentity stamps an AgentIdentity for PROXY v2 TLV egress
// attribution. UID is the parent agent's K8s UID; revName is the
// AgentSandboxRevision name.
func newAgentIdentity(kind proxyproto.AgentKind, ns, name, uid, revName string) proxyproto.AgentIdentity {
	return proxyproto.AgentIdentity{
		Kind:      kind,
		Namespace: ns,
		Name:      name,
		UID:       uid,
		Revision:  revName,
	}
}

// sandboxDeleteTimeout caps the deferred cleanup Delete in the
// dispatcher and warm-pool paths. Without it a hung libcontainer
// destroy would pin the calling goroutine indefinitely.
const sandboxDeleteTimeout = 10 * time.Second

// deleteSandboxBounded is the standard cleanup helper for one-shot
// sandboxes: a bounded-context Delete that logs (but doesn't return)
// errors except ErrNotFound, which is treated as already-gone.
func deleteSandboxBounded(mgr SandboxRuntime, id SandboxID, log logr.Logger, msg string) {
	ctx, cancel := context.WithTimeout(context.Background(), sandboxDeleteTimeout)
	defer cancel()
	if err := mgr.Delete(ctx, id); err != nil && !errors.Is(err, ErrNotFound) {
		log.Error(err, msg)
	}
}

// SandboxRuntime is the subset of SandboxManager the dispatcher relies
// on. Defined as an interface so unit tests can supply a fake runtime
// (the linux-only libcontainer plumbing makes the concrete manager
// untestable off-platform).
//
// All methods correspond 1:1 to *SandboxManager — see sandbox.go for
// the production implementation.
type SandboxRuntime interface {
	Purge(ctx context.Context, id SandboxID)
	Create(
		ctx context.Context,
		id SandboxID,
		agentRef string,
		identity proxyproto.AgentIdentity,
		caPEM []byte,
		sandbox clrkv1alpha1.AgentSandbox,
		resources clrkv1alpha1.ExecutionResources,
		state *clrkv1alpha1.AgentState,
		stdio bool,
		attempt int32,
	) (*SandboxInstance, error)
	SetEgressBackends(id SandboxID, backends []egress.BackendListener) error
	SetEgressPolicy(id SandboxID, policy *egress.SandboxPolicy) error
	SetInvocationID(id SandboxID, invocationID string) error
	Start(ctx context.Context, id SandboxID) error
	Stop(ctx context.Context, id SandboxID) error
	Kill(ctx context.Context, id SandboxID) error
	Wait(ctx context.Context, id SandboxID) (exitCode int, err error)
	Delete(ctx context.Context, id SandboxID) error
}

// SandboxID uniquely identifies a sandbox instance within a worker.
type SandboxID string

// SandboxPhase represents the lifecycle phase of a sandbox.
type SandboxPhase string

const (
	SandboxCreating SandboxPhase = "Creating"
	SandboxReady    SandboxPhase = "Ready"    // Created but not started (warm pool).
	SandboxRunning  SandboxPhase = "Running"  // Active execution.
	SandboxStopping SandboxPhase = "Stopping" // SIGTERM sent, waiting for exit.
	SandboxStopped  SandboxPhase = "Stopped"  // Exited.
)

var (
	// ErrAlreadyExists is returned when a sandbox with the given ID already exists.
	ErrAlreadyExists = errors.New("sandbox already exists")
	// ErrNotFound is returned when a sandbox with the given ID does not exist.
	ErrNotFound = errors.New("sandbox not found")
	// ErrStateOverLimit is returned by Create when a TaskAgent's persistent
	// state directory's on-disk size already exceeds spec.state.sizeLimitMB.
	// The dispatcher surfaces this as 507 Insufficient Storage.
	ErrStateOverLimit = errors.New("agent state over size limit")
)

// SandboxInstance tracks the state of a single sandbox container.
type SandboxInstance struct {
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

	// stdinChild / stdoutChild / stderrChild are the Sentry-facing
	// ends of the stdio pipes. Passed to the runsc-create subprocess
	// as its cmd.Stdin/Stdout/Stderr; the subprocess donates them to
	// the Sentry boot child as the sandboxed process's stdio.
	stdinChild  *os.File
	stdoutChild *os.File
	stderrChild *os.File

	// stdoutInternalR / stderrInternalR are the worker-side read ends
	// of the Sentry's stdout/stderr pipes. drainSentryStdio reads
	// from these and fans bytes out to the slog sink and (in stdio
	// mode) the dispatcher-facing outer pipe.
	stdoutInternalR *os.File
	stderrInternalR *os.File

	// stdoutToDispatcher / stderrToDispatcher are the write ends of
	// the dispatcher-facing outer pipes (paired with sb.Stdout /
	// sb.Stderr). Only allocated when stdio=true; nil otherwise so
	// the drain goroutine knows to skip the dispatcher fan-out.
	stdoutToDispatcher *os.File
	stderrToDispatcher *os.File

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

	CreatedAt time.Time

	// idleTimer is the AfterFunc that evicts this slot after
	// WarmPoolIdleTTL elapses. Nil on cold-path sandboxes and on
	// warm sandboxes once Acquire has Stop'd the timer.
	idleTimer *time.Timer
}
