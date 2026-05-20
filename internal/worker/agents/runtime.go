package agents

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/egress"
	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
	"github.com/apoxy-dev/clrk/internal/worker/sandbox"
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
func newSandboxID(prefix, ns, name string) (sandbox.SandboxID, error) {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("random suffix: %w", err)
	}
	return sandbox.SandboxID(fmt.Sprintf("%s-%s-%s-%s", prefix, ns, name, hex.EncodeToString(b))), nil
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
// errors except sandbox.ErrNotFound, which is treated as already-gone.
func deleteSandboxBounded(mgr SandboxRuntime, id sandbox.SandboxID, log logr.Logger, msg string) {
	ctx, cancel := context.WithTimeout(context.Background(), sandboxDeleteTimeout)
	defer cancel()
	if err := mgr.Delete(ctx, id); err != nil && !errors.Is(err, sandbox.ErrNotFound) {
		log.Error(err, msg)
	}
}

// SandboxRuntime is the subset of sandbox.Manager the dispatcher relies
// on. Defined as an interface so unit tests can supply a fake runtime
// (the linux-only libcontainer plumbing makes the concrete manager
// untestable off-platform).
//
// All methods correspond 1:1 to *sandbox.Manager — see the sandbox
// package for the production implementation.
type SandboxRuntime interface {
	Purge(ctx context.Context, id sandbox.SandboxID)
	Create(
		ctx context.Context,
		id sandbox.SandboxID,
		agentRef string,
		identity proxyproto.AgentIdentity,
		caPEM []byte,
		spec clrkv1alpha1.AgentSandbox,
		resources clrkv1alpha1.ExecutionResources,
		state *clrkv1alpha1.AgentState,
		stdio bool,
		attempt int32,
	) (*sandbox.Instance, error)
	SetEgressBackends(id sandbox.SandboxID, backends []egress.BackendListener) error
	SetEgressPolicy(id sandbox.SandboxID, policy *egress.SandboxPolicy) error
	SetInvocationID(id sandbox.SandboxID, invocationID string) error
	Start(ctx context.Context, id sandbox.SandboxID) error
	Stop(ctx context.Context, id sandbox.SandboxID) error
	Kill(ctx context.Context, id sandbox.SandboxID) error
	Wait(ctx context.Context, id sandbox.SandboxID) (exitCode int, err error)
	Delete(ctx context.Context, id sandbox.SandboxID) error
}
