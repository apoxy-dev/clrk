package worker

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/egress"
	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
)

// WorkerLogsDir is the path inside a worker container where per-agent
// stdio is teed for `clrk agents logs` to `tail -F` via docker exec /
// kubectl exec. Untagged so the CLI on darwin can reference it without
// pulling in the linux-only runtime.
const WorkerLogsDir = "/run/clrk/logs"

// AgentLogPath returns the on-disk file the worker tees agent stdio to,
// shaped so `clrk agents logs` can construct the same path it asks the
// worker container to `tail -F`. Per-agent (not per-sandbox) so
// restarts append to the same file and the CLI doesn't have to chase
// SandboxID generations.
func AgentLogPath(rootDir, namespace, name string) string {
	if namespace == "" {
		namespace = "default"
	}
	return filepath.Join(rootDir, fmt.Sprintf("%s__%s.log", namespace, name))
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
)

// SandboxInstance tracks the state of a single sandbox container.
type SandboxInstance struct {
	ID        SandboxID
	AgentRef  string
	Namespace string
	Phase     SandboxPhase
	NetNS     string    // /run/netns/run-<id>
	TAPName   string    // TAP device name in the netns.
	TAPFD     *os.File  // Host-side TAP fd for netstack (APO-536).
	RootFS    string    // Extracted rootfs path.
	Stack     io.Closer // Per-sandbox netstack (*netstack.SandboxStack on linux).

	Sandbox   clrkv1alpha1.AgentSandbox
	Resources clrkv1alpha1.ExecutionResources

	// Identity is stamped into PROXY v2 TLVs on every egress connection
	// dialed through this sandbox's netstack so the Envoy MITM gateway can
	// attribute traffic back to its parent agent.
	Identity proxyproto.AgentIdentity

	// EgressBackends are the EG listener entries this sandbox can be
	// steered to (one per EgressListener in the gateway's spec). The
	// IdentityDialer picks one per outbound dial based on shape +
	// destination port. Empty slice means direct dial.
	EgressBackends []egress.BackendListener

	// EgressPolicy is the per-sandbox authorization plane built from
	// the bound EgressGateway's DefaultPolicy and the EgressL4Routes
	// targeting it. Nil means no enforcement (sandboxes with no
	// EgressRefs). The handle is stable across CRD edits — the
	// router updates its underlying state in place.
	EgressPolicy *egress.SandboxPolicy

	CreatedAt time.Time
}
