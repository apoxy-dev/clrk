package worker

import (
	"errors"
	"io"
	"os"
	"time"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
)

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
	NetNS     string   // /run/netns/run-<id>
	TAPName   string   // TAP device name in the netns.
	TAPFD     *os.File // Host-side TAP fd for netstack (APO-536).
	RootFS    string   // Extracted rootfs path.
	Stack     io.Closer // Per-sandbox netstack (*netstack.SandboxStack on linux).

	Sandbox   clrkv1alpha1.AgentSandbox
	Resources clrkv1alpha1.ExecutionResources

	// Identity is stamped into PROXY v2 TLVs on every egress connection
	// dialed through this sandbox's netstack so the Envoy MITM gateway can
	// attribute traffic back to its parent agent.
	Identity proxyproto.AgentIdentity

	CreatedAt time.Time
}
