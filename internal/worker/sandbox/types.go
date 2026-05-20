package sandbox

import "errors"

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
