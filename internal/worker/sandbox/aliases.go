package sandbox

import (
	"errors"

	sandboxcore "github.com/apoxy-dev/apoxy/pkg/sandbox"
)

// This package is the tenant/egress WRAPPER around the neutral sandbox
// runtime core in apoxy's pkg/sandbox (github.com/apoxy-dev/apoxy/pkg/
// sandbox). The core owns the gVisor/runsc
// lifecycle (image pull, OCI bundle, cgroup, runsc create/start/wait/
// delete) over a neutral Spec/Instance; this package adapts clrk's CRD
// CreateRequest down to that Spec, re-adds the egress data path + OTLP
// stdio + persistent-state/trust mounts, and tracks agent identity. These
// aliases re-export the core's neutral leaf types so existing callers
// (internal/worker/agents, runtime_linux, cmd/worker) keep their import
// path and type spellings unchanged — single source of truth, not a copy.

// SandboxID identifies a sandbox. Aliased from the core.
type SandboxID = sandboxcore.SandboxID

// SandboxPhase is the lifecycle phase. Aliased from the core.
type SandboxPhase = sandboxcore.SandboxPhase

// Lifecycle phases, re-exported from the core so callers compare against
// the same constants the runtime sets.
const (
	SandboxCreating = sandboxcore.SandboxCreating
	SandboxReady    = sandboxcore.SandboxReady
	SandboxRunning  = sandboxcore.SandboxRunning
	SandboxStopping = sandboxcore.SandboxStopping
	SandboxStopped  = sandboxcore.SandboxStopped
)

var (
	// ErrAlreadyExists / ErrNotFound are re-exported from the core so
	// callers can errors.Is against the same sentinels the runtime returns.
	ErrAlreadyExists = sandboxcore.ErrAlreadyExists
	ErrNotFound      = sandboxcore.ErrNotFound

	// ErrStateOverLimit is returned by Create when a TaskAgent's persistent
	// state directory already exceeds spec.state.sizeLimitMB. The dispatcher
	// surfaces this as 507 Insufficient Storage. Wrapper-only (the core has
	// no persistent-state concept).
	ErrStateOverLimit = errors.New("agent state over size limit")
)

// ImageStore / ImageInfo / NewImageStore re-export the core's ORAS image
// store so the worker root and status publisher keep their call sites.
type (
	ImageStore = sandboxcore.ImageStore
	ImageInfo  = sandboxcore.ImageInfo
)

// NewImageStore constructs the core ORAS image store.
var NewImageStore = sandboxcore.NewImageStore
