// Package worker implements the sandbox runtime for CLRK worker pods.
package worker

import (
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/apoxy-dev/clrk/internal/otelemit"
)

// Runtime is the top-level worker runtime that manages sandbox lifecycle.
// It implements manager.Runnable so it can be added to a controller-runtime manager.
type Runtime struct {
	Client    client.Client
	Manager   manager.Manager
	PoolName  string
	PodName   string
	Namespace string

	// CMNATSAddr is the controller-manager's NATS/JetStream client
	// address (host:port). The dispatcher publishes Invocation lifecycle
	// events here; empty disables worker-side publishing. Sourced from
	// invevent.CMNATSAddrEnv, set by the WorkerPool controller.
	CMNATSAddr string

	// Emitter is always non-nil; falls back to Noop when no
	// EgressGateway / OTLP endpoint is configured so consumers emit
	// unconditionally.
	Emitter otelemit.Emitter
}

// NeedLeaderElection returns false — every worker pod runs independently.
func (r *Runtime) NeedLeaderElection() bool { return false }
