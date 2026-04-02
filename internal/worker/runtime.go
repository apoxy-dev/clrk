// Package worker implements the sandbox runtime for CLRK worker pods.
package worker

import (
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// Runtime is the top-level worker runtime that manages sandbox lifecycle.
// It implements manager.Runnable so it can be added to a controller-runtime manager.
type Runtime struct {
	Client    client.Client
	Manager   manager.Manager
	PoolName  string
	PodName   string
	Namespace string
}

// NeedLeaderElection returns false — every worker pod runs independently.
func (r *Runtime) NeedLeaderElection() bool { return false }
