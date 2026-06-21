//go:build linux

package sandbox

import (
	sandboxcore "github.com/apoxy-dev/apoxy/pkg/sandbox"
)

// Linux-only re-exports: these reference core symbols defined only on
// linux (the runsc reaper and the cgroup bootstrap), so they can't live in
// the cross-platform aliases.go.

// ShouldSkipReap re-exports the core's reaper-exclusion predicate so
// cmd/worker's SIGCHLD reaper (via internal/worker/reaper_alias_linux.go)
// keeps its existing import path.
var ShouldSkipReap = sandboxcore.ShouldSkipReap

// InitWorkerCgroup re-exports the core's one-time cgroup-v2 bootstrap
// (renamed InitHostCgroup in the neutral core) under the name the worker
// root's startup already calls.
var InitWorkerCgroup = sandboxcore.InitHostCgroup
