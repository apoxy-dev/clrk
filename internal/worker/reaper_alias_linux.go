//go:build linux

package worker

import "github.com/apoxy-dev/clrk/internal/worker/sandbox"

// ShouldSkipReap re-exports sandbox.ShouldSkipReap so cmd/worker's
// reaper (cmd/worker/reaper_linux.go) keeps its existing import path
// after the internal/worker refactor split sandbox lifecycle into its
// own sub-package.
var ShouldSkipReap = sandbox.ShouldSkipReap
