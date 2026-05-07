// Package workerlog holds the on-disk layout of per-agent stdio logs
// teed by the worker. Split out of internal/worker so cmd/clrk can
// reference these paths without pulling in the linux-only worker
// runtime — which transitively imports the bazel-generated proto stubs
// that aren't part of the standalone go-install build.
package workerlog

import (
	"fmt"
	"path/filepath"
)

// Dir is the path inside a worker container where per-agent stdio is
// teed for `clrk agents logs` to `tail -F` via docker exec / kubectl
// exec.
const Dir = "/run/clrk/logs"

// AgentPath returns the on-disk file the worker tees agent stdio to,
// shaped so `clrk agents logs` can construct the same path it asks the
// worker container to `tail -F`. Per-agent (not per-sandbox) so
// restarts append to the same file and the CLI doesn't have to chase
// SandboxID generations.
func AgentPath(rootDir, namespace, name string) string {
	if namespace == "" {
		namespace = "default"
	}
	return filepath.Join(rootDir, fmt.Sprintf("%s__%s.log", namespace, name))
}
