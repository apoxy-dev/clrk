// Package workerlog holds the on-disk layout of per-agent stdio logs
// teed by the worker. Split out of internal/worker so cmd/clrk can
// reference these paths without pulling in the linux-only worker
// runtime — which transitively imports the bazel-generated proto stubs
// that aren't part of the standalone go-install build.
package workerlog

import (
	"fmt"
	"path/filepath"
	"strings"
)

// safeToken folds any byte that is not an unreserved filename character
// to '_', so a caller-influenced token cannot inject a path separator or
// ".." and escape Dir. This matters for the invocation id: on the
// direct-to-dispatcher path (cron / in-cluster, bypassing the ingress
// edge that mints a fresh UUID) it can be an arbitrary
// X-Clrk-Execution-ID header value. UUIDs and DNS-label object names
// pass through unchanged; both the worker writer and the `clrk agents
// logs` reader go through these helpers, so the on-disk name stays
// consistent.
func safeToken(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '_'
		}
	}, s)
}

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
	return filepath.Join(rootDir, fmt.Sprintf("%s__%s.log", safeToken(namespace), safeToken(name)))
}

// InvocationPath returns the on-disk file the worker tees a single
// invocation's stdio to — per (namespace, agent, invocation id) so each
// run of a TaskAgent gets its own log that `clrk agents logs
// <name>/<id>` can tail without interleaving with concurrent runs. The
// worker keeps AgentPath as a symlink to the most recent invocation's
// file, so `clrk agents logs <name>` still resolves to the latest run.
// Every component is run through safeToken so a caller-supplied
// invocation id cannot inject a path separator or "..".
func InvocationPath(rootDir, namespace, name, invocationID string) string {
	if namespace == "" {
		namespace = "default"
	}
	return filepath.Join(rootDir, fmt.Sprintf("%s__%s__%s.log",
		safeToken(namespace), safeToken(name), safeToken(invocationID)))
}
