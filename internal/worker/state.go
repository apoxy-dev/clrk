//go:build linux

package worker

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const (
	// clrkStateHostRoot is the worker host directory backing per-(ns,
	// agent) bind-mounts. Persists across sandbox restarts within a
	// worker pod's lifetime; replaced workers lose state unless the
	// deployment mounts a PV here.
	clrkStateHostRoot = "/var/lib/clrk/state"

	// defaultStateGuestMount is the in-sandbox path for state when
	// AgentState.MountPath is unset.
	defaultStateGuestMount = "/var/clrk/state"
)

// stateHostPath returns the on-host directory backing the bind-mount
// for (ns, agentRef). Layout: /var/lib/clrk/state/<ns>/<agent>/.
// Same directory is shared across all sandbox executions for the
// same TaskAgent on this worker.
func stateHostPath(ns, agent string) string {
	return filepath.Join(clrkStateHostRoot, ns, agent)
}

// ensureStateDir mkdirs the per-(ns,agent) state directory and
// enforces sizeLimitMB. Returns ErrStateOverLimit if the directory
// is already over the cap. sizeLimitMB <= 0 means no cap.
//
// The size check is best-effort: a TOCTOU window exists between this
// check and the agent process writing more bytes inside the sandbox.
// The cap is a soft guarantee — agents that try to OOM the disk
// trip it on the next dispatch.
func ensureStateDir(ns, agent string, sizeLimitMB int32) (string, error) {
	host := stateHostPath(ns, agent)
	if err := os.MkdirAll(host, 0700); err != nil {
		return "", fmt.Errorf("mkdir state dir %s: %w", host, err)
	}
	if sizeLimitMB <= 0 {
		return host, nil
	}
	cap := int64(sizeLimitMB) * 1024 * 1024
	used, err := dirBytesUpTo(host, cap)
	if err != nil {
		return "", fmt.Errorf("measuring state dir %s: %w", host, err)
	}
	if used >= cap {
		return "", fmt.Errorf("%w: %d bytes used vs %d cap", ErrStateOverLimit, used, cap)
	}
	return host, nil
}

// dirBytesUpTo recursively sums regular-file sizes under root, short-
// circuiting once total reaches cap so a many-million-file state dir
// doesn't dominate cold-start latency. Symlinks are skipped so a
// hostile agent can't game the cap by symlinking the rootfs.
func dirBytesUpTo(root string, cap int64) (int64, error) {
	var total int64
	stop := errors.New("cap reached")
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			if os.IsNotExist(ierr) {
				return nil
			}
			return ierr
		}
		total += info.Size()
		if total >= cap {
			return stop
		}
		return nil
	})
	if err == stop {
		return total, nil
	}
	return total, err
}

