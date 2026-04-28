package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/apoxy-dev/clrk/pkg/drivers"
)

// reloadWorker stops and recreates clrk-worker-<idx> with the current
// `clrk/worker:latest` image. Used by --reload-tar's mtime watcher
// inside the running `clrk dev` process.
func reloadWorker(ctx context.Context, o *devOpts, idx int) error {
	w := drivers.NewWorkerDriver(idx)
	slog.Info("Stopping worker", "container", w.Name())
	if err := w.Stop(ctx); err != nil {
		return fmt.Errorf("stopping worker: %w", err)
	}
	opts, err := workerOpts(o, w)
	if err != nil {
		return err
	}
	if _, err := w.Start(ctx, opts...); err != nil {
		return fmt.Errorf("starting worker: %w", err)
	}
	slog.Info("Worker reloaded", "container", w.Name(), "image", o.workerImage)
	return nil
}

// reloadControllerManager stops and recreates clrk-controller-manager.
// Cluster-side bootstrapping (clrk APIService, EG bridge,
// extensionManager wiring) is intentionally NOT re-run — those records
// are idempotent on bringUp and remain valid for as long as the k3s
// container lives.
func reloadControllerManager(ctx context.Context, o *devOpts) error {
	cm := drivers.NewControllerManagerDriver()
	slog.Info("Stopping controller-manager", "container", drivers.ControllerManagerContainerName)
	if err := cm.Stop(ctx); err != nil {
		return fmt.Errorf("stopping controller-manager: %w", err)
	}
	opts, err := controllerManagerOpts(o)
	if err != nil {
		return err
	}
	if _, err := cm.Start(ctx, opts...); err != nil {
		return fmt.Errorf("starting controller-manager: %w", err)
	}
	slog.Info("Controller-manager reloaded", "container", drivers.ControllerManagerContainerName, "image", o.controllerImage)
	return nil
}

// dockerLoad imports an OCI tarball into the host docker daemon, the
// same way `docker load -i …` does. Reused both at startup (when
// --reload-tar is supplied to seed the image) and on every detected
// mtime change.
func dockerLoad(ctx context.Context, path string) error {
	if _, err := os.Stat(path); err != nil {
		return fmt.Errorf("tarball not found: %w", err)
	}
	slog.Info("Loading OCI tarball", "path", path)
	out, err := exec.CommandContext(ctx, "docker", "load", "-i", path).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker load: %w: %s", err, strings.TrimSpace(string(out)))
	}
	slog.Info("Loaded image", "output", strings.TrimSpace(string(out)))
	return nil
}

// reloadTarSpec is one parsed `--reload-tar=COMPONENT=PATH` flag. The
// loop owning the spec polls the tarball's mtime and triggers a reload
// when it advances.
type reloadTarSpec struct {
	Component string // worker | controller-manager
	Path      string
	Index     int // worker replica index, ignored for controller-manager
}

// parseReloadTar parses a list of `--reload-tar=COMPONENT=PATH` flags
// where COMPONENT is "worker" (optionally "worker-N") or
// "controller-manager".
func parseReloadTar(raw []string) ([]reloadTarSpec, error) {
	out := make([]reloadTarSpec, 0, len(raw))
	for _, s := range raw {
		eq := strings.IndexByte(s, '=')
		if eq <= 0 || eq == len(s)-1 {
			return nil, fmt.Errorf("--reload-tar=%s: expected COMPONENT=PATH", s)
		}
		comp, path := s[:eq], s[eq+1:]
		idx := 0
		switch {
		case comp == "controller-manager":
		case comp == "worker":
		case strings.HasPrefix(comp, "worker-"):
			suffix := comp[len("worker-"):]
			n, err := atoiStrict(suffix)
			if err != nil {
				return nil, fmt.Errorf("--reload-tar=%s: %w", s, err)
			}
			comp = "worker"
			idx = n
		default:
			return nil, fmt.Errorf("--reload-tar=%s: COMPONENT must be worker[-N] or controller-manager", s)
		}
		if _, err := os.Stat(path); err != nil {
			return nil, fmt.Errorf("--reload-tar=%s: %w", s, err)
		}
		out = append(out, reloadTarSpec{Component: comp, Path: path, Index: idx})
	}
	return out, nil
}

func atoiStrict(s string) (int, error) {
	if s == "" {
		return 0, fmt.Errorf("empty number")
	}
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("not a number: %q", s)
		}
		n = n*10 + int(r-'0')
	}
	return n, nil
}

// runReloadWatcher polls path's mtime every interval and fires the
// component's reload sequence whenever it advances. Returns when ctx is
// done. Errors during reload are logged and the watcher keeps running —
// a transient docker hiccup shouldn't kill the dev session.
func runReloadWatcher(ctx context.Context, o *devOpts, spec reloadTarSpec) {
	const interval = 500 * time.Millisecond
	last := mtime(spec.Path)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		now := mtime(spec.Path)
		if now.IsZero() || !now.After(last) {
			continue
		}
		last = now
		slog.Info("Reload-tar mtime changed; reloading", "component", spec.Component, "index", spec.Index, "path", spec.Path)
		if err := dockerLoad(ctx, spec.Path); err != nil {
			slog.Error("dockerLoad failed; will retry on next change", "err", err)
			continue
		}
		var err error
		switch spec.Component {
		case "worker":
			err = reloadWorker(ctx, o, spec.Index)
		case "controller-manager":
			err = reloadControllerManager(ctx, o)
		}
		if err != nil {
			slog.Error("Reload failed; will retry on next change", "component", spec.Component, "err", err)
		}
	}
}

func mtime(path string) time.Time {
	st, err := os.Stat(path)
	if err != nil {
		return time.Time{}
	}
	return st.ModTime()
}
