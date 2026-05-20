package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/apoxy-dev/clrk/internal/drivers"
	"github.com/apoxy-dev/clrk/internal/drivers/dockerutils"
)

// devSessionFileName is the JSON sidecar bringUp writes next to dev.lock.
// Holds the effective image refs and run options for the live session so
// a second `clrk dev reload <component>` process can recreate the
// matching docker container with identical settings.
const devSessionFileName = "dev.json"

// devSession persists everything `clrk dev reload <component>` needs
// without re-parsing flags. Kept intentionally small — fields are
// extracted from devOpts at end-of-bringUp and serialized verbatim.
type devSession struct {
	DataDir          string `json:"data_dir"`
	Workers          int    `json:"workers"`
	ControllerImage  string `json:"controller_image"`
	WorkerImage      string `json:"worker_image"`
	Pull             string `json:"pull"`
	Watch            bool   `json:"watch"`
	OtelEndpoint     string `json:"otel_endpoint"`
	RegistryHostPort int    `json:"registry_host_port"`
}

// writeDevSession serializes the effective devOpts state to
// <dataDir>/dev.json so `clrk dev reload` can recover it. Called at the
// end of bringUp after o.controllerImage / o.workerImage / o.pull have
// settled (--registry-image overrides applied).
func writeDevSession(o *devOpts, state *devState) error {
	s := devSession{
		DataDir:          o.dataDir,
		Workers:          o.workers,
		ControllerImage:  o.controllerImage,
		WorkerImage:      o.workerImage,
		Pull:             o.pull,
		Watch:            o.watch,
		OtelEndpoint:     o.otelEndpoint,
		RegistryHostPort: state.cluster.RegistryHostPort(),
	}
	path := filepath.Join(o.dataDir, devSessionFileName)
	tmp := path + ".tmp"
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// readDevSession recovers the session sidecar written by writeDevSession.
// Returns a clear error when no live session is present so users see
// "run clrk dev first" instead of a generic missing-file.
func readDevSession(dataDir string) (*devSession, error) {
	path := filepath.Join(dataDir, devSessionFileName)
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("no live clrk dev session at %s — start one with `clrk dev`", dataDir)
		}
		return nil, err
	}
	var s devSession
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	return &s, nil
}

// sessionToDevOpts reconstructs the subset of devOpts needed by
// workerOpts / controllerManagerOpts. Reload doesn't touch
// k3d/registry/preflight/secrets/apply paths, so those fields stay zero.
func sessionToDevOpts(s *devSession) *devOpts {
	return &devOpts{
		dataDir:         s.DataDir,
		workers:         s.Workers,
		controllerImage: s.ControllerImage,
		workerImage:     s.WorkerImage,
		pull:            s.Pull,
		watch:           s.Watch,
		otelEndpoint:    s.OtelEndpoint,
	}
}

// applyRegistryImageOverrides folds --registry-image flags into the
// effective image refs and forces --pull always on the matching
// component. Must run before bringUp consumes o.controllerImage /
// o.workerImage so the launched container references the local-registry
// tag and re-pulls each restart.
func applyRegistryImageOverrides(o *devOpts) error {
	if len(o.registryImages) == 0 {
		return nil
	}
	for _, raw := range o.registryImages {
		comp, ref, ok := strings.Cut(raw, "=")
		if !ok || comp == "" || ref == "" {
			return fmt.Errorf("--registry-image=%s: expected COMPONENT=REF", raw)
		}
		switch {
		case comp == "controller-manager":
			o.controllerImage = ref
		case comp == "worker", strings.HasPrefix(comp, "worker-"):
			// Worker-N indexing accepted, but o.workerImage is shared
			// across all replicas — there's no per-index image today.
			// Apply globally; revisit if per-replica images become
			// useful.
			o.workerImage = ref
		default:
			return fmt.Errorf("--registry-image=%s: COMPONENT must be worker[-N] or controller-manager", raw)
		}
	}
	// Force re-pull so `docker push <ref>` on the host is visible to
	// the subsequent container start without manual intervention.
	o.pull = "always"
	return nil
}

// newDevReloadCmd is `clrk dev reload <component>`. Reads dev.json next
// to the live session's lock file, stops + restarts the matching
// container with `--pull always` so a freshly-pushed local-registry tag
// takes effect. Designed to be invoked from a second terminal while
// `clrk dev` is running.
//
// Re-runs ApplyControllerManagerBridge after a controller-manager reload
// so the cm's new docker-network IP is reflected in the bridge
// Endpoints object — without that step the in-cluster Envoy data-plane
// black-holes ext_proc requests at the previous cm IP.
func newDevReloadCmd() *cobra.Command {
	var (
		dataDir string
		workerN int
	)
	cmd := &cobra.Command{
		Use:   "reload <component>",
		Short: "Stop and restart a clrk dev component with --pull always",
		Long: "Reads the live session's <data-dir>/dev.json sidecar and " +
			"recreates the matching docker container. Component is " +
			"`worker[-N]` or `controller-manager`. Pair with a prior " +
			"`docker push localhost:<registry-port>/...` to roll a new image.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if dataDir == "" {
				dataDir = clrkDir
			}
			sess, err := readDevSession(dataDir)
			if err != nil {
				return err
			}
			comp := args[0]
			o := sessionToDevOpts(sess)
			switch {
			case comp == "controller-manager":
				return reloadControllerManager(cmd.Context(), o)
			case comp == "worker":
				if workerN >= sess.Workers {
					return fmt.Errorf("worker-%d out of range; session has %d workers", workerN, sess.Workers)
				}
				return reloadWorker(cmd.Context(), o, workerN)
			case strings.HasPrefix(comp, "worker-"):
				n, err := strconv.Atoi(strings.TrimPrefix(comp, "worker-"))
				if err != nil {
					return fmt.Errorf("worker index in %q: %w", comp, err)
				}
				if n >= sess.Workers {
					return fmt.Errorf("worker-%d out of range; session has %d workers", n, sess.Workers)
				}
				return reloadWorker(cmd.Context(), o, n)
			default:
				return fmt.Errorf("component must be worker[-N] or controller-manager, got %q", comp)
			}
		},
	}
	cmd.Flags().StringVar(&dataDir, "data-dir", "", "Host path of the running session's data dir (defaults to --clrk-dir).")
	cmd.Flags().IntVar(&workerN, "worker-index", 0, "Worker replica to reload when component is `worker` (default 0). Ignored if component is `worker-N`.")
	return cmd
}

// reloadWorker stops and recreates clrk-worker-<idx> with --pull always
// forced via o.pull. Image ref comes from o.workerImage which the live
// session persisted via dev.json. Safe to invoke while the parent
// `clrk dev` is running — `docker rm -f` evicts the old container, and
// the parent's log streamer reconnects to the new one.
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

// reloadControllerManager stops and recreates clrk-controller-manager
// and re-points the cluster's bridge Endpoints at the new docker-network
// IP. The Endpoints rewrite is what makes this safe to call against a
// live in-cluster Envoy data plane — without it, ext_proc requests
// silently fail at the cm's previous IP until the cluster eviction
// timer kicks in.
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

	// Re-bridge: the cm's IP on the docker network changed when the
	// container was recreated, but the in-cluster Service Endpoints
	// object still points at the previous IP. Without this rewrite,
	// every in-cluster Envoy / EG / ingress request to the cm
	// black-holes until the next `clrk dev` cold start. Cheap to redo
	// — KubectlApply is idempotent and only touches the Endpoints
	// object's addresses.
	cluster := drivers.NewClusterDriver(o.dataDir, "", 0)
	cmIP, err := dockerutils.IPOnNetwork(ctx, drivers.ControllerManagerContainerName, drivers.NetworkName)
	if err != nil {
		return fmt.Errorf("reading cm IP after reload: %w", err)
	}
	if err := cluster.ApplyControllerManagerBridge(ctx, devClrkNamespace, cmIP, 9443, 9444, 8082, 18000); err != nil {
		return fmt.Errorf("re-bridging controller-manager: %w", err)
	}
	slog.Info("Controller-manager bridge updated", "ip", cmIP)
	return nil
}
