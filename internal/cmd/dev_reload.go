package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/apoxy-dev/clrk/internal/drivers"
)

// errNoSession is the sentinel returned by readDevSession when the
// dev.json sidecar is absent. Callers that want to distinguish a
// missing session from a parse/permission error use
// errors.Is(err, errNoSession) instead of message-sniffing.
var errNoSession = errors.New("no live clrk dev session")

// devSessionFileName is the JSON sidecar bringUp writes next to dev.lock.
// Holds the effective image refs and run options for the live session so
// a second `clrk dev reload <component>` process can recreate the
// matching docker container with identical settings.
const devSessionFileName = "dev.json"

// devSession persists everything `clrk dev reload <component>` needs
// without re-parsing flags. Kept intentionally small — fields are
// extracted from devOpts at end-of-bringUp and serialized verbatim.
// The Forwards slice is the auto-expose reconciler's mirror; it owns
// rewrites via rewriteForwards while the rest of the session stays
// immutable for the lifetime of the dev process.
//
// ConfigHash + StartedAt are stamped by writeDevSessionStamp the moment
// the current invocation commits to bringing the cluster up. That early
// stamp survives an ungraceful exit so the next `clrk dev` can detect
// drift against the orphaned cluster's last-known config.
type devSession struct {
	ConfigHash       string           `json:"config_hash,omitempty"`
	StartedAt        time.Time        `json:"started_at,omitempty"`
	DataDir          string           `json:"data_dir"`
	Workers          int              `json:"workers"`
	ControllerImage  string           `json:"controller_image"`
	WorkerImage      string           `json:"worker_image"`
	K3sImage         string           `json:"k3s_image,omitempty"`
	Pull             string           `json:"pull"`
	RegistryEnabled  bool             `json:"registry_enabled,omitempty"`
	Watch            bool             `json:"watch"`
	OtelEndpoint     string           `json:"otel_endpoint"`
	RegistryHostPort int              `json:"registry_host_port"`
	Forwards         []ExposedForward `json:"forwards,omitempty"`
}

// ExposedForward is one auto-opened host-port forward managed by the
// `clrk dev` expose reconciler. Mirrored into dev.json on every change
// so `clrk dev status` can introspect without contacting the cluster.
type ExposedForward struct {
	Namespace   string `json:"namespace"`
	Name        string `json:"name"`
	HostPort    int    `json:"host_port"`
	ServicePort int32  `json:"service_port"`
}

// writeDevSession serializes the effective devOpts state to
// <dataDir>/dev.json so `clrk dev reload` can recover it. Called at the
// end of bringUp after o.controllerImage / o.workerImage / o.pull have
// settled (--registry-image overrides applied). Preserves the
// ConfigHash + StartedAt stamped earlier by writeDevSessionStamp.
func writeDevSession(o *devOpts, state *devState) error {
	s := devSession{
		ConfigHash:       o.cfgHash,
		StartedAt:        o.startedAt,
		DataDir:          o.dataDir,
		Workers:          o.workers,
		ControllerImage:  o.controllerImage,
		WorkerImage:      o.workerImage,
		K3sImage:         o.k3sImage,
		Pull:             o.pull,
		RegistryEnabled:  o.registryEnabled(),
		Watch:            o.watch,
		OtelEndpoint:     o.otelEndpoint,
		RegistryHostPort: state.cluster.RegistryHostPort(),
	}
	return atomicWriteDevSession(o.dataDir, s)
}

// writeDevSessionStamp writes a partial dev.json containing only the
// config hash + start time. Called from runDev the moment we commit to
// bringing the cluster up — before any container work — so the marker
// survives an ungraceful exit. A subsequent `clrk dev` invocation reads
// this hash to detect drift against an orphaned cluster.
func writeDevSessionStamp(dataDir, cfgHash string, startedAt time.Time) error {
	return atomicWriteDevSession(dataDir, devSession{
		ConfigHash: cfgHash,
		StartedAt:  startedAt,
		DataDir:    dataDir,
	})
}

// atomicWriteDevSession writes the encoded session via the same
// tmp-file + rename dance used everywhere else in this file. Single
// helper so callers (writeDevSession, writeDevSessionStamp,
// rewriteForwards) can't drift on file permissions or fsync semantics.
func atomicWriteDevSession(dataDir string, s devSession) error {
	path := filepath.Join(dataDir, devSessionFileName)
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

// rewriteForwards atomically rewrites the Forwards slice in dev.json.
// Reads the current session, swaps the slice, writes back via the same
// temp-file + rename dance writeDevSession uses. Safe for the expose
// reconciler to call from any goroutine; missing dev.json (e.g. before
// bringUp finishes) is treated as a no-op rather than an error.
func rewriteForwards(dataDir string, fs []ExposedForward) error {
	path := filepath.Join(dataDir, devSessionFileName)
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var s devSession
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("parsing %s: %w", path, err)
	}
	s.Forwards = fs
	out, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, out, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// readDevSession recovers the session sidecar written by writeDevSession.
// Returns errNoSession (wrapped, so errors.Is matches) when no live
// session is present so callers can distinguish missing-file from
// parse/permission failures.
func readDevSession(dataDir string) (*devSession, error) {
	path := filepath.Join(dataDir, devSessionFileName)
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w at %s — start one with `clrk dev`", errNoSession, dataDir)
		}
		return nil, err
	}
	var s devSession
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	return &s, nil
}

// registryEnabled reports whether the dev session needs the local OCI
// registry. The registry is opt-in — it's only useful for the inner
// dev loop where `clrk dev push-image` lands locally-built images into
// the in-cluster registry, gated by at least one --registry-image=
// flag on launch.
func (o *devOpts) registryEnabled() bool {
	return len(o.registryImages) > 0
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
			o.workerImage = ref
		default:
			return fmt.Errorf("--registry-image=%s: COMPONENT must be worker[-N] or controller-manager", raw)
		}
	}
	o.pull = "always"
	return nil
}

// newDevReloadCmd is `clrk dev reload <component>`. Triggers a rolling
// restart of the named Deployment so the next pod pulls the freshest
// `:dev` tag from the local registry. Pair with a prior
// `docker push localhost:<registry-port>/...` to roll a new image.
func newDevReloadCmd() *cobra.Command {
	var (
		dataDir string
		_       int // workerN — retained for backward-compat flag parsing
	)
	cmd := &cobra.Command{
		Use:   "reload <component>",
		Short: "Roll out the named clrk dev Deployment to pick up a freshly-pushed image",
		Long: "Triggers a `kubectl rollout restart`-equivalent on the matching " +
			"Deployment and blocks until the new pod has actually rolled out " +
			"(observed, available, and Ready). Component is `worker` (default " +
			"WorkerPool's Deployment) or `controller-manager`.",
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
			switch {
			case comp == "controller-manager":
				return reloadControllerManager(cmd.Context(), sess)
			case comp == "worker" || strings.HasPrefix(comp, "worker-"):
				// `worker-N` accepted for backwards compat; rolling a
				// single replica isn't useful with Deployments, so any
				// worker-* form rolls the whole default-workers Deployment.
				return reloadWorker(cmd.Context(), sess)
			default:
				return fmt.Errorf("component must be worker[-N] or controller-manager, got %q", comp)
			}
		},
	}
	cmd.Flags().StringVar(&dataDir, "data-dir", "", "Host path of the running session's data dir (defaults to --clrk-dir).")
	// `--worker-index` is retained as a no-op flag so existing scripts don't
	// break; a Deployment rollout restarts every replica anyway.
	var unused int
	cmd.Flags().IntVar(&unused, "worker-index", 0, "Ignored — `clrk dev reload worker` rolls the whole Deployment.")
	_ = strconv.Atoi
	return cmd
}

// rolloutTimeout bounds how long a reload waits for the new pods to come
// up healthy on the freshly-pushed image. Matches the cm-Available budget
// in bringUp so a reload can't outlive a fresh launch.
const rolloutTimeout = 3 * time.Minute

// deploymentFor maps a `clrk dev` component name to the namespace/name of
// the Deployment that runs it.
func deploymentFor(comp string) (ns, name string, ok bool) {
	switch comp {
	case "controller-manager":
		return devClrkNamespace, cmAccountName, true
	case "worker":
		return "default", "default-workers", true
	}
	return "", "", false
}

// reloadAndWait triggers a rolling restart of comp's Deployment and blocks
// until the rollout converges with the live pods reporting wantDigests
// (pass none to wait for rollout completion only). This is what makes
// `--reload` trustworthy: rather than returning the instant the rollout is
// *triggered*, it returns only once the new pod is actually running — and,
// when a digest is supplied, fails loudly if the node served a cached
// `:dev` tag instead of the image just pushed.
func reloadAndWait(ctx context.Context, sess *devSession, comp string, wantDigests ...string) error {
	ns, name, ok := deploymentFor(comp)
	if !ok {
		return fmt.Errorf("component must be worker or controller-manager, got %q", comp)
	}
	cluster := drivers.NewClusterDriver(sess.DataDir, "", 0)
	slog.Info("Rolling out Deployment", "component", comp, "namespace", ns, "name", name)
	if err := cluster.Rollout(ctx, ns, name); err != nil {
		return fmt.Errorf("rolling %s Deployment: %w", comp, err)
	}
	if err := cluster.WaitRolloutComplete(ctx, ns, name, rolloutTimeout, wantDigests...); err != nil {
		return err
	}
	slog.Info("Rollout complete", "component", comp)
	return nil
}

// reloadWorker rolls the default-workers Deployment and waits for it to
// converge.
func reloadWorker(ctx context.Context, sess *devSession) error {
	return reloadAndWait(ctx, sess, "worker")
}

// reloadControllerManager rolls the cm Deployment and waits for it to
// converge.
func reloadControllerManager(ctx context.Context, sess *devSession) error {
	return reloadAndWait(ctx, sess, "controller-manager")
}
