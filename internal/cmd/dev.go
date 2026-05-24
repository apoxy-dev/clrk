package cmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/apoxy-dev/clrk/internal/cmd/devagents"
	"github.com/apoxy-dev/clrk/internal/cmd/devotel"
	"github.com/apoxy-dev/clrk/internal/cmd/devtui"
	"github.com/apoxy-dev/clrk/internal/drivers"
)

// devOtelPort is the host TCP port the in-process OTLP/HTTP receiver
// listens on. Hardcoded to keep `clrk dev` flag-free; the first time
// it conflicts with another tool we'll expose --otel-port.
const devOtelPort = 14318

// devClrkNamespace is the in-cluster namespace the dev environment
// uses for clrk's bridge Services + the EG control-plane resources
// (envoy-gateway TLS Secret, per-Gateway data-plane Deployments). The
// controller-manager binary is told to use this via POD_NAMESPACE so
// its runtimeNamespace() resolves consistently with the bridge URIs
// it advertises. Same name in dev + prod: `clrk`.
const devClrkNamespace = "clrk"

type devOpts struct {
	watch           bool
	controllerImage string
	workerImage     string
	k3sImage        string
	pull            string
	skipPreflight   bool
	workers         int
	dataDir         string
	tui             bool
	tuiSet          bool
	applyPaths      []string
	applyRecursive  bool
	registryImages  []string
	registryPort    int
	secrets         []string
	parsedSecrets   []secretSpec
	otelEndpoint    string
	forceRecreate   bool
	// gatewayIP is the IP a Pod on drivers.NetworkName uses to reach
	// the host. Resolved once via drivers.HostGatewayIP and injected as
	// the host.docker.internal HostAlias on the cm PodSpec so OTLP
	// records reach the devtui receiver bound on the host.
	gatewayIP string
	// cfgHash + startedAt are computed once in runDev and stamped into
	// dev.json by writeDevSessionStamp so the next `clrk dev` invocation
	// can detect drift against an orphaned cluster (see runDev's drift
	// gate).
	cfgHash   string
	startedAt time.Time
}

func newDevCmd() *cobra.Command {
	o := &devOpts{}
	cmd := &cobra.Command{
		Use:   "dev",
		Short: "Run controller-manager and worker locally in Docker",
		Long: "Starts a controller-manager container with an embedded apiserver " +
			"and N worker containers on a shared docker network.",
		RunE: func(cmd *cobra.Command, args []string) error {
			o.tuiSet = cmd.Flags().Changed("tui")
			return runDev(cmd.Context(), o)
		},
	}

	cmd.Flags().BoolVar(&o.watch, "watch", false, "Rebuild and hot-reload binaries on source changes (experimental).")
	cmd.Flags().StringVar(&o.controllerImage, "controller-image", drivers.DefaultControllerManagerImage, "Controller-manager image ref.")
	cmd.Flags().StringVar(&o.workerImage, "worker-image", drivers.DefaultWorkerImage, "Worker image ref.")
	cmd.Flags().StringVar(&o.k3sImage, "k3s-image", drivers.DefaultK3sImage, "k3s image ref.")
	cmd.Flags().StringVar(&o.pull, "pull", "", "Forwarded to 'docker run --pull' for every clrk-managed container. Accepted: \"always\", \"missing\" (docker default), \"never\". Use \"always\" to force a re-pull when a SHA-tagged image was retagged out-of-band.")
	cmd.Flags().BoolVar(&o.skipPreflight, "skip-preflight", false, "Skip the host-readiness checks (docker daemon, /dev/net/tun, IPv6, image-pullable). Use only when the checks are wrong about your environment.")
	cmd.Flags().IntVar(&o.workers, "workers", 1, "Number of worker replicas.")
	cmd.Flags().StringVar(&o.dataDir, "data-dir", "", "Host path for ~/.clrk state (defaults to --clrk-dir).")
	cmd.Flags().BoolVar(&o.tui, "tui", true, "Render the dev TUI (auto-disabled when stdout isn't a TTY).")
	cmd.Flags().StringArrayVarP(&o.applyPaths, "apply", "f", nil, "YAML file or directory of CRDs to server-side apply once the apiserver is ready (repeatable).")
	cmd.Flags().BoolVarP(&o.applyRecursive, "recursive", "R", false, "Recurse into subdirectories when --apply targets a directory.")
	cmd.Flags().StringArrayVar(&o.registryImages, "registry-image", nil, "Override an image to a local-registry ref and force '--pull always' on the matching container (repeatable). Format: COMPONENT=REF where COMPONENT is 'worker[-N]' or 'controller-manager'. Example: --registry-image=worker=clrk-registry:5000/clrk/worker:dev — pair with 'clrk dev reload <component>' after pushing to the local registry.")
	cmd.Flags().IntVar(&o.registryPort, "registry-port", 0, "Host port to publish the local OCI registry on. 0 picks a free port; the actual port is logged at startup and is the target for 'docker push localhost:<port>/clrk/...'.")
	cmd.Flags().StringArrayVar(&o.secrets, "secret", nil, "Materialize an Opaque Secret from the host env before --apply runs (repeatable). Format: NAME=ENVVAR[:KEY]. KEY defaults to ENVVAR lowercased with '_' → '-' (e.g. ANTHROPIC_API_KEY → anthropic-api-key). Multiple --secret flags sharing a NAME merge into one Secret with multiple keys.")
	cmd.Flags().BoolVar(&o.forceRecreate, "force-recreate", false, "Tear down any orphaned dev cluster (and the clrk docker network) before starting, instead of attaching. Useful when a prior clrk dev died ungracefully and left poisoned in-cluster state.")
	cmd.AddCommand(newDevStatusCmd())
	cmd.AddCommand(newDevLogsCmd())
	cmd.AddCommand(newDevWaitReadyCmd())
	cmd.AddCommand(newDevReloadCmd())
	cmd.AddCommand(newDevPushImageCmd())
	return cmd
}

func runDev(ctx context.Context, o *devOpts) error {
	if o.dataDir == "" {
		o.dataDir = clrkDir
	}
	if err := os.MkdirAll(o.dataDir, 0o755); err != nil {
		return fmt.Errorf("creating data dir: %w", err)
	}

	// Validate --secret early so a typo or missing env var fails before
	// we pull images and start k3s. Resolved values stay on devOpts for
	// bringUp to apply once the cluster is reachable.
	for _, raw := range o.secrets {
		s, err := parseDevSecretFlag(raw)
		if err != nil {
			return err
		}
		o.parsedSecrets = append(o.parsedSecrets, s)
	}

	// Refuse to start a second `clrk dev` against the same dataDir.
	// Without this guard the second invocation `docker rm -f`s the first
	// instance's containers in RemoveIfExists during bringUp, silently
	// destroying the first session's state.
	lock, err := acquireDevLock(o.dataDir)
	if err != nil {
		var busy *devLockBusy
		if errors.As(err, &busy) {
			fmt.Fprintf(os.Stderr,
				"clrk dev is already running (pid %d) under %s.\n"+
					"  • inspect: clrk dev status\n"+
					"  • follow logs: clrk dev logs -f\n"+
					"  • stop and replace: kill %d && clrk dev …\n",
				busy.OwnerPID, o.dataDir, busy.OwnerPID)
			return err
		}
		return err
	}
	defer lock.Release()

	// Auto-disable TUI on non-TTY stdout (CI, piped output) unless the user
	// explicitly set --tui=true.
	if !o.tuiSet && !term.IsTerminal(int(os.Stdout.Fd())) {
		o.tui = false
	}

	// `--registry-image=<component>=<ref>` swaps the matching image
	// ref to the local-registry tag and forces `--pull always`, so
	// `docker push` to the local registry on the host re-rolls the
	// container on the next `clrk dev reload`. Must happen before
	// bringUp consumes o.controllerImage / o.workerImage / o.pull.
	if err := applyRegistryImageOverrides(o); err != nil {
		return err
	}

	// Pre-flight checks run after image-ref resolution so
	// checkImagePullable looks at the actual refs bringUp will use.
	// On failure we want users to see the report and stop — bringUp
	// would deliver the same diagnoses as opaque docker errors a
	// minute later.
	if !o.skipPreflight {
		if err := preflight(ctx, o); err != nil {
			return err
		}
	}

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Compute the config hash now (after applyRegistryImageOverrides so
	// registry mode counts) and use it both for the drift gate below and
	// for the on-disk session marker writeDevSessionStamp leaves
	// immediately afterward. Stamping early — before any container work
	// — is the whole point: a session that dies mid-bringUp still leaves
	// a hash for the next session to compare against.
	o.cfgHash = computeConfigHash(o)
	o.startedAt = time.Now().UTC()

	ds, err := inspectDrift(ctx, o)
	if err != nil {
		return err
	}
	proceed, err := resolveDrift(ctx, o, ds)
	if err != nil {
		return err
	}
	if !proceed {
		return fmt.Errorf("clrk dev aborted: cluster config drift not resolved")
	}
	if err := writeDevSessionStamp(o.dataDir, o.cfgHash, o.startedAt); err != nil {
		slog.Warn("Failed to stamp dev session marker", "err", err)
	}

	// Bring up the docker network and the in-process OTLP receiver
	// before any container starts so controller-manager can dial
	// `host.docker.internal:14318` on its first tick. Receiver
	// lifetime is tied to ctx — when the user Ctrl-C's, the listener
	// closes via the goroutine inside devotel.Start.
	if err := drivers.EnsureNetwork(ctx); err != nil {
		return fmt.Errorf("ensuring docker network: %w", err)
	}
	gw, err := drivers.HostGatewayIP(ctx, drivers.NetworkName)
	if err != nil {
		return fmt.Errorf("resolving docker host-gateway: %w", err)
	}
	o.gatewayIP = gw
	receiver, err := devotel.Start(ctx, fmt.Sprintf("0.0.0.0:%d", devOtelPort))
	if err != nil {
		return fmt.Errorf("starting OTel receiver on :%d: %w", devOtelPort, err)
	}
	o.otelEndpoint = fmt.Sprintf("http://host.docker.internal:%d", devOtelPort)

	if o.tui {
		return runDevTUI(ctx, o, receiver)
	}
	return runDevPlain(ctx, o, receiver)
}

// runDevPlain is the original streaming-stdout path, kept for CI / non-TTY.
func runDevPlain(ctx context.Context, o *devOpts, receiver *devotel.Receiver) error {
	slog.Info("Starting clrk dev", "data_dir", o.dataDir, "workers", o.workers)
	state, err := bringUp(ctx, o, nil)
	if err != nil {
		state.teardown()
		return err
	}
	defer state.teardown()

	// k3d server is the only docker container we still tail directly;
	// cm + worker pods are streamed via the apiserver.
	streamDockerLogs(ctx, drivers.ClusterServerContainerName)
	streamPodLogsPlain(ctx, state.cluster.HostKubeconfigPath(),
		devClrkNamespace, podSelectorControllerManager, 0, controllerManagerComponent)
	for i := 0; i < o.workers; i++ {
		streamPodLogsPlain(ctx, state.cluster.HostKubeconfigPath(),
			"default", podSelectorWorkers, i, workerComponent(i))
	}
	go startExposeReconciler(ctx, state.cluster.HostKubeconfigPath(), o.dataDir, nil)

	go forwardOtel(ctx, receiver, nil, func(name, line string) {
		fmt.Fprintf(os.Stdout, "[%s] %s\n", name, line)
	})

	if o.watch {
		slog.Warn("--watch is temporarily disabled while we wire pod-based reloads; rebuild and push to the local registry, then `clrk dev reload <component>`.")
	}

	<-ctx.Done()
	slog.Info("Shutting down")
	return nil
}

// forwardOtel pumps decoded OTLP records into sink, tagged with the
// pane / prefix name. Caller picks the sink (TUI or stdout); the
// select loop is identical regardless. When store is non-nil, every
// record is also folded into the per-agent rolling stats so the
// agents screen has data to render.
func forwardOtel(ctx context.Context, receiver *devotel.Receiver, store *devagents.Store, sink func(name, line string)) {
	for {
		select {
		case <-ctx.Done():
			return
		case rec := <-receiver.Logs():
			if store != nil {
				store.AddLog(rec)
			}
			sink(devtui.OtelLogsSource, devotel.FormatLog(rec))
		case sp := <-receiver.Traces():
			if store != nil {
				store.AddSpan(sp)
			}
			sink(devtui.OtelTracesSource, devotel.FormatSpan(sp))
		}
	}
}

// runDevTUI orchestrates components in a background goroutine while the TUI
// renders status and per-component logs on the main goroutine.
func runDevTUI(ctx context.Context, o *devOpts, receiver *devotel.Receiver) error {
	componentNames := []string{
		drivers.ClusterServerContainerName,
		controllerManagerComponent,
	}
	for i := 0; i < o.workers; i++ {
		componentNames = append(componentNames, workerComponent(i))
	}
	componentNames = append(componentNames, devtui.OtelLogsSource, devtui.OtelTracesSource)

	store := devagents.New()
	prog := devtui.New(componentNames, store)
	devtui.MarkSyntheticReady(prog, devtui.OtelLogsSource)
	devtui.MarkSyntheticReady(prog, devtui.OtelTracesSource)

	prevSlog := slog.Default()
	slog.SetDefault(slog.New(devtui.NewSlogHandler(prog, slog.LevelInfo)))
	defer slog.SetDefault(prevSlog)

	orchestrateCtx, orchestrateCancel := context.WithCancel(ctx)
	defer orchestrateCancel()

	stateCh := make(chan *devState, 1)
	orchErrCh := make(chan error, 1)
	go func() {
		state, err := bringUp(orchestrateCtx, o, prog)
		stateCh <- state
		if err != nil {
			orchErrCh <- err
			return
		}
		// Docker logs for the k3d server; pod logs for cm + workers.
		go streamDockerLogsTo(orchestrateCtx, prog, drivers.ClusterServerContainerName)
		kubeconfig := state.cluster.HostKubeconfigPath()
		go streamPodLogsTUI(orchestrateCtx, prog, kubeconfig,
			devClrkNamespace, podSelectorControllerManager, 0, controllerManagerComponent)
		for i := 0; i < o.workers; i++ {
			i := i
			go streamPodLogsTUI(orchestrateCtx, prog, kubeconfig,
				"default", podSelectorWorkers, i, workerComponent(i))
		}
		go startExposeReconciler(orchestrateCtx, kubeconfig, o.dataDir, prog)
		go forwardOtel(orchestrateCtx, receiver, store, func(name, line string) {
			prog.SendLog(name, line, devtui.StreamStdout)
		})
		// Begin watching TaskAgent + DaemonAgent now that the apiserver
		// is reachable. Failures back off internally; we don't surface
		// them to the user — the agents pane simply stays empty until
		// the watcher reconnects.
		go func() {
			if err := store.Run(orchestrateCtx, state.cluster.HostKubeconfigPath()); err != nil && !errors.Is(err, context.Canceled) {
				slog.Warn("Agent watcher exited", "err", err)
			}
		}()
		if o.watch {
			slog.Warn("--watch is temporarily disabled while we wire pod-based reloads; rebuild and push to the local registry, then `clrk dev reload <component>`.")
		}
		// Block until shutdown signaled; orchErrCh stays nil for clean exit.
		<-orchestrateCtx.Done()
		orchErrCh <- nil
	}()

	runErr := prog.Run(ctx)

	orchestrateCancel()
	state := <-stateCh
	state.teardown()
	orchErr := <-orchErrCh

	// Restore stderr-bound slog before printing the summary so it lands in the
	// terminal scrollback rather than the now-torn-down TUI.
	slog.SetDefault(prevSlog)
	if orchErr != nil && !errors.Is(orchErr, context.Canceled) {
		return orchErr
	}
	return runErr
}

// devState bundles everything bringUp produced so the caller can tear down
// regardless of which step failed.
type devState struct {
	cluster *drivers.ClusterDriver
}

func (s *devState) teardown() {
	if s == nil {
		return
	}
	shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if s.cluster != nil {
		_ = s.cluster.Stop(shutCtx)
	}
}

// Component name constants used by the TUI, log streamers, and the
// `clrk dev reload` subcommand. The k3d server keeps its docker
// container name because it's still a docker container; cm + worker
// are symbolic component names mapping to in-cluster pods.
const (
	controllerManagerComponent = "controller-manager"

	// podSelectorControllerManager and podSelectorWorkers are the label
	// selectors the cm bootstrap (this file's bootstrapControllerManager)
	// and the WorkerPoolDeploymentReconciler stamp on their pods. Used
	// by the log streamers + status command to locate live pods.
	podSelectorControllerManager = "app.kubernetes.io/name=clrk-controller-manager"
	podSelectorWorkers           = "clrk.apoxy.dev/workerpool=default"
)

// workerComponent returns the canonical component name for worker
// replica i (0-based). Used as a TUI sidebar entry and as the
// argument to `clrk dev reload worker -i N`.
func workerComponent(i int) string {
	return fmt.Sprintf("worker-%d", i)
}

// bringUp drives the linear startup sequence: cluster → namespace →
// controller-manager Deployment → APIService discoverable → default
// WorkerPool (reconciler creates worker Deployment) → secrets/manifests.
// When prog is non-nil, each step also emits a ComponentStatusMsg so
// the TUI can flip the matching glyph.
func bringUp(ctx context.Context, o *devOpts, prog *devtui.Program) (*devState, error) {
	state := &devState{}

	slog.Info("Starting clrk dev", "data_dir", o.dataDir, "workers", o.workers)

	state.cluster = drivers.NewClusterDriver(o.dataDir, o.k3sImage, o.registryPort)
	state.cluster.EnableRegistry = o.registryEnabled()
	if err := withStatus(prog, drivers.ClusterServerContainerName, func() error {
		if err := state.cluster.Start(ctx); err != nil {
			return fmt.Errorf("starting cluster: %w", err)
		}
		if state.cluster.EnableRegistry {
			slog.Info("k3d cluster + registry running",
				"node", drivers.ClusterServerContainerName,
				"registry_port", state.cluster.RegistryHostPort())
		} else {
			slog.Info("k3d cluster running",
				"node", drivers.ClusterServerContainerName)
		}
		return nil
	}); err != nil {
		return state, err
	}
	slog.Info("Cluster API is ready", "kubeconfig", state.cluster.KubeconfigPath())

	// Promote the dev cluster to the current context so subsequent
	// `clrk apply` / `clrk agents` invocations from any shell target
	// the live dev cluster without --local. Best-effort.
	if err := writeDevContext(state.cluster.HostKubeconfigPath()); err != nil {
		slog.Warn("Failed to register dev context in ~/.clrk/config", "err", err)
	}

	if err := state.cluster.EnsureNamespace(ctx, devClrkNamespace); err != nil {
		return state, fmt.Errorf("ensuring %q namespace: %w", devClrkNamespace, err)
	}

	if err := withStatus(prog, controllerManagerComponent, func() error {
		if err := bootstrapControllerManager(ctx, state.cluster, o.controllerImage, k8sPullPolicy(o.pull), o.otelEndpoint, o.gatewayIP); err != nil {
			return fmt.Errorf("bootstrapping controller-manager: %w", err)
		}
		slog.Info("Controller-manager Deployment applied; waiting Available")
		if err := state.cluster.WaitDeploymentAvailable(ctx, devClrkNamespace, cmAccountName, 3*time.Minute); err != nil {
			return fmt.Errorf("controller-manager never became available: %w", err)
		}
		return nil
	}); err != nil {
		return state, err
	}

	if err := waitClrkAPIDiscoverable(ctx, state.cluster.HostKubeconfigPath(), 60*time.Second); err != nil {
		return state, fmt.Errorf("waiting for clrk APIService aggregation: %w", err)
	}

	if err := bootstrapDefaultWorkerPool(ctx, state.cluster, o.workerImage, int32(o.workers), k8sPullPolicy(o.pull)); err != nil {
		return state, fmt.Errorf("bootstrapping default WorkerPool: %w", err)
	}
	// Wait for the worker Deployment (created by WorkerPoolDeploymentReconciler)
	// to become Available. Each replica is surfaced separately in the TUI
	// for visibility, but we wait once on the Deployment as a whole.
	for i := 0; i < o.workers; i++ {
		setStatus(prog, workerComponent(i), devtui.StatusStarting)
	}
	if err := state.cluster.WaitDeploymentAvailable(ctx, "default", "default-workers", 3*time.Minute); err != nil {
		for i := 0; i < o.workers; i++ {
			setStatus(prog, workerComponent(i), devtui.StatusError)
		}
		return state, fmt.Errorf("default workers never became available: %w", err)
	}
	for i := 0; i < o.workers; i++ {
		setStatus(prog, workerComponent(i), devtui.StatusReady)
	}

	if len(o.parsedSecrets) > 0 {
		if err := applySecretSpecs(ctx, state.cluster.HostKubeconfigPath(), o.parsedSecrets, "default"); err != nil {
			return state, fmt.Errorf("applying --secret: %w", err)
		}
	}
	if len(o.applyPaths) > 0 {
		if err := applyManifests(ctx, state.cluster.HostKubeconfigPath(), o.applyPaths, o.applyRecursive); err != nil {
			return state, fmt.Errorf("applying manifests: %w", err)
		}
	}

	if err := writeDevSession(o, state); err != nil {
		slog.Warn("Failed to write dev.json (reload subcommand will be unavailable)", "err", err)
	}

	return state, nil
}

// k8sPullPolicy maps the docker --pull value (always|missing|never) to
// the matching corev1.PullPolicy (Always|IfNotPresent|Never).
func k8sPullPolicy(pull string) string {
	switch pull {
	case "always":
		return "Always"
	case "never":
		return "Never"
	default:
		return "IfNotPresent"
	}
}

// withStatus wraps a starting → ready/error transition around a step's
// execution so the TUI sidebar flips its glyph in lockstep with the
// driver call.
func withStatus(prog *devtui.Program, name string, step func() error) error {
	setStatus(prog, name, devtui.StatusStarting)
	if err := step(); err != nil {
		setStatus(prog, name, devtui.StatusError)
		return err
	}
	setStatus(prog, name, devtui.StatusReady)
	return nil
}

func setStatus(prog *devtui.Program, name string, s devtui.Status) {
	if prog == nil {
		return
	}
	prog.SetStatus(name, s)
}

// waitClrkAPIDiscoverable polls k3s discovery until clrk.apoxy.dev/v1alpha1
// shows up. The aggregated APIService is registered as soon as the
// controller-manager container is up, but kube-aggregator still has to
// probe the backend's TLS and mark the service Available — until then
// REST mappings for clrk kinds resolve to "no matches".
func waitClrkAPIDiscoverable(ctx context.Context, kubeconfig string, timeout time.Duration) error {
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return fmt.Errorf("loading kubeconfig %s: %w", kubeconfig, err)
	}
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return fmt.Errorf("discovery client: %w", err)
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		groups, err := dc.ServerGroups()
		if err == nil {
			for _, g := range groups.Groups {
				if g.Name == "clrk.apoxy.dev" {
					for _, v := range g.Versions {
						if v.Version == "v1alpha1" {
							return nil
						}
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return errors.New("timed out waiting for clrk.apoxy.dev/v1alpha1 in discovery")
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// findRepoRoot walks up from cwd until it finds a directory containing
// go.mod, returning its absolute path.
func findRepoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("go.mod not found; run clrk from inside the repo when using --watch")
		}
		dir = parent
	}
}

// streamDockerLogs spawns `docker logs -f <container>` and pipes its
// output into the current process stdio. Used by the non-TUI fallback
// for the k3d server (still a docker container).
func streamDockerLogs(ctx context.Context, container string) {
	cmd := exec.CommandContext(ctx, "docker", "logs", "-f", container)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	_ = cmd.Start()
}

// streamDockerLogsTo runs `docker logs -f` and forwards each line into
// the TUI as a LogLineMsg under the container's name. Stdout and stderr
// are demuxed so the TUI can color stderr distinctly.
func streamDockerLogsTo(ctx context.Context, prog *devtui.Program, container string) {
	cmd := exec.CommandContext(ctx, "docker", "logs", "-f", container)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return
	}
	if err := cmd.Start(); err != nil {
		return
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go pipeLines(&wg, stdout, prog, container, devtui.StreamStdout)
	go pipeLines(&wg, stderr, prog, container, devtui.StreamStderr)
	wg.Wait()
	_ = cmd.Wait()
}

func pipeLines(wg *sync.WaitGroup, r io.Reader, prog *devtui.Program, source string, stream devtui.LogStream) {
	defer wg.Done()
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		prog.SendLog(source, scanner.Text(), stream)
	}
}
