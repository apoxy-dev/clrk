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
	"golang.org/x/sync/errgroup"
	"golang.org/x/term"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/apoxy-dev/clrk/internal/cmd/devagents"
	"github.com/apoxy-dev/clrk/internal/cmd/devotel"
	"github.com/apoxy-dev/clrk/internal/cmd/devtui"
	"github.com/apoxy-dev/clrk/internal/drivers"
	"github.com/apoxy-dev/clrk/internal/drivers/dockerutils"
	"github.com/apoxy-dev/clrk/internal/ports"
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
	watch              bool
	controllerImage    string
	controllerImageSet bool
	workerImage        string
	workerImageSet     bool
	k3sImage           string
	pull               string
	skipPreflight      bool
	workers            int
	dataDir            string
	tui                bool
	tuiSet             bool
	applyPaths         []string
	applyRecursive     bool
	reloadTar          []string
	secrets            []string
	parsedSecrets      []secretSpec
	otelEndpoint       string
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
			o.controllerImageSet = cmd.Flags().Changed("controller-image")
			o.workerImageSet = cmd.Flags().Changed("worker-image")
			return runDev(cmd.Context(), o)
		},
	}

	cmd.Flags().BoolVar(&o.watch, "watch", false, "Rebuild and hot-reload binaries on source changes (experimental).")
	cmd.Flags().StringVar(&o.controllerImage, "controller-image", drivers.DefaultControllerManagerImage, "Controller-manager image ref.")
	cmd.Flags().StringVar(&o.workerImage, "worker-image", drivers.DefaultWorkerImage, "Worker image ref.")
	cmd.Flags().StringVar(&o.k3sImage, "k3s-image", drivers.DefaultK3sImage, "k3s image ref.")
	cmd.Flags().StringVar(&o.pull, "pull", "", "Forwarded to `docker run --pull` for every clrk-managed container. Accepted: \"always\", \"missing\" (docker default), \"never\". Use \"always\" to force a re-pull when a SHA-tagged image was retagged out-of-band.")
	cmd.Flags().BoolVar(&o.skipPreflight, "skip-preflight", false, "Skip the host-readiness checks (docker daemon, /dev/net/tun, IPv6, image-pullable). Use only when the checks are wrong about your environment.")
	cmd.Flags().IntVar(&o.workers, "workers", 1, "Number of worker replicas.")
	cmd.Flags().StringVar(&o.dataDir, "data-dir", "", "Host path for ~/.clrk state (defaults to --clrk-dir).")
	cmd.Flags().BoolVar(&o.tui, "tui", true, "Render the dev TUI (auto-disabled when stdout isn't a TTY).")
	cmd.Flags().StringArrayVarP(&o.applyPaths, "apply", "f", nil, "YAML file or directory of CRDs to server-side apply once the apiserver is ready (repeatable).")
	cmd.Flags().BoolVarP(&o.applyRecursive, "recursive", "R", false, "Recurse into subdirectories when --apply targets a directory.")
	cmd.Flags().StringArrayVar(&o.reloadTar, "reload-tar", nil, "Watch a bazel-built OCI tarball and reload the matching component on every mtime change (repeatable). Format: COMPONENT=PATH where COMPONENT is `worker[-N]` or `controller-manager`. Example: --reload-tar=worker=bazel-bin/clrk/worker_oci_tarball/tarball.tar")
	cmd.Flags().StringArrayVar(&o.secrets, "secret", nil, "Materialize an Opaque Secret from the host env before --apply runs (repeatable). Format: NAME=ENVVAR[:KEY]. KEY defaults to ENVVAR lowercased with `_` → `-` (e.g. ANTHROPIC_API_KEY → anthropic-api-key). Multiple --secret flags sharing a NAME merge into one Secret with multiple keys.")
	cmd.AddCommand(newDevStatusCmd())
	cmd.AddCommand(newDevLogsCmd())
	cmd.AddCommand(newDevWaitReadyCmd())
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

	// `--reload-tar=<component>=…` without an explicit image override
	// flips the corresponding image ref to the bazel-stamped local tag
	// (`clrk/{controller-manager,worker}:latest`) so the loaded tarball
	// is what the container actually runs. This must happen before
	// bringUp consumes o.controllerImage / o.workerImage.
	if err := applyReloadTarImageOverrides(o); err != nil {
		return err
	}

	// Seed each --reload-tar tarball into the docker daemon before
	// bringUp tries to `docker run` against the local-only ref. The
	// mtime watcher only handles subsequent reloads — without this
	// the first container start fails with "pull access denied" on
	// the bazel-stamped tag.
	if err := seedReloadTarImages(ctx, o); err != nil {
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

	// Bring up the docker network and the in-process OTLP receiver
	// before any container starts so controller-manager can dial
	// `host.docker.internal:14318` on its first tick. Receiver
	// lifetime is tied to ctx — when the user Ctrl-C's, the listener
	// closes via the goroutine inside devotel.Start.
	if err := drivers.EnsureNetwork(ctx); err != nil {
		return fmt.Errorf("ensuring docker network: %w", err)
	}
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

	streamLogs(ctx, drivers.K3sContainerName)
	streamLogs(ctx, drivers.ControllerManagerContainerName)
	for _, w := range state.workers {
		streamLogs(ctx, w.Name())
	}

	go forwardOtel(ctx, receiver, nil, func(name, line string) {
		fmt.Fprintf(os.Stdout, "[%s] %s\n", name, line)
	})

	if o.watch {
		state.startWatcher(ctx, o, nil)
	}

	if err := startReloadWatchers(ctx, o); err != nil {
		return err
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

// startReloadWatchers parses --reload-tar flags and spawns one watcher
// goroutine per spec. Returns the parse error early so a typo halts
// startup instead of being lost in the log stream.
func startReloadWatchers(ctx context.Context, o *devOpts) error {
	specs, err := parseReloadTar(o.reloadTar)
	if err != nil {
		return err
	}
	for _, s := range specs {
		slog.Info("Watching tarball for reload", "component", s.Component, "index", s.Index, "path", s.Path)
		go runReloadWatcher(ctx, o, s)
	}
	return nil
}

// seedReloadTarImages loads each --reload-tar tarball into the local
// docker daemon. Required before bringUp because the mtime watcher
// only fires on subsequent changes — the initial `docker run` against
// the bazel-stamped local tag would otherwise pull-and-fail.
func seedReloadTarImages(ctx context.Context, o *devOpts) error {
	specs, err := parseReloadTar(o.reloadTar)
	if err != nil {
		return err
	}
	for _, s := range specs {
		if err := dockerLoad(ctx, s.Path); err != nil {
			return fmt.Errorf("seeding %s tarball: %w", s.Component, err)
		}
	}
	return nil
}

// applyReloadTarImageOverrides flips the controller-manager / worker
// image refs to the bazel-stamped local tag when the user supplied
// `--reload-tar=<component>=…` without an explicit
// `--controller-image` / `--worker-image`. Without this, the GAR
// default would shadow the freshly-loaded local tarball and the
// container would either pull the published image (defeating the
// dev workflow) or fail with `manifest unknown` if the matching tag
// hasn't been pushed for the running clrk SHA.
func applyReloadTarImageOverrides(o *devOpts) error {
	specs, err := parseReloadTar(o.reloadTar)
	if err != nil {
		return err
	}
	for _, s := range specs {
		switch s.Component {
		case "controller-manager":
			if !o.controllerImageSet {
				o.controllerImage = drivers.BazelLocalControllerManagerTag
			}
		case "worker":
			if !o.workerImageSet {
				o.workerImage = drivers.BazelLocalWorkerTag
			}
		}
	}
	return nil
}

// runDevTUI orchestrates components in a background goroutine while the TUI
// renders status and per-component logs on the main goroutine.
func runDevTUI(ctx context.Context, o *devOpts, receiver *devotel.Receiver) error {
	componentNames := []string{
		drivers.K3sContainerName,
		drivers.ControllerManagerContainerName,
	}
	for i := 0; i < o.workers; i++ {
		componentNames = append(componentNames, drivers.NewWorkerDriver(i).Name())
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
		for _, name := range []string{drivers.K3sContainerName, drivers.ControllerManagerContainerName} {
			go streamLogsTo(orchestrateCtx, prog, name)
		}
		for _, w := range state.workers {
			go streamLogsTo(orchestrateCtx, prog, w.Name())
		}
		go forwardOtel(orchestrateCtx, receiver, store, func(name, line string) {
			prog.SendLog(name, line, devtui.StreamStdout)
		})
		// Begin watching TaskAgent + DaemonAgent now that the apiserver
		// is reachable. Failures back off internally; we don't surface
		// them to the user — the agents pane simply stays empty until
		// the watcher reconnects.
		go func() {
			if err := store.Run(orchestrateCtx, state.k3s.HostKubeconfigPath()); err != nil && !errors.Is(err, context.Canceled) {
				slog.Warn("Agent watcher exited", "err", err)
			}
		}()
		if o.watch {
			state.startWatcher(orchestrateCtx, o, prog)
		}
		if err := startReloadWatchers(orchestrateCtx, o); err != nil {
			orchErrCh <- err
			return
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
	k3s     *drivers.K3sDriver
	cm      *drivers.ControllerManagerDriver
	workers []*drivers.WorkerDriver
}

func (s *devState) teardown() {
	if s == nil {
		return
	}
	shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, w := range s.workers {
		_ = w.Stop(shutCtx)
	}
	if s.cm != nil {
		_ = s.cm.Stop(shutCtx)
	}
	if s.k3s != nil {
		_ = s.k3s.Stop(shutCtx)
	}
}

// bringUp drives the linear startup sequence (k3s → gateway-api → cm →
// APIService → /readyz → workers). When prog is non-nil, each step also
// emits a ComponentStatusMsg so the TUI can flip the matching glyph.
func bringUp(ctx context.Context, o *devOpts, prog *devtui.Program) (*devState, error) {
	state := &devState{}

	slog.Info("Starting clrk dev", "data_dir", o.dataDir, "workers", o.workers)

	state.k3s = drivers.NewK3sDriver(o.dataDir)
	// Publish the apiserver port on the host so bazel/kubectl/int-tests
	// running outside docker can reach k3s via 127.0.0.1:<HostPort>. The
	// controller-manager still talks to k3s over the docker-DNS name
	// clrk-k3s via its own in-container kubeconfig. Non-standard port
	// avoids collision with other local clusters (kind, k3d, minikube).
	state.k3s.HostPort = 16443
	if err := withStatus(prog, drivers.K3sContainerName, func() error {
		if _, err := state.k3s.Start(ctx, drivers.WithImage(o.k3sImage), drivers.WithPull(o.pull)); err != nil {
			return fmt.Errorf("starting k3s: %w", err)
		}
		slog.Info("k3s running", "container", drivers.K3sContainerName)
		if err := state.k3s.WaitAPIReady(ctx, 120*time.Second); err != nil {
			return fmt.Errorf("k3s API never became ready: %w", err)
		}
		return nil
	}); err != nil {
		return state, err
	}
	slog.Info("k3s API is ready", "kubeconfig", state.k3s.KubeconfigPath())

	// Promote the dev cluster to the current context so subsequent
	// `clrk apply` / `clrk agents` invocations from any shell target
	// the live dev cluster without --local. Best-effort — the user
	// can still pass --local or --kubeconfig if this fails.
	if err := writeDevContext(state.k3s.HostKubeconfigPath()); err != nil {
		slog.Warn("Failed to register dev context in ~/.clrk/config", "err", err)
	}

	// Materialize the controller-manager's runtime namespace before the
	// container starts — the supervised envoy-gateway certgen writes
	// its TLS Secret to clrk/envoy-gateway as its very first action
	// and crash-loops if the namespace doesn't exist.
	if err := state.k3s.EnsureNamespace(ctx, devClrkNamespace); err != nil {
		return state, fmt.Errorf("ensuring %q namespace: %w", devClrkNamespace, err)
	}

	state.cm = drivers.NewControllerManagerDriver()
	cmOpts, err := controllerManagerOpts(o)
	if err != nil {
		return state, err
	}

	// cm and workers both depend only on k3s (which is up). Bring them
	// up in parallel — workers spend the cm /readyz wait pulling their
	// own image instead of sitting idle.
	state.workers = make([]*drivers.WorkerDriver, o.workers)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return withStatus(prog, drivers.ControllerManagerContainerName, func() error {
			if _, err := state.cm.Start(gctx, cmOpts...); err != nil {
				return err
			}
			slog.Info("Controller-manager running", "container", drivers.ControllerManagerContainerName)

			cmIP, err := dockerutils.IPOnNetwork(gctx, drivers.ControllerManagerContainerName, drivers.NetworkName)
			if err != nil {
				return fmt.Errorf("getting controller-manager IP: %w", err)
			}
			if err := bootstrapClrkAPIService(gctx, state.k3s, cmIP, 8443); err != nil {
				return fmt.Errorf("registering clrk APIService: %w", err)
			}
			if err := state.k3s.ApplyControllerManagerBridge(gctx, devClrkNamespace, cmIP, 9443, 9444, 8082, 18000); err != nil {
				return fmt.Errorf("bridging controller-manager: %w", err)
			}
			slog.Info("Controller-manager registered + bridged", "backend", cmIP)

			if err := waitReadyzInContainer(gctx, drivers.ControllerManagerContainerName, "https://localhost:8443/readyz", 90*time.Second); err != nil {
				return fmt.Errorf("controller-manager never became ready: %w", err)
			}
			return nil
		})
	})
	for i := 0; i < o.workers; i++ {
		i := i
		w := drivers.NewWorkerDriver(i)
		state.workers[i] = w
		wOpts, err := workerOpts(o, w)
		if err != nil {
			return state, err
		}
		g.Go(func() error {
			return withStatus(prog, w.Name(), func() error {
				if _, err := w.Start(gctx, wOpts...); err != nil {
					return fmt.Errorf("starting worker %d: %w", i, err)
				}
				slog.Info("Worker running", "container", w.Name())
				return nil
			})
		})
	}
	if err := g.Wait(); err != nil {
		return state, err
	}

	if err := waitClrkAPIDiscoverable(ctx, state.k3s.HostKubeconfigPath(), 60*time.Second); err != nil {
		return state, fmt.Errorf("waiting for clrk APIService aggregation: %w", err)
	}
	if err := bootstrapDefaultWorkerPool(ctx, state.k3s.HostKubeconfigPath()); err != nil {
		return state, fmt.Errorf("bootstrapping default WorkerPool: %w", err)
	}
	// Bridge the default WorkerPool's dispatch port into k3s so
	// in-cluster pods (e.g. integration tests, future cron-fired
	// invokers) can dial workers by Service DNS the same way they
	// would in cluster mode. WorkerPoolDeploymentReconciler creates
	// this Service in cluster mode but isn't wired in clrk dev.
	workerIPs := make([]string, 0, len(state.workers))
	for _, w := range state.workers {
		ip, err := dockerutils.IPOnNetwork(ctx, w.Name(), drivers.NetworkName)
		if err != nil {
			return state, fmt.Errorf("getting worker IP for %s: %w", w.Name(), err)
		}
		workerIPs = append(workerIPs, ip)
	}
	if err := state.k3s.ApplyDefaultWorkerPoolBridge(ctx, workerIPs, ports.DispatchPort); err != nil {
		return state, fmt.Errorf("bridging default WorkerPool dispatch port: %w", err)
	}
	if len(o.parsedSecrets) > 0 {
		if err := applySecretSpecs(ctx, state.k3s.HostKubeconfigPath(), o.parsedSecrets, "default"); err != nil {
			return state, fmt.Errorf("applying --secret: %w", err)
		}
	}
	if len(o.applyPaths) > 0 {
		if err := applyManifests(ctx, state.k3s.HostKubeconfigPath(), o.applyPaths, o.applyRecursive); err != nil {
			return state, fmt.Errorf("applying manifests: %w", err)
		}
	}

	return state, nil
}

// controllerManagerOpts returns the docker run options used by both bringUp
// and `clrk dev reload controller-manager` so the two paths cannot drift.
// Cluster-side bootstrapping (APIService, EG bridge, extensionManager wiring)
// is intentionally NOT here — it's idempotent on bringUp and a reload skips
// it entirely since k3s already has those records.
func controllerManagerOpts(o *devOpts) ([]drivers.Option, error) {
	env := map[string]string{
		"KUBECONFIG": "/var/lib/clrk/kubeconfig",
		// Tolerate duplicate Envoy protobuf registrations: envoy-go and
		// envoyproxy/go-control-plane both register TLS proto files
		// because envoyproxy/gateway's extension proto transitively
		// imports go-control-plane's TLS types. Matches the pattern
		// apoxy-cloud uses (see cmd/backplane/BUILD.bazel's x_def).
		"GOLANG_PROTOBUF_REGISTRATION_CONFLICT": "ignore",
		// Tell the controller-manager its runtime namespace so the
		// supervised envoy-gateway child gets ENVOY_GATEWAY_NAMESPACE
		// pointing at the dev bridge ns (matches the --grpc-advertise-uri
		// + --ingress-extproc-host flags below). Production sets this
		// via downward API from the Deployment manifest.
		"POD_NAMESPACE": devClrkNamespace,
	}
	if o.otelEndpoint != "" {
		// Default OTLP target for any EgressGateway whose Spec.OTLP.Endpoint
		// is empty — see internal/extproc/sinks.go effectiveOTLP. Production
		// controller-manager Deployments don't set this env, so behaviour
		// there is unchanged.
		env["CLRK_DEV_OTEL_ENDPOINT"] = o.otelEndpoint
	}
	opts := []drivers.Option{
		drivers.WithImage(o.controllerImage),
		drivers.WithPull(o.pull),
		drivers.WithVolume(o.dataDir, "/var/lib/clrk"),
		drivers.WithEnv(env),
		// Resolve `host.docker.internal` to the docker bridge IP so the
		// controller-manager can dial the in-process devotel receiver
		// the host runs. macOS Docker Desktop already publishes this
		// name; the explicit add-host makes the same name work on
		// Linux without a config-file edit.
		drivers.WithExtraHost("host.docker.internal", "host-gateway"),
		drivers.WithArgs(
			"--db=/var/lib/clrk/data.db",
			"--bind-addr=0.0.0.0",
			"--bind-port=8443",
			// Auth is disabled in dev and the binary inside the container
			// must bind 0.0.0.0 so docker -p 8443:8443 can reach it; ack
			// the unauthenticated public bind so the apiserver guard lets
			// us start. The guard refuses this combo without the ack to
			// stop production deployments from accidentally exposing an
			// unauthenticated control plane.
			"--insecure-allow-public",
			// Ingress reconciler is on (k3s has gateway-api installed);
			// worker-deployment is off because clrk dev runs the worker
			// directly via docker — a Deployment would create a duplicate
			// worker pod inside k3s with broken nested-container semantics.
			"--ingress-controller=true",
			// EG-managed Envoy pods reach the controller-manager's ingress
			// ext_proc port via the manually-managed Endpoints in
			// clrk/clrk-controller-manager (same pattern as
			// --grpc-advertise-uri above).
			"--ingress-extproc-host=clrk-controller-manager.clrk.svc.cluster.local",
			// EgressGateway reconciler is on.
			"--egressgateway-controller=true",
			// Advertise the bridge Service DNS so EG data plane Envoy pods
			// dial the controller-manager via in-cluster coredns →
			// manually-managed Endpoints → docker-network IP.
			"--grpc-advertise-uri=clrk-controller-manager.clrk.svc.cluster.local:9443",
			// Workers in clrk dev run on the docker network and can't
			// route to k3s ClusterIPs, so the EgressGateway controller
			// publishes Status.Listeners[*].BackendAddress as
			// <k3s-container>:<NodePort> instead of the in-cluster
			// Service DNS name. The k3s container is reachable from
			// workers by docker DNS.
			"--dev-egress-backend-host=" + drivers.K3sContainerName,
		),
	}
	if o.watch {
		hostBin, err := filepath.Abs(filepath.Join(o.dataDir, "bin", "controller-manager"))
		if err != nil {
			return nil, err
		}
		opts = append(opts, drivers.WithWatchBinary(hostBin))
	}
	return opts, nil
}

// workerOpts returns the docker run options used by both bringUp and
// `clrk dev reload worker` so the two paths cannot drift.
func workerOpts(o *devOpts, w *drivers.WorkerDriver) ([]drivers.Option, error) {
	opts := []drivers.Option{
		drivers.WithImage(o.workerImage),
		drivers.WithPull(o.pull),
		drivers.WithVolume(o.dataDir, "/var/lib/clrk"),
		drivers.WithEnv(map[string]string{
			// Route all resource access through k3s; clrk.apoxy.dev is
			// served by the controller-manager container via an
			// aggregated APIService and transparently proxied by k3s.
			//
			// CLRK_POOL_NAME is unset; the worker binary defaults to
			// "default" when missing. In prod the WorkerPoolDeployment
			// controller injects it via downward API off the
			// `clrk.apoxy.dev/workerpool` PodTemplate label.
			"KUBECONFIG":    "/var/lib/clrk/kubeconfig",
			"POD_NAME":      w.Name(),
			"POD_NAMESPACE": "default",
		}),
	}
	if o.watch {
		hostBin, err := filepath.Abs(filepath.Join(o.dataDir, "bin", "worker"))
		if err != nil {
			return nil, err
		}
		opts = append(opts, drivers.WithWatchBinary(hostBin))
	}
	return opts, nil
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

// startWatcher fires up the --watch goroutine. When prog is non-nil, the
// watcher's lifecycle events are surfaced in the TUI sidebar.
func (s *devState) startWatcher(ctx context.Context, o *devOpts, prog *devtui.Program) {
	reloadAllWorkers := func(ctx context.Context) error {
		var firstErr error
		for _, w := range s.workers {
			if err := w.Reload(ctx); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		return firstErr
	}
	reload := map[string]func(context.Context) error{
		"cmd/controller-manager": s.cm.Reload,
		"internal/controller":    s.cm.Reload,
		"api":                    s.cm.Reload,
		"cmd/worker":             reloadAllWorkers,
		"internal/worker":        reloadAllWorkers,
		"internal/netstack":      reloadAllWorkers,
		"internal/egress":        reloadAllWorkers,
	}
	repoRoot, err := findRepoRoot()
	if err != nil {
		slog.Error("Watch mode disabled", "err", err)
		return
	}
	go func() {
		w := newWatcher(repoRoot, o.dataDir, reload)
		w.events = watcherSink(prog)
		if err := w.run(ctx); err != nil {
			slog.Error("Watcher exited", "err", err)
		}
	}()
}

func setStatus(prog *devtui.Program, name string, s devtui.Status) {
	if prog == nil {
		return
	}
	prog.SetStatus(name, s)
}

func watcherSink(prog *devtui.Program) func(devtui.WatcherEvent, string, time.Duration, error) {
	if prog == nil {
		return nil
	}
	return prog.SendWatcher
}

// waitReadyzInContainer polls the given URL via `docker exec <container>
// curl ...` until it returns 200 or the timeout fires. We go through
// docker exec because Docker for Mac's port-forward layer breaks TLS
// handshakes — handshakes from inside the docker network succeed.
func waitReadyzInContainer(ctx context.Context, container, url string, timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		execCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := exec.CommandContext(execCtx, "docker", "exec", container,
			"curl", "-ksSf", "--max-time", "1", url).Run()
		cancel()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return errors.New("timed out")
		case <-time.After(500 * time.Millisecond):
		}
	}
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

// streamLogs spawns `docker logs -f <container>` and pipes its output into
// the current process stdio. Used by the non-TUI fallback only.
func streamLogs(ctx context.Context, container string) {
	cmd := exec.CommandContext(ctx, "docker", "logs", "-f", container)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	_ = cmd.Start()
}

// streamLogsTo runs `docker logs -f` and forwards each line into the TUI as a
// LogLineMsg under the container's name. Stdout and stderr are demuxed so
// the TUI can color stderr distinctly.
func streamLogsTo(ctx context.Context, prog *devtui.Program, container string) {
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
