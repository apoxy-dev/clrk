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

	"github.com/apoxy-dev/clrk/pkg/cmd/devtui"
	"github.com/apoxy-dev/clrk/pkg/drivers"
	"github.com/apoxy-dev/clrk/pkg/drivers/dockerutils"
)

type devOpts struct {
	watch           bool
	controllerImage string
	workerImage     string
	k3sImage        string
	workers         int
	dataDir         string
	tui             bool
	tuiSet          bool
	applyPaths      []string
	applyRecursive  bool
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
	cmd.Flags().IntVar(&o.workers, "workers", 1, "Number of worker replicas.")
	cmd.Flags().StringVar(&o.dataDir, "data-dir", "", "Host path for ~/.clrk state (defaults to --clrk-dir).")
	cmd.Flags().BoolVar(&o.tui, "tui", true, "Render the dev TUI (auto-disabled when stdout isn't a TTY).")
	cmd.Flags().StringArrayVarP(&o.applyPaths, "apply", "f", nil, "YAML file or directory of CRDs to server-side apply once the apiserver is ready (repeatable).")
	cmd.Flags().BoolVarP(&o.applyRecursive, "recursive", "R", false, "Recurse into subdirectories when --apply targets a directory.")
	cmd.AddCommand(newDevReloadCmd())
	cmd.AddCommand(newDevStatusCmd())
	return cmd
}

func runDev(ctx context.Context, o *devOpts) error {
	if o.dataDir == "" {
		o.dataDir = clrkDir
	}
	if err := os.MkdirAll(o.dataDir, 0o755); err != nil {
		return fmt.Errorf("creating data dir: %w", err)
	}

	// Auto-disable TUI on non-TTY stdout (CI, piped output) unless the user
	// explicitly set --tui=true.
	if !o.tuiSet && !term.IsTerminal(int(os.Stdout.Fd())) {
		o.tui = false
	}

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	if o.tui {
		return runDevTUI(ctx, o)
	}
	return runDevPlain(ctx, o)
}

// runDevPlain is the original streaming-stdout path, kept for CI / non-TTY.
func runDevPlain(ctx context.Context, o *devOpts) error {
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

	if o.watch {
		state.startWatcher(ctx, o, nil)
	}

	<-ctx.Done()
	slog.Info("Shutting down")
	return nil
}

// runDevTUI orchestrates components in a background goroutine while the TUI
// renders status and per-component logs on the main goroutine.
func runDevTUI(ctx context.Context, o *devOpts) error {
	componentNames := []string{
		drivers.K3sContainerName,
		drivers.ControllerManagerContainerName,
	}
	for i := 0; i < o.workers; i++ {
		componentNames = append(componentNames, drivers.NewWorkerDriver(i).Name())
	}

	prog := devtui.New(componentNames)

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
		if o.watch {
			state.startWatcher(orchestrateCtx, o, prog)
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
		if _, err := state.k3s.Start(ctx, drivers.WithImage(o.k3sImage)); err != nil {
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

	// Order matters: EG install.yaml ships its own (older) gateway-api
	// CRDs and would clobber a freshly-installed v1.2.1 set, dropping the
	// Gateway.spec.infrastructure field our EgressGateway controller
	// programs. Install EG first, then upgrade the CRDs to v1.2.1.
	if err := state.k3s.InstallEnvoyGateway(ctx); err != nil {
		return state, fmt.Errorf("installing Envoy Gateway operator: %w", err)
	}
	slog.Info("Envoy Gateway operator installed")

	if err := state.k3s.InstallGatewayAPI(ctx); err != nil {
		return state, fmt.Errorf("installing Gateway API CRDs: %w", err)
	}
	slog.Info("Gateway API CRDs installed")

	state.cm = drivers.NewControllerManagerDriver()
	cmOpts, err := controllerManagerOpts(o)
	if err != nil {
		return state, err
	}

	if err := withStatus(prog, drivers.ControllerManagerContainerName, func() error {
		if _, err := state.cm.Start(ctx, cmOpts...); err != nil {
			return err
		}
		slog.Info("Controller-manager running", "container", drivers.ControllerManagerContainerName)

		cmIP, err := dockerutils.IPOnNetwork(ctx, drivers.ControllerManagerContainerName, drivers.NetworkName)
		if err != nil {
			return fmt.Errorf("getting controller-manager IP: %w", err)
		}
		if err := bootstrapClrkAPIService(ctx, state.k3s, cmIP, 8443); err != nil {
			return fmt.Errorf("registering clrk APIService: %w", err)
		}
		slog.Info("clrk APIService registered in k3s", "backend", cmIP)

		// Bridge Envoy-Gateway-provisioned Envoy pods (inside k3s) to the
		// controller-manager's gRPC on the docker network. ConfigMap patch
		// restarts the EG operator so it picks up the new extensionManager.
		if err := state.k3s.ApplyControllerManagerBridge(ctx, cmIP, 9443); err != nil {
			return fmt.Errorf("bridging controller-manager gRPC: %w", err)
		}
		if err := state.k3s.ConfigureEnvoyGatewayExtension(ctx,
			"clrk-controller-manager.clrk-system.svc.cluster.local", 9443); err != nil {
			return fmt.Errorf("configuring Envoy Gateway extension: %w", err)
		}
		slog.Info("Envoy Gateway extensionManager wired to controller-manager",
			"bridge", fmt.Sprintf("clrk-controller-manager.clrk-system.svc.cluster.local:%d → %s:%d", 9443, cmIP, 9443))

		if err := waitReadyzInContainer(ctx, drivers.ControllerManagerContainerName, "https://localhost:8443/readyz", 90*time.Second); err != nil {
			return fmt.Errorf("controller-manager never became ready: %w", err)
		}
		return nil
	}); err != nil {
		return state, err
	}
	slog.Info("Apiserver /readyz is OK")

	// Apply user-supplied manifests before workers start so reconcilers and
	// workers see the desired world at boot rather than racing against it.
	if len(o.applyPaths) > 0 {
		if err := applyManifests(ctx, state.k3s.KubeconfigPath(), o.applyPaths, o.applyRecursive); err != nil {
			return state, fmt.Errorf("applying manifests: %w", err)
		}
	}

	state.workers = make([]*drivers.WorkerDriver, 0, o.workers)
	for i := 0; i < o.workers; i++ {
		w := drivers.NewWorkerDriver(i)
		wOpts, err := workerOpts(o, w)
		if err != nil {
			return state, err
		}
		if err := withStatus(prog, w.Name(), func() error {
			_, err := w.Start(ctx, wOpts...)
			return err
		}); err != nil {
			return state, fmt.Errorf("starting worker %d: %w", i, err)
		}
		slog.Info("Worker running", "container", w.Name())
		state.workers = append(state.workers, w)
	}

	return state, nil
}

// controllerManagerOpts returns the docker run options used by both bringUp
// and `clrk dev reload controller-manager` so the two paths cannot drift.
// Cluster-side bootstrapping (APIService, EG bridge, extensionManager wiring)
// is intentionally NOT here — it's idempotent on bringUp and a reload skips
// it entirely since k3s already has those records.
func controllerManagerOpts(o *devOpts) ([]drivers.Option, error) {
	opts := []drivers.Option{
		drivers.WithImage(o.controllerImage),
		drivers.WithVolume(o.dataDir, "/var/lib/clrk"),
		drivers.WithEnv(map[string]string{
			"KUBECONFIG": "/var/lib/clrk/kubeconfig",
			// Tolerate duplicate Envoy protobuf registrations: envoy-go and
			// envoyproxy/go-control-plane both register TLS proto files
			// because envoyproxy/gateway's extension proto transitively
			// imports go-control-plane's TLS types. Matches the pattern
			// apoxy-cloud uses (see cmd/backplane/BUILD.bazel's x_def).
			"GOLANG_PROTOBUF_REGISTRATION_CONFLICT": "ignore",
		}),
		drivers.WithArgs(
			"--db=/var/lib/clrk/data.db",
			"--bind-addr=0.0.0.0",
			"--bind-port=8443",
			// Ingress reconciler is on (k3s has gateway-api installed);
			// worker-deployment is off because clrk dev runs the worker
			// directly via docker — a Deployment would create a duplicate
			// worker pod inside k3s with broken nested-container semantics.
			"--ingress-controller=true",
			// EgressGateway reconciler is on.
			"--egressgateway-controller=true",
			// Advertise the bridge Service DNS so EG data plane Envoy pods
			// dial the controller-manager via in-cluster coredns →
			// manually-managed Endpoints → docker-network IP.
			"--grpc-advertise-uri=clrk-controller-manager.clrk-system.svc.cluster.local:9443",
			// Dev wrapper image. EG hardcodes command:["envoy"] but the
			// private build's entrypoint is /envoy-static; the wrapper
			// only adds /usr/local/bin/envoy. Built + loaded manually.
			"--envoy-image=docker.io/clrk/envoy:dev",
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
		drivers.WithVolume(o.dataDir, "/var/lib/clrk"),
		drivers.WithEnv(map[string]string{
			// Route all resource access through k3s; clrk.apoxy.dev is
			// served by the controller-manager container via an
			// aggregated APIService and transparently proxied by k3s.
			"KUBECONFIG":     "/var/lib/clrk/kubeconfig",
			"CLRK_POOL_NAME": "default",
			"POD_NAME":       w.Name(),
			"POD_NAMESPACE":  "default",
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
