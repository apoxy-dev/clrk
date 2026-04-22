package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"

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
	logStream       bool
}

func newDevCmd() *cobra.Command {
	o := &devOpts{}
	cmd := &cobra.Command{
		Use:   "dev",
		Short: "Run controller-manager and worker locally in Docker",
		Long: "Starts a controller-manager container with an embedded apiserver " +
			"and N worker containers on a shared docker network.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDev(cmd.Context(), o)
		},
	}

	cmd.Flags().BoolVar(&o.watch, "watch", false, "Rebuild and hot-reload binaries on source changes (experimental).")
	cmd.Flags().StringVar(&o.controllerImage, "controller-image", drivers.DefaultControllerManagerImage, "Controller-manager image ref.")
	cmd.Flags().StringVar(&o.workerImage, "worker-image", drivers.DefaultWorkerImage, "Worker image ref.")
	cmd.Flags().StringVar(&o.k3sImage, "k3s-image", drivers.DefaultK3sImage, "k3s image ref.")
	cmd.Flags().IntVar(&o.workers, "workers", 1, "Number of worker replicas.")
	cmd.Flags().StringVar(&o.dataDir, "data-dir", "", "Host path for ~/.clrk state (defaults to --clrk-dir).")
	cmd.Flags().BoolVar(&o.logStream, "logs", true, "Stream container logs to stdout.")
	return cmd
}

func runDev(ctx context.Context, o *devOpts) error {
	if o.dataDir == "" {
		o.dataDir = clrkDir
	}
	if err := os.MkdirAll(o.dataDir, 0o755); err != nil {
		return fmt.Errorf("creating data dir: %w", err)
	}

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("Starting clrk dev", "data_dir", o.dataDir, "workers", o.workers)

	// Bring up k3s first so other components can target its kubeconfig.
	// k3s writes its kubeconfig into DataDir; downstream drivers mount
	// that path to pick it up.
	k3s := drivers.NewK3sDriver(o.dataDir)
	k3sName, err := k3s.Start(ctx, drivers.WithImage(o.k3sImage))
	if err != nil {
		return fmt.Errorf("starting k3s: %w", err)
	}
	slog.Info("k3s running", "container", k3sName)

	if err := k3s.WaitAPIReady(ctx, 120*time.Second); err != nil {
		return fmt.Errorf("k3s API never became ready: %w", err)
	}
	slog.Info("k3s API is ready", "kubeconfig", k3s.KubeconfigPath())

	if err := k3s.InstallGatewayAPI(ctx); err != nil {
		return fmt.Errorf("installing Gateway API CRDs: %w", err)
	}
	slog.Info("Gateway API CRDs installed")

	cm := drivers.NewControllerManagerDriver()
	cmOpts := []drivers.Option{
		drivers.WithImage(o.controllerImage),
		drivers.WithVolume(o.dataDir, "/var/lib/clrk"),
		drivers.WithEnv(map[string]string{
			"KUBECONFIG": "/var/lib/clrk/kubeconfig",
		}),
		drivers.WithArgs(
			"--db=/var/lib/clrk/data.db",
			"--bind-addr=0.0.0.0",
			"--bind-port=8443",
			"--cluster-mode=true",
		),
	}
	if o.watch {
		hostBin, err := filepath.Abs(filepath.Join(o.dataDir, "bin", "controller-manager"))
		if err != nil {
			return err
		}
		cmOpts = append(cmOpts, drivers.WithWatchBinary(hostBin))
	}

	cmName, err := cm.Start(ctx, cmOpts...)
	if err != nil {
		return err
	}
	slog.Info("Controller-manager running", "container", cmName)

	// Tear down on exit so `Ctrl-C` leaves a clean docker state. The defer
	// resolves `teardown` at call time so the post-workers reassignment below
	// is honored even on early-error returns.
	var (
		teardownOnce sync.Once
		workers      []*drivers.WorkerDriver
	)
	teardown := func() {
		teardownOnce.Do(func() {
			shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			for _, w := range workers {
				_ = w.Stop(shutCtx)
			}
			_ = cm.Stop(shutCtx)
			_ = k3s.Stop(shutCtx)
		})
	}
	defer func() { teardown() }()

	// Register the clrk apiserver as an aggregated extension in k3s so
	// the controller-manager's ctrl.Manager (and workers below) see
	// clrk.apoxy.dev types through the unified k3s kubeconfig. Do this
	// before waiting for /readyz — docker assigns the container's IP
	// at run-time before the process starts, so the IP is already
	// stable, and this gives the controller-manager's discovery a
	// registered APIService to hit as soon as its ctrl.Manager starts.
	cmIP, err := dockerutils.IPOnNetwork(ctx, cmName, drivers.NetworkName)
	if err != nil {
		return fmt.Errorf("getting controller-manager IP: %w", err)
	}
	if err := bootstrapClrkAPIService(ctx, k3s, cmIP, 8443); err != nil {
		return fmt.Errorf("registering clrk APIService: %w", err)
	}
	slog.Info("clrk APIService registered in k3s", "backend", cmIP)

	// Poll /readyz from inside the container; Docker for Mac's port-forward
	// silently breaks TLS handshakes, so the host-side curl never completes.
	if err := waitReadyzInContainer(ctx, cmName, "https://localhost:8443/readyz", 90*time.Second); err != nil {
		return fmt.Errorf("controller-manager never became ready: %w", err)
	}
	slog.Info("Apiserver /readyz is OK")

	workers = make([]*drivers.WorkerDriver, 0, o.workers)
	for i := 0; i < o.workers; i++ {
		w := drivers.NewWorkerDriver(i)
		wOpts := []drivers.Option{
			drivers.WithImage(o.workerImage),
			drivers.WithVolume(o.dataDir, "/var/lib/clrk"),
			drivers.WithEnv(map[string]string{
				// Route all resource access through k3s; clrk.apoxy.dev
				// is served by the controller-manager container via an
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
				return err
			}
			wOpts = append(wOpts, drivers.WithWatchBinary(hostBin))
		}
		name, err := w.Start(ctx, wOpts...)
		if err != nil {
			return fmt.Errorf("starting worker %d: %w", i, err)
		}
		slog.Info("Worker running", "container", name)
		workers = append(workers, w)
	}

	if o.logStream {
		streamLogs(ctx, cmName)
		for _, w := range workers {
			streamLogs(ctx, w.Name())
		}
	}

	if o.watch {
		// reloadAllWorkers fans the signal out to every worker so the map can
		// keep its single-function-per-prefix shape without losing replicas.
		reloadAllWorkers := func(ctx context.Context) error {
			var firstErr error
			for _, w := range workers {
				if err := w.Reload(ctx); err != nil && firstErr == nil {
					firstErr = err
				}
			}
			return firstErr
		}
		reload := map[string]func(context.Context) error{
			"cmd/controller-manager": cm.Reload,
			"internal/controller":    cm.Reload,
			"api":                    cm.Reload,
			"cmd/worker":             reloadAllWorkers,
			"internal/worker":        reloadAllWorkers,
			"internal/netstack":      reloadAllWorkers,
			"internal/egress":        reloadAllWorkers,
		}
		repoRoot, err := findRepoRoot()
		if err != nil {
			return err
		}
		go func() {
			w := newWatcher(repoRoot, o.dataDir, reload)
			if err := w.run(ctx); err != nil {
				slog.Error("Watcher exited", "err", err)
			}
		}()
	}

	<-ctx.Done()
	slog.Info("Shutting down")
	teardown()
	return nil
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
// the current process stdio. Returns immediately; the child exits when the
// container stops.
func streamLogs(ctx context.Context, container string) {
	cmd := exec.CommandContext(ctx, "docker", "logs", "-f", container)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	_ = cmd.Start()
}
