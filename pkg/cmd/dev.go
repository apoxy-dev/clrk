package cmd

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/apoxy-dev/clrk/pkg/drivers"
)

type devOpts struct {
	watch            bool
	controllerImage  string
	workerImage      string
	workers          int
	dataDir          string
	logStream        bool
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

	cm := drivers.NewControllerManagerDriver()
	cmOpts := []drivers.Option{
		drivers.WithImage(o.controllerImage),
		drivers.WithVolume(o.dataDir, "/var/lib/clrk"),
		drivers.WithArgs(
			"--db=/var/lib/clrk/data.db",
			"--bind-addr=0.0.0.0",
			"--bind-port=8443",
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
		})
	}
	defer func() { teardown() }()

	if err := waitReadyz(ctx, "https://localhost:8443/readyz", 90*time.Second); err != nil {
		return fmt.Errorf("controller-manager never became ready: %w", err)
	}
	slog.Info("Apiserver /readyz is OK")

	workers = make([]*drivers.WorkerDriver, 0, o.workers)
	for i := 0; i < o.workers; i++ {
		w := drivers.NewWorkerDriver(i)
		wOpts := []drivers.Option{
			drivers.WithImage(o.workerImage),
			drivers.WithEnv(map[string]string{
				"CLRK_APISERVER_URL":      "https://" + drivers.ControllerManagerContainerName + ":8443",
				"CLRK_APISERVER_INSECURE": "true",
				"CLRK_POOL_NAME":          "default",
				"POD_NAME":                w.Name(),
				"POD_NAMESPACE":           "default",
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

// waitReadyz polls the apiserver's /readyz endpoint until it returns 200 or
// the timeout fires.
func waitReadyz(ctx context.Context, url string, timeout time.Duration) error {
	client := &http.Client{
		Timeout:   500 * time.Millisecond,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		resp, err := client.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
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
