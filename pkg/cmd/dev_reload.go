package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"

	"github.com/spf13/cobra"

	"github.com/apoxy-dev/clrk/pkg/drivers"
)

// newDevReloadCmd is `clrk dev reload <component>`. It tears down a single
// component container managed by `clrk dev` and recreates it with the
// current image — useful when iterating on worker or controller-manager
// code without re-bootstrapping k3s, EG, and the apiserver state.
//
// `clrk dev` doesn't auto-respawn dead containers, so without this users
// have to kill the whole `clrk dev` process and lose all in-cluster
// state. This subcommand is a no-state drop-in: it does not need to talk
// to the running `clrk dev` process — it just re-derives the same
// `docker run` args from defaults and applies them.
func newDevReloadCmd() *cobra.Command {
	o := &devOpts{}
	var (
		tarball     string
		workerIndex int
	)

	cmd := &cobra.Command{
		Use:       "reload [worker|controller-manager]",
		Short:     "Restart a clrk dev component container with the current image",
		Long:      "Stops and re-creates one of the container images managed by `clrk dev` (worker or controller-manager) so a freshly-built `clrk/<name>:latest` image is picked up. Optionally docker-loads a bazel-built OCI tarball first.",
		Args:      cobra.ExactArgs(1),
		ValidArgs: []string{"worker", "controller-manager"},
		RunE: func(cmd *cobra.Command, args []string) error {
			if o.dataDir == "" {
				o.dataDir = clrkDir
			}
			ctx := cmd.Context()
			if tarball != "" {
				if err := dockerLoad(ctx, tarball); err != nil {
					return err
				}
			}
			switch args[0] {
			case "worker":
				return reloadWorker(ctx, o, workerIndex)
			case "controller-manager":
				return reloadControllerManager(ctx, o)
			default:
				return fmt.Errorf("unknown component %q (want worker|controller-manager)", args[0])
			}
		},
	}

	cmd.Flags().StringVar(&o.controllerImage, "controller-image", drivers.DefaultControllerManagerImage, "Controller-manager image ref.")
	cmd.Flags().StringVar(&o.workerImage, "worker-image", drivers.DefaultWorkerImage, "Worker image ref.")
	cmd.Flags().StringVar(&o.dataDir, "data-dir", "", "Host path for ~/.clrk state (defaults to --clrk-dir).")
	cmd.Flags().BoolVar(&o.watch, "watch", false, "Re-attach the host binary as a bind mount (matches `clrk dev --watch`).")
	cmd.Flags().StringVar(&tarball, "tarball", "", "Path to a bazel-built OCI tarball (e.g. //clrk:worker_oci_tarball). Loaded into host docker before the restart so the new image is what gets picked up.")
	cmd.Flags().IntVar(&workerIndex, "index", 0, "Worker replica index (matches `clrk dev --workers`).")
	return cmd
}

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
