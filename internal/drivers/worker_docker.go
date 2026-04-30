package drivers

import (
	"context"
	"fmt"
	"os/exec"
	"strings"

	"github.com/apoxy-dev/clrk/internal/drivers/dockerutils"
)

const (
	// DefaultWorkerImage is used when no override is supplied.
	DefaultWorkerImage = "clrk/worker:latest"

	// workerContainerPrefix is the name prefix for worker containers.
	// Workers are numbered to support N>1 replicas.
	workerContainerPrefix = "clrk-worker-"
)

// WorkerDriver runs a single worker container. Instances are keyed by index
// so a caller can manage a fleet.
type WorkerDriver struct {
	Index int
}

// NewWorkerDriver returns a driver for the worker at the given index.
func NewWorkerDriver(index int) *WorkerDriver {
	return &WorkerDriver{Index: index}
}

// Name returns the container name for this driver's worker.
func (d *WorkerDriver) Name() string {
	return fmt.Sprintf("%s%d", workerContainerPrefix, d.Index)
}

// Start launches the worker container attached to the shared network and
// pointed at the controller-manager via CLRK_APISERVER_URL.
func (d *WorkerDriver) Start(ctx context.Context, opts ...Option) (string, error) {
	if err := EnsureNetwork(ctx); err != nil {
		return "", err
	}
	name := d.Name()
	if err := dockerutils.RemoveIfExists(ctx, name); err != nil {
		return "", err
	}

	o := Apply(opts...)
	if o.Image == "" {
		o.Image = DefaultWorkerImage
	}

	// The worker uses libcontainer / runc / gvisor which require extensive
	// host privileges. This is dev-only; production workers run as a
	// Kubernetes DaemonSet with pod-level securityContext.
	baseArgs := []string{
		"run", "--detach",
		"--name", name,
		"--network", NetworkName,
		"--label", ownerLabel,
		"--restart", "on-failure",
		"--privileged",
		"--cap-add", "SYS_ADMIN",
		"--cap-add", "NET_ADMIN",
		"--security-opt", "seccomp=unconfined",
		"--security-opt", "apparmor=unconfined",
	}
	for _, k := range sortedStringKeys(o.Labels) {
		baseArgs = append(baseArgs, "--label", k+"="+o.Labels[k])
	}
	for _, k := range sortedStringKeys(o.Env) {
		baseArgs = append(baseArgs, "--env", k+"="+o.Env[k])
	}
	for _, k := range sortedStringKeys(o.Volumes) {
		baseArgs = append(baseArgs, "--volume", k+":"+o.Volumes[k])
	}
	if o.WatchBinary != "" {
		baseArgs = append(baseArgs, "--volume", o.WatchBinary+":/usr/local/bin/worker:ro")
	}

	baseArgs = append(baseArgs, o.Image)
	baseArgs = append(baseArgs, o.Args...)

	cmd := exec.CommandContext(ctx, "docker", baseArgs...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("starting worker: %w: %s", err, strings.TrimSpace(string(out)))
	}
	if err := dockerutils.WaitRunning(ctx, name); err != nil {
		return "", err
	}
	return name, nil
}

// Stop removes the worker container.
func (d *WorkerDriver) Stop(ctx context.Context) error {
	return dockerutils.RemoveIfExists(ctx, d.Name())
}

// Reload signals the container to re-exec after a watch-mode rebuild.
func (d *WorkerDriver) Reload(ctx context.Context) error {
	return dockerutils.SignalContainer(ctx, d.Name(), "TERM")
}

// GetAddr returns the container's docker-network hostname.
func (d *WorkerDriver) GetAddr(ctx context.Context) (string, error) {
	return d.Name(), nil
}
