package drivers

import (
	"context"
	"fmt"

	"github.com/apoxy-dev/clrk/internal/drivers/dockerutils"
)

const (
	// ControllerManagerContainerName is a stable name for the single
	// controller-manager container. `clrk dev` is single-replica.
	ControllerManagerContainerName = "clrk-controller-manager"

	// DefaultControllerManagerImage is used when no override is supplied.
	DefaultControllerManagerImage = "clrk/controller-manager:latest"

	// DefaultAPIServerHostPort is the host port the apiserver is published
	// on for access from the host (e.g. `kubectl --server=…`).
	DefaultAPIServerHostPort = 8443
)

// ControllerManagerDriver runs the controller-manager container.
type ControllerManagerDriver struct {
	HostPort int
}

// NewControllerManagerDriver creates a driver with sensible defaults.
func NewControllerManagerDriver() *ControllerManagerDriver {
	return &ControllerManagerDriver{HostPort: DefaultAPIServerHostPort}
}

// Start ensures the shared network exists, removes any stale container from a
// previous run, then launches the controller-manager detached.
func (d *ControllerManagerDriver) Start(ctx context.Context, opts ...Option) (string, error) {
	if err := EnsureNetwork(ctx); err != nil {
		return "", err
	}
	if err := dockerutils.RemoveIfExists(ctx, ControllerManagerContainerName); err != nil {
		return "", err
	}

	o := Apply(opts...)
	if o.Image == "" {
		o.Image = DefaultControllerManagerImage
	}
	if _, ok := o.Ports[d.HostPort]; !ok {
		o.Ports[d.HostPort] = 8443
	}

	args := runArgs(ControllerManagerContainerName, o.Image, o)
	if _, err := runDocker(ctx, args...); err != nil {
		return "", fmt.Errorf("starting controller-manager: %w", err)
	}
	if err := dockerutils.WaitRunning(ctx, ControllerManagerContainerName); err != nil {
		return "", err
	}
	return ControllerManagerContainerName, nil
}

// Stop removes the controller-manager container.
func (d *ControllerManagerDriver) Stop(ctx context.Context) error {
	return dockerutils.RemoveIfExists(ctx, ControllerManagerContainerName)
}

// Reload signals the container to re-exec after a watch-mode rebuild.
// restart=on-failure causes docker to restart the container once the process
// exits non-zero, picking up the updated bind-mounted binary.
func (d *ControllerManagerDriver) Reload(ctx context.Context) error {
	return dockerutils.SignalContainer(ctx, ControllerManagerContainerName, "TERM")
}

// GetAddr returns the host-reachable apiserver URL.
func (d *ControllerManagerDriver) GetAddr(ctx context.Context) (string, error) {
	return fmt.Sprintf("https://localhost:%d", d.HostPort), nil
}
