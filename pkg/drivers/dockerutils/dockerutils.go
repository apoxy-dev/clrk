// Package dockerutils wraps the subset of `docker` CLI functionality the
// drivers need: querying container status, waiting for ready states, and
// collecting containers by label for garbage collection.
package dockerutils

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

const (
	waitTimeout  = 60 * time.Second
	pollInterval = 500 * time.Millisecond
)

// CollectByLabels returns the names of all containers (running or stopped)
// whose labels match every key/value pair supplied. An empty map matches
// every container.
func CollectByLabels(ctx context.Context, labels map[string]string) ([]string, error) {
	args := []string{"ps", "-a", "--no-trunc", "--format", "{{.Names}}"}
	for k, v := range labels {
		args = append(args, "--filter", "label="+k+"="+v)
	}
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("docker ps: %w: %s", err, bytes.TrimSpace(out))
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) == 1 && lines[0] == "" {
		return nil, nil
	}
	return lines, nil
}

// WaitStatus polls `docker inspect` until the container's State.Status equals
// want or the timeout fires. Returns the final observed status on error.
func WaitStatus(ctx context.Context, name, want string) error {
	deadline := time.NewTimer(waitTimeout)
	defer deadline.Stop()

	for {
		out, err := exec.CommandContext(ctx, "docker", "inspect",
			"--format", "{{.State.Status}}", name).Output()
		if err == nil {
			if got := strings.TrimSpace(string(out)); got == want {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("timed out waiting for %q to reach status %q", name, want)
		case <-time.After(pollInterval):
		}
	}
}

// WaitRunning is a convenience wrapper around WaitStatus for the most common
// case.
func WaitRunning(ctx context.Context, name string) error {
	return WaitStatus(ctx, name, "running")
}

// RemoveIfExists force-removes the named container if it exists. No error is
// returned when the container is absent.
func RemoveIfExists(ctx context.Context, name string) error {
	// `docker rm -f` returns 0 for an absent container on recent Docker
	// versions; older ones exit non-zero. Inspect first to normalize.
	if err := exec.CommandContext(ctx, "docker", "inspect", name).Run(); err != nil {
		return nil
	}
	out, err := exec.CommandContext(ctx, "docker", "rm", "-f", name).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker rm -f %s: %w: %s", name, err, bytes.TrimSpace(out))
	}
	return nil
}

// SignalContainer sends the given signal to PID 1 of the container via
// `docker kill`. Used by watch mode to trigger restart-on-failure re-execs.
func SignalContainer(ctx context.Context, name, signal string) error {
	out, err := exec.CommandContext(ctx, "docker", "kill",
		"--signal", signal, name).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker kill -s %s %s: %w: %s", signal, name, err, bytes.TrimSpace(out))
	}
	return nil
}

// IPOnNetwork returns the container's IP on the named docker network.
// Used by the APIService bootstrap to wire k3s Endpoints at the real
// backend IP — Endpoints.addresses must be IPs, not hostnames.
func IPOnNetwork(ctx context.Context, name, network string) (string, error) {
	format := fmt.Sprintf(`{{(index .NetworkSettings.Networks %q).IPAddress}}`, network)
	out, err := exec.CommandContext(ctx, "docker", "inspect",
		"--format", format, name).Output()
	if err != nil {
		return "", fmt.Errorf("docker inspect %s on %s: %w", name, network, err)
	}
	ip := strings.TrimSpace(string(out))
	if ip == "" {
		return "", fmt.Errorf("container %s has no IP on network %s", name, network)
	}
	return ip, nil
}
