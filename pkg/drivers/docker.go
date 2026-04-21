package drivers

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"sort"
	"strconv"
	"strings"
)

// NetworkName is the shared docker network all clrk containers attach to.
const NetworkName = "clrk"

// ownerLabel marks containers created by `clrk dev` so we can garbage-collect
// leftovers from previous runs without touching unrelated containers.
const ownerLabel = "dev.apoxy.clrk/owner=clrk"

// EnsureNetwork creates the shared docker network if it does not already
// exist. Safe to call concurrently; the docker daemon serializes creates.
func EnsureNetwork(ctx context.Context) error {
	if err := exec.CommandContext(ctx, "docker", "network", "inspect", NetworkName).Run(); err == nil {
		return nil
	}
	out, err := exec.CommandContext(ctx, "docker", "network", "create", NetworkName).CombinedOutput()
	if err != nil {
		return fmt.Errorf("creating docker network %q: %w: %s", NetworkName, err, bytes.TrimSpace(out))
	}
	return nil
}

// runArgs builds the argv for `docker run`. The caller supplies the container
// name and image; everything else comes from Options. Keys/values are sorted
// so the resulting argv is deterministic (useful for tests).
func runArgs(name, image string, o *Options) []string {
	args := []string{
		"run", "--detach",
		"--name", name,
		"--network", NetworkName,
		"--label", ownerLabel,
		"--restart", "on-failure",
	}

	for _, k := range sortedStringKeys(o.Labels) {
		args = append(args, "--label", k+"="+o.Labels[k])
	}
	for _, k := range sortedStringKeys(o.Env) {
		args = append(args, "--env", k+"="+o.Env[k])
	}
	for _, k := range sortedStringKeys(o.Volumes) {
		args = append(args, "--volume", k+":"+o.Volumes[k])
	}
	if o.WatchBinary != "" {
		args = append(args, "--volume", o.WatchBinary+":/usr/local/bin/"+entryBinary(image)+":ro")
	}
	for _, host := range sortedIntKeys(o.Ports) {
		args = append(args, "--publish", strconv.Itoa(host)+":"+strconv.Itoa(o.Ports[host]))
	}

	args = append(args, image)
	args = append(args, o.Args...)
	return args
}

// entryBinary extracts the entrypoint binary name from an image reference.
// We rely on the convention `<registry>/clrk-<name>:<tag>` so e.g.
// "ghcr.io/apoxy-dev/clrk-worker:dev" → "worker".
func entryBinary(image string) string {
	// strip tag
	if idx := strings.LastIndex(image, ":"); idx > 0 && !strings.Contains(image[idx:], "/") {
		image = image[:idx]
	}
	if idx := strings.LastIndex(image, "/"); idx >= 0 {
		image = image[idx+1:]
	}
	return strings.TrimPrefix(image, "clrk-")
}

func sortedStringKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedIntKeys(m map[int]int) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	return keys
}

// runDocker executes `docker <args...>` and returns combined output for
// diagnostics. Errors include the tail of stderr so callers can surface them.
func runDocker(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, bytes.TrimSpace(out))
	}
	return string(out), nil
}
