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

// V4Subnet and V6Subnet are the explicit subnets declared on the
// shared bridge. Both must be present: k3d scans IPAM.Config to decide
// which address families k3s should enable, and an --ipv6 network with
// only a v6 subnet declared causes k3s inside k3d to crash-loop with no
// log output. Declaring v4 as well makes k3d see a dual-stack network
// (matching how plain `docker run` sees one regardless of IPAM config).
const (
	V4Subnet = "192.168.231.0/24"
	V6Subnet = "fd00:dead:beef::/64"
)

// EnsureNetwork creates the shared docker network if it does not already
// exist. Safe to call concurrently; the docker daemon serializes creates.
//
// IPv6 is enabled on the bridge so Envoy's DFP cluster can reach AAAA-only
// hops. If we find a pre-existing `clrk` network whose IPAM does not
// declare both subnets (e.g. created by an older clrk dev that only set
// --subnet for v6, or a v4-only network from before IPv6 landed), tear it
// down and recreate — the alternative is silent ENETUNREACH on every v6
// upstream or k3s crash-looping under k3d.
func EnsureNetwork(ctx context.Context) error {
	out, err := exec.CommandContext(ctx, "docker", "network", "inspect",
		"-f", "{{.EnableIPv6}};{{range .IPAM.Config}}{{.Subnet}},{{end}}", NetworkName).Output()
	if err == nil {
		state := strings.TrimSpace(string(out))
		if want := "true;" + V4Subnet + "," + V6Subnet + ","; state == want || state == "true;"+V6Subnet+","+V4Subnet+"," {
			return nil
		}
		if rmOut, rmErr := exec.CommandContext(ctx, "docker", "network", "rm", NetworkName).CombinedOutput(); rmErr != nil {
			return fmt.Errorf("removing stale docker network %q (detach attached containers first): %w: %s", NetworkName, rmErr, bytes.TrimSpace(rmOut))
		}
	}
	createOut, err := exec.CommandContext(ctx, "docker", "network", "create",
		"--ipv6",
		"--subnet", V4Subnet,
		"--subnet", V6Subnet,
		NetworkName).CombinedOutput()
	if err != nil {
		return fmt.Errorf("creating docker network %q: %w: %s", NetworkName, err, bytes.TrimSpace(createOut))
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
	if o.Pull != "" {
		args = append(args, "--pull", o.Pull)
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
	for _, k := range sortedStringKeys(o.ExtraHosts) {
		args = append(args, "--add-host", k+":"+o.ExtraHosts[k])
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
