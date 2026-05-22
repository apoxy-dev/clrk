// Package drivers orchestrates the docker-side state of `clrk dev`: the
// shared bridge network the k3d cluster + local registry sit on, and
// the k3d-library-driven cluster controller that brings them up. The
// k3d node and the (optional) local registry both attach to the
// `clrk` bridge so in-cluster pods reach the registry via docker DNS.
package drivers

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
)

// NetworkName is the shared docker network k3d and the local registry
// attach to. Pods inside the cluster reach the registry as
// `clrk-registry:5000` over this network.
const NetworkName = "clrk"

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

// HostGatewayIP returns the IP that docker resolves the magic
// "host-gateway" alias to for a container attached to the given network.
// On Linux native docker this is the bridge gateway (which is the host).
// On Docker Desktop for Mac/Windows it's the VPN-kit address that bridges
// container traffic back to the host process. We discover it by spawning
// a one-shot container on the same network with
// `--add-host host.docker.internal:host-gateway` and parsing the
// /etc/hosts entry docker writes — the only portable way to get the same
// IP a k8s HostAlias entry needs.
//
// network must be the same docker network k3d's node is attached to —
// otherwise on Linux the resolved IP is the gateway of a *different*
// bridge and may not be on the routing path the in-cluster Pod uses.
// On Docker Desktop the network choice doesn't affect the result.
func HostGatewayIP(ctx context.Context, network string) (string, error) {
	// Output() drops stderr so first-run image pull progress doesn't
	// pollute the IP we parse from stdout. awk on /etc/hosts works in
	// busybox shells where `getent` isn't available.
	out, err := exec.CommandContext(ctx, "docker", "run", "--rm",
		"--network", network,
		"--add-host", "host.docker.internal:host-gateway",
		"busybox:1.37",
		"awk", "/host.docker.internal/ {print $1; exit}", "/etc/hosts",
	).Output()
	if err != nil {
		return "", fmt.Errorf("resolving host-gateway IP via docker probe: %w", err)
	}
	ip := strings.TrimSpace(string(out))
	if ip == "" {
		return "", fmt.Errorf("docker did not resolve host-gateway alias")
	}
	return ip, nil
}
