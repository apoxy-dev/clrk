// Package drivers orchestrates the docker-side state of `clrk dev`: the
// shared bridge network the k3d cluster + local registry sit on, and
// the ctlptl-driven cluster controller that brings them up.
//
// Originally also owned the docker drivers for controller-manager and
// worker containers; those were retired when both moved into the
// cluster as Pods. The package keeps the docker network helpers because
// the k3d node + registry still attach to the same bridge.
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
