//go:build linux

package sandbox

import (
	"log/slog"
	"net/netip"

	"github.com/docker/docker/libnetwork/resolvconf"
)

// ReadWorkerResolvers parses the worker's own /etc/resolv.conf via
// libnetwork/resolvconf (the same parser dockerd uses) and returns one
// AddrPort per nameserver entry, all on port 53.
//
// These addresses are the *destination* the in-Sentry UDP forwarder dials
// for resolution. The dial happens from the worker's network namespace, so
// loopback entries like Docker's embedded resolver at 127.0.0.11 are
// reachable here even though they aren't in the sandbox netns. Exposed so
// the worker root (cross-package) can drive the lookup at startup; the
// result feeds each sandbox's sentrystack init payload via
// ManagerConfig.Resolvers.
func ReadWorkerResolvers() []netip.AddrPort {
	host, err := resolvconf.GetSpecific("/etc/resolv.conf")
	if err != nil {
		slog.Error("Failed to read worker /etc/resolv.conf; sandbox DNS will fail", "error", err)
		return nil
	}
	nameservers := resolvconf.GetNameservers(host.Content, resolvconf.IP)
	out := make([]netip.AddrPort, 0, len(nameservers))
	for _, ns := range nameservers {
		addr, err := netip.ParseAddr(ns)
		if err != nil {
			slog.Info("Skipping unparseable nameserver", "value", ns, "error", err.Error())
			continue
		}
		out = append(out, netip.AddrPortFrom(addr, 53))
	}
	slog.Info("Loaded worker resolvers for sandbox DNS rewrite", "resolvers", out)
	return out
}
