//go:build linux

package worker

import (
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"syscall"

	"github.com/docker/docker/libnetwork/resolvconf"
	"github.com/opencontainers/runc/libcontainer/configs"
	ctrl "sigs.k8s.io/controller-runtime"
)

// readWorkerResolvers parses the worker's own /etc/resolv.conf via
// libnetwork/resolvconf (the same parser dockerd uses) and returns one
// AddrPort per nameserver entry, all on port 53.
//
// These addresses are the *destination* the IdentityDialer rewrites DNS
// queries to. The dial happens from the worker's network namespace, so
// loopback entries like Docker's embedded resolver at 127.0.0.11 are
// reachable here even though they aren't in the sandbox netns.
func readWorkerResolvers() []netip.AddrPort {
	log := ctrl.Log.WithName("worker.dns")
	host, err := resolvconf.GetSpecific("/etc/resolv.conf")
	if err != nil {
		log.Error(err, "Reading worker /etc/resolv.conf, sandbox DNS will fail")
		return nil
	}
	nameservers := resolvconf.GetNameservers(host.Content, resolvconf.IP)
	out := make([]netip.AddrPort, 0, len(nameservers))
	for _, ns := range nameservers {
		addr, err := netip.ParseAddr(ns)
		if err != nil {
			log.Info("Skipping unparseable nameserver", "value", ns, "error", err.Error())
			continue
		}
		out = append(out, netip.AddrPortFrom(addr, 53))
	}
	log.Info("Loaded worker resolvers for sandbox DNS rewrite", "resolvers", out)
	return out
}

// writeSandboxResolvConf materializes a per-sandbox /etc/resolv.conf
// pointing at the netns gateway IP. The file is bind-mounted over the
// sandbox's /etc/resolv.conf at container-start time.
//
// We can't use a loopback nameserver (e.g. dockerd's 127.0.0.11) here
// even though that's what the worker has in its own resolv.conf:
// loopback addresses route via `lo` in the sandbox netns, where nothing
// listens on :53, so the DNS query never reaches the gVisor netstack.
// The gateway IP, on the other hand, is the only off-link address the
// sandbox's default route knows about — packets to it land on the TAP
// device and get picked up by the netstack's UDP forwarder. The
// IdentityDialer then rewrites the destination back to the worker's
// real resolver before dialing out.
func (m *SandboxManager) writeSandboxResolvConf(id SandboxID, gw netip.Addr) (string, error) {
	dir := filepath.Join(m.rootDir, string(id)+"-net")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("creating netconfig dir: %w", err)
	}
	path := filepath.Join(dir, "resolv.conf")
	// ndots:0 keeps glibc/musl from prepending search domains and
	// burning round-trips on suffixed queries that will all NXDOMAIN.
	content := fmt.Sprintf("nameserver %s\noptions ndots:0\n", gw.String())
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return "", fmt.Errorf("writing resolv.conf: %w", err)
	}
	return path, nil
}

// removeSandboxNetConfig cleans up the per-sandbox netconfig dir on
// sandbox delete.
func (m *SandboxManager) removeSandboxNetConfig(id SandboxID) {
	_ = os.RemoveAll(filepath.Join(m.rootDir, string(id)+"-net"))
}

// buildResolvMount returns a bind mount of the per-sandbox resolv.conf
// over /etc/resolv.conf inside the container.
func buildResolvMount(source string) *configs.Mount {
	return &configs.Mount{
		Source:      source,
		Destination: "/etc/resolv.conf",
		Device:      "bind",
		Flags:       syscall.MS_BIND | syscall.MS_RDONLY,
	}
}
