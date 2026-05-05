//go:build linux

package egress

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/netip"

	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
	"github.com/apoxy-dev/clrk/internal/netstack"
)

// IdentityDialer wraps a base netstack Dialer. When an EgressGateway
// backend is configured it redirects every dial to one of the
// configured backend listeners and prepends a PROXY v2 frame so Envoy
// can attribute the connection to this sandbox's agent. With no
// backends it delegates directly to the wrapped dialer.
type IdentityDialer struct {
	Base     netstack.Dialer
	Identity proxyproto.AgentIdentity

	// Backends are the EG listeners this sandbox can be steered to.
	// Multiple entries cover the protocol-split listener model:
	// e.g. one tls-terminate listener + one tcp listener on
	// distinct ports. Empty slice = direct-dial only (no MITM).
	Backends []BackendListener

	// DNSResolvers, when non-empty, redirects every :53 dial to the
	// first entry. Required because the sandbox's resolv.conf points at
	// its TAP gateway IP — an address that exists nowhere — so DNS
	// packets actually route via the netstack instead of getting stuck
	// on the sandbox's loopback. The dial happens from the worker
	// netns, so loopback nameservers like Docker's embedded resolver at
	// 127.0.0.11 are reachable here.
	DNSResolvers []netip.AddrPort

	// DNSCache supplies the DNS-bound destination name per dial:
	// emitted as PROXY v2 TLVDstName (Backend mode) and threaded
	// into ctx for Router hostname matching (passthrough mode).
	// Nil-safe; direct-IP / expired lookups return "".
	DNSCache *netstack.DNSCache
}

// pickBackend selects the best-matching configured listener for the
// sandbox's destination port. Most-specific wins (MatchPort-constrained
// before catch-all); within a tie, BackendListener.Priority decides.
// Returns nil when no listener matches or all matches have empty Addr.
func (d *IdentityDialer) pickBackend(dstPort uint16) *BackendListener {
	var best *BackendListener
	bestSpecific := false
	for i := range d.Backends {
		b := &d.Backends[i]
		if b.Addr == "" {
			continue
		}
		specific := b.MatchPort != 0 && uint16(b.MatchPort) == dstPort
		if b.MatchPort != 0 && !specific {
			continue
		}
		if best == nil {
			best, bestSpecific = b, specific
			continue
		}
		if specific && !bestSpecific {
			best, bestSpecific = b, specific
			continue
		}
		if specific == bestSpecific && b.Priority > best.Priority {
			best = b
		}
	}
	return best
}

var _ netstack.Dialer = (*IdentityDialer)(nil)

// DialContext satisfies netstack.Dialer. Every dial is logged with the
// owning agent's identity and the original sandbox destination, so even
// in direct-dial mode (no MITM backend wired up) the worker still emits
// an attributable record per outbound connection.
func (d *IdentityDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	origAddr := addr
	// Parse once. Every code path below — DNS rewrite, cache
	// lookup, EG-backend PROXY v2 framing — wants the dst as
	// netip.AddrPort. Tests pass non-host:port forms (unix sockets);
	// fall back to the string for those.
	origDst, parseErr := netip.ParseAddrPort(addr)

	var dnsRewrite bool
	if parseErr == nil {
		if rewritten, ok := d.rewriteDNS(origDst); ok {
			addr = rewritten
			dnsRewrite = true
		}
	}

	var backend *BackendListener
	if !dnsRewrite && parseErr == nil {
		backend = d.pickBackend(origDst.Port())
	}
	d.logDial(network, origAddr, addr, dnsRewrite, backend)

	var dstName string
	if !dnsRewrite && parseErr == nil && d.DNSCache != nil {
		dstName = d.DNSCache.Lookup(origDst.Addr())
	}

	// DNS dials must bypass the egress-gateway path — the EG listener is
	// HTTPS-only and would silently drop UDP/53 packets wrapped in PROXY
	// v2. The destination has already been rewritten to the worker's real
	// resolver; just hand off to the base dialer.
	if backend == nil || dnsRewrite {
		return d.Base.DialContext(withDstName(ctx, dstName), network, addr)
	}

	if parseErr != nil {
		return nil, fmt.Errorf("parsing original destination %q: %w", origAddr, parseErr)
	}

	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, network, backend.Addr)
	if err != nil {
		return nil, fmt.Errorf("dialing egress backend %s (%s): %w", backend.Addr, backend.Name, err)
	}

	src := sanitizedSrc(conn.LocalAddr(), origDst)
	hdr, err := proxyproto.EncodeHeader(src, origDst, d.Identity, dstName)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("encoding PROXY v2: %w", err)
	}
	if _, err := conn.Write(hdr); err != nil {
		conn.Close()
		return nil, fmt.Errorf("writing PROXY v2 header: %w", err)
	}
	return conn, nil
}

// logDial emits one structured slog line per outbound dial with agent
// identity. This is the L4 attribution surface — useful even before the
// L7 ext_proc + OTLP path is operational. Logged at INFO so it shows up
// in `clrk dev logs` and production worker stdout without extra config.
//
// origDst is what the sandbox addressed; effDst is what we actually
// dial out (differs only when DNS rewrite kicks in).
func (d *IdentityDialer) logDial(network, origDst, effDst string, dnsRewrite bool, backend *BackendListener) {
	mode := "direct"
	backendAddr := ""
	backendName := ""
	backendShape := ""
	if backend != nil && !dnsRewrite {
		mode = "egress-gateway"
		backendAddr = backend.Addr
		backendName = backend.Name
		backendShape = backend.Shape
	}
	slog.Info("egress dial",
		"agent.kind", d.Identity.Kind,
		"agent.namespace", d.Identity.Namespace,
		"agent.name", d.Identity.Name,
		"agent.uid", d.Identity.UID,
		"agent.revision", d.Identity.Revision,
		"invocation.id", d.Identity.InvocationID,
		"network", network,
		"dst", origDst,
		"effective_dst", effDst,
		"mode", mode,
		"backend", backendAddr,
		"backend.name", backendName,
		"backend.shape", backendShape,
	)
}

// rewriteDNS swaps a :53 destination for the first configured worker
// resolver. Returns (rewritten, true) if it triggered, ("", false)
// otherwise.
func (d *IdentityDialer) rewriteDNS(dst netip.AddrPort) (string, bool) {
	if len(d.DNSResolvers) == 0 || dst.Port() != 53 {
		return "", false
	}
	return d.DNSResolvers[0].String(), true
}

// sanitizedSrc returns the conn's local address as a netip.AddrPort,
// falling back to the destination family with a zero addr when the
// conn's local addr can't be parsed (e.g. unix sockets in tests).
func sanitizedSrc(la net.Addr, dst netip.AddrPort) netip.AddrPort {
	if tcpAddr, ok := la.(*net.TCPAddr); ok && tcpAddr != nil {
		addr, _ := netip.AddrFromSlice(tcpAddr.IP)
		addr = addr.Unmap()
		return netip.AddrPortFrom(addr, uint16(tcpAddr.Port))
	}
	return netip.AddrPortFrom(zeroAddrFor(dst.Addr()), 0)
}

func zeroAddrFor(a netip.Addr) netip.Addr {
	if a.Is4() {
		return netip.AddrFrom4([4]byte{})
	}
	return netip.AddrFrom16([16]byte{})
}
