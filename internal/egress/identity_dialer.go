package egress

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/netip"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
	"github.com/apoxy-dev/clrk/internal/netstack"
)

// DialSlot bundles the per-sandbox, per-dispatch state the
// IdentityDialer needs at dial time: the InvocationID stamped into
// proxyproto TLVs, the EG backend listeners this sandbox can be
// steered to, and the policy handle that decides allow/deny. The
// RevisionStack owns a map keyed by per-sandbox source IP; the
// dispatcher writes the slot fields between Acquire and Start and
// clears them on dispatch teardown.
type DialSlot struct {
	InvocationID string
	Backends     []BackendListener
	Policy       *SandboxPolicy
}

// SlotLookupFunc resolves the live DialSlot for a sandbox identified
// by its per-NIC source IP on the shared RevisionStack. Returns
// (zero, false) when no sandbox is attached for that source — falls
// through to a direct-dial-no-policy path that matches DaemonAgent
// semantics.
type SlotLookupFunc func(srcIP netip.Addr) (DialSlot, bool)

// IdentityDialer wraps a base netstack Dialer. It is constructed
// once per (TaskAgent, revision) by the worker's RevisionStackManager
// and shared across every concurrent sandbox of that revision —
// per-dispatch state (InvocationID, Backends, Policy) is resolved
// per-dial via SlotLookup, keyed by the intercepted connection's
// source IP (threaded through ctx by the TCP forwarder).
//
// Sharing the dialer fixes a latent staleness bug in the old
// per-sandbox model where InvocationID was captured on the dialer
// at sandbox Start and never refreshed for subsequent warm
// dispatches.
type IdentityDialer struct {
	Base     netstack.Dialer
	Identity proxyproto.AgentIdentity

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
	// Shared per-revision; entries are keyed by resolved IP so
	// cross-sandbox sharing is correctness-safe.
	DNSCache *netstack.DNSCache

	// SlotLookup resolves the per-sandbox, per-dispatch state from
	// the connection's source IP. Nil-safe — a nil lookup falls
	// through to direct-dial with no proxyproto framing (the path
	// DaemonAgents take today).
	SlotLookup SlotLookupFunc
}

// pickBackend selects the best-matching configured listener for the
// sandbox's destination port. Most-specific wins (MatchPort-constrained
// before catch-all); within a tie, BackendListener.Priority decides.
// Returns nil when no listener matches or all matches have empty Addr.
func (d *IdentityDialer) pickBackend(backends []BackendListener, dstPort uint16) *BackendListener {
	var best *BackendListener
	bestSpecific := false
	for i := range backends {
		b := &backends[i]
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

// resolveSlot reads the per-dispatch slot for the source IP carried
// in ctx by the TCP forwarder. Returns (zero, false) when no slot is
// registered — DaemonAgent dials and dials originating outside the
// RevisionStack flow take this path and direct-dial without
// proxyproto framing.
func (d *IdentityDialer) resolveSlot(ctx context.Context) (DialSlot, bool) {
	if d.SlotLookup == nil {
		return DialSlot{}, false
	}
	src := netstack.SourceAddrFromContext(ctx)
	if !src.IsValid() {
		return DialSlot{}, false
	}
	return d.SlotLookup(src)
}

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

	var dstName string
	if !dnsRewrite && parseErr == nil && d.DNSCache != nil {
		dstName = d.DNSCache.Lookup(origDst.Addr())
	}

	slot, slotOK := d.resolveSlot(ctx)

	// Policy check before backend selection. Applies to both the EG
	// backend path and the direct-dial fall-through, so default-deny
	// denies even traffic that would otherwise match a listener.
	// DNS rewrites bypass: the rewrite target is a worker-internal
	// resolver, not the agent's intended destination — denying it
	// would break name resolution for permitted destinations.
	if !dnsRewrite && parseErr == nil && slotOK && slot.Policy != nil {
		proto := l4ProtocolFromNetwork(network)
		if !slot.Policy.Allow(origDst, proto, dstName) {
			d.logDeny(slot, network, origAddr, dstName)
			return nil, fmt.Errorf("connection to %s denied by egress policy", origAddr)
		}
	}

	var backend *BackendListener
	if !dnsRewrite && parseErr == nil && slotOK {
		backend = d.pickBackend(slot.Backends, origDst.Port())
	}
	d.logDial(slot, network, origAddr, addr, dnsRewrite, backend)

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
	id := d.Identity
	id.InvocationID = slot.InvocationID
	hdr, err := proxyproto.EncodeHeader(src, origDst, id, dstName)
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
func (d *IdentityDialer) logDial(slot DialSlot, network, origDst, effDst string, dnsRewrite bool, backend *BackendListener) {
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
		"invocation.id", slot.InvocationID,
		"network", network,
		"dst", origDst,
		"effective_dst", effDst,
		"mode", mode,
		"backend", backendAddr,
		"backend.name", backendName,
		"backend.shape", backendShape,
	)
}

// logDeny emits a structured slog line on a policy-denied dial. Same
// shape as logDial so log consumers can join allow/deny on the agent
// identity tuple.
func (d *IdentityDialer) logDeny(slot DialSlot, network, origDst, dstName string) {
	defaultPolicy := ""
	if slot.Policy != nil {
		defaultPolicy = string(slot.Policy.DefaultPolicy())
	}
	slog.Info("egress dial denied",
		"agent.kind", d.Identity.Kind,
		"agent.namespace", d.Identity.Namespace,
		"agent.name", d.Identity.Name,
		"agent.uid", d.Identity.UID,
		"agent.revision", d.Identity.Revision,
		"invocation.id", slot.InvocationID,
		"network", network,
		"dst", origDst,
		"dst.name", dstName,
		"default_policy", defaultPolicy,
	)
}

// l4ProtocolFromNetwork maps a Go net package network string to the
// CRD-level L4Protocol used for route matching. Unknown networks
// (unix, etc.) return "" — which never matches a Protocol-pinned
// route, so policy enforcement falls through to the default.
func l4ProtocolFromNetwork(network string) clrkv1alpha1.L4Protocol {
	switch network {
	case "tcp", "tcp4", "tcp6":
		return clrkv1alpha1.L4ProtocolTCP
	case "udp", "udp4", "udp6":
		return clrkv1alpha1.L4ProtocolUDP
	}
	return ""
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
