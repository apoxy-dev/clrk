//go:build linux

package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dpeckett/contextio"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/egress"
	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
)

// egressHandshakeTimeout bounds how long the bridge waits for the
// PROXY v2 frame to arrive on a newly accepted conn. Sized like
// metadata.proxyHandshakeTimeout — the frame is ~64 bytes, half a
// second is generous without delaying real traffic.
const egressHandshakeTimeout = 500 * time.Millisecond

// EgressState is the per-sandbox snapshot the bridge consults on
// every accepted conn. The SandboxManager maintains one record per
// live sandbox and updates it from SetEgressBackends /
// SetEgressPolicy / SetInvocationID; the bridge reads it under
// RWMutex so the central decision is consistent with the
// dispatcher's most recent push.
type EgressState struct {
	Identity     proxyproto.AgentIdentity
	InvocationID string
	Backends     []egress.BackendListener
	Policy       *egress.SandboxPolicy
}

// EgressStateLookup resolves the per-sandbox egress snapshot at
// dial time. Returns (zero, false) when no sandbox is registered
// for the given ID — bridge answers with RST so a stray dial from
// an orphaned Sentry can't fall through into open egress.
type EgressStateLookup func(sandboxID string) (EgressState, bool)

// EgressBridge listens on the worker-bound egress port for TCP
// streams tunneled out of every Sentry's PluginStack. Each accepted
// conn is fronted with a PROXY v2 frame carrying SandboxID + the
// sandbox-visible (src, dst); the bridge resolves the per-sandbox
// state, applies policy, picks a backend, and dials the chosen
// upstream — direct for unrouted dsts, Envoy MITM with enriched
// PROXY v2 (identity + InvocationID) otherwise.
type EgressBridge struct {
	ln     net.Listener
	lookup EgressStateLookup

	wg     sync.WaitGroup
	stopCh chan struct{}
}

// NewEgressBridge binds a TCP listener on hostAddr ("127.0.0.1:port")
// and starts serving in the background. lookup is the SandboxManager-
// owned getter into the per-sandbox EgressState map.
func NewEgressBridge(hostAddr string, lookup EgressStateLookup) (*EgressBridge, error) {
	ln, err := net.Listen("tcp", hostAddr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", hostAddr, err)
	}
	b := &EgressBridge{
		ln:     ln,
		lookup: lookup,
		stopCh: make(chan struct{}),
	}
	b.wg.Add(1)
	go b.serve()
	return b, nil
}

// Addr returns the listener's bound address. Tests use this to
// dial the bridge without hardcoding a port.
func (b *EgressBridge) Addr() net.Addr {
	if b.ln == nil {
		return nil
	}
	return b.ln.Addr()
}

// Close stops accepting new conns. In-flight bridged streams keep
// running until either side closes; this method returns once the
// accept loop has exited but does not block on stream drains.
func (b *EgressBridge) Close() error {
	select {
	case <-b.stopCh:
	default:
		close(b.stopCh)
	}
	err := b.ln.Close()
	b.wg.Wait()
	return err
}

func (b *EgressBridge) serve() {
	defer b.wg.Done()
	for {
		conn, err := b.ln.Accept()
		if err != nil {
			select {
			case <-b.stopCh:
				return
			default:
			}
			if errors.Is(err, net.ErrClosed) {
				return
			}
			slog.Warn("Egress bridge accept failed", slog.Any("error", err))
			continue
		}
		go b.handleConn(conn)
	}
}

func (b *EgressBridge) handleConn(client net.Conn) {
	defer client.Close()

	// Bound the PROXY v2 frame read so a stuck or malformed peer
	// can't pin the goroutine.
	_ = client.SetReadDeadline(time.Now().Add(egressHandshakeTimeout))
	hdr, err := proxyproto.ParseHeader(client)
	if err != nil {
		slog.Warn("Egress bridge: parse PROXY v2", slog.Any("error", err))
		return
	}
	_ = client.SetReadDeadline(time.Time{})

	sandboxID := hdr.Identity.SandboxID
	origDst := hdr.Destination
	logger := slog.With(
		slog.String("sandbox.id", sandboxID),
		slog.String("src", hdr.Source.String()),
		slog.String("dst", origDst.String()),
		slog.String("dst.name", hdr.DstName),
	)

	state, ok := b.lookup(sandboxID)
	if !ok {
		logger.Warn("Egress bridge: no live sandbox for ID; dropping")
		return
	}

	if state.Policy != nil {
		proto := clrkv1alpha1.L4ProtocolTCP
		if !state.Policy.Allow(origDst, proto, hdr.DstName) {
			logger.Info("Egress dial denied",
				slog.String("agent.kind", fmt.Sprintf("%d", state.Identity.Kind)),
				slog.String("agent.namespace", state.Identity.Namespace),
				slog.String("agent.name", state.Identity.Name),
				slog.String("default_policy", string(state.Policy.DefaultPolicy())),
			)
			return
		}
	}

	backend := pickBackend(state.Backends, origDst.Port())
	logger.Info("Egress dial",
		slog.String("agent.kind", fmt.Sprintf("%d", state.Identity.Kind)),
		slog.String("agent.namespace", state.Identity.Namespace),
		slog.String("agent.name", state.Identity.Name),
		slog.String("agent.uid", state.Identity.UID),
		slog.String("agent.revision", state.Identity.Revision),
		slog.String("invocation.id", state.InvocationID),
		slog.String("mode", backendMode(backend)),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var d net.Dialer
	var upstream net.Conn

	if backend == nil {
		// Direct dial: no PROXY framing on the wire.
		upstream, err = d.DialContext(ctx, "tcp", origDst.String())
		if err != nil {
			logger.Warn("Egress direct dial failed", slog.Any("error", err))
			return
		}
	} else {
		upstream, err = d.DialContext(ctx, "tcp", backend.Addr)
		if err != nil {
			logger.Warn("Egress backend dial failed",
				slog.String("backend", backend.Addr),
				slog.Any("error", err))
			return
		}
		id := state.Identity
		id.InvocationID = state.InvocationID
		// SandboxID is intentionally NOT echoed on the Envoy-bound
		// frame — Envoy uses identity/InvocationID TLVs for
		// attribution; SandboxID is the worker-internal demux key.
		id.SandboxID = ""
		src := sanitizedSrc(upstream.LocalAddr(), origDst)
		// Propagate the Sentry-side DNS-cache binding (qname the
		// agent looked up to reach origDst.Addr()) so Envoy's
		// network_ext_proc can emit it as clrk.dst.name on the
		// L4Record.
		hdrBytes, encErr := proxyproto.EncodeHeader(src, origDst, id, hdr.DstName)
		if encErr != nil {
			logger.Warn("Egress backend PROXY v2 encode failed", slog.Any("error", encErr))
			_ = upstream.Close()
			return
		}
		if _, werr := upstream.Write(hdrBytes); werr != nil {
			logger.Warn("Egress backend PROXY v2 write failed", slog.Any("error", werr))
			_ = upstream.Close()
			return
		}
	}
	defer upstream.Close()

	if _, err := contextio.SpliceContext(ctx, client, upstream, nil); err != nil {
		if errors.Is(err, context.Canceled) || isBenignEgressClose(err) {
			logger.Debug("Egress splice peer-close", slog.Any("error", err))
			return
		}
		logger.Warn("Egress splice error", slog.Any("error", err))
	}
}

func backendMode(backend *egress.BackendListener) string {
	if backend == nil {
		return "direct"
	}
	return "egress-gateway"
}

// pickBackend selects the best-matching configured listener for the
// sandbox's destination port. Most-specific wins (MatchPort-constrained
// before catch-all); within a tie, BackendListener.Priority decides.
// Returns nil when no listener matches or all matches have empty Addr.
// Lifted unchanged from the retired internal/egress/identity_dialer.go
// pickBackend — this is the same selection algorithm the worker used
// before the gVisor migration; the only thing that changed is where
// it runs.
func pickBackend(backends []egress.BackendListener, dstPort uint16) *egress.BackendListener {
	var best *egress.BackendListener
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

// sanitizedSrc returns the upstream conn's local addr as a netip.AddrPort,
// falling back to the destination family with a zero addr when the
// conn's local addr can't be parsed. Echoes the helper that lived in
// the retired internal/egress/identity_dialer.go.
func sanitizedSrc(la net.Addr, dst netip.AddrPort) netip.AddrPort {
	if tcpAddr, ok := la.(*net.TCPAddr); ok && tcpAddr != nil {
		addr, _ := netip.AddrFromSlice(tcpAddr.IP)
		addr = addr.Unmap()
		return netip.AddrPortFrom(addr, uint16(tcpAddr.Port))
	}
	zero := netip.AddrFrom4([4]byte{})
	if dst.Addr().Is6() && !dst.Addr().Is4In6() {
		zero = netip.AddrFrom16([16]byte{})
	}
	return netip.AddrPortFrom(zero, 0)
}

// isBenignEgressClose tells routine close-on-write (ECONNRESET after
// the upstream finished writing, broken pipe in the reverse direction)
// apart from real forwarding failures.
func isBenignEgressClose(err error) bool {
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "broken pipe")
}
