//go:build linux

package sandbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/dpeckett/contextio"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	otellog "go.opentelemetry.io/otel/log"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/egress"
	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
	"github.com/apoxy-dev/clrk/internal/otelemit"
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
	Identity proxyproto.AgentIdentity
	Backends []egress.BackendListener
	Policy   *egress.SandboxPolicy
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
	ln      net.Listener
	lookup  EgressStateLookup
	filter  *localDstFilter
	emitter otelemit.Emitter

	wg     sync.WaitGroup
	stopCh chan struct{}
}

// NewEgressBridge binds a TCP listener on hostAddr ("127.0.0.1:port")
// and starts serving in the background. lookup is the SandboxManager-
// owned getter into the per-sandbox EgressState map. emitter is the
// worker-wide OTLP emitter used to emit egress.dial.{denied,failed} and
// egress.proxy.malformed alongside the existing slog trail; a nil
// emitter is replaced with otelemit.Noop() so the bridge can emit
// unconditionally. Snapshots the worker's local interface IPs up front
// for the worker-local destination filter; a failure to enumerate
// interfaces is fatal so the runtime crashes loudly rather than
// running with a half-built deny set.
func NewEgressBridge(hostAddr string, lookup EgressStateLookup, emitter otelemit.Emitter) (*EgressBridge, error) {
	filter, err := newLocalDstFilter()
	if err != nil {
		return nil, fmt.Errorf("build local destination filter: %w", err)
	}
	return newBridge(hostAddr, lookup, filter, emitter)
}

// newBridge is the shared constructor used by NewEgressBridge and the
// test seams in testseams.go. Centralises the listen + struct + serve
// goroutine setup so behavior can't drift between the production and
// test paths.
func newBridge(hostAddr string, lookup EgressStateLookup, filter *localDstFilter, emitter otelemit.Emitter) (*EgressBridge, error) {
	ln, err := net.Listen("tcp", hostAddr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", hostAddr, err)
	}
	if emitter == nil {
		emitter = otelemit.Noop()
	}
	b := &EgressBridge{
		ln:      ln,
		lookup:  lookup,
		filter:  filter,
		emitter: emitter,
		stopCh:  make(chan struct{}),
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
		b.emitMalformed(context.Background(), client.RemoteAddr(), err)
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
		// hdr.Identity is the orphan's claimed identity from the
		// PROXY v2 frame; not authoritative (no live state to back
		// it) but it's the best signal we have for attribution.
		b.emitDeny(
			context.Background(),
			otelemit.DenyReasonOrphanSandbox,
			b.commonDialAttrs(hdr.Identity, sandboxID, origDst, hdr.DstName),
			"egress bridge: no live sandbox for ID",
		)
		return
	}

	// Reject sandbox-supplied destinations that point back at the
	// worker itself. The bridge runs in the worker's netns (see
	// oci_spec.go: NetworkNamespace pinned to /proc/self/ns/net), so
	// loopback / worker pod IPs would otherwise reach control surfaces
	// like the dispatcher (:8090).
	if reason, deny := b.filter.deny(origDst); deny {
		logger.Warn("Egress dial denied: worker-local destination", append(
			identityLogFields(state.Identity),
			slog.String("reason", string(reason)),
		)...)
		b.emitDeny(
			context.Background(),
			reason,
			b.commonDialAttrs(state.Identity, sandboxID, origDst, hdr.DstName),
			"egress dial denied: worker-local destination",
		)
		return
	}

	if state.Policy != nil {
		proto := clrkv1alpha1.L4ProtocolTCP
		if !state.Policy.Allow(origDst, proto, hdr.DstName) {
			logger.Info("Egress dial denied", append(
				identityLogFields(state.Identity),
				slog.String("default_policy", string(state.Policy.DefaultPolicy())),
			)...)
			attrs := b.commonDialAttrs(state.Identity, sandboxID, origDst, hdr.DstName)
			attrs = append(attrs, attribute.String(
				otelemit.AttrEgressPolicyDefault,
				string(state.Policy.DefaultPolicy()),
			))
			b.emitDeny(
				context.Background(),
				otelemit.DenyReasonPolicy,
				attrs,
				"egress dial denied: policy",
			)
			return
		}
	}

	backend := pickBackend(state.Backends, origDst.Port())
	logger.Info("Egress dial", append(
		identityLogFields(state.Identity),
		slog.String("mode", backendMode(backend)),
	)...)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var d net.Dialer
	var upstream net.Conn

	if backend == nil {
		// Direct dial: no PROXY framing on the wire.
		upstream, err = d.DialContext(ctx, "tcp", origDst.String())
		if err != nil {
			logger.Warn("Egress direct dial failed", slog.Any("error", err))
			b.emitFailure(
				ctx,
				otelemit.FailureReasonDirectDial,
				err,
				b.commonDialAttrs(state.Identity, sandboxID, origDst, hdr.DstName),
				"egress direct dial failed",
			)
			return
		}
	} else {
		upstream, err = d.DialContext(ctx, "tcp", backend.Addr)
		if err != nil {
			logger.Warn("Egress backend dial failed",
				slog.String("backend", backend.Addr),
				slog.Any("error", err))
			attrs := b.commonDialAttrs(state.Identity, sandboxID, origDst, hdr.DstName)
			attrs = append(attrs, attribute.String(otelemit.AttrEgressBackendAddr, backend.Addr))
			b.emitFailure(
				ctx,
				otelemit.FailureReasonBackendDial,
				err,
				attrs,
				"egress backend dial failed",
			)
			return
		}
		// SandboxID is intentionally NOT echoed on the Envoy-bound
		// frame — Envoy uses identity/InvocationID TLVs for
		// attribution; SandboxID is the worker-internal demux key.
		id := state.Identity
		id.SandboxID = ""
		src := sanitizedSrc(upstream.LocalAddr(), origDst)
		// Propagate the Sentry-side DNS-cache binding (qname the
		// agent looked up to reach origDst.Addr()) so Envoy's
		// network_ext_proc can emit it as clrk.dst.name on the
		// L4Record.
		hdrBytes, encErr := proxyproto.EncodeHeader(src, origDst, id, hdr.DstName)
		if encErr != nil {
			logger.Warn("Egress backend PROXY v2 encode failed", slog.Any("error", encErr))
			attrs := b.commonDialAttrs(state.Identity, sandboxID, origDst, hdr.DstName)
			attrs = append(attrs, attribute.String(otelemit.AttrEgressBackendAddr, backend.Addr))
			b.emitFailure(
				ctx,
				otelemit.FailureReasonProxyEncode,
				encErr,
				attrs,
				"egress backend PROXY v2 encode failed",
			)
			_ = upstream.Close()
			return
		}
		if _, werr := upstream.Write(hdrBytes); werr != nil {
			logger.Warn("Egress backend PROXY v2 write failed", slog.Any("error", werr))
			attrs := b.commonDialAttrs(state.Identity, sandboxID, origDst, hdr.DstName)
			attrs = append(attrs, attribute.String(otelemit.AttrEgressBackendAddr, backend.Addr))
			b.emitFailure(
				ctx,
				otelemit.FailureReasonProxyWrite,
				werr,
				attrs,
				"egress backend PROXY v2 write failed",
			)
			_ = upstream.Close()
			return
		}
	}
	defer upstream.Close()

	if _, err := contextio.SpliceContext(ctx, client, upstream, nil); err != nil {
		if errors.Is(err, context.Canceled) || egress.IsBenignClose(err) {
			logger.Debug("Egress splice peer-close", slog.Any("error", err))
			return
		}
		logger.Warn("Egress splice error", slog.Any("error", err))
	}
}

// commonDialAttrs builds the shared attribute prefix for every
// egress.dial.{denied,failed} emission: agent identity, sandbox id,
// dst name + addr + port + transport. Per-event extras (deny reason,
// failure reason, policy default, backend addr) are appended at the
// call site.
func (b *EgressBridge) commonDialAttrs(
	id proxyproto.AgentIdentity,
	sandboxID string,
	dst netip.AddrPort,
	dstName string,
) []attribute.KeyValue {
	attrs := otelemit.AgentAttrs(id)
	attrs = append(attrs,
		attribute.String(otelemit.AttrSandboxID, sandboxID),
		attribute.String(otelemit.AttrL4DstName, dstName),
		semconv.NetworkPeerAddress(dst.Addr().String()),
		semconv.NetworkPeerPort(int(dst.Port())),
		semconv.NetworkTransportTCP,
	)
	return attrs
}

// emitDeny publishes one OTLP span and one OTLP log record for an
// EgressBridge refusal. The slog line is always emitted at the call
// site beforehand — see `feedback_envoy_logging_endpoint` reasoning:
// stderr is the local trail that survives OTLP exporter outages.
func (b *EgressBridge) emitDeny(
	ctx context.Context,
	reason otelemit.DenyReason,
	extra []attribute.KeyValue,
	bodyMsg string,
) {
	attrs := append(extra,
		attribute.String(otelemit.AttrEgressDenyReason, string(reason)),
	)
	_, span := b.emitter.Tracer().Start(ctx, "egress.dial.denied",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(attrs...),
	)
	span.SetStatus(codes.Error, string(reason))
	span.End()

	var rec otellog.Record
	rec.SetSeverity(otellog.SeverityWarn)
	rec.SetSeverityText("WARN")
	rec.SetBody(otellog.StringValue(bodyMsg))
	otelemit.AddLogAttrs(&rec, attrs)
	b.emitter.Logger().Emit(ctx, rec)
}

// emitFailure publishes one OTLP span and one OTLP log record for a
// post-allow upstream or handoff failure. err is attached via
// span.RecordError and an "error" log attribute.
func (b *EgressBridge) emitFailure(
	ctx context.Context,
	reason otelemit.FailureReason,
	err error,
	extra []attribute.KeyValue,
	bodyMsg string,
) {
	attrs := append(extra,
		attribute.String(otelemit.AttrEgressFailureReason, string(reason)),
	)
	_, span := b.emitter.Tracer().Start(ctx, "egress.dial.failed",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(attrs...),
	)
	span.RecordError(err)
	span.SetStatus(codes.Error, string(reason))
	span.End()

	var rec otellog.Record
	rec.SetSeverity(otellog.SeverityWarn)
	rec.SetSeverityText("WARN")
	rec.SetBody(otellog.StringValue(bodyMsg))
	otelemit.AddLogAttrs(&rec, attrs)
	rec.AddAttributes(otellog.String("error", err.Error()))
	b.emitter.Logger().Emit(ctx, rec)
}

// emitMalformed publishes one OTLP span and one OTLP log record for a
// PROXY v2 parse failure. No identity/dst attrs are available — the
// frame couldn't be parsed — so this site uses its own minimal
// attribute schema rather than commonDialAttrs.
func (b *EgressBridge) emitMalformed(ctx context.Context, remote net.Addr, err error) {
	attrs := []attribute.KeyValue{
		attribute.String(otelemit.AttrEgressProxyError, err.Error()),
	}
	if remote != nil {
		attrs = append(attrs, semconv.ClientAddress(remote.String()))
	}
	_, span := b.emitter.Tracer().Start(ctx, "egress.proxy.malformed",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(attrs...),
	)
	span.RecordError(err)
	span.SetStatus(codes.Error, "proxy_v2_parse")
	span.End()

	var rec otellog.Record
	rec.SetSeverity(otellog.SeverityWarn)
	rec.SetSeverityText("WARN")
	rec.SetBody(otellog.StringValue("egress bridge: PROXY v2 parse failed"))
	otelemit.AddLogAttrs(&rec, attrs)
	b.emitter.Logger().Emit(ctx, rec)
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

// localDstFilter rejects sandbox-supplied destinations that point at
// the worker itself. See handleConn for the call-site reasoning. IMDS
// targets are pre-empted in forwarder.go's IMDS bridge before reaching
// this path, so blanket-denying link-local here is safe.
type localDstFilter struct {
	// K8s pod IPs are stable for a pod's lifetime, so we snapshot
	// once at construction rather than refreshing on every conn.
	localIPs map[netip.Addr]struct{}
}

// newLocalDstFilter snapshots all IPs assigned to the worker's
// interfaces.
func newLocalDstFilter() (*localDstFilter, error) {
	ifs, err := net.Interfaces()
	if err != nil {
		return nil, fmt.Errorf("enumerate interfaces: %w", err)
	}
	ips := make(map[netip.Addr]struct{})
	for _, ifi := range ifs {
		addrs, err := ifi.Addrs()
		if err != nil {
			return nil, fmt.Errorf("addrs for %s: %w", ifi.Name, err)
		}
		for _, a := range addrs {
			var ip net.IP
			switch v := a.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			default:
				continue
			}
			na, ok := netip.AddrFromSlice(ip)
			if !ok {
				continue
			}
			ips[na.Unmap()] = struct{}{}
		}
	}
	return &localDstFilter{localIPs: ips}, nil
}

// deny reports whether dst is a worker-local destination that must
// not be reached via the egress bridge, plus the categorical reason.
// A nil receiver allows everything (see testseams.go).
func (f *localDstFilter) deny(dst netip.AddrPort) (otelemit.DenyReason, bool) {
	if f == nil {
		return "", false
	}
	addr := dst.Addr()
	if addr.IsLoopback() {
		return otelemit.DenyReasonLoopback, true
	}
	if _, ok := f.localIPs[addr]; ok {
		return otelemit.DenyReasonWorkerLocalIfAddr, true
	}
	switch {
	case addr.IsUnspecified():
		return otelemit.DenyReasonUnspecified, true
	case addr.IsLinkLocalUnicast():
		return otelemit.DenyReasonLinkLocal, true
	case addr.IsMulticast():
		return otelemit.DenyReasonMulticast, true
	}
	return "", false
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

