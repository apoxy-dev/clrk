//go:build linux

package egress

import (
	"context"
	"fmt"
	"net"
	"net/netip"

	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
	"github.com/apoxy-dev/clrk/internal/netstack"
)

// IdentityDialer wraps a base netstack Dialer. When an EgressGateway
// backend is configured it redirects every dial to that backend and
// prepends a PROXY v2 frame so Envoy can attribute the connection to this
// sandbox's agent. With no backend it delegates directly to the wrapped
// dialer.
type IdentityDialer struct {
	Base    netstack.Dialer
	Identity proxyproto.AgentIdentity

	// Backend, when non-empty, is the "host:port" address of the Envoy
	// Gateway egress listener. Dials are re-pointed there + PROXY v2
	// framed.
	Backend string
}

var _ netstack.Dialer = (*IdentityDialer)(nil)

// DialContext satisfies netstack.Dialer.
func (d *IdentityDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if d.Backend == "" {
		return d.Base.DialContext(ctx, network, addr)
	}

	dst, err := netip.ParseAddrPort(addr)
	if err != nil {
		return nil, fmt.Errorf("parsing original destination %q: %w", addr, err)
	}

	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, network, d.Backend)
	if err != nil {
		return nil, fmt.Errorf("dialing egress backend %s: %w", d.Backend, err)
	}

	src := sanitizedSrc(conn.LocalAddr(), dst)
	hdr, err := proxyproto.EncodeHeader(src, dst, d.Identity)
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
