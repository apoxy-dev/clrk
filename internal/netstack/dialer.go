package netstack

import (
	"context"
	"net"
)

// Dialer abstracts upstream connection establishment for the TCP
// and UDP forwarders. The egress Router implements it to apply
// routing decisions; the IdentityDialer wraps a base Dialer to add
// proxyproto framing and policy enforcement per dial.
//
// Defined in a platform-agnostic file so test harnesses on non-linux
// hosts can also construct fakes (gVisor itself is linux-only, but
// the interface and the IdentityDialer-side logic that consumes it
// are not).
type Dialer interface {
	DialContext(ctx context.Context, network, addr string) (net.Conn, error)
}
