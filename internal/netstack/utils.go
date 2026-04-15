//go:build linux

package netstack

import (
	"net/netip"

	"gvisor.dev/gvisor/pkg/tcpip"
)

// Unmap4in6 converts an IPv4-mapped IPv6 address to its IPv4 form.
// Non-mapped addresses are returned unchanged. If the resulting IPv4 address
// is zero, 127.0.0.1 is returned instead.
// Follows the /96 embedding scheme from RFC 6052.
func Unmap4in6(addr netip.Addr) netip.Addr {
	if !addr.Is4In6() {
		return addr
	}
	v4 := addr.Unmap()
	if !v4.IsValid() || v4.IsUnspecified() {
		return netip.AddrFrom4([4]byte{127, 0, 0, 1})
	}
	return v4
}

// addrFromNetstackIP converts a gVisor tcpip.Address to a standard netip.Addr.
func addrFromNetstackIP(ip tcpip.Address) netip.Addr {
	switch ip.Len() {
	case 4:
		return netip.AddrFrom4(ip.As4())
	case 16:
		return netip.AddrFrom16(ip.As16())
	}
	return netip.Addr{}
}

// ToFullAddress converts a netip.AddrPort to a gVisor tcpip.FullAddress.
func ToFullAddress(addrPort netip.AddrPort) *tcpip.FullAddress {
	if addrPort.Addr().Is4() {
		addrv4 := addrPort.Addr().As4()
		return &tcpip.FullAddress{
			Addr: tcpip.AddrFrom4Slice(addrv4[:]),
			Port: addrPort.Port(),
		}
	}
	addrv6 := addrPort.Addr().As16()
	return &tcpip.FullAddress{
		Addr: tcpip.AddrFrom16Slice(addrv6[:]),
		Port: addrPort.Port(),
	}
}
