package proxyproto

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
)

// PROXY v2 wire format (see https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt).
var signature = []byte{
	0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D,
	0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A,
}

const (
	versionCmdV2Proxy byte = 0x21 // v2 (0x20) | PROXY (0x01)

	famInet  byte = 0x11 // AF_INET  | STREAM
	famInet6 byte = 0x21 // AF_INET6 | STREAM
)

// EncodeHeader builds a PROXY v2 header for a TCP connection from src → dst,
// tagged with the given agent identity as custom TLVs.
//
// dstName is the DNS-bound destination name for this connection (the
// hostname the agent's resolver answered for dst.Addr). Empty when no
// binding existed — direct-IP dial, DNS bypass, or expired entry —
// and the dst-name TLV is then omitted.
//
// src and dst must be the same address family (both IPv4 or both IPv6).
// Callers typically pass the sandbox's gateway-assigned src and the original
// upstream dst observed by the netstack TCP forwarder.
func EncodeHeader(src, dst netip.AddrPort, id AgentIdentity, dstName string) ([]byte, error) {
	if src.Addr().Is4In6() {
		src = netip.AddrPortFrom(src.Addr().Unmap(), src.Port())
	}
	if dst.Addr().Is4In6() {
		dst = netip.AddrPortFrom(dst.Addr().Unmap(), dst.Port())
	}
	if src.Addr().Is4() != dst.Addr().Is4() {
		return nil, errors.New("proxyproto: src/dst address family mismatch")
	}

	var addrBlock []byte
	var family byte
	if src.Addr().Is4() {
		family = famInet
		srcB := src.Addr().As4()
		dstB := dst.Addr().As4()
		addrBlock = make([]byte, 0, 12)
		addrBlock = append(addrBlock, srcB[:]...)
		addrBlock = append(addrBlock, dstB[:]...)
		addrBlock = binary.BigEndian.AppendUint16(addrBlock, src.Port())
		addrBlock = binary.BigEndian.AppendUint16(addrBlock, dst.Port())
	} else {
		family = famInet6
		srcB := src.Addr().As16()
		dstB := dst.Addr().As16()
		addrBlock = make([]byte, 0, 36)
		addrBlock = append(addrBlock, srcB[:]...)
		addrBlock = append(addrBlock, dstB[:]...)
		addrBlock = binary.BigEndian.AppendUint16(addrBlock, src.Port())
		addrBlock = binary.BigEndian.AppendUint16(addrBlock, dst.Port())
	}

	tlvs := encodeTLVs(id, dstName)

	payloadLen := len(addrBlock) + len(tlvs)
	if payloadLen > 0xFFFF {
		return nil, fmt.Errorf("proxyproto: payload too large (%d bytes)", payloadLen)
	}

	hdr := make([]byte, 0, 16+payloadLen)
	hdr = append(hdr, signature...)
	hdr = append(hdr, versionCmdV2Proxy, family)
	hdr = binary.BigEndian.AppendUint16(hdr, uint16(payloadLen))
	hdr = append(hdr, addrBlock...)
	hdr = append(hdr, tlvs...)
	return hdr, nil
}

func encodeTLVs(id AgentIdentity, dstName string) []byte {
	out := make([]byte, 0, 128)
	out = appendTLV(out, TLVAgentKind, []byte{byte(id.Kind)})
	if id.Namespace != "" {
		out = appendTLV(out, TLVAgentNamespace, []byte(id.Namespace))
	}
	if id.Name != "" {
		out = appendTLV(out, TLVAgentName, []byte(id.Name))
	}
	if id.UID != "" {
		out = appendTLV(out, TLVAgentUID, []byte(id.UID))
	}
	if id.Revision != "" {
		out = appendTLV(out, TLVAgentRevision, []byte(id.Revision))
	}
	if id.InvocationID != "" {
		out = appendTLV(out, TLVInvocationID, []byte(id.InvocationID))
	}
	if dstName != "" {
		out = appendTLV(out, TLVDstName, []byte(dstName))
	}
	return out
}

func appendTLV(dst []byte, typ byte, val []byte) []byte {
	if len(val) > 0xFFFF {
		// Silently truncate: TLV length field is 2 bytes. Our fields are
		// k8s names/UIDs which are bounded well under this limit.
		val = val[:0xFFFF]
	}
	dst = append(dst, typ)
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(val)))
	dst = append(dst, val...)
	return dst
}
