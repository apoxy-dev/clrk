package proxyproto

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/netip"
)

// Header is the parsed PROXY v2 frame. Source/Destination are the addresses
// announced by the writer (e.g. the sentry-side forwarder for a sandbox-
// originated stream); Identity carries clrk-specific TLVs.
type Header struct {
	Source      netip.AddrPort
	Destination netip.AddrPort
	Identity    AgentIdentity
	DstName     string
}

// ParseHeader reads a PROXY v2 frame off r. The frame is consumed in full;
// subsequent reads from r return the original payload (the proxied stream).
// Only the v2 PROXY command + AF_INET/AF_INET6 STREAM families are
// supported — anything else surfaces as an error so misconfigured callers
// fail loudly instead of silently mis-attributing traffic.
//
// Unknown TLVs are skipped without error so the encoder side can add
// fields incrementally without breaking older readers.
func ParseHeader(r io.Reader) (*Header, error) {
	var fixed [16]byte
	if _, err := io.ReadFull(r, fixed[:]); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	for i, b := range signature {
		if fixed[i] != b {
			return nil, errors.New("proxyproto: bad signature")
		}
	}
	if fixed[12] != versionCmdV2Proxy {
		return nil, fmt.Errorf("proxyproto: unsupported version/command %#x", fixed[12])
	}
	family := fixed[13]
	payloadLen := int(binary.BigEndian.Uint16(fixed[14:16]))

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("read payload: %w", err)
	}

	h := &Header{}
	var addrEnd int
	switch family {
	case famInet:
		if payloadLen < 12 {
			return nil, fmt.Errorf("proxyproto: short ipv4 payload (%d)", payloadLen)
		}
		var src4, dst4 [4]byte
		copy(src4[:], payload[0:4])
		copy(dst4[:], payload[4:8])
		srcPort := binary.BigEndian.Uint16(payload[8:10])
		dstPort := binary.BigEndian.Uint16(payload[10:12])
		h.Source = netip.AddrPortFrom(netip.AddrFrom4(src4), srcPort)
		h.Destination = netip.AddrPortFrom(netip.AddrFrom4(dst4), dstPort)
		addrEnd = 12
	case famInet6:
		if payloadLen < 36 {
			return nil, fmt.Errorf("proxyproto: short ipv6 payload (%d)", payloadLen)
		}
		var src6, dst6 [16]byte
		copy(src6[:], payload[0:16])
		copy(dst6[:], payload[16:32])
		srcPort := binary.BigEndian.Uint16(payload[32:34])
		dstPort := binary.BigEndian.Uint16(payload[34:36])
		h.Source = netip.AddrPortFrom(netip.AddrFrom16(src6), srcPort)
		h.Destination = netip.AddrPortFrom(netip.AddrFrom16(dst6), dstPort)
		addrEnd = 36
	default:
		return nil, fmt.Errorf("proxyproto: unsupported family/transport %#x", family)
	}

	for i := addrEnd; i+3 <= payloadLen; {
		typ := payload[i]
		l := int(binary.BigEndian.Uint16(payload[i+1 : i+3]))
		if i+3+l > payloadLen {
			return nil, fmt.Errorf("proxyproto: malformed TLV at offset %d", i)
		}
		val := payload[i+3 : i+3+l]
		switch typ {
		case TLVAgentKind:
			if l == 1 {
				h.Identity.Kind = AgentKind(val[0])
			}
		case TLVAgentNamespace:
			h.Identity.Namespace = string(val)
		case TLVAgentName:
			h.Identity.Name = string(val)
		case TLVAgentUID:
			h.Identity.UID = string(val)
		case TLVAgentRevision:
			h.Identity.Revision = string(val)
		case TLVInvocationID:
			h.Identity.InvocationID = string(val)
		case TLVSandboxID:
			h.Identity.SandboxID = string(val)
		case TLVDstName:
			h.DstName = string(val)
		}
		i += 3 + l
	}

	return h, nil
}
