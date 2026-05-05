package proxyproto

import (
	"bytes"
	"encoding/binary"
	"net/netip"
	"testing"
)

func TestEncodeHeader_IPv4(t *testing.T) {
	src := netip.MustParseAddrPort("10.0.0.5:54321")
	dst := netip.MustParseAddrPort("1.2.3.4:443")
	id := AgentIdentity{
		Kind:      AgentKindDaemon,
		Namespace: "default",
		Name:      "flapper",
		UID:       "abc-123",
	}

	hdr, err := EncodeHeader(src, dst, id, "api.example.com")
	if err != nil {
		t.Fatalf("EncodeHeader: %v", err)
	}

	if !bytes.Equal(hdr[:12], signature) {
		t.Fatalf("signature mismatch: %x", hdr[:12])
	}
	if hdr[12] != versionCmdV2Proxy {
		t.Fatalf("version/command: got %#x", hdr[12])
	}
	if hdr[13] != famInet {
		t.Fatalf("family/transport: got %#x want %#x", hdr[13], famInet)
	}

	payloadLen := binary.BigEndian.Uint16(hdr[14:16])
	if int(payloadLen) != len(hdr)-16 {
		t.Fatalf("payload length mismatch: header says %d, actual %d", payloadLen, len(hdr)-16)
	}

	wantSrc := [4]byte{10, 0, 0, 5}
	wantDst := [4]byte{1, 2, 3, 4}
	if !bytes.Equal(hdr[16:20], wantSrc[:]) {
		t.Fatalf("src ip: got %v want %v", hdr[16:20], wantSrc)
	}
	if !bytes.Equal(hdr[20:24], wantDst[:]) {
		t.Fatalf("dst ip: got %v want %v", hdr[20:24], wantDst)
	}
	if binary.BigEndian.Uint16(hdr[24:26]) != 54321 {
		t.Fatalf("src port: %d", binary.BigEndian.Uint16(hdr[24:26]))
	}
	if binary.BigEndian.Uint16(hdr[26:28]) != 443 {
		t.Fatalf("dst port: %d", binary.BigEndian.Uint16(hdr[26:28]))
	}

	tlvs := hdr[28:]
	assertTLV(t, tlvs, TLVAgentKind, []byte{0})
	assertTLV(t, tlvs, TLVAgentNamespace, []byte("default"))
	assertTLV(t, tlvs, TLVAgentName, []byte("flapper"))
	assertTLV(t, tlvs, TLVAgentUID, []byte("abc-123"))
	assertTLV(t, tlvs, TLVDstName, []byte("api.example.com"))
	if bytes.IndexByte(tlvs, TLVAgentRevision) >= 0 {
		t.Fatalf("empty Revision should not produce a TLV")
	}
	if bytes.IndexByte(tlvs, TLVInvocationID) >= 0 {
		t.Fatalf("empty InvocationID should not produce a TLV")
	}
}

func TestEncodeHeader_IPv6(t *testing.T) {
	src := netip.MustParseAddrPort("[fd00::5]:54321")
	dst := netip.MustParseAddrPort("[2001:db8::1]:443")
	id := AgentIdentity{Kind: AgentKindTask, InvocationID: "task-xyz"}

	hdr, err := EncodeHeader(src, dst, id, "")
	if err != nil {
		t.Fatalf("EncodeHeader: %v", err)
	}
	if hdr[13] != famInet6 {
		t.Fatalf("family/transport: got %#x want %#x", hdr[13], famInet6)
	}
	// 16 signature+header + 36 addr block = 52 before TLVs.
	if len(hdr) < 52 {
		t.Fatalf("header too short for ipv6: %d", len(hdr))
	}
	tlvs := hdr[52:]
	assertTLV(t, tlvs, TLVAgentKind, []byte{byte(AgentKindTask)})
	assertTLV(t, tlvs, TLVInvocationID, []byte("task-xyz"))
}

func TestEncodeHeader_FamilyMismatch(t *testing.T) {
	src := netip.MustParseAddrPort("10.0.0.5:1")
	dst := netip.MustParseAddrPort("[2001:db8::1]:1")
	if _, err := EncodeHeader(src, dst, AgentIdentity{}, ""); err == nil {
		t.Fatalf("expected family-mismatch error")
	}
}

// assertTLV locates a TLV of the given type in the buffer and fails the test
// if absent or value mismatches.
func assertTLV(t *testing.T, buf []byte, typ byte, want []byte) {
	t.Helper()
	for i := 0; i+3 <= len(buf); {
		t2 := buf[i]
		l := int(binary.BigEndian.Uint16(buf[i+1 : i+3]))
		if i+3+l > len(buf) {
			t.Fatalf("malformed TLV at offset %d", i)
		}
		if t2 == typ {
			got := buf[i+3 : i+3+l]
			if !bytes.Equal(got, want) {
				t.Fatalf("TLV %#x: got %q want %q", typ, got, want)
			}
			return
		}
		i += 3 + l
	}
	t.Fatalf("TLV %#x not found", typ)
}
