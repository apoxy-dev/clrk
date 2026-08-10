package cmd

import (
	"net"
	"testing"
)

func TestPortFreeChecksIPv6Loopback(t *testing.T) {
	if len(exposeLoopbackAddresses) < 2 {
		t.Skip("IPv6 loopback is not available")
	}

	listener, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Fatalf("listen on IPv6 loopback: %v", err)
	}
	defer listener.Close()

	port := listener.Addr().(*net.TCPAddr).Port
	if portFree(port) {
		t.Fatalf("port %d reported free while IPv6 loopback was in use", port)
	}
}
