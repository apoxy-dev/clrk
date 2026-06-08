//go:build linux

package sandbox

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
)

// inboundSockPath returns the host filesystem path of the AF_UNIX listening
// socket that fronts a sandbox's resident server. Lives under the runsc root
// dir alongside the rest of the per-sandbox state. Kept short to stay under
// the 108-byte sun_path limit.
func inboundSockPath(rootDir string, id SandboxID) string {
	return filepath.Join(rootDir, string(id)+".in.sock")
}

// openInboundListener binds a host AF_UNIX listening socket at path and returns
// a *os.File for it to donate to the runsc-start subprocess via cmd.ExtraFiles.
// The Sentry's PluginStack.PreInit surfaces that fd, runsc ships it across the
// urpc boundary, and the in-Sentry inbound forwarder accepts on it. Host-side
// callers (the Envoy MITM, the backplane bridge, or a spike test) reach the
// in-sandbox resident server by dialing path.
//
// Ownership: the returned *os.File is the caller's to Close once it has been
// handed to the start subprocess — the Sentry holds its own dup by then, which
// keeps the socket alive and accepting. The path is intentionally NOT unlinked
// when the worker's own listener fd is dropped (SetUnlinkOnClose(false)), so it
// stays connectable for the sandbox's lifetime; remove it at teardown.
func openInboundListener(path string) (*os.File, error) {
	// Clear any stale socket file left by a previous incarnation of this id.
	_ = os.Remove(path)

	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, fmt.Errorf("listen unix %q: %w", path, err)
	}
	// Keep the path after we drop our listener fd: the donated dup (held by the
	// Sentry) keeps the bound socket alive and accepting. We unlink explicitly
	// at teardown instead.
	ln.SetUnlinkOnClose(false)
	defer ln.Close()

	f, err := ln.File()
	if err != nil {
		return nil, fmt.Errorf("dup inbound listener fd for %q: %w", path, err)
	}
	return f, nil
}

// removeInboundSock unlinks the inbound socket file for a sandbox at teardown.
// No-op when the sandbox never enabled ingress (the file won't exist). The
// path is a sibling of the per-sandbox state dir, not inside it, so the
// teardown RemoveAll(stateDir/id) doesn't catch it — hence the explicit unlink.
func removeInboundSock(rootDir string, id SandboxID) {
	if err := os.Remove(inboundSockPath(rootDir, id)); err != nil && !os.IsNotExist(err) {
		slog.Warn("Failed to remove inbound socket",
			slog.String("sandbox.id", string(id)), slog.Any("error", err))
	}
}
