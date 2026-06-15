//go:build linux

package main

// Blank import to register clrk's egress/IMDS/DNS forwarder data path with
// the sandbox core's sentrystack PluginStack: internal/sentrystack's init
// sets pkg/sandbox/sentrystack.ForwarderInstaller, so every sandbox this
// worker boots gets the full egress data path layered onto the core's
// lo+eth0 wiring. The core PluginStack registration and the runsc
// subcommand dispatch (sandbox.DispatchRunsc) come from pkg/sandbox, which
// internal/worker/sandbox already pulls in; this import is what adds the
// tenant/egress half. Without it the sandbox would boot lo+eth0-only with
// no outbound forwarder.
import (
	_ "github.com/apoxy-dev/clrk/internal/sentrystack"
)
