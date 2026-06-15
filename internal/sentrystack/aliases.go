package sentrystack

import (
	sandboxsentrystack "github.com/apoxy-dev/clrk/pkg/sandbox/sentrystack"
)

// This package is now the tenant/egress WRAPPER around the neutral
// sentrystack core in pkg/sandbox/sentrystack (APO-713). The core owns the
// PluginStack singleton + lo/eth0 NIC wiring; this package keeps the
// egress/IMDS/DNS forwarder data path and plugs it back into the core's
// Init via the ForwarderInstaller hook (see install_linux.go). These
// aliases re-export the core's tenant-neutral envelope so existing clrk
// callers (cmd/worker, internal/worker/sandbox) keep their import path and
// type spellings unchanged — single source of truth, not a second copy.
//
// Kept un-tagged so the cross-platform halves of those callers still
// resolve InitStr / InitStrEnv on darwin (the core's initstr.go is
// likewise un-tagged); the gvisor-coupled re-exports live in
// install_linux.go.

// InitStr is the sentrystack init envelope. Aliased from the core.
type InitStr = sandboxsentrystack.InitStr

// InitStrEnv is the env var carrying the per-sandbox InitStr to PreInit.
const InitStrEnv = sandboxsentrystack.InitStrEnv

// DecodeInitStr parses an encoded InitStr. Re-exported from the core.
var DecodeInitStr = sandboxsentrystack.DecodeInitStr
