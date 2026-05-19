package worker

import (
	"io"

	"github.com/apoxy-dev/clrk/internal/sandbox/metadata"
)

// registerMetadataEntry registers the per-dispatch *metadata.Entry
// against the worker-wide central registry keyed by SandboxID. The
// returned io.Closer clears the slot on dispatch teardown so the
// next dispatch on a reused warm sandbox gets a fresh registration.
//
// Under sentrystack the IMDS listener is shared across all sandboxes
// on the worker — demux happens via PROXY v2 TLVSandboxID on each
// incoming connection — so the registry is the single source of
// truth for which sandbox owns the live Entry.
func registerMetadataEntry(reg *metadata.Registry, sb *SandboxInstance, entry *metadata.Entry) (io.Closer, error) {
	clear := reg.Register(string(sb.ID), entry)
	return closerFunc(clear), nil
}

// closerFunc adapts a void callback to io.Closer.
type closerFunc func()

func (c closerFunc) Close() error {
	c()
	return nil
}
