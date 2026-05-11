//go:build linux

package worker

import (
	"fmt"
	"io"

	"github.com/apoxy-dev/clrk/internal/sandbox/metadata"
)

// registerMetadataEntry attaches a per-dispatch *metadata.Entry to
// the sandbox's slot on the shared per-revision RevisionStack so the
// IMDS HTTP handler can resolve it via source-IP demux. The returned
// io.Closer clears the slot's Entry when the dispatch teardown runs
// — leaving the slot itself intact (the sandbox may be reused on a
// warm-pool path).
//
// Replaces the pre-refactor startMetadataServer which stood up a
// fresh dual-listener http.Server per dispatch on the per-sandbox
// gVisor stack. The listener is now bound once per RevisionStack at
// stack construction; this call is pure slot-table mutation.
func registerMetadataEntry(sb *SandboxInstance, entry *metadata.Entry) (io.Closer, error) {
	handle, ok := sb.stack.(*RevisionStackHandle)
	if !ok || handle == nil {
		return nil, fmt.Errorf("sandbox has no RevisionStack handle (got %T)", sb.stack)
	}
	clear := handle.RegisterMetadataEntry(entry)
	return closerFunc(clear), nil
}

// closerFunc adapts a void callback to io.Closer.
type closerFunc func()

func (c closerFunc) Close() error {
	c()
	return nil
}
