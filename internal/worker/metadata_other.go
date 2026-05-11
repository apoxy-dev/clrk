//go:build !linux

package worker

import (
	"errors"
	"io"

	"github.com/apoxy-dev/clrk/internal/sandbox/metadata"
)

// registerMetadataEntry is a stub on non-Linux platforms. The
// gVisor-backed RevisionStack is Linux-only; metadata mode requires it.
func registerMetadataEntry(_ *SandboxInstance, _ *metadata.Entry) (io.Closer, error) {
	return nil, errors.New("metadata delivery mode requires linux")
}
