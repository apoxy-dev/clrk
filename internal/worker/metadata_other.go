//go:build !linux

package worker

import (
	"errors"
	"io"

	"github.com/apoxy-dev/clrk/internal/sandbox/metadata"
)

// startMetadataServer is a stub on non-Linux platforms. The
// gVisor-backed netstack is Linux-only; metadata mode requires it.
func startMetadataServer(_ *SandboxInstance, _ *metadata.Entry) (io.Closer, error) {
	return nil, errors.New("metadata delivery mode requires linux")
}
