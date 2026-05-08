//go:build linux

package worker

import (
	"fmt"
	"io"

	"github.com/apoxy-dev/clrk/internal/netstack"
	"github.com/apoxy-dev/clrk/internal/sandbox/metadata"
)

// startMetadataServer binds the per-execution IMDS server on the
// sandbox's gVisor netstack. The dispatcher calls this when delivery
// mode is Metadata; the returned io.Closer is invoked on dispatch
// teardown to stop the server.
func startMetadataServer(sb *SandboxInstance, entry *metadata.Entry) (io.Closer, error) {
	st, ok := sb.Stack.(*netstack.SandboxStack)
	if !ok {
		return nil, fmt.Errorf("sandbox stack is not a netstack.SandboxStack (got %T)", sb.Stack)
	}
	srv, err := metadata.New(st.Stack(), st.NICID(), entry)
	if err != nil {
		return nil, err
	}
	return srv, nil
}
