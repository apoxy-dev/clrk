//go:build !linux

package worker

import (
	"net/netip"

	"github.com/apoxy-dev/clrk/internal/egress"
)

// revStackStub satisfies revisionStackAttachment on non-linux builds
// so the platform-agnostic SandboxInstance compiles everywhere.
// Non-linux builds have no real netstack (gVisor is linux-only), so
// all operations are no-ops — the linux-only sandbox.go is where
// any real Attach happens.
type revStackStub struct{ sandboxIP netip.Addr }

func (s *revStackStub) SandboxIP() netip.Addr                     { return s.sandboxIP }
func (s *revStackStub) SetEgressBackends(_ []egress.BackendListener) {}
func (s *revStackStub) SetEgressPolicy(_ *egress.SandboxPolicy)      {}
func (s *revStackStub) SetInvocationID(_ string)                     {}
func (s *revStackStub) Detach()                                      {}
