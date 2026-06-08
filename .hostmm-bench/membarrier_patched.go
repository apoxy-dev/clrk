// Copyright 2020 The gVisor Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hostmm

import (
	"sync"

	"golang.org/x/sys/unix"
	"gvisor.dev/gvisor/pkg/abi/linux"
	"gvisor.dev/gvisor/pkg/log"
)

var (
	haveMembarrierGlobal = false

	// membarrierPrivateExpeditedSupported records whether the kernel
	// advertises MEMBARRIER_CMD_(REGISTER_)PRIVATE_EXPEDITED (from the cheap
	// QUERY in init). Registration itself is deferred; see
	// ensureMembarrierPrivateExpeditedRegistered.
	membarrierPrivateExpeditedSupported = false

	// registerPrivateExpeditedOnce guards the one-time, lazy
	// MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED. haveMembarrierPrivateExpedited
	// is set true only after that registration succeeds; it is read after
	// registerPrivateExpeditedOnce.Do, which establishes the necessary
	// happens-before.
	registerPrivateExpeditedOnce   sync.Once
	haveMembarrierPrivateExpedited = false
)

func init() {
	supported, _, e := unix.RawSyscall(unix.SYS_MEMBARRIER, linux.MEMBARRIER_CMD_QUERY, 0 /* flags */, 0 /* unused */)
	if e != 0 {
		if e != unix.ENOSYS {
			log.Warningf("membarrier(MEMBARRIER_CMD_QUERY) failed: %s", e.Error())
		}
		return
	}
	// We don't use MEMBARRIER_CMD_GLOBAL_EXPEDITED because this sends IPIs to
	// all CPUs running tasks that have previously invoked
	// MEMBARRIER_CMD_REGISTER_GLOBAL_EXPEDITED, which presents a DOS risk.
	// (MEMBARRIER_CMD_GLOBAL is synchronize_rcu(), i.e. it waits for an RCU
	// grace period to elapse without bothering other CPUs.
	// MEMBARRIER_CMD_PRIVATE_EXPEDITED sends IPIs only to CPUs running tasks
	// sharing the caller's MM.)
	if supported&linux.MEMBARRIER_CMD_GLOBAL != 0 {
		haveMembarrierGlobal = true
	}
	// Only record support here. Registering for private-expedited barriers is
	// deferred to first use: MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED forces a
	// synchronize_rcu() grace period in the kernel
	// (membarrier_register_private_expedited -> sync_runqueues_membarrier_state),
	// which costs multiple milliseconds at process startup. Platforms that use
	// UseHostGlobalMemoryBarrier (systrap, ptrace) never call ProcessMemoryBarrier
	// and so never need this registration; only UseHostProcessMemoryBarrier (the
	// KVM platform) does. Deferring keeps the grace period off the cold-boot path
	// for the common case.
	if req := uintptr(linux.MEMBARRIER_CMD_PRIVATE_EXPEDITED | linux.MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED); supported&req == req {
		membarrierPrivateExpeditedSupported = true
	}
}

// ensureMembarrierPrivateExpeditedRegistered performs the one-time
// MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED registration on first use. It is
// safe to call concurrently and from any goroutine.
func ensureMembarrierPrivateExpeditedRegistered() {
	registerPrivateExpeditedOnce.Do(func() {
		if !membarrierPrivateExpeditedSupported {
			return
		}
		if _, _, e := unix.RawSyscall(unix.SYS_MEMBARRIER, linux.MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED, 0 /* flags */, 0 /* unused */); e != 0 {
			log.Warningf("membarrier(MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED) failed: %s", e.Error())
			return
		}
		haveMembarrierPrivateExpedited = true
	})
}

// HaveGlobalMemoryBarrier returns true if GlobalMemoryBarrier is supported.
func HaveGlobalMemoryBarrier() bool {
	return haveMembarrierGlobal
}

// GlobalMemoryBarrier blocks until "all running threads [in the host OS] have
// passed through a state where all memory accesses to user-space addresses
// match program order between entry to and return from [GlobalMemoryBarrier]",
// as for membarrier(2).
//
// Preconditions: HaveGlobalMemoryBarrier() == true.
func GlobalMemoryBarrier() error {
	if _, _, e := unix.Syscall(unix.SYS_MEMBARRIER, linux.MEMBARRIER_CMD_GLOBAL, 0 /* flags */, 0 /* unused */); e != 0 {
		return e
	}
	return nil
}

// HaveProcessMemoryBarrier returns true if ProcessMemoryBarrier is supported.
//
// Calling this lazily performs the one-time private-expedited registration if
// the kernel supports it, so that the registration's RCU grace period is only
// paid by callers that actually use process-local barriers.
func HaveProcessMemoryBarrier() bool {
	ensureMembarrierPrivateExpeditedRegistered()
	return haveMembarrierPrivateExpedited
}

// ProcessMemoryBarrier is equivalent to GlobalMemoryBarrier, but only
// synchronizes with threads sharing a virtual address space (from the host OS'
// perspective) with the calling thread.
//
// Preconditions: HaveProcessMemoryBarrier() == true.
func ProcessMemoryBarrier() error {
	ensureMembarrierPrivateExpeditedRegistered()
	if _, _, e := unix.RawSyscall(unix.SYS_MEMBARRIER, linux.MEMBARRIER_CMD_PRIVATE_EXPEDITED, 0 /* flags */, 0 /* unused */); e != 0 {
		return e
	}
	return nil
}
