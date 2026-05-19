//go:build linux

package main

import (
	"log/slog"
	"os"
	"os/signal"
	"unsafe"

	"github.com/apoxy-dev/clrk/internal/worker"
	"golang.org/x/sys/unix"
)

// startChildReaper installs a SIGCHLD-driven reaper for orphan child
// processes.
//
// The worker runs as PID 1 in its container. When `runsc create` spawns
// the Sentry+gofer and then exits, those processes are re-parented to
// PID 1 (us). When they eventually exit, they become zombies under us
// until we reap them. The Go runtime does not reap children we did not
// explicitly cmd.Wait() on.
//
// The fallout without a reaper is that `runsc wait <id>` (run as a
// sibling subprocess) calls `unix.Kill(sentry_pid, 0)` as its liveness
// probe. Linux returns success on signal-0 to a zombie PID, so the
// probe sees the sentry as "still running" for the full 2-minute
// waitForStopped backoff. Result: the dispatcher's HTTP response sits
// open for ~2 min after the sandbox has actually exited.
//
// To avoid racing with cmd.Wait()'s own waitid() call on direct
// children, the reaper consults worker.ShouldSkipReap and skips PIDs
// that the worker is actively waiting on. The skipped zombies will be
// reaped by their owning cmd.Wait() shortly; the rest (orphan Sentries
// and gofers) get collected here.
func startChildReaper() {
	ch := make(chan os.Signal, 16)
	signal.Notify(ch, unix.SIGCHLD)
	go func() {
		for range ch {
			drainReapable()
		}
	}()
}

// drainReapable reaps every currently-reapable child that is NOT owned
// by a Go-side cmd.Wait(). It peeks the next reapable child with
// WNOWAIT, checks ownership, and either skips (Go's Wait will take
// over) or reaps with wait4.
func drainReapable() {
	for {
		pid, err := peekReapablePid()
		if err != nil || pid <= 0 {
			return
		}
		if worker.ShouldSkipReap(pid) {
			// cmd.Wait() will collect this one; stop the drain.
			// Linux will redeliver SIGCHLD when another orphan is
			// reapable, so we don't lose progress.
			return
		}
		var status unix.WaitStatus
		reaped, err := unix.Wait4(pid, &status, unix.WNOHANG, nil)
		if err != nil || reaped <= 0 {
			return
		}
		slog.Info("Reaped orphan child",
			"pid", reaped,
			"exited", status.Exited(),
			"exit_status", status.ExitStatus(),
			"signaled", status.Signaled(),
			"signal", status.Signal().String(),
			"raw_status", uint32(status))
	}
}

// peekReapablePid returns the PID of the next reapable child without
// consuming its zombie status. Raw waitid via Syscall6 because the
// x/sys/unix Siginfo type opaque-bytes its si_pid field.
//
// Linux's siginfo_t for SIGCHLD: si_signo / si_errno / si_code / _pad
// (four int32) followed by si_pid (int32) at offset 16. Stable across
// amd64/arm64/x86 on Linux.
func peekReapablePid() (int, error) {
	var buf [128]byte
	_, _, errno := unix.Syscall6(
		unix.SYS_WAITID,
		uintptr(unix.P_ALL),                             // idtype
		0,                                               // id
		uintptr(unsafe.Pointer(&buf[0])),                // siginfo_t*
		uintptr(unix.WEXITED|unix.WNOHANG|unix.WNOWAIT), // options
		0, 0,
	)
	if errno != 0 {
		if errno == unix.ECHILD {
			return 0, nil
		}
		return 0, errno
	}
	pid := int32(buf[16]) | int32(buf[17])<<8 | int32(buf[18])<<16 | int32(buf[19])<<24
	return int(pid), nil
}
