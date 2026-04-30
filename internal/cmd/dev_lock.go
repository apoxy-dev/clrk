package cmd

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// devLock is an advisory exclusive lock on a clrk dev session's dataDir.
// Held for the lifetime of the primary `clrk dev` process so a second
// invocation can detect and refuse instead of clobbering containers.
type devLock struct {
	path string
	f    *os.File
}

// errDevLocked indicates another clrk dev process already owns the
// dataDir. ownerPID is the recorded PID (best-effort; may be stale).
var errDevLocked = errors.New("clrk dev already running")

// acquireDevLock takes an exclusive flock on <dataDir>/dev.lock and
// records our PID inside. Returns errDevLocked wrapping a struct that
// carries the existing owner's PID when contended.
type devLockBusy struct{ OwnerPID int }

func (e *devLockBusy) Error() string { return fmt.Sprintf("%s (pid %d)", errDevLocked.Error(), e.OwnerPID) }
func (e *devLockBusy) Unwrap() error { return errDevLocked }

func acquireDevLock(dataDir string) (*devLock, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dataDir, "dev.lock")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("opening lock %s: %w", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		owner := readLockPID(path)
		f.Close()
		return nil, &devLockBusy{OwnerPID: owner}
	}
	if err := f.Truncate(0); err != nil {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
		return nil, err
	}
	if _, err := f.WriteAt([]byte(strconv.Itoa(os.Getpid())), 0); err != nil {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
		return nil, err
	}
	return &devLock{path: path, f: f}, nil
}

// Release unlocks and removes the lock file. Idempotent.
func (l *devLock) Release() {
	if l == nil || l.f == nil {
		return
	}
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	l.f.Close()
	_ = os.Remove(l.path)
	l.f = nil
}

func readLockPID(path string) int {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		return 0
	}
	return pid
}
