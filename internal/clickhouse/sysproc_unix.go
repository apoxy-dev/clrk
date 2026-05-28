//go:build unix

package clickhouse

import (
	"os"
	"syscall"
)

// Sibling of internal/egcontrolplane/sysproc_unix.go. Pull both into a
// shared internal/subproc package the next time a third caller appears.

func procAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}

func signalGroup(p *os.Process, sig syscall.Signal) error {
	if p == nil {
		return nil
	}
	pgid, err := syscall.Getpgid(p.Pid)
	if err == nil {
		return syscall.Kill(-pgid, sig)
	}
	return p.Signal(sig)
}
