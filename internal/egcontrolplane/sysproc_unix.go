//go:build unix

package egcontrolplane

import (
	"os"
	"syscall"
)

func procAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}

// signalGroup sends sig to the entire process group of p, falling back
// to the process itself if the group lookup fails.
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
