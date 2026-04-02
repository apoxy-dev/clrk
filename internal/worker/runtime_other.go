//go:build !linux

package worker

import (
	"context"
	"fmt"
	"runtime"
)

// Start is not supported on non-linux platforms.
func (r *Runtime) Start(ctx context.Context) error {
	return fmt.Errorf("worker runtime requires linux, got %s", runtime.GOOS)
}
