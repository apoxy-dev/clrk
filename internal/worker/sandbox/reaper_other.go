//go:build !linux

package sandbox

// StartChildReaper is a no-op on non-Linux. Sandboxes only run on Linux in
// production; the stub exists so go build ./... is green on darwin.
func StartChildReaper() {}
