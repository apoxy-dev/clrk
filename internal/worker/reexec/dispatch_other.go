//go:build !linux

package reexec

// TryDispatchRunsc is a no-op on non-linux platforms. The gVisor runsc
// dispatch (and the sentrystack PluginStack registration) is linux-only;
// off-platform builds exist solely to keep `go build ./...` / `go vet ./...`
// green for cross-platform contributors and so that linux-only tests can call
// it unconditionally from a cross-platform TestMain.
func TryDispatchRunsc() {}
