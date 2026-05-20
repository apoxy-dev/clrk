//go:build linux

package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/apoxy-dev/clrk/internal/sentrystack"
)

// Worker → runsc plumbing. Each lifecycle hook fork+exec's
// /proc/self/exe with the relevant runsc argv; the same binary
// re-enters via cmd/worker/cli_linux.go::tryDispatchRunsc when invoked
// with a runsc subcommand. Subprocess (not in-process) because
// gVisor's sandbox.New donates the calling process's stdio to the
// Sentry boot child — incompatible with one worker serving many
// sandboxes that each need independent pipes.

// runscNetwork is the --network value that makes the Sentry consult
// the registered PluginStack (sentrystack.Singleton()) instead of
// building a NetworkSandbox netstack of its own.
const runscNetwork = "plugin"

// commonRunscFlags are the flags every runsc subcommand we invoke
// (create, start, kill, etc.) shares.
//
//   - --ignore-cgroups: skip runsc's own cgroup-v2 controller
//     delegation. The worker already runs inside a container whose
//     cgroup hierarchy is owned by the host runtime (Docker / k3s /
//     containerd); attempting to enable controllers on the worker's
//     cgroup via cgroup.subtree_control fails with EBUSY as soon as
//     the worker has processes in it. Resource limits baked into the
//     OCI spec are dropped along with cgroup setup; we rely on the
//     parent (k8s) cgroup limits applied to the worker pod.
//
//   - --TESTONLY-unsafe-nonroot: skip runsc's setUpChroot pivot-root
//     dance. With it the Sentry doesn't bind-mount its own binary
//     into a tmpfs chroot and doesn't fork a /proc-umounter helper;
//     both rely on `/proc/self/exe` being resolvable after pivot_root,
//     which only works when the Sentry binary lives at the same
//     filesystem path inside the chroot. Our worker binary is at
//     /worker (the OCI image entrypoint), not /usr/local/bin/runsc,
//     so the umounter fork fails with ENOENT and runsc create aborts.
//     The "test-only" naming is a warning about defence-in-depth, not
//     correctness — clrk's worker already runs inside a k8s-managed
//     container so the worker process's mount namespace is already
//     scoped; skipping the additional Sentry chroot loses only one
//     defence-in-depth layer.
func commonRunscFlags(rootDir string) []string {
	return []string{
		"--root=" + rootDir,
		"--network=" + runscNetwork,
		"--ignore-cgroups",
		"--TESTONLY-unsafe-nonroot=true",
		"--platform=" + runscPlatform,
	}
}

// runscPlatform is the gVisor execution platform. systrap patches
// syscall instructions in-place; ptrace stops the task on every
// syscall (~10x slower under Bun/Node). KVM is faster but needs
// /dev/kvm, which OrbStack containers don't expose.
const runscPlatform = "systrap"

// runscCreateOpts carries the per-sandbox state runsc create needs in
// addition to the bundle dir. initStr ships through the
// CLRK_SENTRYSTACK_INITSTR env var so the Sentry's PluginStack PreInit
// can read it (see internal/sentrystack/initstr.go).
type runscCreateOpts struct {
	id        string
	rootDir   string // runsc --root
	bundleDir string
	initStr   string
	stdin     *os.File
	stdout    *os.File
	stderr    *os.File
}

// sandboxDebugLog returns the per-sandbox runsc/Sentry debug-log path.
// Lives outside the OCI bundle dir so the file survives bundle teardown
// (which fires on every sandbox Delete, including delete-on-failure),
// and can be folded into runsc start / delete errors after the bundle
// is gone. Caller is responsible for cleaning it up via
// removeSandboxDebugLog once the sandbox is fully gone.
func sandboxDebugLog(rootDir, id string) string {
	return filepath.Join(rootDir, id+".debug.log")
}

// removeSandboxDebugLog deletes the per-sandbox debug-log on successful
// teardown. Best-effort — leaves the file behind on EPERM/ENOENT.
func removeSandboxDebugLog(rootDir, id string) {
	_ = os.Remove(sandboxDebugLog(rootDir, id))
}

// runscCreate spawns the Sentry by fork+exec'ing /proc/self/exe with
// runsc's `create` subcommand. The subprocess donates the per-sandbox
// stdio FDs to the Sentry and exits; the Sentry persists. On return
// the sandbox is in runsc's "Created" state — ready for Start.
//
// runsc's own diagnostics + Go panic reports are written to a
// per-sandbox debug-log file under rootDir (NOT the bundle dir — the
// bundle is wiped on Delete, but we want the log to survive long
// enough for runsc start/delete failures to fold its tail into the
// returned error). The Sentry's own stderr (inherited via opts.stderr)
// stays connected for the lifetime of the sandbox so the worker's
// drainSentryStdio can collect user-process stderr.
func runscCreate(ctx context.Context, opts runscCreateOpts) error {
	debugLog := sandboxDebugLog(opts.rootDir, opts.id)
	args := append(commonRunscFlags(opts.rootDir),
		"--debug",
		"--debug-log="+debugLog,
		"--panic-log="+debugLog,
		"create",
		"--bundle="+opts.bundleDir,
		opts.id,
	)
	cmd := exec.CommandContext(ctx, "/proc/self/exe", args...)
	cmd.Stdin = opts.stdin
	cmd.Stdout = opts.stdout
	cmd.Stderr = opts.stderr
	cmd.Env = append(os.Environ(), sentrystack.InitStrEnv+"="+opts.initStr)
	if err := runCmdReapAware(cmd); err != nil {
		// On failure read the tail of the debug log into the error
		// message so callers don't have to ssh into the worker pod
		// just to see why a 1-bit Sentry start refused.
		tail := readLogTail(debugLog, 65536)
		if tail != "" {
			return fmt.Errorf("runsc create: %w\nrunsc-debug.tail:\n%s", err, tail)
		}
		return fmt.Errorf("runsc create: %w", err)
	}
	return nil
}

// readLogTail returns up to last n bytes of path, or "" if path can't
// be read (file doesn't exist, permission denied, etc.). Best-effort —
// used only for diagnostic enrichment on a failed runsc create.
func readLogTail(path string, n int64) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return ""
	}
	start := int64(0)
	if st.Size() > n {
		start = st.Size() - n
	}
	if _, err := f.Seek(start, 0); err != nil {
		return ""
	}
	buf := make([]byte, n)
	read, _ := f.Read(buf)
	return string(buf[:read])
}

// runRunsc runs `/proc/self/exe --root=<rootDir> <args...>` and returns
// its stdout. On *exec.ExitError, stderr is folded into the returned
// error message — runsc prints diagnostics there. Used for the
// stdio-less runsc subcommands (start, kill, delete, wait, state, list).
func runRunsc(ctx context.Context, rootDir string, args ...string) ([]byte, error) {
	return runRunscEnv(ctx, rootDir, nil, args...)
}

// runRunscEnv is runRunsc plus extra env vars layered on top of the
// worker's os.Environ(). Used by runscStart, which must inject
// CLRK_SENTRYSTACK_INITSTR so the in-binary plugin-stack PreInit
// (which gVisor calls from inside `runsc start`, not `runsc create`)
// can find the per-sandbox payload.
func runRunscEnv(ctx context.Context, rootDir string, extraEnv []string, args ...string) ([]byte, error) {
	full := append(commonRunscFlags(rootDir), args...)
	cmd := exec.CommandContext(ctx, "/proc/self/exe", full...)
	if len(extraEnv) > 0 {
		cmd.Env = append(os.Environ(), extraEnv...)
	}
	out, err := outputCmdReapAware(cmd)
	if err == nil {
		return out, nil
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return nil, fmt.Errorf("runsc %s: %w: %s", args[0], err, ee.Stderr)
	}
	return nil, fmt.Errorf("runsc %s: %w", args[0], err)
}

// runscStart signals the Sentry to launch the spec.Process. The
// initStr env var is required: gVisor invokes PluginStack.PreInit
// from inside `runsc start`
// (https://github.com/apoxy-dev/gvisor/blob/5d6cfb0c0960/runsc/sandbox/network.go#L350),
// which reads CLRK_SENTRYSTACK_INITSTR via
// os.Getenv. Without it, the sentrystack boots with empty addressing
// and the Sentry's StartRoot urpc fails because the gofer's serving
// goroutines exit before the root mount can complete. On failure the
// per-sandbox Sentry debug-log tail is folded into the returned error
// — the failure mode we most often hit (urpc EOF from
// containerManager.StartRoot) means the Sentry crashed mid-StartRoot,
// and the panic frame is in that log.
func runscStart(ctx context.Context, rootDir, id, initStr string) error {
	debugLog := sandboxDebugLog(rootDir, id)
	extraEnv := []string{sentrystack.InitStrEnv + "=" + initStr}
	_, err := runRunscEnv(ctx, rootDir, extraEnv,
		"--debug",
		"--debug-log="+debugLog,
		"--panic-log="+debugLog,
		"start", id,
	)
	if err == nil {
		return nil
	}
	tail := readLogTail(debugLog, 65536)
	if tail != "" {
		return fmt.Errorf("%w\nrunsc-debug.tail:\n%s", err, tail)
	}
	return err
}

// runscWait blocks until the sandbox's init process exits and returns
// its exit status.
func runscWait(ctx context.Context, rootDir, id string) (int, error) {
	out, err := runRunsc(ctx, rootDir, "wait", id)
	if err != nil {
		return -1, err
	}
	var resp struct {
		ExitStatus int `json:"exitStatus"`
	}
	if err := json.Unmarshal(out, &resp); err != nil {
		return -1, fmt.Errorf("decoding runsc wait output %q: %w", out, err)
	}
	return resp.ExitStatus, nil
}

// runscKill sends a signal to the sandbox's init process via runsc.
// signal is the symbolic name (e.g. "SIGTERM", "SIGKILL").
func runscKill(ctx context.Context, rootDir, id, signal string) error {
	_, err := runRunsc(ctx, rootDir, "kill", id, signal)
	return err
}

// runscDelete destroys the sandbox container and its Sentry. `--force`
// SIGKILLs anything still running. Idempotent: returns the
// not-exist-shape error if the container is already gone.
func runscDelete(ctx context.Context, rootDir, id string) error {
	_, err := runRunsc(ctx, rootDir, "delete", "--force", id)
	return err
}

// runscState fetches the sandbox's current OCI state via runsc.
func runscState(ctx context.Context, rootDir, id string) (*ociState, error) {
	out, err := runRunsc(ctx, rootDir, "state", id)
	if err != nil {
		return nil, err
	}
	var st ociState
	if err := json.Unmarshal(out, &st); err != nil {
		return nil, fmt.Errorf("decoding runsc state: %w", err)
	}
	return &st, nil
}

// ociState mirrors the OCI runtime-state JSON returned by `runsc state`.
type ociState struct {
	OCIVersion  string            `json:"ociVersion"`
	ID          string            `json:"id"`
	Status      string            `json:"status"` // creating, created, running, stopped
	Pid         int               `json:"pid"`
	Bundle      string            `json:"bundle"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

// writeConfigJSON serializes the OCI spec to bundleDir/config.json so
// `runsc create --bundle` can find it.
func writeConfigJSON(bundleDir string, spec any) error {
	path := filepath.Join(bundleDir, "config.json")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("opening config.json: %w", err)
	}
	defer f.Close()
	if err := json.NewEncoder(f).Encode(spec); err != nil {
		return fmt.Errorf("encoding config.json: %w", err)
	}
	return nil
}
