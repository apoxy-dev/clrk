//go:build linux

package worker

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

// runscCreate spawns the Sentry by fork+exec'ing /proc/self/exe with
// runsc's `create` subcommand. The subprocess donates the per-sandbox
// stdio FDs to the Sentry and exits; the Sentry persists. On return
// the sandbox is in runsc's "Created" state — ready for Start.
func runscCreate(ctx context.Context, opts runscCreateOpts) error {
	cmd := exec.CommandContext(ctx, "/proc/self/exe",
		"--root="+opts.rootDir,
		"--network="+runscNetwork,
		"create",
		"--bundle="+opts.bundleDir,
		opts.id,
	)
	cmd.Stdin = opts.stdin
	cmd.Stdout = opts.stdout
	cmd.Stderr = opts.stderr
	cmd.Env = append(os.Environ(), sentrystack.InitStrEnv+"="+opts.initStr)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("runsc create: %w", err)
	}
	return nil
}

// runRunsc runs `/proc/self/exe --root=<rootDir> <args...>` and returns
// its stdout. On *exec.ExitError, stderr is folded into the returned
// error message — runsc prints diagnostics there. Used for the
// stdio-less runsc subcommands (start, kill, delete, wait, state, list).
func runRunsc(ctx context.Context, rootDir string, args ...string) ([]byte, error) {
	full := append([]string{"--root=" + rootDir}, args...)
	cmd := exec.CommandContext(ctx, "/proc/self/exe", full...)
	out, err := cmd.Output()
	if err == nil {
		return out, nil
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return nil, fmt.Errorf("runsc %s: %w: %s", args[0], err, ee.Stderr)
	}
	return nil, fmt.Errorf("runsc %s: %w", args[0], err)
}

// runscStart signals the Sentry to launch the spec.Process.
func runscStart(ctx context.Context, rootDir, id string) error {
	_, err := runRunsc(ctx, rootDir, "start", id)
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
