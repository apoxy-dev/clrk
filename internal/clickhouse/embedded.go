// Package clickhouse supervises an embedded clickhouse-server
// subprocess inside the controller-manager container. Same pod, same
// network namespace, same PVC — the engine is private to the cm
// process, listening on 127.0.0.1 only. ch-go inside cm dials it on
// loopback; workers reach Invocation storage indirectly via cm's
// existing gRPC surface.
//
// Lifecycle mirrors internal/egcontrolplane: ctx-scoped Run, capped
// exponential restart with a budget, SIGTERM-then-SIGKILL via
// exec.CommandContext's Cancel + WaitDelay.
package clickhouse

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
)

// DefaultBinaryPath is where the cm OCI image layers the upstream
// clickhouse binary. Tests override via WithBinaryPath.
const DefaultBinaryPath = "/usr/local/bin/clickhouse"

// DefaultDataDir is the on-disk location the embedded engine writes
// MergeTree parts + metadata into. In the cm pod this is backed by a
// PVC so data survives pod restarts.
const DefaultDataDir = "/var/lib/clickhouse"

// HTTPPort + NativePort are the ClickHouse ports the embedded engine
// listens on (loopback only). Pinned so any future ch-go dialer
// inside cm agrees with the rendered config without a shared file.
const (
	HTTPPort   = 8123
	NativePort = 9000
)

// Supervisor knobs. Match egcontrolplane's defaults; the embedded
// engine has roughly the same crash-loop semantics — at startup it
// can fail to bind for a few seconds while the kernel reclaims the
// previous tenant's socket, but past 20 crashes in 5 minutes we
// want kubelet to restart the cm pod rather than spin forever.
const (
	gracefulShutdown = 10 * time.Second
	initialBackoff   = 1 * time.Second
	maxBackoff       = 30 * time.Second
	restartBudget    = 20
	restartWindow    = 5 * time.Minute
)

// Embedded supervises a single clickhouse-server child. Run is
// blocking and ctx-scoped.
type Embedded struct {
	binaryPath string
	dataDir    string
}

// Option configures an Embedded.
type Option func(*Embedded)

// WithBinaryPath overrides the clickhouse-server binary path.
func WithBinaryPath(p string) Option { return func(e *Embedded) { e.binaryPath = p } }

// WithDataDir overrides the on-disk data directory.
func WithDataDir(p string) Option { return func(e *Embedded) { e.dataDir = p } }

// New constructs an Embedded with the given options. Defaults match
// the cm container image and PVC mount in dev_bootstrap.go.
func New(opts ...Option) *Embedded {
	e := &Embedded{
		binaryPath: DefaultBinaryPath,
		dataDir:    DefaultDataDir,
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Run supervises the clickhouse-server child for ctx's lifetime.
// Blocks. Returns:
//
//   - nil after a clean ctx cancellation.
//   - nil when the binary path doesn't exist — that's the path during
//     the apoxy-cloud-side image-layering rollout; cm boots without
//     the engine until the binary lands.
//   - error if the child crashes past the restart budget, or on
//     unrecoverable setup failures (un-writable data dir, render).
func (e *Embedded) Run(ctx context.Context) error {
	if err := os.MkdirAll(e.dataDir, 0o755); err != nil {
		return fmt.Errorf("mkdir data dir %s: %w", e.dataDir, err)
	}

	// Config lives in a per-Run tmpfs dir so a pod restart re-renders
	// fresh files; the PVC stays a pure data store.
	configDir, err := os.MkdirTemp("", "clickhouse-config-")
	if err != nil {
		return fmt.Errorf("mkdir config dir: %w", err)
	}
	defer os.RemoveAll(configDir)
	if err := writeConfig(configDir, e.dataDir); err != nil {
		return fmt.Errorf("render clickhouse config: %w", err)
	}
	configPath := filepath.Join(configDir, "config.yaml")

	var (
		exits   []time.Time
		backoff = initialBackoff
	)
	for {
		err := e.runOnce(ctx, configPath)
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, fs.ErrNotExist) {
			slog.Warn("Embedded clickhouse skipped: binary not present",
				"path", e.binaryPath)
			return nil
		}
		if err == nil {
			return errors.New("clickhouse exited cleanly without shutdown signal")
		}
		slog.Error("Embedded clickhouse exited", "err", err)

		now := time.Now()
		exits = append(exits, now)
		cutoff := now.Add(-restartWindow)
		exits = slices.DeleteFunc(exits, func(t time.Time) bool { return t.Before(cutoff) })
		if len(exits) > restartBudget {
			return fmt.Errorf("clickhouse crashed %d times in %s; giving up", len(exits), restartWindow)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// runOnce starts the child and blocks until it exits. Returns the
// exit error, or nil if the child exited 0.
func (e *Embedded) runOnce(ctx context.Context, configPath string) error {
	cmd := exec.CommandContext(ctx, e.binaryPath, "server", "--config-file="+configPath)
	cmd.Env = append(os.Environ(), "CLICKHOUSE_WATCHDOG_ENABLE=0")
	cmd.SysProcAttr = procAttr()
	cmd.Stdin = nil
	cmd.Cancel = func() error {
		// exec.CommandContext defaults to SIGKILL on ctx cancel; we
		// want SIGTERM with a grace window. WaitDelay below provides
		// the timeout, then the runtime escalates to SIGKILL.
		return signalGroup(cmd.Process, syscall.SIGTERM)
	}
	cmd.WaitDelay = gracefulShutdown

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("stderr pipe: %w", err)
	}

	slog.Info("Starting embedded clickhouse",
		"binary", e.binaryPath,
		"data_dir", e.dataDir,
		"http_port", HTTPPort,
		"native_port", NativePort)
	if err := cmd.Start(); err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go pipeLines(&wg, stdout, "stdout")
	go pipeLines(&wg, stderr, "stderr")

	waitErr := cmd.Wait()
	wg.Wait()
	return waitErr
}

// Ready polls http://127.0.0.1:8123/ping until 200 OK with "Ok.\n",
// or ctx is done. Safe to call before Run starts the child — connect
// errors retry via the polling loop.
func (e *Embedded) Ready(ctx context.Context) error {
	url := "http://127.0.0.1:" + strconv.Itoa(HTTPPort) + "/ping"
	client := &http.Client{Timeout: 2 * time.Second}
	return wait.PollUntilContextCancel(ctx, 500*time.Millisecond, true, func(c context.Context) (bool, error) {
		req, err := http.NewRequestWithContext(c, http.MethodGet, url, nil)
		if err != nil {
			return false, err
		}
		resp, err := client.Do(req)
		if err != nil {
			return false, nil
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode == http.StatusOK && string(body) == "Ok.\n", nil
	})
}

func pipeLines(wg *sync.WaitGroup, r io.Reader, stream string) {
	defer wg.Done()
	scan := bufio.NewScanner(r)
	// CH server can emit very long stack-traces on crash; raise the
	// per-line cap to 1 MiB so we don't drop them with ErrTooLong.
	scan.Buffer(make([]byte, 64*1024), 1024*1024)
	for scan.Scan() {
		if stream == "stderr" {
			slog.Warn(scan.Text(), "component", "clickhouse", "stream", stream)
			continue
		}
		slog.Info(scan.Text(), "component", "clickhouse", "stream", stream)
	}
}
