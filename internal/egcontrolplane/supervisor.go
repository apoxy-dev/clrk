// Package egcontrolplane spawns and supervises the upstream
// `envoy-gateway server` binary as a child of clrk-controller-manager.
// We subprocess (rather than import) because EG's setup lives under
// internal/ in the upstream module.
package egcontrolplane

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// LogSink receives one line of stdout or stderr from the EG child.
// stream is "stdout" or "stderr".
type LogSink func(line, stream string)

// Config controls the supervisor.
type Config struct {
	// BinaryPath is the absolute path to the envoy-gateway binary.
	// Defaults to /usr/local/bin/envoy-gateway when empty.
	BinaryPath string

	// Kubeconfig path passed to the child via KUBECONFIG. When empty
	// the child relies on in-cluster discovery.
	Kubeconfig string

	// RestConfig is the parent's apiserver client config. When set, the
	// supervisor uses it directly instead of re-loading Kubeconfig from
	// disk (one fewer place to drift, works in pure in-cluster mode).
	RestConfig *rest.Config

	// ExtensionHost / ExtensionPort identify the gRPC endpoint EG's
	// xDS translator dials for the extension hooks. In a unified
	// container this is loopback ("127.0.0.1", 9443).
	ExtensionHost string
	ExtensionPort int

	// LogSink receives child stdout/stderr line by line. When nil,
	// lines are written to slog with attr component=envoy-gateway.
	LogSink LogSink

	// GracefulShutdown is the grace period between SIGTERM and SIGKILL
	// when the parent ctx is canceled. Defaults to 10s.
	GracefulShutdown time.Duration

	// RestartBudget is the number of unexpected exits tolerated within
	// RestartWindow before the supervisor gives up. Defaults to 5/60s.
	RestartBudget int
	RestartWindow time.Duration

	// InitialBackoff and MaxBackoff bound the post-crash sleep before
	// the next restart attempt. Defaults to 1s / 30s.
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

func (c *Config) defaults() {
	if c.BinaryPath == "" {
		c.BinaryPath = "/usr/local/bin/envoy-gateway"
	}
	if c.ExtensionHost == "" {
		c.ExtensionHost = "127.0.0.1"
	}
	if c.ExtensionPort == 0 {
		c.ExtensionPort = 9443
	}
	if c.GracefulShutdown == 0 {
		c.GracefulShutdown = 10 * time.Second
	}
	// Generous defaults: at startup the EG child can fail cache sync
	// for a minute or two while k3s's aggregator probes clrk's
	// aggregated APIService and marks it Available. We want to retry
	// through that window, not give up.
	if c.RestartBudget == 0 {
		c.RestartBudget = 20
	}
	if c.RestartWindow == 0 {
		c.RestartWindow = 5 * time.Minute
	}
	if c.InitialBackoff == 0 {
		c.InitialBackoff = 2 * time.Second
	}
	if c.MaxBackoff == 0 {
		c.MaxBackoff = 30 * time.Second
	}
	if c.LogSink == nil {
		c.LogSink = defaultSlogSink
	}
}

// Run spawns `envoy-gateway server` as a child and supervises it for
// the lifetime of ctx. Returns when ctx is canceled (after the child
// exits cleanly on SIGTERM) or when the child exits non-zero past the
// restart budget.
func Run(ctx context.Context, cfg Config) error {
	cfg.defaults()

	cfgPath, err := writeConfig(cfg)
	if err != nil {
		return err
	}
	defer os.Remove(cfgPath)
	slog.Info("Wrote EG config", "path", cfgPath, "extension", fmt.Sprintf("%s:%d", cfg.ExtensionHost, cfg.ExtensionPort))

	// `envoy-gateway certgen` upserts the control-plane TLS Secrets in
	// envoy-gateway-system. Idempotent. Replaces the certgen Job from
	// upstream's install.yaml.
	if err := runCertgen(ctx, cfg); err != nil {
		return fmt.Errorf("envoy-gateway certgen: %w", err)
	}

	// Without /certs/tls.crt the EG xDS server falls back to
	// self-signed certs that the data-plane Envoys reject (their
	// bootstrap pins SAN=envoy-gateway against /certs/ca.crt). The
	// operator Deployment gets this via a Secret volume; we materialize
	// it ourselves.
	if err := materializeXDSCerts(ctx, cfg); err != nil {
		return fmt.Errorf("materializing /certs: %w", err)
	}

	var (
		exits   []time.Time
		backoff = cfg.InitialBackoff
	)
	for {
		err := runOnce(ctx, cfg, cfgPath)
		if ctx.Err() != nil {
			// Clean shutdown initiated by the parent.
			return nil
		}
		if err == nil {
			// Child exited 0 without ctx cancel — treat as misconfig.
			return errors.New("envoy-gateway exited cleanly without shutdown signal")
		}
		slog.Error("envoy-gateway child exited", "err", err)

		now := time.Now()
		exits = append(exits, now)
		// Drop exits outside the rolling window.
		cutoff := now.Add(-cfg.RestartWindow)
		exits = slices.DeleteFunc(exits, func(t time.Time) bool { return t.Before(cutoff) })
		if len(exits) > cfg.RestartBudget {
			return fmt.Errorf("envoy-gateway crashed %d times in %s; giving up", len(exits), cfg.RestartWindow)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > cfg.MaxBackoff {
			backoff = cfg.MaxBackoff
		}
	}
}

// runOnce starts one child process and blocks until it exits. Returns
// the exit error, or nil if the child exited 0.
func runOnce(ctx context.Context, cfg Config, cfgPath string) error {
	cmd := exec.CommandContext(ctx, cfg.BinaryPath, "server", "--config-path", cfgPath)
	cmd.Env = os.Environ()
	if cfg.Kubeconfig != "" {
		cmd.Env = append(cmd.Env, "KUBECONFIG="+cfg.Kubeconfig)
	}
	// Process group so we can SIGTERM the whole tree if EG forks
	// helpers. Setpgid is linux-portable.
	cmd.SysProcAttr = procAttr()
	// Discard parent stdin; capture stdout/stderr.
	cmd.Stdin = nil

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("stderr pipe: %w", err)
	}

	cmd.Cancel = func() error {
		// exec.CommandContext defaults to SIGKILL on ctx cancel; we
		// want SIGTERM with a grace window. Send SIGTERM to the
		// whole process group; runOnce returns when the child has
		// actually exited.
		return signalGroup(cmd.Process, syscall.SIGTERM)
	}
	cmd.WaitDelay = cfg.GracefulShutdown

	slog.Info("Starting envoy-gateway", "binary", cfg.BinaryPath, "config", cfgPath)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting %s: %w", cfg.BinaryPath, err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go pipeLines(&wg, stdout, "stdout", cfg.LogSink)
	go pipeLines(&wg, stderr, "stderr", cfg.LogSink)

	waitErr := cmd.Wait()
	wg.Wait()
	return waitErr
}

func pipeLines(wg *sync.WaitGroup, r io.Reader, stream string, sink LogSink) {
	defer wg.Done()
	scan := bufio.NewScanner(r)
	scan.Buffer(make([]byte, 64*1024), 1024*1024)
	for scan.Scan() {
		sink(scan.Text(), stream)
	}
}

func defaultSlogSink(line, stream string) {
	if stream == "stderr" {
		slog.Warn(line, "component", "envoy-gateway", "stream", stream)
		return
	}
	slog.Info(line, "component", "envoy-gateway", "stream", stream)
}

// materializeXDSCerts copies the envoy-gateway TLS Secret keys
// (tls.crt, tls.key, ca.crt) to /certs/ so the EG xDS server's
// `if /certs/tls.crt exists` codepath wins over its self-signed fallback.
func materializeXDSCerts(ctx context.Context, cfg Config) error {
	cs, err := newKubeClient(cfg)
	if err != nil {
		return err
	}
	c, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	sec, err := cs.CoreV1().Secrets("envoy-gateway-system").Get(c, "envoy-gateway", metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return errors.New("envoy-gateway secret not found after certgen — check certgen logs")
		}
		return err
	}
	if err := os.MkdirAll("/certs", 0o755); err != nil {
		return fmt.Errorf("mkdir /certs: %w", err)
	}
	for k, v := range sec.Data {
		if err := os.WriteFile(filepath.Join("/certs", k), v, 0o600); err != nil {
			return fmt.Errorf("writing /certs/%s: %w", k, err)
		}
	}
	slog.Info("Materialized EG xDS certs", "dir", "/certs", "keys", len(sec.Data))
	return nil
}

// newKubeClient prefers cfg.RestConfig (passed by the parent) over
// re-loading kubeconfig from disk.
func newKubeClient(cfg Config) (kubernetes.Interface, error) {
	rc := cfg.RestConfig
	if rc == nil {
		var err error
		rc, err = clientcmd.BuildConfigFromFlags("", cfg.Kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("loading kubeconfig: %w", err)
		}
	}
	return kubernetes.NewForConfig(rc)
}

// runCertgen invokes `envoy-gateway certgen` once to materialize the
// envoy-gateway-system TLS Secrets that the data-plane Envoy pods mount
// to verify the EG xDS server. Subsequent runs are no-ops; certgen
// upserts the Secrets via SSA against the cluster apiserver.
func runCertgen(ctx context.Context, cfg Config) error {
	c, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	// EG ≥ v1.4 certgen also patches a topology-injector mutating
	// webhook ("envoy-gateway-topology-injector") with the freshly
	// rotated EG CA bundle. clrk doesn't install that webhook (see
	// renderConfig in config.go and apoxy-cloud//docs/clrk-envoy-gateway.md
	// for why), and without this flag certgen fails with a
	// `MutatingWebhookConfiguration "..." not found` error — the only
	// webhook surface clrk uses on the EG operator is "none".
	cmd := exec.CommandContext(c, cfg.BinaryPath, "certgen", "--disable-topology-injector")
	cmd.Env = os.Environ()
	if cfg.Kubeconfig != "" {
		cmd.Env = append(cmd.Env, "KUBECONFIG="+cfg.Kubeconfig)
	}
	cmd.Stdin = nil
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, string(out))
	}
	slog.Info("envoy-gateway certgen complete")
	return nil
}
