//go:build linux

package sandbox

import (
	"context"
	"fmt"
	"log/slog"
	"net/netip"
	"strings"
	"sync"

	otellog "go.opentelemetry.io/otel/log"
	corev1 "k8s.io/api/core/v1"

	"github.com/apoxy-dev/clrk/internal/egress"
	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
	"github.com/apoxy-dev/clrk/internal/ports"
	sandboxcore "github.com/apoxy-dev/apoxy/pkg/sandbox"
)

// Manager is clrk's tenant/egress wrapper around the neutral
// sandboxcore.Manager. It implements the agents.SandboxRuntime interface:
// the neutral lifecycle (Create/Start/Stop/Kill/Wait/Delete/Purge) is
// delegated to the core — adapting CreateRequest -> sandboxcore.Spec and
// re-wrapping the returned Instance — while the egress config plane
// (SetEgressBackends/Policy/InvocationID + LookupEgressState) and the OTLP
// stdio fan-out live here.
type Manager struct {
	core    *sandboxcore.Manager
	rootDir string // Per-sandbox trust dir lives under here (<rootDir>/<id>-trust).
	logsDir string
	podName string

	// imdsHostAddr / egressHostAddr / workerResolvers populate each
	// sandbox's sentrystack egress init fields.
	imdsHostAddr    string
	egressHostAddr  string
	workerResolvers []netip.AddrPort

	// logEmitter is the worker's OTLP logs logger; each sandbox's stdio
	// line writer emits one LogRecord per line through it. nil (no OTLP
	// endpoint) disables emission; the file tee + slog path are unaffected.
	logEmitter otellog.Logger

	mu           sync.RWMutex
	instances    map[SandboxID]*Instance
	egressStates map[SandboxID]*EgressState
}

// ManagerConfig bundles the construction-time inputs of NewManager.
type ManagerConfig struct {
	StateDir         string
	RootDir          string
	LogsDir          string
	PodName          string
	ImageStore       *ImageStore
	IMDSHostAddr     string
	EgressHostAddr   string
	Resolvers        []netip.AddrPort
	WorkerCgroupPath string
	LogEmitter       otellog.Logger
}

// NewManager constructs the worker's sandbox lifecycle manager over the
// neutral core, wiring the per-sandbox OTLP stdio sink hook back into it.
func NewManager(cfg ManagerConfig) *Manager {
	m := &Manager{
		rootDir:         cfg.RootDir,
		logsDir:         cfg.LogsDir,
		podName:         cfg.PodName,
		imdsHostAddr:    cfg.IMDSHostAddr,
		egressHostAddr:  cfg.EgressHostAddr,
		workerResolvers: cfg.Resolvers,
		logEmitter:      cfg.LogEmitter,
		instances:       make(map[SandboxID]*Instance),
		egressStates:    make(map[SandboxID]*EgressState),
	}
	m.core = sandboxcore.NewManager(sandboxcore.ManagerConfig{
		StateDir:       cfg.StateDir,
		RootDir:        cfg.RootDir,
		ImageStore:     cfg.ImageStore,
		HostCgroupPath: cfg.WorkerCgroupPath,
		LogSinkFor:     m.logSinkFor,
	})
	return m
}

// EnsureImage pulls (or returns cached metadata for) ref via the core
// image store.
func (m *Manager) EnsureImage(ctx context.Context, ref string) (*ImageInfo, error) {
	return m.core.EnsureImage(ctx, ref)
}

// ImageStore returns the core image store so callers that need the
// cached-refs query (the worker status service) can reach it.
func (m *Manager) ImageStore() *ImageStore { return m.core.ImageStore() }

// LookupEgressState returns the per-sandbox egress snapshot consulted by
// the worker's egress bridge on every accepted conn. Returns (zero, false)
// when no sandbox is registered — the bridge drops stray dials from
// unknown SandboxIDs rather than open-egress them.
func (m *Manager) LookupEgressState(sandboxID string) (EgressState, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	st, ok := m.egressStates[SandboxID(sandboxID)]
	if !ok || st == nil {
		return EgressState{}, false
	}
	return *st, true
}

// egressStateLocked returns the per-sandbox state record, creating it
// lazily on first write. Must be called with m.mu held.
func (m *Manager) egressStateLocked(id SandboxID) *EgressState {
	st, ok := m.egressStates[id]
	if !ok {
		st = &EgressState{}
		m.egressStates[id] = st
	}
	return st
}

// Create adapts the CRD CreateRequest down to a neutral sandboxcore.Spec
// (trust + persistent-state mounts, assembled env, identity annotations,
// egress init fields), drives the core's Create, and tracks the resulting
// instance + seeds its egress state.
func (m *Manager) Create(ctx context.Context, req CreateRequest) (*Instance, error) {
	m.mu.RLock()
	_, exists := m.instances[req.ID]
	m.mu.RUnlock()
	if exists {
		return nil, ErrAlreadyExists
	}

	// Resolve the rootfs up front so the trust-bundle mounts only overlay
	// CA paths that exist in it. The pull is singleflight-cached, so the
	// re-pull inside core.Create is free.
	imgInfo, err := m.core.EnsureImage(ctx, req.Sandbox.Image)
	if err != nil {
		return nil, fmt.Errorf("ensuring image: %w", err)
	}

	var mounts []sandboxcore.Mount
	caWritten := false
	if len(req.CAPEM) > 0 {
		caPath, err := m.writeAgentCA(req.ID, req.CAPEM)
		if err != nil {
			return nil, fmt.Errorf("staging agent CA: %w", err)
		}
		caWritten = true
		mounts = append(mounts, buildTrustMounts(imgInfo.RootFS, caPath)...)
	}
	if req.State != nil {
		hostPath, err := ensureStateDir(req.Identity.Namespace, req.AgentRef, req.State.SizeLimitMB)
		if err != nil {
			if caWritten {
				m.removeAgentCA(req.ID)
			}
			return nil, err
		}
		mounts = append(mounts, buildStateMount(hostPath, req.State))
	}

	var cpuMillis, memBytes int64
	if !req.Resources.CPU.IsZero() {
		cpuMillis = req.Resources.CPU.MilliValue()
	}
	if !req.Resources.Memory.IsZero() {
		memBytes = req.Resources.Memory.Value()
	}

	spec := sandboxcore.Spec{
		ID:          req.ID,
		Image:       req.Sandbox.Image,
		Command:     req.Sandbox.Command,
		Args:        req.Sandbox.Args,
		Env:         buildProcessEnv(req.Sandbox.Env),
		CPUMillis:   cpuMillis,
		MemBytes:    memBytes,
		Mounts:      mounts,
		Annotations: buildSandboxAnnotations(req.Identity, m.podName, req.Attempt),
		Stdio:       req.Stdio,
		Egress: sandboxcore.EgressInit{
			IMDSHostAddr:   m.imdsHostAddr,
			EgressHostAddr: m.egressHostAddr,
			IMDSV4:         fmt.Sprintf("%s:%d", ports.MetadataAddrV4, ports.MetadataPort),
			IMDSV6:         fmt.Sprintf("[%s]:%d", ports.MetadataAddrV6, ports.MetadataPort),
			DNSResolvers:   resolverStrings(m.workerResolvers),
		},
	}

	coreInst, err := m.core.Create(ctx, spec)
	if err != nil {
		if caWritten {
			m.removeAgentCA(req.ID)
		}
		return nil, err
	}

	inst := &Instance{
		Instance:  coreInst,
		AgentRef:  req.AgentRef,
		Namespace: req.Identity.Namespace,
		Sandbox:   req.Sandbox,
		Resources: req.Resources,
		Identity:  req.Identity,
	}
	m.mu.Lock()
	m.instances[req.ID] = inst
	// Seed the egress state record with the sandbox's identity so the
	// bridge has something useful to log before SetEgress* fire (which
	// they may not at all, for DaemonAgents with no EgressRefs).
	m.egressStateLocked(req.ID).Identity = req.Identity
	m.mu.Unlock()
	return inst, nil
}

// Start fires the sandbox via the core; the core's stdio drain pulls the
// per-sandbox OTLP/slog/file sink from m.logSinkFor.
func (m *Manager) Start(ctx context.Context, id SandboxID) error {
	return m.core.Start(ctx, id)
}

// Stop / Kill / Purge delegate to the core (neutral lifecycle).
func (m *Manager) Stop(ctx context.Context, id SandboxID) error { return m.core.Stop(ctx, id) }
func (m *Manager) Kill(ctx context.Context, id SandboxID) error { return m.core.Kill(ctx, id) }
func (m *Manager) Purge(ctx context.Context, id SandboxID)      { m.core.Purge(ctx, id) }

// Wait blocks until the sandbox's init process exits. The core flips the
// (shared, embedded) instance Phase + flushes the stdio sink.
func (m *Manager) Wait(ctx context.Context, id SandboxID) (int, error) {
	return m.core.Wait(ctx, id)
}

// Delete tears the sandbox down via the core, then drops the wrapper's
// per-sandbox trust dir + identity/egress bookkeeping.
func (m *Manager) Delete(ctx context.Context, id SandboxID) error {
	err := m.core.Delete(ctx, id)
	m.removeAgentCA(id)
	m.mu.Lock()
	delete(m.instances, id)
	delete(m.egressStates, id)
	m.mu.Unlock()
	return err
}

// Status refreshes the core instance's phase and returns the wrapper view.
func (m *Manager) Status(ctx context.Context, id SandboxID) (*Instance, error) {
	if _, err := m.core.Status(ctx, id); err != nil {
		return nil, err
	}
	m.mu.RLock()
	inst, ok := m.instances[id]
	m.mu.RUnlock()
	if !ok {
		return nil, ErrNotFound
	}
	return inst, nil
}

// List returns a snapshot of the wrapper instances.
func (m *Manager) List() []*Instance {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Instance, 0, len(m.instances))
	for _, inst := range m.instances {
		out = append(out, inst)
	}
	return out
}

// Cleanup reaps on-host runsc orphans from a previous incarnation (neutral).
func (m *Manager) Cleanup(ctx context.Context) error { return m.core.Cleanup(ctx) }

// SetEgressBackends updates the per-listener egress backends on the
// worker-side egress state map consulted by the egress bridge on every dial.
func (m *Manager) SetEgressBackends(id SandboxID, backends []egress.BackendListener) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.instances[id]; !ok {
		return ErrNotFound
	}
	m.egressStateLocked(id).Backends = backends
	return nil
}

// SetEgressPolicy updates the per-sandbox policy handle. Nil disables
// enforcement; the bridge interprets a nil Policy as "allow all".
func (m *Manager) SetEgressPolicy(id SandboxID, policy *egress.SandboxPolicy) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.instances[id]; !ok {
		return ErrNotFound
	}
	m.egressStateLocked(id).Policy = policy
	return nil
}

// SetInvocationID stamps the per-dispatch InvocationID into both the
// sandbox's identity record and the egress state map (read by the bridge
// on every backend dial so the PROXY v2 TLV announces the current invocation).
func (m *Manager) SetInvocationID(id SandboxID, invocationID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	inst, ok := m.instances[id]
	if !ok {
		return ErrNotFound
	}
	inst.Identity.InvocationID = invocationID
	m.egressStateLocked(id).Identity.InvocationID = invocationID
	return nil
}

// logSinkFor builds the per-sandbox OTLP/slog/file stdio sink. Called by
// the core's Start via ManagerConfig.LogSinkFor; the wrapper resolves the
// sandbox's identity from its instance table to attribute every line.
func (m *Manager) logSinkFor(id SandboxID) sandboxcore.StdioSink {
	m.mu.RLock()
	inst, ok := m.instances[id]
	m.mu.RUnlock()
	var identity proxyproto.AgentIdentity
	if ok {
		identity = inst.Identity
	}

	sbLogger := slog.With(append(
		[]any{slog.String("sandbox.id", string(id))},
		identityLogFields(identity)...,
	)...)
	logFile, err := openAgentLogFile(m.logsDir, identity.Namespace, identity.Name, identity.InvocationID)
	if err != nil {
		slog.Error("Opening agent log file (continuing without file tee)", "error", err)
	}
	stdoutLog := newSandboxLineWriter(sbLogger, slog.LevelInfo, "stdout", logFile, m.logEmitter, identity)
	stderrLog := newSandboxLineWriter(sbLogger, slog.LevelWarn, "stderr", logFile, m.logEmitter, identity)

	return sandboxcore.StdioSink{
		Stdout: stdoutLog,
		Stderr: stderrLog,
		Close: func() {
			stdoutLog.Flush()
			stderrLog.Flush()
			if logFile != nil {
				_ = logFile.Close()
			}
		},
	}
}

// buildProcessEnv assembles the env passed to the sandbox process via the
// OCI spec. Order: TLS trust overrides, IMDS URL constants, then
// user-supplied AgentSandbox.Env. PATH is appended only if not already set.
func buildProcessEnv(userEnv []corev1.EnvVar) []string {
	env := append([]string(nil), trustEnv("/etc/ssl/certs/ca-certificates.crt")...)
	env = append(env,
		fmt.Sprintf("CLRK_METADATA_URL=http://%s/v1", ports.MetadataAddrV4),
		fmt.Sprintf("CLRK_METADATA_URL_V6=http://[%s]/v1", ports.MetadataAddrV6),
	)
	env = append(env, envVarsToStrings(userEnv)...)
	hasPath := false
	for _, e := range env {
		if strings.HasPrefix(e, "PATH=") {
			hasPath = true
			break
		}
	}
	if !hasPath {
		env = append(env, "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
	}
	return env
}

// envVarsToStrings converts a corev1.EnvVar slice to "KEY=VALUE" strings.
func envVarsToStrings(envVars []corev1.EnvVar) []string {
	result := make([]string, 0, len(envVars))
	for _, ev := range envVars {
		result = append(result, fmt.Sprintf("%s=%s", ev.Name, ev.Value))
	}
	return result
}

// resolverStrings stringifies the worker resolver list for the sentrystack
// init payload's DNSResolvers field.
func resolverStrings(resolvers []netip.AddrPort) []string {
	if len(resolvers) == 0 {
		return nil
	}
	out := make([]string, 0, len(resolvers))
	for _, r := range resolvers {
		out = append(out, r.String())
	}
	return out
}
