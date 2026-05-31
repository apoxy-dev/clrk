package agents

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/cloudevents"
	"github.com/apoxy-dev/clrk/internal/egress"
	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
	"github.com/apoxy-dev/clrk/internal/ports"
	"github.com/apoxy-dev/clrk/internal/sandbox/metadata"
	"github.com/apoxy-dev/clrk/internal/worker/sandbox"
)

const (
	// requestBodyLimit caps the buffered request body for envelope
	// construction. Sized to handle the typical TaskAgent dispatch
	// payload (a few KB to a few MB) plus generous headroom; agents
	// that need streaming-larger payloads should opt for delivery
	// mode Metadata where the body is served via /v1/event reads.
	requestBodyLimit = 32 << 20 // 32 MiB
)

const (
	// hardTimeoutCap is the absolute upper bound on a single execution,
	// regardless of TaskAgent.spec.timeout. Matches the cron
	// invoker's per-fire cap so cron and HTTP share the same ceiling.
	hardTimeoutCap = 5 * time.Minute
)

// Dispatcher serves inbound TaskAgent execution requests routed to
// this worker by the per-TaskAgent HTTPRoute. One shared instance per
// worker pod; concurrency is capped per-TaskAgent via semaphores.
//
// Exported so out-of-tree black-box tests at apoxy-cloud//clrk/worker
// can construct a Dispatcher with a fake SandboxRuntime — see
// SandboxRuntime in types.go.
type Dispatcher struct {
	client     client.Client
	sandboxMgr SandboxRuntime
	warmPool   WarmAcquirer
	router     *egress.Router
	podName    string
	namespace  string

	// metaReg is the worker-wide IMDS entry registry. Metadata-mode
	// dispatches Register against it; the central IMDS HTTP server
	// reads from it on every request via sandbox.SandboxID lookup.
	metaReg *metadata.Registry

	active *activeCounter

	// semaphores caps in-flight executions per (TaskAgent ns, name).
	// Stored as *sema so the same capacity value (the chan) is reused
	// across requests for the same agent, and resized lazily when the
	// spec.maxConcurrent value changes.
	semaphores sync.Map // types.NamespacedName -> *sema

	// draining is flipped by Drain() once worker shutdown begins so
	// new dispatches get a synchronous 503 (the ingress picker
	// retries on the next worker) while in-flight requests finish
	// inside the cooperative srv.Shutdown grace.
	draining atomic.Bool

	// invPub publishes Running + terminal Invocation lifecycle events to
	// the controller-manager's JetStream (APO-618). Nil when no cm NATS
	// address is configured; emitInvocation is then a no-op so dispatch
	// is unaffected. Its Run loop is started by RunInvocationPublisher.
	invPub *invPublisher
}

// WarmAcquirer is the subset of *WarmPool the dispatcher uses on the
// hot path. Defined as an interface so the linux-only WarmPool type
// stays out of the platform-agnostic Dispatcher and tests can pass a
// nil-or-fake.
type WarmAcquirer interface {
	Acquire(key WarmKey) *sandbox.Instance
}

type sema struct {
	cap int32
	ch  chan struct{}
}

// DispatcherConfig bundles the construction-time inputs of
// NewDispatcher. The previous 7-arg positional signature mixed
// infrastructure handles (client, router, MetadataReg), pod identity
// (PodName, Namespace), and per-pod shared state (Active) in one
// list; the struct keeps each field's role visible at the call site
// and lets tests partially override config.
//
// MetadataReg is the worker-wide IMDS registry that Metadata-mode
// dispatches use to expose the per-execution *metadata.Entry to the
// central IMDS HTTP server. May be nil in tests that exercise only
// the Stdin delivery path.
type DispatcherConfig struct {
	Client      client.Client
	Runtime     SandboxRuntime
	Router      *egress.Router
	PodName     string
	Namespace   string
	Active      *activeCounter
	MetadataReg *metadata.Registry

	// CMNATSAddr is the controller-manager's NATS/JetStream client
	// address (host:port) the dispatcher publishes Invocation lifecycle
	// events to. Empty disables publishing. Sourced from
	// invevent.CMNATSAddrEnv, injected by the WorkerPool controller.
	CMNATSAddr string
}

// NewDispatcher constructs a Dispatcher. Production callers pass a
// real *sandbox.Manager (which satisfies SandboxRuntime) and a real
// *egress.Router; tests pass fakes / nil where appropriate.
func NewDispatcher(cfg DispatcherConfig) *Dispatcher {
	d := &Dispatcher{
		client:     cfg.Client,
		sandboxMgr: cfg.Runtime,
		router:     cfg.Router,
		podName:    cfg.PodName,
		namespace:  cfg.Namespace,
		active:     cfg.Active,
		metaReg:    cfg.MetadataReg,
	}
	if cfg.CMNATSAddr != "" {
		d.invPub = newInvPublisher(cfg.CMNATSAddr, cfg.PodName)
	}
	return d
}

// RunInvocationPublisher drains and ships queued Invocation lifecycle
// events until ctx is cancelled. The worker runtime starts it in a
// goroutine. No-op (returns nil) when no cm NATS address was configured.
func (d *Dispatcher) RunInvocationPublisher(ctx context.Context) error {
	if d.invPub == nil {
		return nil
	}
	return d.invPub.Run(ctx)
}

// SetWarmPool installs the warm-pool acquirer used by the dispatcher's
// hot path. Optional — a nil warmPool means every dispatch takes the
// cold path (Create + Start). Call once during runtime startup before
// the dispatcher accepts traffic.
func (d *Dispatcher) SetWarmPool(w WarmAcquirer) { d.warmPool = w }

// Drain flips the dispatcher into shutdown mode: subsequent
// ServeHTTP calls return 503 before any sandbox work, while
// already-running requests continue to completion under the
// cooperative srv.Shutdown grace.
func (d *Dispatcher) Drain() { d.draining.Store(true) }

// acquire returns a release func on success or nil if the per-TaskAgent
// MaxConcurrent cap is full. cap == 0 means unlimited (no semaphore
// allocated).
func (d *Dispatcher) acquire(key types.NamespacedName, cap int32) func() {
	if cap <= 0 {
		return func() {}
	}
	for {
		v, ok := d.semaphores.Load(key)
		if !ok {
			fresh := &sema{cap: cap, ch: make(chan struct{}, int(cap))}
			actual, loaded := d.semaphores.LoadOrStore(key, fresh)
			if !loaded {
				v = fresh
			} else {
				v = actual
			}
		}
		s := v.(*sema)
		// If the cap shrank or grew between requests, swap atomically.
		// Old chan drains as in-flight requests release; new requests
		// queue against the new chan immediately.
		if s.cap != cap {
			fresh := &sema{cap: cap, ch: make(chan struct{}, int(cap))}
			if d.semaphores.CompareAndSwap(key, s, fresh) {
				s = fresh
			} else {
				continue
			}
		}
		select {
		case s.ch <- struct{}{}:
			return func() { <-s.ch }
		default:
			return nil
		}
	}
}

// reqCtx is the per-request derived state shared across the
// acceptRequest / acquireSandbox / startAndSetupDelivery /
// drainResponse phases of Dispatcher.ServeHTTP. Built once at the
// top and threaded through; lets each phase take what it needs
// without re-parsing the request.
type reqCtx struct {
	ctx          context.Context
	cancel       context.CancelFunc
	log          logr.Logger
	ns           string
	name         string
	key          types.NamespacedName
	ta           clrkv1alpha1.TaskAgent
	rev          clrkv1alpha1.AgentSandboxRevision
	identity     proxyproto.AgentIdentity
	invocationID string
	release      func()

	// trigger and created stamp the Invocation lifecycle snapshots the
	// dispatcher publishes (APO-618). created is captured once at
	// request receipt and reused for every emitted phase so the
	// read-side argMax (which takes the latest event's whole object)
	// reports a stable creationTimestamp instead of drifting forward by
	// the execution duration on the terminal event.
	trigger clrkv1alpha1.InvocationTriggerType
	created metav1.Time
}

// deliveryState holds the per-request transport plumbing decided by
// startAndSetupDelivery and consumed by drainResponse. mdEntry is
// non-nil iff Mode=Metadata; respCh / stdoutBuf / stdoutBufErr are
// populated only on their respective Stdin sub-branches.
type deliveryState struct {
	mode         clrkv1alpha1.AgentDeliveryMode
	wrapResponse bool
	ceID         string

	mdEntry      *metadata.Entry
	mdUnregister func()

	respCh       chan bool
	stdoutBuf    *bytes.Buffer
	stdoutBufErr chan error
	stdinErr     chan error

	flusher http.Flusher
}

func (d *Dispatcher) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rc, ok := d.acceptRequest(w, r)
	if !ok {
		return
	}
	defer rc.cancel()
	defer rc.release()
	defer d.active.dec(rc.key)

	// Guarantee exactly one terminal lifecycle event per admitted
	// invocation. We've committed to running it (id resolved, semaphore
	// held), so it must not linger non-terminal and inflate
	// ActiveExecutions if setup fails before the agent runs. Default to
	// Failed; the happy path upgrades `terminal` to the real outcome
	// after drainResponse. Registered before the sandbox/delivery defers
	// so it runs after them but before rc.cancel (LIFO) — terminalPhase
	// still observes the live deadline state.
	terminal := clrkv1alpha1.InvocationPhaseFailed
	defer func() { d.emitInvocation(rc, terminal) }()

	sb, sandboxID, bundle, ok := d.acquireSandbox(w, rc)
	if !ok {
		return
	}
	defer deleteSandboxBounded(d.sandboxMgr, sandboxID, rc.log, "Failed to delete sandbox")
	d.applyEgressAndInvocation(rc, sandboxID, bundle)

	ds, ok := d.startAndSetupDelivery(w, r, rc, sb, sandboxID)
	if !ok {
		return
	}
	// Sandbox is running — emit Running before we block on the agent.
	d.emitInvocation(rc, clrkv1alpha1.InvocationPhaseRunning)
	if ds.mdUnregister != nil {
		defer ds.mdUnregister()
	}
	if ds.stdinErr != nil {
		defer func() { <-ds.stdinErr }()
	}

	exitCode, waitErr := waitOrStop(rc.ctx, d.sandboxMgr, sandboxID)
	if ds.mdEntry != nil {
		ds.mdEntry.CancelIfPending()
	}

	delivered := d.drainResponse(w, rc, ds, exitCode, waitErr)
	terminal = terminalPhase(rc, exitCode, waitErr, delivered)
}

// acceptRequest parses headers, looks up the TaskAgent + revision,
// acquires the per-TA concurrency semaphore, and bumps the active
// counter. Writes the appropriate HTTP error (4xx/5xx) and returns
// (nil, false) on any failure. Returned reqCtx owns the timeout
// context, semaphore release, and request-scoped logger.
func (d *Dispatcher) acceptRequest(w http.ResponseWriter, r *http.Request) (*reqCtx, bool) {
	log := ctrl.LoggerFrom(r.Context()).WithName("dispatch")

	hdr := r.Header.Get(ports.HeaderTaskAgent)
	if hdr == "" {
		http.Error(w, "missing "+ports.HeaderTaskAgent+" header", http.StatusBadRequest)
		return nil, false
	}
	if d.draining.Load() {
		// Worker is shutting down. Synchronous 503 so the ingress
		// picker steers to another worker without burning retry
		// budget on a connection-level error.
		http.Error(w, "worker draining", http.StatusServiceUnavailable)
		return nil, false
	}
	ns, name, ok := strings.Cut(hdr, "/")
	if !ok || ns == "" || name == "" {
		http.Error(w, "invalid "+ports.HeaderTaskAgent+" header (want ns/name)", http.StatusBadRequest)
		return nil, false
	}
	key := types.NamespacedName{Namespace: ns, Name: name}
	log = log.WithValues("taskAgent", key.String(), "trigger", r.Header.Get(ports.HeaderTrigger))

	var ta clrkv1alpha1.TaskAgent
	if err := d.client.Get(r.Context(), key, &ta); err != nil {
		if apierrors.IsNotFound(err) {
			http.Error(w, "TaskAgent not found", http.StatusNotFound)
			return nil, false
		}
		log.Error(err, "Failed to get TaskAgent")
		http.Error(w, "TaskAgent lookup failed", http.StatusInternalServerError)
		return nil, false
	}
	if ta.Status.LatestReadyRevisionName == "" {
		http.Error(w, "TaskAgent has no ready revision", http.StatusServiceUnavailable)
		return nil, false
	}

	maxConcurrent := int32(0)
	if ta.Spec.MaxConcurrent != nil {
		maxConcurrent = *ta.Spec.MaxConcurrent
	}
	release := d.acquire(key, maxConcurrent)
	if release == nil {
		http.Error(w, "TaskAgent at MaxConcurrent on this worker", http.StatusTooManyRequests)
		return nil, false
	}
	d.active.inc(key)

	var rev clrkv1alpha1.AgentSandboxRevision
	revKey := types.NamespacedName{Namespace: ns, Name: ta.Status.LatestReadyRevisionName}
	if err := d.client.Get(r.Context(), revKey, &rev); err != nil {
		release()
		d.active.dec(key)
		if apierrors.IsNotFound(err) {
			http.Error(w, "AgentSandboxRevision not found locally", http.StatusServiceUnavailable)
			return nil, false
		}
		log.Error(err, "Failed to get AgentSandboxRevision")
		http.Error(w, "revision lookup failed", http.StatusInternalServerError)
		return nil, false
	}

	timeout := hardTimeoutCap
	if ta.Spec.Timeout != nil && ta.Spec.Timeout.Duration > 0 && ta.Spec.Timeout.Duration < timeout {
		timeout = ta.Spec.Timeout.Duration
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)

	// Per-invocation id is stamped on the sandbox identity (and thus on
	// every PROXY v2 frame the IdentityDialer emits) so the egress
	// ext_proc can look up the inbound traceparent in the
	// invocationctx store on each outbound LLM/MCP call. Resolution
	// order favors what ingress already stamped, falling back to the
	// caller's idempotency key, Envoy's auto-injected x-request-id,
	// and finally a fresh UUID for direct-to-dispatcher invocations
	// that bypass the ingress edge.
	invocationID := resolveInvocationID(r.Header)
	identity := newAgentIdentity(proxyproto.AgentKindTask, ns, name, string(ta.UID), rev.Name)
	identity.InvocationID = invocationID

	return &reqCtx{
		ctx:          ctx,
		cancel:       cancel,
		log:          log,
		ns:           ns,
		name:         name,
		key:          key,
		ta:           ta,
		rev:          rev,
		identity:     identity,
		invocationID: invocationID,
		release:      release,
		trigger:      triggerTypeFromHeader(r.Header.Get(ports.HeaderTrigger)),
		created:      metav1.Now(),
	}, true
}

// acquireSandbox returns a ready sandbox.Instance for the request:
// the warm pool's most-recently-warmed entry when one is available
// for the (ns,agent,revision) key, otherwise a freshly-Created
// instance. The accompanying EgressBundle is re-resolved on both
// paths (warm + cold) so spec changes that don't bump the revision
// (e.g. EgressRefs swap) still take effect at consume time.
//
// Writes HTTP errors and returns (nil, "", _, false) on failure.
// Caller owns the returned sandbox lifecycle and must Delete it.
func (d *Dispatcher) acquireSandbox(w http.ResponseWriter, rc *reqCtx) (*sandbox.Instance, sandbox.SandboxID, EgressBundle, bool) {
	var sb *sandbox.Instance
	var sandboxID sandbox.SandboxID
	warmHit := false
	if d.warmPool != nil {
		if warm := d.warmPool.Acquire(WarmKey{Namespace: rc.ns, Agent: rc.name, Revision: rc.rev.Name}); warm != nil {
			sb = warm
			sandboxID = warm.ID
			warmHit = true
			rc.log = rc.log.WithValues("warm", true, "sandboxID", sandboxID, "invocationID", rc.invocationID)
		}
	}

	bundle, err := ResolveEgress(rc.ctx, d.client, d.router, rc.ns, rc.ta.Spec.EgressRefs)
	if err != nil {
		if warmHit {
			// Cleanup the warm sandbox we already pulled — caller will
			// retry, the warm pool will refill on next Acquire kick.
			deleteSandboxBounded(d.sandboxMgr, sandboxID, rc.log, "Failed to delete unused warm sandbox")
		}
		rc.log.Error(err, "Failed to resolve egress for TaskAgent")
		http.Error(w, "egress not ready", http.StatusServiceUnavailable)
		return nil, "", EgressBundle{}, false
	}

	if !warmHit {
		sandboxID, err = newSandboxID(sandboxIDPrefixTask, rc.ns, rc.name)
		if err != nil {
			rc.log.Error(err, "Failed to generate sandbox id")
			http.Error(w, "internal error", http.StatusInternalServerError)
			return nil, "", EgressBundle{}, false
		}
		d.sandboxMgr.Purge(rc.ctx, sandboxID)
		sb, err = d.sandboxMgr.Create(rc.ctx, sandbox.CreateRequest{
			ID:        sandboxID,
			AgentRef:  rc.name,
			Identity:  rc.identity,
			CAPEM:     bundle.CAPEM,
			Sandbox:   rc.rev.Spec.AgentSandbox,
			Resources: rc.ta.Spec.Resources,
			State:     rc.ta.Spec.State,
			Stdio:     true,
		})
		if err != nil {
			if errors.Is(err, sandbox.ErrStateOverLimit) {
				rc.log.Info("Refusing dispatch — agent state over size limit", "err", err)
				http.Error(w, "agent state over size limit", http.StatusInsufficientStorage)
				return nil, "", EgressBundle{}, false
			}
			rc.log.Error(err, "Failed to create sandbox")
			http.Error(w, "sandbox create failed", http.StatusInternalServerError)
			return nil, "", EgressBundle{}, false
		}
	}
	return sb, sandboxID, bundle, true
}

// applyEgressAndInvocation pushes the bundle's Backends/Policy and
// the per-dispatch InvocationID onto the sandbox's egress state
// slot. SetInvocationID is mandatory on the warm path (the slot was
// created at fill time with no invocation) and a no-op rewrite on
// the cold path — same call shape keeps the two paths identical.
// Errors are logged but not surfaced; the per-dial fallback in the
// egress bridge handles missing state gracefully.
func (d *Dispatcher) applyEgressAndInvocation(rc *reqCtx, sandboxID sandbox.SandboxID, bundle EgressBundle) {
	if len(bundle.Backends) > 0 {
		if err := d.sandboxMgr.SetEgressBackends(sandboxID, bundle.Backends); err != nil {
			rc.log.Error(err, "Set egress backends failed")
		}
	}
	if bundle.Policy != nil {
		if err := d.sandboxMgr.SetEgressPolicy(sandboxID, bundle.Policy); err != nil {
			rc.log.Error(err, "Set egress policy failed")
		}
	}
	if err := d.sandboxMgr.SetInvocationID(sandboxID, rc.invocationID); err != nil {
		rc.log.Error(err, "Set invocation id failed")
	}
}

// startAndSetupDelivery buffers the request body, Starts the
// sandbox, wires the delivery transport (Stdin envelope vs Metadata
// registry register), and arms the response capture (direct stream
// vs CE-wrap buffer vs Metadata response wait). Writes HTTP errors
// and returns (nil, false) on any setup failure.
//
// Returned deliveryState owns mdUnregister (must be defer-called by
// ServeHTTP) and stdinErr (drained by ServeHTTP at end so the stdin
// write goroutine doesn't leak).
func (d *Dispatcher) startAndSetupDelivery(w http.ResponseWriter, r *http.Request, rc *reqCtx, sb *sandbox.Instance, sandboxID sandbox.SandboxID) (*deliveryState, bool) {
	mode := clrkv1alpha1.AgentDeliveryStdin
	if rc.ta.Spec.Delivery != nil && rc.ta.Spec.Delivery.Mode != "" {
		mode = rc.ta.Spec.Delivery.Mode
	}
	ceAttrs := cloudevents.AttrsFromRequest(r, &rc.ta)
	ds := &deliveryState{
		mode:         mode,
		wrapResponse: strings.EqualFold(r.Header.Get("Accept"), cloudevents.MediaType),
		ceID:         ceAttrs[cloudevents.AttrID],
	}

	bodyBytes, err := readBodyBounded(r.Body, requestBodyLimit)
	if err != nil {
		rc.log.Error(err, "Failed to read request body")
		http.Error(w, "request body read failed", http.StatusBadRequest)
		return nil, false
	}

	if err := d.sandboxMgr.Start(rc.ctx, sandboxID); err != nil {
		rc.log.Error(err, "Failed to start sandbox")
		http.Error(w, "sandbox start failed", http.StatusInternalServerError)
		return nil, false
	}

	// Per-mode request delivery.
	switch mode {
	case clrkv1alpha1.AgentDeliveryMetadata:
		ds.mdEntry = metadata.NewEntry(ds.ceID, r.Header.Get("Content-Type"), bodyBytes, ceAttrs)
		if d.metaReg == nil {
			http.Error(w, "metadata delivery not enabled", http.StatusInternalServerError)
			return nil, false
		}
		ds.mdUnregister = d.metaReg.Register(string(sb.ID), ds.mdEntry)
		// Close stdin immediately so an agent that mistakenly reads
		// from it sees EOF rather than blocking forever.
		_ = sb.Stdin.Close()

	default:
		// Stdin (default). Build a structured-mode CE JSON envelope
		// and stream it to stdin in the background; close stdin on
		// EOF so the agent's read returns. The envelope carries the
		// request body (inline for JSON/text, base64 for everything
		// else) plus all CE attributes including httpmethod /
		// httpurl / httpquery — so a body-less GET still reaches the
		// agent with the request line intact, which a raw-body pipe
		// could not represent.
		envelope := buildCEEnvelope(ceAttrs, r.Header.Get("Content-Type"), bodyBytes)
		ds.stdinErr = make(chan error, 1)
		go func() {
			_, werr := io.Copy(sb.Stdin, bytes.NewReader(envelope))
			_ = sb.Stdin.Close()
			ds.stdinErr <- werr
		}()
	}

	// Response-side wiring depends on transport. Stdin mode streams
	// stdout straight back (or, if wrapResponse, buffers it for
	// re-encoding). Metadata mode waits for the agent's POST
	// /v1/response and serves its body.
	ds.flusher, _ = w.(http.Flusher)
	switch {
	case mode == clrkv1alpha1.AgentDeliveryStdin && !ds.wrapResponse:
		w.Header().Set("Content-Type", "application/octet-stream")
		ds.respCh = make(chan bool, 1)
		go func() { ds.respCh <- streamWithFlush(w, sb.Stdout, ds.flusher) }()

	case mode == clrkv1alpha1.AgentDeliveryStdin && ds.wrapResponse:
		ds.stdoutBuf = &bytes.Buffer{}
		ds.stdoutBufErr = make(chan error, 1)
		go func() {
			_, copyErr := io.Copy(ds.stdoutBuf, sb.Stdout)
			ds.stdoutBufErr <- copyErr
		}()

	case mode == clrkv1alpha1.AgentDeliveryMetadata:
		// Drain stdout to the per-agent log only — we don't surface
		// it to the caller. Discard avoids a goroutine leak when the
		// agent writes anything to stdout.
		go func() { _, _ = io.Copy(io.Discard, sb.Stdout) }()
	}
	return ds, true
}

// drainResponse writes the response body based on transport + exit
// shape, then maps any non-success exit/wait/timeout outcome onto
// the appropriate HTTP error. Headers may already be flushed when
// this runs (streaming Stdin mode), in which case errors are folded
// into the (best-effort) HeaderExitCode trailer rather than a fresh
// http.Error. It returns whether any response bytes reached the wire;
// the caller uses that to classify the terminal phase, since a
// delivered response means the client saw a 2xx even if the
// post-delivery runsc wait raced.
func (d *Dispatcher) drainResponse(w http.ResponseWriter, rc *reqCtx, ds *deliveryState, exitCode int, waitErr error) bool {
	success := waitErr == nil && exitCode == 0
	wroteAnyBytes := false

	switch {
	case ds.mode == clrkv1alpha1.AgentDeliveryStdin && !ds.wrapResponse:
		wroteAnyBytes = <-ds.respCh

	case ds.mode == clrkv1alpha1.AgentDeliveryStdin && ds.wrapResponse:
		<-ds.stdoutBufErr
		w.Header().Set("Content-Type", cloudevents.MediaType)
		respEnv := buildCEResponseEnvelope(ds.ceID, "application/octet-stream", ds.stdoutBuf.Bytes())
		if _, werr := w.Write(respEnv); werr == nil {
			wroteAnyBytes = true
			if ds.flusher != nil {
				ds.flusher.Flush()
			}
		}

	case ds.mode == clrkv1alpha1.AgentDeliveryMetadata:
		respBody, respCT, delivered := ds.mdEntry.Response()
		if !delivered {
			// Agent exited without posting a response. Fall through
			// to the exit-code error path below.
			break
		}
		if respCT == "" {
			respCT = "application/octet-stream"
		}
		if ds.wrapResponse {
			w.Header().Set("Content-Type", cloudevents.MediaType)
			env := buildCEResponseEnvelope(ds.ceID, respCT, respBody)
			if _, werr := w.Write(env); werr == nil {
				wroteAnyBytes = true
			}
		} else {
			w.Header().Set("Content-Type", respCT)
			if _, werr := w.Write(respBody); werr == nil {
				wroteAnyBytes = true
			}
		}
		if ds.flusher != nil {
			ds.flusher.Flush()
		}
	}

	switch {
	case rc.ctx.Err() == context.DeadlineExceeded:
		// Headers may already be flushed if we streamed any bytes;
		// in that case the trailer is the only signal we can give.
		if !wroteAnyBytes {
			http.Error(w, "execution timed out", http.StatusGatewayTimeout)
		}
	case waitErr != nil:
		rc.log.Error(waitErr, "Sandbox wait failed")
		if !wroteAnyBytes {
			http.Error(w, "sandbox wait failed", http.StatusInternalServerError)
		}
	case !success:
		w.Header().Set(ports.HeaderExitCode, fmt.Sprintf("%d", exitCode))
		if !wroteAnyBytes {
			http.Error(w, fmt.Sprintf("agent exited with code %d", exitCode), http.StatusInternalServerError)
		}
		// If we already streamed bytes, the response is whatever's
		// on the wire — exit code lives in the (best-effort) header
		// above.
	case ds.mode == clrkv1alpha1.AgentDeliveryMetadata && !wroteAnyBytes:
		// Agent succeeded without posting a response in Metadata
		// mode. Treat as a server-side bug — the agent should
		// always POST.
		http.Error(w, "agent did not post a response", http.StatusBadGateway)
	}
	return wroteAnyBytes
}

// resolveInvocationID returns the per-request invocation id used as
// the PROXY v2 invocation.id TLV value. Ingress stamps
// HeaderInvocationID on every request it forwards; we fall back to
// the caller-supplied execution-id, Envoy's auto-injected
// x-request-id, and finally a fresh UUID so direct-to-dispatcher
// invocations (cron, in-cluster bypass) still have a unique id.
func resolveInvocationID(h http.Header) string {
	if v := h.Get(ports.HeaderInvocationID); v != "" {
		return v
	}
	return cloudevents.ResolveID(cloudevents.HTTPHeader(h))
}

// triggerTypeFromHeader maps the inbound X-Clrk-Trigger value onto the
// Invocation trigger enum. It delegates to the shared API helper so the
// worker and the ingress publisher classify triggers identically.
func triggerTypeFromHeader(v string) clrkv1alpha1.InvocationTriggerType {
	return clrkv1alpha1.TriggerTypeFromHeaderValue(v)
}

// terminalPhase classifies a completed execution into its terminal
// Invocation phase, mirroring the client-visible outcome drainResponse
// produced. A deadline-exceeded context is Timeout (checked first, since
// it can coincide with a nonzero exit). A wait error means runsc could
// not read the exit status — typical for an agent that exits faster than
// the wait can observe it (`runsc wait: exit status 128: ... exit status
// is unavailable`); if the response still reached the client (delivered)
// that is a success, otherwise Failed. A reliably-captured nonzero exit
// is Failed; a clean zero exit is Succeeded.
func terminalPhase(rc *reqCtx, exitCode int, waitErr error, delivered bool) clrkv1alpha1.InvocationPhase {
	switch {
	case rc.ctx.Err() == context.DeadlineExceeded:
		return clrkv1alpha1.InvocationPhaseTimeout
	case waitErr != nil:
		if delivered {
			return clrkv1alpha1.InvocationPhaseSucceeded
		}
		return clrkv1alpha1.InvocationPhaseFailed
	case exitCode != 0:
		return clrkv1alpha1.InvocationPhaseFailed
	default:
		return clrkv1alpha1.InvocationPhaseSucceeded
	}
}

// emitInvocation publishes a complete Invocation snapshot for the given
// lifecycle phase. The snapshot reproduces the spec the ingress birth
// event set (parentRef + trigger) so the read-side argMax, which adopts
// the latest event's whole object, doesn't flip those fields on the
// worker's later events. No-op when no cm NATS address is configured.
func (d *Dispatcher) emitInvocation(rc *reqCtx, phase clrkv1alpha1.InvocationPhase) {
	if d.invPub == nil {
		return
	}
	d.invPub.enqueue(&clrkv1alpha1.Invocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:              rc.invocationID,
			Namespace:         rc.ns,
			CreationTimestamp: rc.created,
		},
		Spec: clrkv1alpha1.InvocationSpec{
			ParentRef: clrkv1alpha1.InvocationParentRef{
				Kind: clrkv1alpha1.InvocationParentTaskAgent,
				Name: rc.name,
			},
			Trigger: clrkv1alpha1.InvocationTrigger{Type: rc.trigger},
		},
		Status: clrkv1alpha1.InvocationStatus{Phase: phase},
	})
}

// readBodyBounded reads up to limit bytes from r and returns them.
// Anything beyond limit is rejected with an explicit error so we
// fail loudly rather than silently truncate the agent's input.
func readBodyBounded(r io.Reader, limit int64) ([]byte, error) {
	if r == nil {
		return nil, nil
	}
	buf, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(buf)) > limit {
		return nil, fmt.Errorf("request body exceeds %d bytes", limit)
	}
	return buf, nil
}

// buildCEEnvelope produces a structured-mode CloudEvents JSON
// envelope. JSON / text bodies inline as `data`; everything else
// goes into `data_base64`. Empty bodies emit no `data`/`data_base64`
// field at all so the agent can distinguish "no payload" from
// "empty payload".
func buildCEEnvelope(attrs map[string]string, contentType string, body []byte) []byte {
	env := map[string]any{
		cloudevents.AttrSpecVersion: cloudevents.SpecVersion,
	}
	for k, v := range attrs {
		env[k] = v
	}
	switch {
	case len(body) == 0:
		// no data field
	case isCEJSONInlineable(contentType):
		env["data"] = json.RawMessage(body)
	case isCETextInlineable(contentType):
		env["data"] = string(body)
	default:
		env["data_base64"] = base64.StdEncoding.EncodeToString(body)
	}
	out, _ := json.Marshal(env)
	return out
}

// buildCEResponseEnvelope builds a CE response envelope with
// ce-relationid pointing back at the request id.
func buildCEResponseEnvelope(reqID, contentType string, body []byte) []byte {
	env := map[string]any{
		cloudevents.AttrSpecVersion: cloudevents.SpecVersion,
		cloudevents.AttrType:        cloudevents.TypeResponse,
		cloudevents.AttrID:          reqID + ".response",
		cloudevents.AttrTime:        time.Now().UTC().Format(time.RFC3339Nano),
		cloudevents.AttrRelationID:  reqID,
	}
	if contentType != "" {
		env[cloudevents.AttrDataContentType] = contentType
	}
	switch {
	case len(body) == 0:
		// no data field
	case isCEJSONInlineable(contentType):
		env["data"] = json.RawMessage(body)
	case isCETextInlineable(contentType):
		env["data"] = string(body)
	default:
		env["data_base64"] = base64.StdEncoding.EncodeToString(body)
	}
	out, _ := json.Marshal(env)
	return out
}

func isCEJSONInlineable(ct string) bool {
	mt := strings.SplitN(ct, ";", 2)[0]
	mt = strings.ToLower(strings.TrimSpace(mt))
	return mt == "application/json" || strings.HasSuffix(mt, "+json")
}

func isCETextInlineable(ct string) bool {
	mt := strings.SplitN(ct, ";", 2)[0]
	mt = strings.ToLower(strings.TrimSpace(mt))
	return strings.HasPrefix(mt, "text/")
}

// streamWithFlush copies src → dst and flushes after each chunk.
// Returns true if any bytes were written, so the caller can decide
// whether it's still safe to write a 5xx error.
func streamWithFlush(dst io.Writer, src io.Reader, flusher http.Flusher) bool {
	buf := make([]byte, 32*1024)
	wrote := false
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return wrote
			}
			wrote = true
			if flusher != nil {
				flusher.Flush()
			}
		}
		if err != nil {
			return wrote
		}
	}
}

// waitOrStop blocks on the sandbox's Wait, but races against ctx so a
// cancellation triggers a sandbox Stop and lets Wait return. Mirrors
// the daemon path's waitOrCancel; duplicated to avoid coupling the
// dispatcher to a daemon-only file.
func waitOrStop(ctx context.Context, mgr SandboxRuntime, id sandbox.SandboxID) (int, error) {
	type result struct {
		code int
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		c, e := mgr.Wait(context.Background(), id)
		ch <- result{code: c, err: e}
	}()
	select {
	case r := <-ch:
		return r.code, r.err
	case <-ctx.Done():
		_ = mgr.Stop(context.Background(), id)
		r := <-ch
		return r.code, r.err
	}
}

// runHTTP starts the dispatcher's HTTP server and returns when ctx is
// cancelled. Server.Shutdown drains in-flight requests cleanly.
func (d *Dispatcher) Run(ctx context.Context, addr string) error {
	srv := &http.Server{
		Addr:    addr,
		Handler: d,
		// No read/write timeouts: per-execution deadlines are enforced
		// inside ServeHTTP via spec.timeout, and streaming
		// responses can run as long as the agent does (up to the cap).
		BaseContext: func(_ net.Listener) context.Context {
			return ctrl.LoggerInto(context.Background(), ctrl.Log.WithName("dispatch"))
		},
	}

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}
