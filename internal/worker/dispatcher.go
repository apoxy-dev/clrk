package worker

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
	"os"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/cloudevents"
	clrkcontroller "github.com/apoxy-dev/clrk/internal/controller"
	"github.com/apoxy-dev/clrk/internal/egress"
	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
	"github.com/apoxy-dev/clrk/internal/ports"
	"github.com/apoxy-dev/clrk/internal/sandbox/metadata"
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
	// regardless of TaskAgent.spec.timeoutSeconds. Matches the cron
	// invoker's per-fire cap so cron and HTTP share the same ceiling.
	hardTimeoutCap = 5 * time.Minute
)

// activeCounter tracks in-flight TaskAgent executions per
// (taskAgentNS, taskAgentName). The WorkerStatusService streams the
// snapshot to the controller-manager which feeds it into the ingress
// ext_proc cluster-wide MaxConcurrent enforcement.
type activeCounter struct {
	mu       sync.Mutex
	counts   map[types.NamespacedName]int32
	notifier *changeNotifier
}

func newActiveCounter() *activeCounter {
	return &activeCounter{
		counts:   make(map[types.NamespacedName]int32),
		notifier: newChangeNotifier(),
	}
}

func (c *activeCounter) inc(key types.NamespacedName) {
	c.mu.Lock()
	c.counts[key]++
	c.mu.Unlock()
	c.notifier.broadcast()
}

func (c *activeCounter) dec(key types.NamespacedName) {
	c.mu.Lock()
	if c.counts[key] > 0 {
		c.counts[key]--
	}
	if c.counts[key] == 0 {
		delete(c.counts, key)
	}
	c.mu.Unlock()
	c.notifier.broadcast()
}

// get returns the current count for key (0 if absent).
func (c *activeCounter) get(key types.NamespacedName) int32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[key]
}

// Snapshot returns a copy of all (key, count) pairs. Used by the
// WorkerStatusService to build a stream message.
func (c *activeCounter) Snapshot() map[types.NamespacedName]int32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[types.NamespacedName]int32, len(c.counts))
	for k, v := range c.counts {
		out[k] = v
	}
	return out
}

// Notifier returns the change notifier for this counter so external
// publishers (e.g. the WorkerStatusService) can subscribe to inc/dec
// events.
func (c *activeCounter) Notifier() *changeNotifier { return c.notifier }

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

	active *activeCounter

	// semaphores caps in-flight executions per (TaskAgent ns, name).
	// Stored as *sema so the same capacity value (the chan) is reused
	// across requests for the same agent, and resized lazily when the
	// spec.maxConcurrent value changes.
	semaphores sync.Map // types.NamespacedName -> *sema
}

// WarmAcquirer is the subset of *WarmPool the dispatcher uses on the
// hot path. Defined as an interface so the linux-only WarmPool type
// stays out of the platform-agnostic Dispatcher and tests can pass a
// nil-or-fake.
type WarmAcquirer interface {
	Acquire(key WarmKey) *SandboxInstance
}

type sema struct {
	cap int32
	ch  chan struct{}
}

// NewDispatcher constructs a Dispatcher. Production callers pass a
// real *SandboxManager (which satisfies SandboxRuntime) and a real
// *egress.Router; tests pass fakes / nil where appropriate.
func NewDispatcher(c client.Client, mgr SandboxRuntime, r *egress.Router, podName, namespace string, active *activeCounter) *Dispatcher {
	return &Dispatcher{
		client:     c,
		sandboxMgr: mgr,
		router:     r,
		podName:    podName,
		namespace:  namespace,
		active:     active,
	}
}

// SetWarmPool installs the warm-pool acquirer used by the dispatcher's
// hot path. Optional — a nil warmPool means every dispatch takes the
// cold path (Create + Start). Call once during runtime startup before
// the dispatcher accepts traffic.
func (d *Dispatcher) SetWarmPool(w WarmAcquirer) { d.warmPool = w }

// NewActiveCounter returns an empty activeCounter usable as the
// `active` argument to NewDispatcher. Exported so tests can inspect
// the counter independently.
func NewActiveCounter() *activeCounter { return newActiveCounter() }

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

func (d *Dispatcher) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log := ctrl.LoggerFrom(r.Context()).WithName("dispatch")

	hdr := r.Header.Get(ports.HeaderTaskAgent)
	if hdr == "" {
		http.Error(w, "missing "+ports.HeaderTaskAgent+" header", http.StatusBadRequest)
		return
	}
	ns, name, ok := strings.Cut(hdr, "/")
	if !ok || ns == "" || name == "" {
		http.Error(w, "invalid "+ports.HeaderTaskAgent+" header (want ns/name)", http.StatusBadRequest)
		return
	}
	key := types.NamespacedName{Namespace: ns, Name: name}
	log = log.WithValues("taskAgent", key.String(), "trigger", r.Header.Get(ports.HeaderTrigger))

	var ta clrkv1alpha1.TaskAgent
	if err := d.client.Get(r.Context(), key, &ta); err != nil {
		if apierrors.IsNotFound(err) {
			http.Error(w, "TaskAgent not found", http.StatusNotFound)
			return
		}
		log.Error(err, "Failed to get TaskAgent")
		http.Error(w, "TaskAgent lookup failed", http.StatusInternalServerError)
		return
	}

	if ta.Status.LatestReadyRevisionName == "" {
		http.Error(w, "TaskAgent has no ready revision", http.StatusServiceUnavailable)
		return
	}

	maxConcurrent := int32(0)
	if ta.Spec.MaxConcurrent != nil {
		maxConcurrent = *ta.Spec.MaxConcurrent
	}
	release := d.acquire(key, maxConcurrent)
	if release == nil {
		http.Error(w, "TaskAgent at MaxConcurrent on this worker", http.StatusTooManyRequests)
		return
	}
	defer release()

	d.active.inc(key)
	defer d.active.dec(key)

	var rev clrkv1alpha1.AgentSandboxRevision
	revKey := types.NamespacedName{Namespace: ns, Name: ta.Status.LatestReadyRevisionName}
	if err := d.client.Get(r.Context(), revKey, &rev); err != nil {
		if apierrors.IsNotFound(err) {
			http.Error(w, "AgentSandboxRevision not found locally", http.StatusServiceUnavailable)
			return
		}
		log.Error(err, "Failed to get AgentSandboxRevision")
		http.Error(w, "revision lookup failed", http.StatusInternalServerError)
		return
	}

	timeout := hardTimeoutCap
	if ta.Spec.TimeoutSeconds != nil && *ta.Spec.TimeoutSeconds > 0 {
		t := time.Duration(*ta.Spec.TimeoutSeconds) * time.Second
		if t < timeout {
			timeout = t
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

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

	// Try the warm pool first. A warm sandbox already has the rootfs
	// mounted, the libcontainer container Created, the TAP+netns
	// provisioned, and the EG MITM CA staged into the rootfs — so we
	// skip Purge + Create and go straight to egress wiring + Start.
	// CA is captured at warm time; egress backends/policy are
	// re-resolved here so spec changes that don't bump the revision
	// (e.g. EgressRefs swap) still take effect at consume time.
	var sb *SandboxInstance
	var sandboxID SandboxID
	warmHit := false
	if d.warmPool != nil {
		if warm := d.warmPool.Acquire(WarmKey{Namespace: ns, Agent: name, Revision: rev.Name}); warm != nil {
			sb = warm
			sandboxID = warm.ID
			warmHit = true
			// Warm sandboxes were created with no invocation-id (the
			// fill happens before any specific request exists). Stamp
			// the per-request id now, before Start builds the
			// IdentityDialer off sb.Identity.
			sb.Identity.InvocationID = invocationID
			log = log.WithValues("warm", true, "sandboxID", sandboxID, "invocationID", invocationID)
		}
	}

	caPEM, backends, policy, err := resolveEgressForExecution(ctx, d.client, d.router, ns, ta.Spec.EgressRefs)
	if err != nil {
		if warmHit {
			// Cleanup the warm sandbox we already pulled — caller will
			// retry, the warm pool will refill on next Acquire kick.
			deleteSandboxBounded(d.sandboxMgr, sandboxID, log, "Failed to delete unused warm sandbox")
		}
		log.Error(err, "Failed to resolve egress for TaskAgent")
		http.Error(w, "egress not ready", http.StatusServiceUnavailable)
		return
	}

	if !warmHit {
		sandboxID, err = newSandboxID(sandboxIDPrefixTask, ns, name)
		if err != nil {
			log.Error(err, "Failed to generate sandbox id")
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		d.sandboxMgr.Purge(ctx, sandboxID)
		sb, err = d.sandboxMgr.Create(ctx, sandboxID, name, identity, caPEM, rev.Spec.AgentSandbox, ta.Spec.Resources, ta.Spec.State, true, 0)
		if err != nil {
			if errors.Is(err, ErrStateOverLimit) {
				log.Info("Refusing dispatch — agent state over size limit", "err", err)
				http.Error(w, "agent state over size limit", http.StatusInsufficientStorage)
				return
			}
			log.Error(err, "Failed to create sandbox")
			http.Error(w, "sandbox create failed", http.StatusInternalServerError)
			return
		}
	}
	defer deleteSandboxBounded(d.sandboxMgr, sandboxID, log, "Failed to delete sandbox")
	if len(backends) > 0 {
		if err := d.sandboxMgr.SetEgressBackends(sandboxID, backends); err != nil {
			log.Error(err, "Set egress backends failed")
		}
	}
	if policy != nil {
		if err := d.sandboxMgr.SetEgressPolicy(sandboxID, policy); err != nil {
			log.Error(err, "Set egress policy failed")
		}
	}

	// Resolve the delivery transport. Stdin (default) writes a CE
	// JSON envelope to the agent's stdin; Metadata closes stdin and
	// exposes the request via the per-sandbox IMDS HTTP service.
	deliveryMode := clrkv1alpha1.AgentDeliveryStdin
	if ta.Spec.Delivery != nil && ta.Spec.Delivery.Mode != "" {
		deliveryMode = ta.Spec.Delivery.Mode
	}

	// CE attribute set is shared by both transports — Stdin folds it
	// into the JSON envelope, Metadata serves it via /v1/event headers.
	ceAttrs := cloudevents.AttrsFromRequest(r, &ta)
	ceID := ceAttrs[cloudevents.AttrID]
	wrapResponse := strings.EqualFold(r.Header.Get("Accept"), cloudevents.MediaType)

	// Buffer the request body once. Both delivery modes need the
	// payload in-memory to produce the envelope/serve it from /v1/event.
	bodyBytes, err := readBodyBounded(r.Body, requestBodyLimit)
	if err != nil {
		log.Error(err, "Failed to read request body")
		http.Error(w, "request body read failed", http.StatusBadRequest)
		return
	}

	if err := d.sandboxMgr.Start(ctx, sandboxID); err != nil {
		log.Error(err, "Failed to start sandbox")
		http.Error(w, "sandbox start failed", http.StatusInternalServerError)
		return
	}

	// Per-mode request delivery + response capture.
	var (
		mdEntry  *metadata.Entry
		mdServer io.Closer
	)
	switch deliveryMode {
	case clrkv1alpha1.AgentDeliveryMetadata:
		mdEntry = metadata.NewEntry(ceID, r.Header.Get("Content-Type"), bodyBytes, ceAttrs)
		srv, err := startMetadataServer(sb, mdEntry)
		if err != nil {
			log.Error(err, "Failed to start metadata server")
			http.Error(w, "metadata server failed", http.StatusInternalServerError)
			return
		}
		mdServer = srv
		defer mdServer.Close()
		// Close stdin immediately so an agent that mistakenly reads
		// from it sees EOF rather than blocking forever.
		_ = sb.Stdin.Close()

	case clrkv1alpha1.AgentDeliveryStdin:
		fallthrough
	default:
		// Build a structured-mode CE JSON envelope and stream it to
		// stdin in the background; close stdin on EOF so the agent's
		// read returns. The envelope carries the request body (inline
		// for JSON/text, base64 for everything else) plus all CE
		// attributes including httpmethod/httpurl/httpquery — so a
		// body-less GET still reaches the agent with the request line
		// intact, which a raw-body pipe could not represent.
		envelope := buildCEEnvelope(ceAttrs, r.Header.Get("Content-Type"), bodyBytes)
		stdinErr := make(chan error, 1)
		go func() {
			_, werr := io.Copy(sb.Stdin, bytes.NewReader(envelope))
			_ = sb.Stdin.Close()
			stdinErr <- werr
		}()
		defer func() { <-stdinErr }()
	}

	// Response-side wiring depends on transport. In Stdin mode we
	// stream stdout straight back (or, if wrapResponse, buffer it
	// for re-encoding). In Metadata mode we wait for the agent's
	// POST /v1/response and serve its body.
	flusher, _ := w.(http.Flusher)

	var (
		respCh       chan bool
		stdoutBuf    *bytes.Buffer
		stdoutBufErr chan error
	)
	switch {
	case deliveryMode == clrkv1alpha1.AgentDeliveryStdin && !wrapResponse:
		// Hot path: stream stdout straight to the wire.
		w.Header().Set("Content-Type", "application/octet-stream")
		respCh = make(chan bool, 1)
		go func() { respCh <- streamWithFlush(w, sb.Stdout, flusher) }()

	case deliveryMode == clrkv1alpha1.AgentDeliveryStdin && wrapResponse:
		// Caller asked for CE response envelope; buffer stdout so we
		// can re-encode after the agent exits with its full output.
		stdoutBuf = &bytes.Buffer{}
		stdoutBufErr = make(chan error, 1)
		go func() {
			_, copyErr := io.Copy(stdoutBuf, sb.Stdout)
			stdoutBufErr <- copyErr
		}()

	case deliveryMode == clrkv1alpha1.AgentDeliveryMetadata:
		// Drain stdout to the per-agent log only — we don't surface
		// it to the caller. The agent's POST /v1/response is the
		// response body. Discard avoids a goroutine leak when the
		// agent writes anything to stdout.
		go func() { _, _ = io.Copy(io.Discard, sb.Stdout) }()
	}

	state, waitErr := waitOrStop(ctx, d.sandboxMgr, sandboxID)

	exitCode := -1
	success := false
	if state != nil {
		success = state.Success()
		if success {
			exitCode = 0
		} else if ws, ok := state.Sys().(interface{ ExitStatus() int }); ok {
			exitCode = ws.ExitStatus()
		}
	}

	// Sandbox is gone; cancel any pending Metadata-mode response so
	// downstream selects unblock.
	if mdEntry != nil {
		mdEntry.CancelIfPending()
	}

	wroteAnyBytes := false

	switch {
	case deliveryMode == clrkv1alpha1.AgentDeliveryStdin && !wrapResponse:
		wroteAnyBytes = <-respCh

	case deliveryMode == clrkv1alpha1.AgentDeliveryStdin && wrapResponse:
		<-stdoutBufErr
		w.Header().Set("Content-Type", cloudevents.MediaType)
		respEnv := buildCEResponseEnvelope(ceID, "application/octet-stream", stdoutBuf.Bytes())
		if _, werr := w.Write(respEnv); werr == nil {
			wroteAnyBytes = true
			if flusher != nil {
				flusher.Flush()
			}
		}

	case deliveryMode == clrkv1alpha1.AgentDeliveryMetadata:
		respBody, respCT, delivered := mdEntry.Response()
		if !delivered {
			// Agent exited without posting a response. Fall through
			// to the exit-code error path below.
			break
		}
		if respCT == "" {
			respCT = "application/octet-stream"
		}
		if wrapResponse {
			w.Header().Set("Content-Type", cloudevents.MediaType)
			env := buildCEResponseEnvelope(ceID, respCT, respBody)
			if _, werr := w.Write(env); werr == nil {
				wroteAnyBytes = true
			}
		} else {
			w.Header().Set("Content-Type", respCT)
			if _, werr := w.Write(respBody); werr == nil {
				wroteAnyBytes = true
			}
		}
		if flusher != nil {
			flusher.Flush()
		}
	}

	switch {
	case ctx.Err() == context.DeadlineExceeded:
		// Headers may already be flushed if we streamed any bytes; in
		// that case the trailer is the only signal we can give.
		if !wroteAnyBytes {
			http.Error(w, "execution timed out", http.StatusGatewayTimeout)
		}
	case waitErr != nil:
		log.Error(waitErr, "Sandbox wait failed")
		if !wroteAnyBytes {
			http.Error(w, "sandbox wait failed", http.StatusInternalServerError)
		}
	case !success:
		w.Header().Set(ports.HeaderExitCode, fmt.Sprintf("%d", exitCode))
		if !wroteAnyBytes {
			http.Error(w, fmt.Sprintf("agent exited with code %d", exitCode), http.StatusInternalServerError)
		}
		// If we already streamed bytes, the response is whatever's on
		// the wire — exit code lives in the (best-effort) header above.
	case deliveryMode == clrkv1alpha1.AgentDeliveryMetadata && !wroteAnyBytes:
		// Agent succeeded without posting a response in Metadata mode.
		// Treat as a server-side bug — the agent should always POST.
		http.Error(w, "agent did not post a response", http.StatusBadGateway)
	}
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
func waitOrStop(ctx context.Context, mgr SandboxRuntime, id SandboxID) (*os.ProcessState, error) {
	type result struct {
		state *os.ProcessState
		err   error
	}
	ch := make(chan result, 1)
	go func() {
		s, e := mgr.Wait(context.Background(), id)
		ch <- result{state: s, err: e}
	}()
	select {
	case r := <-ch:
		return r.state, r.err
	case <-ctx.Done():
		_ = mgr.Stop(context.Background(), id)
		r := <-ch
		return r.state, r.err
	}
}

// resolveEgressForExecution loads the EG MITM CA, listener backends,
// and per-sandbox policy for a one-shot execution. Returns nil/nil/nil
// when the agent has no EgressRefs (no MITM, direct dial). Returns an
// error if the EG exists but isn't ready yet — the caller should map
// that to a retryable status (we use 503).
//
// Unlike the daemon path's waitForEgressReady, this returns immediately
// without polling: a TaskAgent execution can't afford a multi-minute
// warm-up loop, and the caller (cron or HTTP client) is responsible for
// retry semantics.
func resolveEgressForExecution(ctx context.Context, c client.Client, router *egress.Router, namespace string, refs []clrkv1alpha1.AgentEgressRef) ([]byte, []egress.BackendListener, *egress.SandboxPolicy, error) {
	if len(refs) == 0 {
		return nil, nil, nil, nil
	}
	egName := refs[0].GatewayRef

	var sec corev1.Secret
	caKey := types.NamespacedName{Namespace: namespace, Name: clrkcontroller.EgressGatewayCASecretName(egName)}
	if err := c.Get(ctx, caKey, &sec); err != nil {
		return nil, nil, nil, fmt.Errorf("fetching EgressGateway CA secret %s: %w", caKey, err)
	}
	caPEM := sec.Data[corev1.TLSCertKey]
	if len(caPEM) == 0 {
		return nil, nil, nil, fmt.Errorf("EgressGateway CA secret %s has empty %s", caKey, corev1.TLSCertKey)
	}

	var eg clrkv1alpha1.EgressGateway
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: egName}, &eg); err != nil {
		return nil, nil, nil, fmt.Errorf("get EgressGateway %s/%s: %w", namespace, egName, err)
	}

	specByName := make(map[string]clrkv1alpha1.EgressListener, len(eg.Spec.Listeners))
	for _, el := range eg.Spec.Listeners {
		specByName[el.Name] = el
	}
	backends := make([]egress.BackendListener, 0, len(eg.Status.Listeners))
	for _, ls := range eg.Status.Listeners {
		if ls.BackendAddress == "" {
			continue
		}
		spec, ok := specByName[ls.Name]
		if !ok {
			continue
		}
		shape, err := clrkv1alpha1.ShapeForListener(spec)
		if err != nil {
			continue
		}
		var matchPort int32
		if spec.Port != nil {
			matchPort = *spec.Port
		}
		backends = append(backends, egress.BackendListener{
			Name:      ls.Name,
			Addr:      ls.BackendAddress,
			Shape:     string(shape),
			MatchPort: matchPort,
			Priority:  clrkv1alpha1.ShapePriority(shape),
		})
	}
	if len(backends) == 0 {
		return nil, nil, nil, fmt.Errorf("EgressGateway %s/%s has no listener backends ready", namespace, egName)
	}

	var l4Routes clrkv1alpha1.EgressL4RouteList
	if err := c.List(ctx, &l4Routes, client.InNamespace(namespace)); err != nil {
		return nil, nil, nil, fmt.Errorf("list EgressL4Routes in %s: %w", namespace, err)
	}
	policy := router.SyncPolicy(eg, l4Routes.Items)

	return caPEM, backends, policy, nil
}

// runHTTP starts the dispatcher's HTTP server and returns when ctx is
// cancelled. Server.Shutdown drains in-flight requests cleanly.
func (d *Dispatcher) Run(ctx context.Context, addr string) error {
	srv := &http.Server{
		Addr:    addr,
		Handler: d,
		// No read/write timeouts: per-execution deadlines are enforced
		// inside ServeHTTP via spec.timeoutSeconds, and streaming
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
