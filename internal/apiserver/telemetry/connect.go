// Package telemetry serves the read API for an agent's OTel logs and
// traces: the taskagents/{name}/{logs,traces} and
// daemonagents/{name}/{logs,traces} connect subresources. Each is a
// rest.Connecter that reads the embedded ClickHouse otel_logs /
// otel_traces tables (the columnar store chwriter materializes from the
// OTLP receiver), reconstructs the OTLP LogsData / TracesData proto from
// the columns, and returns protojson.
//
// The agent is the path scope: RequestInfo.Name pins the Agent column
// (agent.name) and the namespace pins agent.namespace, both
// server-enforced so a caller can only read its own path's agent. The
// taskagents vs daemonagents mount additionally pins agent.kind.
// Everything else (invocation, component, iostream, since, until, limit,
// follow) is a query-param filter read straight off the request, the
// same raw-options approach the invoke Connecter uses — the repo has no
// conversion-gen, so a typed ConnectOptions kind would cost a hand-rolled
// url.Values conversion for no functional gain over query-param parsing.
//
// JSON only: the body is protojson of LogsData / TracesData (no
// application/x-protobuf). With ?follow=true the handler polls CH on an
// interval and streams NDJSON chunks of LogsData / TracesData instead.
// Note that protojson renders the trace/span id bytes as base64 (proto3
// JSON), not the hex the OTLP/JSON spec prefers; canonical-hex output is
// a documented follow-up.
package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/generic"
	registryrest "k8s.io/apiserver/pkg/registry/rest"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/apiserver/chsql"
)

// Signal selects which telemetry table a Connecter reads.
type Signal string

const (
	SignalLogs   Signal = "logs"
	SignalTraces Signal = "traces"
)

// continueHeader carries the keyset continue token to the client. The
// body is raw OTLP LogsData/TracesData (which has no metadata.continue
// field), so paging rides a header; the client echoes it back via the
// ?continue= query param. application/x-ndjson is the follow stream.
const (
	continueHeader  = "X-Clrk-Continue"
	contentJSON     = "application/json"
	contentNDJSON   = "application/x-ndjson"
	followInterval  = 1 * time.Second
	followChunkRows = maxListLimit
)

// Doer is the shared ClickHouse-pool seam (internal/apiserver/chsql),
// aliased here so the read model's Deps/field references stay local. The
// manager passes the shared LazyPool (which satisfies it structurally);
// unit tests inject a fake.
type Doer = chsql.Doer

// Deps are the process-wide dependencies shared by every telemetry
// mount; only signal/singular/agentKind differ per GVR.
type Deps struct {
	// Pool is the ClickHouse read pool (the shared LazyPool in prod).
	Pool Doer
}

// Connecter is the rest.Connecter backing one {logs,traces} subresource
// of one parent kind.
type Connecter struct {
	deps     Deps
	signal   Signal
	singular string
	// agentKind pins the agent.kind attribute filter so a TaskAgent and
	// a DaemonAgent that happen to share a name in a namespace don't read
	// each other's telemetry. Empty disables the filter.
	agentKind string
}

var (
	_ registryrest.Storage              = (*Connecter)(nil)
	_ registryrest.Scoper               = (*Connecter)(nil)
	_ registryrest.Connecter            = (*Connecter)(nil)
	_ registryrest.SingularNameProvider = (*Connecter)(nil)
)

// New constructs a read Connecter. agentKind is the agent.kind filter
// value ("TaskAgent"/"DaemonAgent") for the parent mount, or "".
func New(deps Deps, signal Signal, singular, agentKind string) *Connecter {
	return &Connecter{deps: deps, signal: signal, singular: singular, agentKind: agentKind}
}

// NewProvider adapts New to the apoxy-cli builder StorageProvider shape.
// The *runtime.Scheme / RESTOptionsGetter args are unused: the Connecter
// reads ClickHouse directly, not through the generic registry.
func NewProvider(deps Deps, signal Signal, singular, agentKind string) func(*runtime.Scheme, generic.RESTOptionsGetter) (registryrest.Storage, error) {
	return func(*runtime.Scheme, generic.RESTOptionsGetter) (registryrest.Storage, error) {
		return New(deps, signal, singular, agentKind), nil
	}
}

// New returns the discovery kind for the subresource. The response body
// is raw OTLP/JSON written straight to the ResponseWriter, so this kind
// is only a discovery placeholder (it is never serialized); we reuse the
// already-registered Invocation kind exactly as the invoke Connecter
// does, avoiding a new scheme type + codegen.
func (c *Connecter) New() runtime.Object     { return &clrkv1alpha1.Invocation{} }
func (c *Connecter) Destroy()                {}
func (c *Connecter) NamespaceScoped() bool   { return true }
func (c *Connecter) GetSingularName() string { return c.singular }

// ConnectMethods: read-only.
func (c *Connecter) ConnectMethods() []string { return []string{http.MethodGet} }

// NewConnectOptions returns no typed options; the handler reads its
// filters straight off req.URL.Query() (see the package doc on why this
// repo uses the raw-options path).
func (c *Connecter) NewConnectOptions() (runtime.Object, bool, string) {
	return nil, false, ""
}

// Connect validates the request scope and returns a handler that streams
// the reconstructed OTLP/JSON. name is the parent agent name from the
// path; the namespace comes from the request context. Both pin the
// server-enforced scope.
func (c *Connecter) Connect(ctx context.Context, name string, _ runtime.Object, responder registryrest.Responder) (http.Handler, error) {
	ns := request.NamespaceValue(ctx)
	if ns == "" {
		return nil, apierrors.NewBadRequest("namespace is required")
	}
	if name == "" {
		return nil, apierrors.NewBadRequest("agent name is required in the request path")
	}
	if c.deps.Pool == nil {
		return nil, apierrors.NewServiceUnavailable("clickhouse read pool not ready")
	}
	sc := scope{namespace: ns, agent: name, agentKind: c.agentKind}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f, err := parseFilters(r.URL.Query(), c.signal)
		if err != nil {
			responder.Error(apierrors.NewBadRequest(err.Error()))
			return
		}
		if f.follow {
			c.serveFollow(w, r, responder, sc, f)
			return
		}
		c.serveOnce(w, r, responder, sc, f)
	}), nil
}

// serveOnce runs a single paged query and writes one protojson
// LogsData/TracesData object, with the next keyset token (if any) on the
// continue header.
func (c *Connecter) serveOnce(w http.ResponseWriter, r *http.Request, responder registryrest.Responder, sc scope, f filters) {
	data, next, err := c.queryPage(r.Context(), sc, f)
	if err != nil {
		responder.Error(toAPIError(err))
		return
	}
	out, err := protojson.Marshal(data)
	if err != nil {
		responder.Error(apierrors.NewInternalError(err))
		return
	}
	w.Header().Set("Content-Type", contentJSON)
	if next != "" {
		w.Header().Set(continueHeader, next)
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(out)
}

// serveFollow polls CH on followInterval and streams NDJSON chunks of
// new records (Timestamp strictly after a moving watermark). With no
// ?since it tails from now; pre-existing history is read via the
// non-follow GET. Errors after the 200 header are best-effort logged and
// end the stream, since a clean Status can no longer be written.
func (c *Connecter) serveFollow(w http.ResponseWriter, r *http.Request, responder registryrest.Responder, sc scope, f filters) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		responder.Error(apierrors.NewInternalError(errors.New("streaming unsupported by the response writer")))
		return
	}
	since := f.since
	if since.IsZero() {
		since = time.Now()
	}
	w.Header().Set("Content-Type", contentNDJSON)
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ticker := time.NewTicker(followInterval)
	defer ticker.Stop()
	for {
		data, maxTS, err := c.followPoll(r.Context(), sc, f, since)
		if err != nil {
			if r.Context().Err() == nil {
				slog.Warn("telemetry follow: poll failed", "signal", c.signal, "agent", sc.agent, "err", err)
			}
			return
		}
		if data != nil {
			out, merr := protojson.Marshal(data)
			if merr != nil {
				slog.Warn("telemetry follow: marshal failed", "signal", c.signal, "err", merr)
				return
			}
			if _, werr := w.Write(append(out, '\n')); werr != nil {
				return
			}
			flusher.Flush()
			since = maxTS
		}
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
		}
	}
}

// queryPage dispatches a single paged read to the right signal.
func (c *Connecter) queryPage(ctx context.Context, sc scope, f filters) (proto.Message, string, error) {
	switch c.signal {
	case SignalLogs:
		return c.queryLogsPage(ctx, sc, f)
	case SignalTraces:
		return c.queryTracesPage(ctx, sc, f)
	default:
		return nil, "", apierrors.NewInternalError(errors.New("unknown signal " + string(c.signal)))
	}
}

// followPoll fetches one chunk of records newer than since (ASC) and
// returns the reconstructed signal plus the new watermark. data is nil
// when the chunk is empty.
func (c *Connecter) followPoll(ctx context.Context, sc scope, f filters, since time.Time) (proto.Message, time.Time, error) {
	switch c.signal {
	case SignalLogs:
		return c.followLogs(ctx, sc, f, since)
	case SignalTraces:
		return c.followTraces(ctx, sc, f, since)
	default:
		return nil, since, errors.New("unknown signal " + string(c.signal))
	}
}

// apierrFromCursor renders a bad continue token as a 400 rather than
// leaking it as a 500 through toAPIError.
func apierrFromCursor(err error) error {
	return apierrors.NewBadRequest(err.Error())
}

// toAPIError maps a pre-stream query failure to an apierror. An error
// that is already an apimachinery Status (e.g. the 400 from a bad
// cursor) passes through unchanged; a cancelled or deadline-exceeded
// context (the LazyPool blocking on a CH that is not up yet) surfaces as
// 503; everything else is a 500 (the SQL is server-built, so a real
// query error is our bug).
func toAPIError(err error) error {
	var statusErr *apierrors.StatusError
	if errors.As(err, &statusErr) {
		return statusErr
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return apierrors.NewServiceUnavailable("clickhouse not ready: " + err.Error())
	}
	return apierrors.NewInternalError(err)
}
