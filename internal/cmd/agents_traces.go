package cmd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/apoxy-dev/clrk/internal/cmd/spangraph"
	"github.com/apoxy-dev/clrk/internal/otelemit"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"golang.org/x/term"
	"google.golang.org/protobuf/encoding/protojson"
)

// newAgentsTracesCmd is `clrk agents traces <name>[/<invocation>]` -- the
// traces counterpart to `agents logs`. It reads an agent's OTLP spans
// from the aggregated apiserver's {taskagents,daemonagents}/{name}/traces
// subresource (the same read model that backs logs), which serves
// protojson TracesData -- including the egress/ingress ext_proc spans that
// carry the agent's LLM/MCP request and response bodies as span events.
//
// In an interactive terminal the default is a full-screen Bubble Tea TUI:
// the spans drawn as a lazygit-style hierarchy graph (a tree per trace
// with colored lanes and a status node per span), scrollable, pinned to
// the bottom while following. +/- step a global expand level through four
// stops -- graph, attributes, event bodies (capped), event bodies in full
// -- so every span opens its attributes and captured LLM/MCP request and
// response bodies at once. --raw turns the TUI off and emits NDJSON -- one
// span per line (each line a self-contained single-span TracesData) -- for
// piping to jq and friends. Piped (non-interactive) output defaults to
// --raw for the same reason.
//
//   - DaemonAgent <name>: the daemon's spans.
//   - TaskAgent <name>: every invocation's spans for the agent.
//   - TaskAgent <name>/<invocation>: one invocation's spans (?invocation=).
//
// The graph assembles whatever spans the backlog page (and each follow
// chunk) carries; a span whose parent is not yet present renders as a
// root until the parent arrives and the tree is rebuilt.
func newAgentsTracesCmd() *cobra.Command {
	var (
		namespace  string
		local      bool
		kubeconfig string
		follow     bool
		tailSpans  int
		components []string
		color      string
		raw        bool
	)
	cmd := &cobra.Command{
		Use:   "traces NAME[/INVOCATION]",
		Short: "Browse an agent's OTLP spans as a hierarchy graph TUI (--raw for NDJSON)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if color != "auto" && color != "always" && color != "never" {
				return fmt.Errorf("--color must be auto, always, or never, got %q", color)
			}
			kc, err := resolveKubeconfig(kubeconfig, local)
			if err != nil {
				return err
			}
			cfg, err := clientcmd.BuildConfigFromFlags("", kc)
			if err != nil {
				return fmt.Errorf("loading kubeconfig %s: %w", kc, err)
			}
			dyn, err := dynamic.NewForConfig(cfg)
			if err != nil {
				return fmt.Errorf("dynamic client: %w", err)
			}
			ns := namespace
			if ns == "" {
				if ns, err = contextNamespace(kc); err != nil {
					return err
				}
			}
			src, err := resolveTraceSource(cmd.Context(), dyn, cfg, ns, args[0], components)
			if err != nil {
				return err
			}
			// Run the interactive TUI in a real terminal; fall back to NDJSON
			// when the output is piped (so a downstream tool gets one span per
			// line) or when --raw is set. The TUI needs a TTY, so --raw=false
			// on a non-TTY still emits NDJSON.
			stdout := cmd.OutOrStdout()
			if !raw && isTerminalWriter(stdout) {
				return runTracesTUI(cmd.Context(), src, follow, tailSpans, wantColor(color, stdout))
			}
			return runTracesNDJSON(cmd.Context(), stdout, src, follow, tailSpans)
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Target namespace (default: kubeconfig context).")
	cmd.Flags().BoolVar(&local, "local", false, "Target the kubeconfig of the running 'clrk dev' session (~/.clrk/kubeconfig.host).")
	cmd.Flags().StringVar(&kubeconfig, "kubeconfig", "", "Explicit kubeconfig path (takes precedence over --local and $KUBECONFIG).")
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "Follow the trace stream (default false).")
	cmd.Flags().IntVar(&tailSpans, "tail", 0, "Number of most recent spans to fetch (0 = server default, max 1000).")
	cmd.Flags().StringSliceVar(&components, "component", nil, "Restrict to one or more components, e.g. egress-extproc,worker (default: all).")
	cmd.Flags().StringVar(&color, "color", "auto", "Colorize the graph: auto (TTY only), always, or never.")
	cmd.Flags().BoolVar(&raw, "raw", false, "Emit NDJSON (one span per line) instead of the interactive graph. Default when piped.")
	return cmd
}

// traceSource captures the resolved /traces request for one agent: a fresh
// request builder (so each follow reconnect starts clean) plus the shared
// query filters. It is independent of the renderer, so both the NDJSON and
// the TUI paths read through it.
type traceSource struct {
	newReq func() *rest.Request
	base   url.Values
}

// resolveTraceSource resolves which parent kind owns the name (the /traces
// subresource is scoped per kind) and builds the request through the
// client-go request builder. There is no generated clientset method:
// /traces is a custom rest.Connecter streaming raw OTLP/JSON, so only the
// subresource name is a literal.
func resolveTraceSource(ctx context.Context, dyn dynamic.Interface, cfg *rest.Config, ns, arg string, components []string) (*traceSource, error) {
	name, invID, _ := strings.Cut(arg, "/")

	var resource string
	if _, err := dyn.Resource(daemonAgentGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{}); err == nil {
		if invID != "" {
			return nil, fmt.Errorf("%s/%s is a DaemonAgent; per-invocation traces apply only to TaskAgents", ns, name)
		}
		resource = daemonAgentGVR.Resource
	} else if _, err := dyn.Resource(taskAgentGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{}); err == nil {
		resource = taskAgentGVR.Resource
	} else {
		return nil, fmt.Errorf("no TaskAgent or DaemonAgent %s/%s found", ns, name)
	}

	rc, err := clrkRESTClient(cfg)
	if err != nil {
		return nil, err
	}
	// By default the read spans every component that emitted spans for the
	// agent (egress/ingress ext_proc carry the request/response telemetry;
	// the worker emits some too). --component and the /<invocation> selector
	// narrow it. Traces have no iostream.
	base := url.Values{}
	if len(components) > 0 {
		base.Set("component", strings.Join(components, ","))
	}
	if invID != "" {
		base.Set("invocation", invID)
	}
	return &traceSource{
		newReq: func() *rest.Request {
			return rc.Get().Namespace(ns).Resource(resource).Name(name).SubResource("traces")
		},
		base: base,
	}, nil
}

// withParams rides the shared filters on a fresh request via .Param() so
// client-go URL-encodes them as a real query string (never the path).
func withParams(req *rest.Request, base url.Values) *rest.Request {
	for k, vs := range base {
		for _, v := range vs {
			req = req.Param(k, v)
		}
	}
	return req
}

// backlogTraceChunk GETs one page of the newest spans. --tail N caps the
// page to N spans; tail<=0 lets the server apply its default.
func backlogTraceChunk(ctx context.Context, src *traceSource, tailSpans int) ([]byte, error) {
	req := withParams(src.newReq(), src.base)
	if tailSpans > 0 {
		req = req.Param("limit", strconv.Itoa(tailSpans))
	}
	raw, err := req.DoRaw(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading traces: %w", err)
	}
	return raw, nil
}

// onTraceChunk consumes one NDJSON follow chunk and returns the newest
// span start time it carried (the follow watermark). It must not retain
// the bytes past the call.
type onTraceChunk func(chunk []byte) (time.Time, error)

// followTraceChunks streams the ?follow=true endpoint and hands each
// NDJSON TracesData chunk to onChunk, reconnecting transparently when the
// stream ends. A single aggregated connection is capped at ~60s by the
// kube-apiserver front proxy (a named subresource is never long-running
// there), so the loop re-opens from the newest span already seen -- the
// server's strict "Timestamp > since" filter makes the resume gap- and
// duplicate-free. since seeds the watermark from the backlog; a zero since
// (empty backlog) tails from now and is only sent once a real span has
// advanced it. The loop runs until ctx is cancelled (the TUI quit, or
// Ctrl-C) or a permanent (4xx) error. It shares minFollowBackoff /
// maxFollowBackoff / permanentFollowErr with the logs reader.
func followTraceChunks(ctx context.Context, src *traceSource, since time.Time, onChunk onTraceChunk) error {
	lastSeen := since
	backoff := minFollowBackoff
	for {
		opened, err := streamTraceFollowOnce(ctx, src, onChunk, &lastSeen)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil && permanentFollowErr(err) {
			return fmt.Errorf("follow stream: %w", err)
		}
		if opened {
			backoff = minFollowBackoff
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		if !opened {
			if backoff *= 2; backoff > maxFollowBackoff {
				backoff = maxFollowBackoff
			}
		}
	}
}

// streamTraceFollowOnce opens one follow connection and feeds chunks to
// onChunk until it ends, advancing *lastSeen to the newest span start time
// so a reconnect resumes strictly after it. opened=true once the stream is
// established (so the caller can tell a mid-stream reset -- expected ~every
// 60s -- from a failure to connect).
//
// A bufio.Reader (not a Scanner) reads the NDJSON: one follow chunk is a
// whole TracesData of up to the server page limit, and egress spans carry
// base64'd request/response bodies as events, so a single line can be many
// MiB and would overrun a Scanner's token cap. ReadBytes grows as needed.
func streamTraceFollowOnce(ctx context.Context, src *traceSource, onChunk onTraceChunk, lastSeen *time.Time) (bool, error) {
	req := withParams(src.newReq(), src.base).Param("follow", "true")
	// Only send ?since once a real span has set the watermark. A zero time
	// formats to year 0001 (not an empty value), which the server would read
	// as "since the epoch" and replay all history on every reconnect;
	// omitting it lets the server tail from now instead.
	if !lastSeen.IsZero() {
		req = req.Param("since", lastSeen.Format(time.RFC3339Nano))
	}
	stream, err := req.Stream(ctx)
	if err != nil {
		return false, err
	}
	defer stream.Close()

	br := bufio.NewReaderSize(stream, 64*1024)
	for {
		line, rerr := br.ReadBytes('\n')
		if chunk := bytes.TrimSpace(line); len(chunk) > 0 {
			// A malformed chunk is skipped (onChunk returns an error) rather
			// than tearing down the stream.
			if ts, perr := onChunk(chunk); perr == nil && ts.After(*lastSeen) {
				*lastSeen = ts
			}
		}
		if rerr != nil {
			if rerr == io.EOF {
				return true, nil
			}
			return true, rerr
		}
	}
}

// runTracesNDJSON prints the backlog as NDJSON (one span per line) and,
// with --follow, keeps printing each follow chunk the same way. Each line
// is a self-contained single-span TracesData, so a downstream consumer
// sees one stable record format regardless of paged vs follow.
func runTracesNDJSON(ctx context.Context, out io.Writer, src *traceSource, follow bool, tailSpans int) error {
	chunk, err := backlogTraceChunk(ctx, src, tailSpans)
	if err != nil {
		return err
	}
	watermark, err := writeTracesNDJSON(out, chunk)
	if err != nil {
		return fmt.Errorf("decoding traces (%d bytes): %w", len(chunk), err)
	}
	if !follow {
		return nil
	}
	return followTraceChunks(ctx, src, watermark, func(c []byte) (time.Time, error) {
		return writeTracesNDJSON(out, c)
	})
}

// writeTracesNDJSON decodes one protojson TracesData and writes one span
// per line, each line a compact single-span TracesData (so the span keeps
// its Resource and Scope context). It returns the newest span start time
// for the follow watermark.
func writeTracesNDJSON(out io.Writer, chunk []byte) (time.Time, error) {
	var td tracepb.TracesData
	if err := protojson.Unmarshal(chunk, &td); err != nil {
		return time.Time{}, err
	}
	var newest time.Time
	for _, rs := range td.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			for _, sp := range ss.GetSpans() {
				one := &tracepb.TracesData{ResourceSpans: []*tracepb.ResourceSpans{{
					Resource:  rs.GetResource(),
					SchemaUrl: rs.GetSchemaUrl(),
					ScopeSpans: []*tracepb.ScopeSpans{{
						Scope:     ss.GetScope(),
						SchemaUrl: ss.GetSchemaUrl(),
						Spans:     []*tracepb.Span{sp},
					}},
				}}}
				line, err := protojson.Marshal(one)
				if err != nil {
					continue
				}
				out.Write(line)
				io.WriteString(out, "\n")
				if t := time.Unix(0, int64(sp.GetStartTimeUnixNano())); t.After(newest) {
					newest = t
				}
			}
		}
	}
	return newest, nil
}

// decodeTraceSpans decodes one protojson TracesData into the renderer's
// neutral Span slice (the TUI's input) and returns the newest span start
// time (the follow watermark). Each span carries the component from its
// ResourceSpans (a trace can cross components).
func decodeTraceSpans(chunk []byte) ([]spangraph.Span, time.Time, error) {
	var td tracepb.TracesData
	if err := protojson.Unmarshal(chunk, &td); err != nil {
		return nil, time.Time{}, err
	}
	var spans []spangraph.Span
	var newest time.Time
	for _, rs := range td.GetResourceSpans() {
		comp := attrValue(rs.GetResource().GetAttributes(), otelemit.AttrComponent)
		for _, ss := range rs.GetScopeSpans() {
			for _, sp := range ss.GetSpans() {
				s := toSpangraphSpan(sp, comp)
				if s.Start.After(newest) {
					newest = s.Start
				}
				spans = append(spans, s)
			}
		}
	}
	return spans, newest, nil
}

// toSpangraphSpan maps one OTLP span to the renderer's neutral Span,
// preserving attribute order and decoding the egress event bodies so the
// detail view can show the captured payloads.
func toSpangraphSpan(sp *tracepb.Span, component string) spangraph.Span {
	s := spangraph.Span{
		TraceID:    hex.EncodeToString(sp.GetTraceId()),
		SpanID:     hex.EncodeToString(sp.GetSpanId()),
		ParentID:   hex.EncodeToString(sp.GetParentSpanId()),
		Name:       sp.GetName(),
		Start:      time.Unix(0, int64(sp.GetStartTimeUnixNano())),
		End:        time.Unix(0, int64(sp.GetEndTimeUnixNano())),
		Component:  component,
		HTTPStatus: attrValue(sp.GetAttributes(), string(semconv.HTTPResponseStatusCodeKey)),
		Attributes: kvSlice(sp.GetAttributes()),
	}
	if st := sp.GetStatus(); st != nil {
		switch st.GetCode() {
		case tracepb.Status_STATUS_CODE_OK:
			s.Status = spangraph.StatusOk
		case tracepb.Status_STATUS_CODE_ERROR:
			s.Status = spangraph.StatusError
		}
		s.StatusMsg = st.GetMessage()
	}
	for _, ev := range sp.GetEvents() {
		s.Events = append(s.Events, spangraph.Event{
			Time:       time.Unix(0, int64(ev.GetTimeUnixNano())),
			Name:       ev.GetName(),
			Attributes: eventKVs(ev.GetAttributes()),
		})
	}
	return s
}

// kvSlice flattens OTLP attributes into ordered KV pairs (emit order is
// preserved so the detail view is stable).
func kvSlice(kvs []*commonpb.KeyValue) []spangraph.KV {
	if len(kvs) == 0 {
		return nil
	}
	out := make([]spangraph.KV, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, spangraph.KV{Key: kv.GetKey(), Value: otelemit.AnyValueString(kv.GetValue())})
	}
	return out
}

// eventKVs is kvSlice for a span event, with one special case: the egress
// sink base64-encodes captured request/response bodies into a single
// clrk.body.b64 attribute (bodyAttrs in internal/extproc/sink_otlp.go), so
// here it is decoded back to a readable "body" value -- the whole point of
// expanding a span.
func eventKVs(kvs []*commonpb.KeyValue) []spangraph.KV {
	if len(kvs) == 0 {
		return nil
	}
	out := make([]spangraph.KV, 0, len(kvs))
	for _, kv := range kvs {
		k := kv.GetKey()
		v := otelemit.AnyValueString(kv.GetValue())
		if k == otelemit.AttrBodyB64 {
			out = append(out, spangraph.KV{Key: "body", Value: decodeBody(v)})
			continue
		}
		out = append(out, spangraph.KV{Key: k, Value: v})
	}
	return out
}

// decodeBody base64-decodes a captured body and, when it is valid JSON,
// pretty-prints it (LLM/MCP payloads are JSON). A non-UTF-8 body reads as
// a byte-count placeholder rather than mojibake; an undecodable value
// falls back to the raw string.
func decodeBody(b64 string) string {
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return b64
	}
	if !utf8.Valid(raw) {
		return fmt.Sprintf("<%d bytes binary>", len(raw))
	}
	if json.Valid(raw) {
		var buf bytes.Buffer
		if json.Indent(&buf, raw, "", "  ") == nil {
			return buf.String()
		}
	}
	return string(raw)
}

// isTerminalWriter reports whether w is a real terminal (an *os.File whose
// fd is a tty). It gates the interactive TUI independently of NO_COLOR,
// which only governs color.
func isTerminalWriter(w io.Writer) bool {
	f, ok := w.(*os.File)
	return ok && term.IsTerminal(int(f.Fd()))
}
