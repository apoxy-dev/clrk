package cmd

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/apoxy-dev/clrk/internal/otelemit"
	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"golang.org/x/term"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	clrkAPIGroup   = "clrk.apoxy.dev"
	clrkAPIVersion = "v1alpha1"

	// logsBacklogLimit is the default ceiling on the one-shot (non --tail)
	// read: the most-recent N agent log lines. The server caps a single
	// page at 1000, so this is also the max a single GET returns.
	logsBacklogLimit = 1000
)

// newAgentsLogsCmd is `clrk agents logs <name>[/<invocation>]` — it reads
// an agent's logs from the aggregated apiserver's
// {taskagents,daemonagents}/{name}/logs subresource (APO-719/APO-720),
// which serves the OTLP LogRecords clrk components emit for the agent. By
// default it shows every component (today that is the worker's sandbox
// stdout/stderr, APO-718); --component narrows it. This replaces the old
// `kubectl exec`/`tail -F` transport: no pod-exec rights are required,
// any worker can be the source (the API reads ClickHouse, not a
// per-worker file), and the same endpoint backs a web UI.
//
//   - DaemonAgent <name>: the daemon's logs.
//   - TaskAgent <name>: every invocation's logs for the agent.
//   - TaskAgent <name>/<invocation>: one invocation's logs (?invocation=).
func newAgentsLogsCmd() *cobra.Command {
	var (
		namespace  string
		local      bool
		kubeconfig string
		follow     bool
		tailLines  int
		iostream   string
		components []string
		color      string
	)
	cmd := &cobra.Command{
		Use:   "logs NAME[/INVOCATION]",
		Short: "Stream an agent's logs (sandbox stdio plus any other components)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if iostream != "" && iostream != otelemit.IoStreamStdout && iostream != otelemit.IoStreamStderr {
				return fmt.Errorf("--iostream must be stdout or stderr, got %q", iostream)
			}
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
			printer := newLogPrinter(cmd.OutOrStdout(), cmd.ErrOrStderr(), color)
			return streamAgentLogs(cmd.Context(), printer, dyn, cfg, ns, args[0], follow, tailLines, iostream, components)
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Target namespace (default: kubeconfig context).")
	cmd.Flags().BoolVar(&local, "local", false, "Target the kubeconfig of the running 'clrk dev' session (~/.clrk/kubeconfig.host).")
	cmd.Flags().StringVar(&kubeconfig, "kubeconfig", "", "Explicit kubeconfig path (takes precedence over --local and $KUBECONFIG).")
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "Follow the log stream (default false).")
	cmd.Flags().IntVar(&tailLines, "tail", 0, "Number of trailing lines to print (0 = up to the most recent 1000).")
	cmd.Flags().StringVar(&iostream, "iostream", "", "Restrict to one stream: stdout or stderr (default: both).")
	cmd.Flags().StringSliceVar(&components, "component", nil, "Restrict to one or more components, e.g. worker,egress-extproc (default: all).")
	cmd.Flags().StringVar(&color, "color", "auto", "Colorize output: auto (TTY only), always, or never.")
	return cmd
}

func streamAgentLogs(ctx context.Context, printer *logPrinter, dyn dynamic.Interface, cfg *rest.Config, ns, arg string, follow bool, tailLines int, iostream string, components []string) error {
	name, invID, _ := strings.Cut(arg, "/")

	// The /logs subresource is scoped per parent kind, so resolve which one
	// owns the name. DaemonAgents have no per-invocation notion.
	var resource string
	if _, err := dyn.Resource(daemonAgentGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{}); err == nil {
		if invID != "" {
			return fmt.Errorf("%s/%s is a DaemonAgent; per-invocation logs apply only to TaskAgents", ns, name)
		}
		resource = daemonAgentGVR.Resource
	} else if _, err := dyn.Resource(taskAgentGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{}); err == nil {
		resource = taskAgentGVR.Resource
	} else {
		return fmt.Errorf("no TaskAgent or DaemonAgent %s/%s found", ns, name)
	}

	rc, err := clrkLogsRESTClient(cfg)
	if err != nil {
		return err
	}
	// Build the /logs subresource request through the client-go request
	// builder -- group/version come from the RESTClient config -- rather
	// than hand-formatting the URL. There is no generated clientset method:
	// /logs is a custom rest.Connecter that streams raw OTLP/JSON, not a
	// typed resource client-gen knows about. newReq returns a fresh request
	// per call so each follow reconnect starts clean.
	newReq := func() *rest.Request {
		return rc.Get().Namespace(ns).Resource(resource).Name(name).SubResource("logs")
	}

	// Shared filters. By default the read spans every component that
	// emitted logs for the agent (today that is the worker's sandbox
	// stdio; egress/ingress ext_proc telemetry lands in traces). The
	// optional --component / invocation / --iostream selectors narrow it.
	base := url.Values{}
	if len(components) > 0 {
		base.Set("component", strings.Join(components, ","))
	}
	if invID != "" {
		base.Set("invocation", invID)
	}
	if iostream != "" {
		base.Set("iostream", iostream)
	}

	// Print the existing backlog oldest-first and capture the newest
	// timestamp so --follow can resume exactly after it without a gap.
	watermark, err := printLogBacklog(ctx, printer, newReq, base, tailLines)
	if err != nil {
		return err
	}
	if !follow {
		return nil
	}
	return followAgentLogs(ctx, printer, newReq, base, watermark)
}

// clrkLogsRESTClient builds a RESTClient scoped to the clrk API group.
// DoRaw/Stream bypass typed decoding, so the serializer here only
// satisfies RESTClientFor.
func clrkLogsRESTClient(cfg *rest.Config) (rest.Interface, error) {
	c := rest.CopyConfig(cfg)
	c.GroupVersion = &schema.GroupVersion{Group: clrkAPIGroup, Version: clrkAPIVersion}
	c.APIPath = "/apis"
	c.NegotiatedSerializer = scheme.Codecs.WithoutConversion()
	return rest.RESTClientFor(c)
}

// printLogBacklog GETs one page of the newest records (DESC), prints them
// oldest-first, and returns the newest timestamp seen. --tail N caps the
// page to N; otherwise it reads up to logsBacklogLimit. Query params ride
// .Param() (never the path) so client-go URL-encodes them as a real query
// string.
func printLogBacklog(ctx context.Context, printer *logPrinter, newReq func() *rest.Request, base url.Values, tailLines int) (time.Time, error) {
	limit := logsBacklogLimit
	if tailLines > 0 {
		limit = tailLines
	}
	req := newReq()
	for k, vs := range base {
		for _, v := range vs {
			req = req.Param(k, v)
		}
	}
	req = req.Param("limit", strconv.Itoa(limit))
	raw, err := req.DoRaw(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("reading logs: %w", err)
	}
	var ld logspb.LogsData
	if err := protojson.Unmarshal(raw, &ld); err != nil {
		return time.Time{}, fmt.Errorf("decoding logs (%d bytes): %w", len(raw), err)
	}
	lines := flattenLogLines(&ld)
	sortLinesByTime(lines)
	printer.widen(lines)
	var watermark time.Time
	for _, l := range lines {
		printer.print(l)
		if l.t.After(watermark) {
			watermark = l.t
		}
	}
	return watermark, nil
}

// Follow reconnection bounds. A single streamed connection to a named
// subresource is capped at the kube-apiserver front proxy's
// non-long-running request timeout (~60s) -- which clrk cannot
// reconfigure -- so the stream is expected to end roughly every minute
// and the loop reconnects from the last record seen. minFollowBackoff
// also paces retries when the server is briefly unreachable, growing to
// maxFollowBackoff for repeated open failures.
const (
	minFollowBackoff = 1 * time.Second
	maxFollowBackoff = 30 * time.Second
)

// followAgentLogs streams the ?follow=true endpoint and prints each
// NDJSON LogsData chunk, reconnecting transparently when the stream
// ends. A single aggregated connection is capped at ~60s by the
// kube-apiserver front proxy (a named subresource is never long-running
// there), so the loop re-opens from the newest record already printed --
// the server's strict "Timestamp > since" filter makes the resume gap-
// and duplicate-free. since seeds the watermark from the backlog; a zero
// since (empty backlog) tails from now and is only sent once a real
// record has advanced the watermark, so a reconnect never replays all
// history. The loop runs until ctx is cancelled (Ctrl-C) or a permanent
// (4xx) error.
func followAgentLogs(ctx context.Context, printer *logPrinter, newReq func() *rest.Request, base url.Values, since time.Time) error {
	lastSeen := since
	backoff := minFollowBackoff
	for {
		opened, err := streamFollowOnce(ctx, printer, newReq, base, &lastSeen)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil && permanentFollowErr(err) {
			return fmt.Errorf("follow stream: %w", err)
		}
		if opened {
			// A productive connection (it streamed, then hit the front
			// proxy's ~60s wall or a clean EOF). Reconnect promptly.
			backoff = minFollowBackoff
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		if !opened {
			// Repeated failures to even open the stream: back off.
			if backoff *= 2; backoff > maxFollowBackoff {
				backoff = maxFollowBackoff
			}
		}
	}
}

// streamFollowOnce opens one follow connection and prints records until
// it ends, advancing *lastSeen to the newest record time so a reconnect
// resumes strictly after it. It reports opened=true once the stream is
// established -- so the caller can tell a mid-stream reset (expected
// ~every 60s) from a failure to connect -- along with any open/read
// error.
func streamFollowOnce(ctx context.Context, printer *logPrinter, newReq func() *rest.Request, base url.Values, lastSeen *time.Time) (bool, error) {
	req := newReq()
	for k, vs := range base {
		for _, v := range vs {
			req = req.Param(k, v)
		}
	}
	req = req.Param("follow", "true")
	// Only send ?since once a real record has set the watermark. A zero
	// time formats to year 0001 (not an empty value), which the server
	// would read as "since the epoch" and replay all history on every
	// reconnect; omitting it lets the server tail from now instead.
	if !lastSeen.IsZero() {
		req = req.Param("since", lastSeen.Format(time.RFC3339Nano))
	}
	stream, err := req.Stream(ctx)
	if err != nil {
		return false, err
	}
	defer stream.Close()

	sc := bufio.NewScanner(stream)
	// Allow large lines (a captured stdio line can be up to 64 KiB).
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var ld logspb.LogsData
		if err := protojson.Unmarshal(line, &ld); err != nil {
			// Skip a malformed chunk rather than tearing down the stream.
			continue
		}
		lines := flattenLogLines(&ld)
		sortLinesByTime(lines)
		printer.widen(lines)
		for _, l := range lines {
			printer.print(l)
			if l.t.After(*lastSeen) {
				*lastSeen = l.t
			}
		}
	}
	return true, sc.Err()
}

// permanentFollowErr reports whether a follow open error is fatal (the
// request will never succeed: the agent is gone, the request is
// malformed, or authz denies it) rather than a transient or mid-stream
// condition worth reconnecting through. A 429 is treated as transient.
func permanentFollowErr(err error) bool {
	if err == nil {
		return false
	}
	var se *apierrors.StatusError
	if errors.As(err, &se) {
		code := se.Status().Code
		return code >= 400 && code < 500 && code != http.StatusTooManyRequests
	}
	// A network/transport error (incl. a mid-stream HTTP/2 reset surfaced
	// via the scanner) is transient -- reconnect.
	return false
}

// logLine is one display-ready log record: the timestamp, the emitting
// component (clrk.component resource attribute), the stdio stream
// (log.iostream record attribute, empty for non-stdio components), and
// the body.
type logLine struct {
	t         time.Time
	component string
	iostream  string
	body      string
	// status is the HTTP response status code for an access-log record
	// (egress/ingress ext_proc), or 0 when the record carries none. It
	// drives status-class body coloring; worker stdio records have no
	// status attribute, so their bodies are left verbatim.
	status int
}

// flattenLogLines walks the OTLP nesting into a flat, display-ready
// slice, carrying the component down from each ResourceLogs (it is a
// resource attribute, shared by every record under it) so the printer
// can label each line with its source.
func flattenLogLines(ld *logspb.LogsData) []logLine {
	var out []logLine
	for _, rl := range ld.GetResourceLogs() {
		component := attrValue(rl.GetResource().GetAttributes(), otelemit.AttrComponent)
		for _, sl := range rl.GetScopeLogs() {
			for _, lr := range sl.GetLogRecords() {
				status := 0
				if s := attrValue(lr.GetAttributes(), string(semconv.HTTPResponseStatusCodeKey)); s != "" {
					status, _ = strconv.Atoi(s)
				}
				out = append(out, logLine{
					t:         time.Unix(0, int64(lr.GetTimeUnixNano())),
					component: component,
					iostream:  attrValue(lr.GetAttributes(), otelemit.AttrIoStream),
					body:      lr.GetBody().GetStringValue(),
					status:    status,
				})
			}
		}
	}
	return out
}

func sortLinesByTime(lines []logLine) {
	sort.SliceStable(lines, func(i, j int) bool {
		return lines[i].t.Before(lines[j].t)
	})
}

// attrValue returns the string value of the first attribute with key, or
// "" if none. OTLP attribute values are AnyValue; clrk stores component
// and iostream as plain strings, so a non-string value reads as "".
func attrValue(kvs []*commonpb.KeyValue, key string) string {
	for _, kv := range kvs {
		if kv.GetKey() == key {
			return kv.GetValue().GetStringValue()
		}
	}
	return ""
}

// logPrinter renders log lines, routing stderr-stream records to the
// stderr writer (so shell redirection can still split the two streams)
// and styling the timestamp + source token via lipgloss. srcWidth is the
// padding width of the "component[stream]" token; it tracks the widest
// token seen so far so the body column stays aligned. It only grows
// (never shrinks) so columns don't jump left mid-follow.
type logPrinter struct {
	stdout   io.Writer
	stderr   io.Writer
	out      *logStyles
	err      *logStyles
	srcWidth int
}

func newLogPrinter(stdout, stderr io.Writer, mode string) *logPrinter {
	return &logPrinter{
		stdout: stdout,
		stderr: stderr,
		out:    newLogStyles(stdout, wantColor(mode, stdout)),
		err:    newLogStyles(stderr, wantColor(mode, stderr)),
	}
}

// widen grows the source-token column to fit every line in batch before
// it is printed, so a page (the backlog, or one follow chunk) lines up.
func (p *logPrinter) widen(lines []logLine) {
	for _, l := range lines {
		if w := len(sourceToken(l)); w > p.srcWidth {
			p.srcWidth = w
		}
	}
}

func (p *logPrinter) print(l logLine) {
	w, st := p.stdout, p.out
	if l.iostream == otelemit.IoStreamStderr {
		w, st = p.stderr, p.err
	}
	// The timestamp and source token are always styled. lipgloss measures
	// width ignoring its escape bytes, so Width() pads the colored token to
	// the same visible column for every line. An access-log record (status
	// > 0, i.e. an egress/ingress ext_proc summary line) is colorized
	// field-by-field; every other body -- worker stdio in particular -- is
	// written verbatim so an agent's own ANSI sequences survive and a line
	// that merely looks like a request isn't reinterpreted.
	ts := st.ts.Render(l.t.Format(logTimeLayout))
	token := st.token(l.component).Width(p.srcWidth).Render(sourceToken(l))
	body := l.body
	if l.status > 0 {
		body = st.renderAccessLog(body)
	}
	fmt.Fprintf(w, "%s  %s  %s\n", ts, token, body)
}

// wantColor resolves the --color mode against the destination writer.
// "auto" enables color only when the writer is a real terminal, and
// honors the NO_COLOR convention. "always"/"never" override both.
func wantColor(mode string, w io.Writer) bool {
	switch mode {
	case "always":
		return true
	case "never":
		return false
	default:
		if _, ok := os.LookupEnv("NO_COLOR"); ok {
			return false
		}
		f, ok := w.(*os.File)
		return ok && term.IsTerminal(int(f.Fd()))
	}
}

const logTimeLayout = "2006-01-02 15:04:05.000"

// logStyles holds the lipgloss styles for one destination writer, bound
// to a renderer whose color profile is forced on/off from wantColor so
// the --color decision is honored even when the writer isn't a TTY (e.g.
// --color=always piped into a pager).
type logStyles struct {
	ts       lipgloss.Style
	byComp   map[string]lipgloss.Style
	fallback lipgloss.Style
	plain    lipgloss.Style
	dim      lipgloss.Style
	// byMethod colors an access-log HTTP verb per method; methodOther is
	// the bold fallback. status2xx..5xx color the status code by class.
	byMethod    map[string]lipgloss.Style
	methodOther lipgloss.Style
	status2xx   lipgloss.Style
	status3xx   lipgloss.Style
	status4xx   lipgloss.Style
	status5xx   lipgloss.Style
}

func newLogStyles(w io.Writer, colorOn bool) *logStyles {
	r := lipgloss.NewRenderer(w)
	if colorOn {
		r.SetColorProfile(termenv.ANSI)
	} else {
		r.SetColorProfile(termenv.Ascii)
	}
	fg := func(code string) lipgloss.Style {
		return r.NewStyle().Foreground(lipgloss.Color(code))
	}
	fgb := func(code string) lipgloss.Style {
		return r.NewStyle().Foreground(lipgloss.Color(code)).Bold(true)
	}
	return &logStyles{
		ts: r.NewStyle().Faint(true),
		// ANSI 16-color indices: a per-component hue so the source column
		// is scannable. Unknown components fall back to faint.
		byComp: map[string]lipgloss.Style{
			otelemit.ComponentWorker:         fg("6"), // cyan
			otelemit.ComponentEgressExtproc:  fg("5"), // magenta
			otelemit.ComponentIngressExtproc: fg("4"), // blue
			otelemit.ComponentSentryStack:    fg("2"), // green
		},
		fallback: r.NewStyle().Faint(true),
		plain:    r.NewStyle(),
		dim:      r.NewStyle().Faint(true),
		// Per-verb method hues (bold) and per-class status hues.
		byMethod: map[string]lipgloss.Style{
			http.MethodGet:     fgb("2"), // green
			http.MethodPost:    fgb("4"), // blue
			http.MethodPut:     fgb("4"), // blue
			http.MethodPatch:   fgb("5"), // magenta
			http.MethodDelete:  fgb("1"), // red
			http.MethodHead:    fgb("6"), // cyan
			http.MethodOptions: fgb("6"), // cyan
		},
		methodOther: r.NewStyle().Bold(true),
		status2xx:   fgb("2"), // green
		status3xx:   fgb("6"), // cyan
		status4xx:   fgb("3"), // yellow
		status5xx:   fgb("1"), // red
	}
}

// token returns the style for a component, or the faint fallback.
func (s *logStyles) token(component string) lipgloss.Style {
	if st, ok := s.byComp[component]; ok {
		return st
	}
	return s.fallback
}

// methodStyle returns the style for an HTTP verb, or the bold fallback.
func (s *logStyles) methodStyle(method string) lipgloss.Style {
	if st, ok := s.byMethod[method]; ok {
		return st
	}
	return s.methodOther
}

// statusStyle colors an HTTP status code by class: 5xx red, 4xx yellow,
// 3xx cyan, 2xx green, 1xx plain.
func (s *logStyles) statusStyle(code int) lipgloss.Style {
	switch {
	case code >= 500:
		return s.status5xx
	case code >= 400:
		return s.status4xx
	case code >= 300:
		return s.status3xx
	case code >= 200:
		return s.status2xx
	default:
		return s.plain
	}
}

// renderAccessLog colorizes an egress/ingress ext_proc summary line
// field-by-field. The format (internal/extproc summaryLine) is
//
//	METHOD AUTHORITY+PATH STATUS DURms req=NB resp=NB [key=value ...]
//
// so the first four fields are positional and the rest are key=value
// pairs. Method gets a per-verb hue, the status its class hue, the
// duration is dimmed, and each pair's "key=" is dimmed so the value
// stays readable. The URL and any unrecognized field render plain, and
// a field that doesn't match its expected shape degrades to plain rather
// than mis-coloring.
func (s *logStyles) renderAccessLog(body string) string {
	fields := strings.Fields(body)
	if len(fields) == 0 {
		return body
	}
	var b strings.Builder
	for i, f := range fields {
		if i > 0 {
			b.WriteByte(' ')
		}
		switch {
		case i == 0:
			b.WriteString(s.methodStyle(f).Render(f))
		case i == 2 && isStatusCode(f):
			n, _ := strconv.Atoi(f)
			b.WriteString(s.statusStyle(n).Render(f))
		case i == 3:
			b.WriteString(s.dim.Render(f)) // duration ("123ms" or "?")
		case i >= 4 && strings.ContainsRune(f, '='):
			k, v, _ := strings.Cut(f, "=")
			b.WriteString(s.dim.Render(k+"=") + v)
		default:
			b.WriteString(f) // URL (i == 1) and anything unexpected
		}
	}
	return b.String()
}

// isStatusCode reports whether f is a bare HTTP status code (100-599).
func isStatusCode(f string) bool {
	n, err := strconv.Atoi(f)
	return err == nil && n >= 100 && n < 600
}

// sourceToken is the per-line origin label. A record carrying a stdio
// stream (worker stdout/stderr) renders as "component[stream]"; any other
// record is just the bare "component". A missing component reads as "-".
func sourceToken(l logLine) string {
	comp := l.component
	if comp == "" {
		comp = "-"
	}
	if l.iostream != "" {
		return comp + "[" + l.iostream + "]"
	}
	return comp
}
