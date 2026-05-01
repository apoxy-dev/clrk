package devotel

import (
	"fmt"
	"strings"
	"time"
)

// FormatLog renders a LogRecord as a single line in the same shape as
// extproc's `summaryLine` so the dev TUI's `otel-logs` pane matches
// what production OTel backends would surface. The Body field already
// carries that line for clrk-emitted records; we only fall back to a
// per-attribute reconstruction when Body is empty (non-clrk emitter).
func FormatLog(r LogRecord) string {
	ts := r.Time.Format("15:04:05.000")
	body := r.Body
	if body == "" {
		body = reconstructLogBody(r)
	}
	tail := ""
	if r.TraceID != "" {
		tail = " trace=" + shortTraceID(r.TraceID)
	}
	return fmt.Sprintf("%s %s%s", ts, body, tail)
}

// FormatSpan renders a Span as a single line. Mirrors the Log
// formatter's prefix so panes side-by-side line up visually.
func FormatSpan(s Span) string {
	ts := s.Time.Format("15:04:05.000")
	dur := s.Duration.Truncate(time.Millisecond).String()
	if s.Duration <= 0 {
		dur = "?"
	}
	status := s.Status
	if status == "" {
		status = "UNSET"
	}
	tail := ""
	if s.TraceID != "" {
		tail = " trace=" + shortTraceID(s.TraceID)
	}
	return fmt.Sprintf("%s %s [%s] %s%s", ts, s.Name, status, dur, tail)
}

// reconstructLogBody is a best-effort rebuild of summaryLine semantics
// for log records that didn't ship a Body string (third-party emitters
// pointed at the dev receiver). We keep it terse — the goal is "you can
// see something happened", not full fidelity.
func reconstructLogBody(r LogRecord) string {
	method := r.Attributes["http.request.method"]
	authority := r.Attributes["server.address"]
	path := r.Attributes["url.path"]
	status := r.Attributes["http.response.status_code"]

	var b strings.Builder
	if method != "" {
		b.WriteString(method)
		b.WriteByte(' ')
	}
	if authority != "" {
		b.WriteString(authority)
	}
	if path != "" {
		b.WriteString(path)
	}
	if status != "" {
		b.WriteByte(' ')
		b.WriteString(status)
	}
	if b.Len() == 0 {
		// Final fallback so the pane shows _something_ for every record.
		return "<empty body>"
	}
	return b.String()
}

func shortTraceID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
