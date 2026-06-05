package devtui

import (
	"fmt"
	"strings"
	"time"
)

// ClrkSource is the synthetic component name used for clrk dev's own
// orchestration log (slog output redirected into the TUI).
const ClrkSource = "cli"

// OtelLogsSource and OtelTracesSource are the synthetic component
// names for the in-process OTLP receiver panes. Records pushed via
// prog.SendLog under these names land in their dedicated sidebar
// panes instead of being mixed into the controller-manager pane.
const (
	OtelLogsSource   = "otel-logs"
	OtelTracesSource = "otel-traces"
)

// maxLogLines bounds each component's ring buffer. Older lines are dropped
// when the buffer fills.
const maxLogLines = 2000

// component holds renderable state for one container plus its log buffer.
// name is the routing key (matches docker container names so log streamers
// can address it); display is the trimmed label rendered in the sidebar.
//
// The log buffer is a circular slice: head is the index of the oldest line,
// size is the count of valid entries. This keeps append O(1) even at the
// maxLogLines cap.
type component struct {
	name      string
	display   string
	status    Status
	startedAt time.Time
	buf       []string
	head      int
	size      int
}

func newComponent(name string) *component {
	return &component{name: name, display: displayName(name), status: StatusPending}
}

// displayName drops the redundant "clrk-" container prefix so the sidebar
// reads "k3s", "controller-manager", "worker-0" instead of the full docker
// container names.
func displayName(name string) string {
	return strings.TrimPrefix(name, "clrk-")
}

func (c *component) append(line string) {
	if c.buf == nil {
		c.buf = make([]string, 0, maxLogLines)
	}
	if c.size < maxLogLines {
		c.buf = append(c.buf, line)
		c.size++
		return
	}
	c.buf[c.head] = line
	c.head = (c.head + 1) % maxLogLines
}

func (c *component) clear() {
	c.buf = c.buf[:0]
	c.head = 0
	c.size = 0
}

// joined returns the buffer contents in chronological order, unwrapping the
// ring. The result is a single string suitable for viewport.SetContent.
func (c *component) joined() string {
	if c.size == 0 {
		return ""
	}
	if c.head == 0 {
		return strings.Join(c.buf[:c.size], "\n")
	}
	parts := make([]string, c.size)
	n := copy(parts, c.buf[c.head:])
	copy(parts[n:], c.buf[:c.head])
	return strings.Join(parts, "\n")
}

// tail returns the last n buffered lines in chronological order, joined. Used
// by the compact status view's toggleable per-step log peek.
func (c *component) tail(n int) string {
	if c.size == 0 || n <= 0 {
		return ""
	}
	var parts []string
	if c.head == 0 {
		parts = c.buf[:c.size]
	} else {
		parts = make([]string, c.size)
		k := copy(parts, c.buf[c.head:])
		copy(parts[k:], c.buf[:c.head])
	}
	if len(parts) > n {
		parts = parts[len(parts)-n:]
	}
	return strings.Join(parts, "\n")
}

// glyph returns the single-rune status indicator for the sidebar.
func (c *component) glyph() string {
	switch c.status {
	case StatusReady:
		return "●"
	case StatusStarting:
		return "◐"
	case StatusError:
		return "✕"
	default:
		return "◌"
	}
}

// uptime returns a short human-readable duration since startedAt, or "-" if
// the component hasn't started yet.
func (c *component) uptime(now time.Time) string {
	if c.startedAt.IsZero() {
		return "-"
	}
	d := now.Sub(c.startedAt)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm%02ds", int(d.Minutes()), int(d.Seconds())%60)
	default:
		return fmt.Sprintf("%dh%02dm", int(d.Hours()), int(d.Minutes())%60)
	}
}
