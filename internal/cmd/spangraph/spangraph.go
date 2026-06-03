// Package spangraph renders OTLP spans as a lazygit-style hierarchy graph:
// each trace drawn as a tree by parent/child with colored lanes, a
// status-colored node per span, a compact metadata tail, and a detail
// block (attributes plus event bodies) the caller expands or collapses for
// the whole graph at once via a global expand level.
//
// The renderer is source-agnostic: it consumes a neutral Span slice, not
// OTLP protobuf or any store type, so the standalone `clrk agents traces`
// reader (decoding protojson TracesData) and the `clrk dev` TUI (reading
// its in-memory span store) draw the identical graph. It owns no I/O and
// no event loop -- Render returns a flat slice of Lines that a caller
// drops into a Bubble Tea viewport (live) or joins and prints (static).
package spangraph

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
)

// Tree-drawing glyphs, written as \u escapes so the source stays ASCII
// (greppable) while the terminal renders the box-drawing graph. treeSpace
// and the trailing space in treeVertical keep the node column aligned two
// cells per depth.
const (
	glyphNode    = "\u25cf"       // filled circle: one span
	treeBranch   = "\u251c\u2500" // ancestor with more siblings below
	treeCorner   = "\u2514\u2500" // last child
	treeVertical = "\u2502 "      // an ancestor lane continues down
	treeSpace    = "  "           // an ancestor lane has ended
)

// Status is the span's OTLP status, mapped to the three forms the graph
// colors by (a neutral enum so callers don't pass OTLP/string codes in).
type Status int

const (
	StatusUnset Status = iota
	StatusOk
	StatusError
)

// KV is one attribute kept as an ordered pair: OTLP attributes arrive in
// emit order, and the detail view preserves it (a map would shuffle).
type KV struct {
	Key   string
	Value string
}

// Event is a span event. The egress ext_proc sink emits the agent's LLM/
// MCP request and response headers and bodies as events, so this is where
// the captured payloads live (the caller decodes them into Attributes).
type Event struct {
	Time       time.Time
	Name       string
	Attributes []KV
}

// Span is the neutral input row: every field the graph and the detail
// expansion need, independent of where it was decoded from.
type Span struct {
	TraceID    string
	SpanID     string
	ParentID   string
	Name       string
	Start      time.Time
	End        time.Time
	Status     Status
	StatusMsg  string
	Component  string
	HTTPStatus string // e.g. "200"; "" when the span carries no HTTP status.
	Attributes []KV
	Events     []Event
}

// Duration is the span's wall-clock span, clamped at zero (a span whose
// end precedes its start -- a clock skew or an unfinished span -- reads as
// 0 rather than a negative).
func (s Span) Duration() time.Duration {
	d := s.End.Sub(s.Start)
	if d < 0 {
		return 0
	}
	return d
}

// Node is a span plus its children in the display tree.
type Node struct {
	Span
	Children []*Node
}

// Tree is one trace (TraceId) and its root spans, with the trace's
// wall-clock bounds for the header.
type Tree struct {
	ID    string
	Roots []*Node
	Count int
	Start time.Time
	End   time.Time
}

// BuildTrees groups spans by TraceId (preserving first-seen order so a
// follow stream appends new traces below), links each trace's spans into
// a parent/child forest, and returns the trees ordered by earliest start.
// A span whose ParentID is empty -- or points outside the batch -- is a
// root, so a partial page still renders as a forest.
func BuildTrees(spans []Span) []*Tree {
	groups := map[string][]Span{}
	var order []string
	for _, s := range spans {
		if _, ok := groups[s.TraceID]; !ok {
			order = append(order, s.TraceID)
		}
		groups[s.TraceID] = append(groups[s.TraceID], s)
	}
	trees := make([]*Tree, 0, len(order))
	for _, id := range order {
		trees = append(trees, buildTree(id, groups[id]))
	}
	sort.SliceStable(trees, func(i, j int) bool { return trees[i].Start.Before(trees[j].Start) })
	return trees
}

func buildTree(id string, spans []Span) *Tree {
	nodes := make([]*Node, len(spans))
	byID := make(map[string]*Node, len(spans))
	for i := range spans {
		n := &Node{Span: spans[i]}
		nodes[i] = n
		if _, ok := byID[n.SpanID]; !ok {
			byID[n.SpanID] = n
		}
	}
	// Resolve each node's parent pointer (nil = root). A span whose ParentID
	// is empty, self-referential, or outside this batch is a root.
	parent := make(map[*Node]*Node, len(nodes))
	for _, n := range nodes {
		if p, ok := byID[n.ParentID]; ok && n.ParentID != "" && p != n {
			parent[n] = p
		}
	}
	// Break parent/child cycles before building the Children forest. Real
	// producers emit a DAG, but a malformed longer cycle (A's parent is B,
	// B's parent is A) would otherwise build a cyclic Children graph -- which
	// recurses forever in sortByStart/walk -- and render nowhere while still
	// counting toward Count. Walking up from each node, the first node that
	// closes a loop has its parent edge cut, demoting it to a root; the result
	// is an acyclic forest with no span lost.
	for _, n := range nodes {
		seen := map[*Node]bool{}
		for cur := n; cur != nil; cur = parent[cur] {
			if seen[cur] {
				delete(parent, cur)
				break
			}
			seen[cur] = true
		}
	}

	t := &Tree{ID: id, Count: len(nodes)}
	for i, n := range nodes {
		if i == 0 || n.Start.Before(t.Start) {
			t.Start = n.Start
		}
		if i == 0 || n.End.After(t.End) {
			t.End = n.End
		}
		if p := parent[n]; p != nil {
			p.Children = append(p.Children, n)
		} else {
			t.Roots = append(t.Roots, n)
		}
	}
	sortByStart(t.Roots)
	return t
}

func sortByStart(nodes []*Node) {
	sort.SliceStable(nodes, func(i, j int) bool { return nodes[i].Start.Before(nodes[j].Start) })
	for _, n := range nodes {
		sortByStart(n.Children)
	}
}

// LineKind classifies a rendered Line so a caller can tell a span row from
// the headers, separators, and detail lines around it.
type LineKind int

const (
	KindBlank  LineKind = iota // trace separator
	KindHeader                 // trace header
	KindSpan                   // one span row (SpanID set)
	KindDetail                 // an attribute/event line under an expanded span
)

// Line is one rendered display row. Text carries the (possibly ANSI-
// styled) content; SpanID is set on KindSpan lines so a caller can relate a
// row back to its span.
type Line struct {
	Text   string
	Kind   LineKind
	SpanID string
}

// ExpandLevel selects how much per-span detail Render emits beneath each
// span row. It is global to a Render pass (the whole graph expands or
// collapses together, not one span at a time) and cumulative: each level
// adds to the rows the level below it already shows.
type ExpandLevel int

const (
	LevelGraph      ExpandLevel = iota // just the span graph, one row per span
	LevelAttributes                    // + each span's attributes
	LevelBodies                        // + event headers and bodies, bodies capped
	LevelBodiesFull                    // + event bodies in full (no line cap)
)

// bodyLineCap bounds a single body's wrapped height at LevelBodies so a
// multi-KiB LLM payload stays scannable; LevelBodiesFull lifts the cap.
const bodyLineCap = 20

// Options tunes one Render pass. Level selects the global expand depth (see
// ExpandLevel). Width is the available content width (for soft-wrapping long
// values and bodies); 0 disables wrapping.
type Options struct {
	Level ExpandLevel
	Width int
}

// Renderer holds the lipgloss styles, bound to a color profile forced on
// or off so the same render is reproducible on or off a TTY.
type Renderer struct {
	st styles
}

type styles struct {
	header lipgloss.Style
	name   lipgloss.Style
	dim    lipgloss.Style
	detail lipgloss.Style
	ok     lipgloss.Style // 2xx / Ok
	bad    lipgloss.Style // 5xx / Error
	warn   lipgloss.Style // 4xx
	info   lipgloss.Style // 3xx
	unset  lipgloss.Style // node glyph, no status
	lanes  []lipgloss.Style
}

// NewRenderer builds a renderer whose color profile is forced from color
// (so --color=always still paints when w is not a TTY, and the TUI paints
// to its in-memory buffer). w only seeds termenv's profile, which color
// then overrides.
func NewRenderer(w io.Writer, color bool) *Renderer {
	r := lipgloss.NewRenderer(w)
	if color {
		r.SetColorProfile(termenv.ANSI)
	} else {
		r.SetColorProfile(termenv.Ascii)
	}
	fg := func(code string) lipgloss.Style { return r.NewStyle().Foreground(lipgloss.Color(code)) }
	return &Renderer{st: styles{
		header: r.NewStyle().Bold(true),
		name:   r.NewStyle(),
		dim:    r.NewStyle().Faint(true),
		detail: r.NewStyle().Faint(true),
		ok:     fg("2"),            // green
		bad:    fg("1").Bold(true), // red
		warn:   fg("3"),            // yellow
		info:   fg("6"),            // cyan
		unset:  r.NewStyle().Faint(true),
		// ANSI 16-color lane palette (red reserved for errors): a per-depth
		// hue so nesting is scannable.
		lanes: []lipgloss.Style{fg("5"), fg("6"), fg("3"), fg("4"), fg("2")},
	}}
}

func (r *Renderer) lane(depth int) lipgloss.Style { return r.st.lanes[depth%len(r.st.lanes)] }

// Render walks the trees and returns the flat display rows. Per trace it
// prints a header, then the span forest depth-first (pre-order) with tree
// guides, aligning each span's metadata tail to a common column. At any
// expand level above LevelGraph each span is followed by its detail block.
func (r *Renderer) Render(trees []*Tree, opts Options) []Line {
	var out []Line
	for ti, t := range trees {
		if ti > 0 {
			out = append(out, Line{Kind: KindBlank})
		}
		out = append(out, Line{Kind: KindHeader, Text: r.st.header.Render(r.headerText(t))})
		out = append(out, r.treeLines(t, opts)...)
	}
	return out
}

// row is one span's rendered pieces before tail alignment. detailPrefix is
// the indent a detail line under this span gets, so its attributes nest
// within the span's lane.
type row struct {
	spanID       string
	left         string
	tail         string
	node         *Node
	detailPrefix string
}

func (r *Renderer) treeLines(t *Tree, opts Options) []Line {
	var rows []row
	visited := map[string]bool{}
	nameW := nameColWidth(opts.Width)
	timeBlank := strings.Repeat(" ", timeColWidth)

	var walk func(nodes []*Node, prefix string, depth int)
	walk = func(nodes []*Node, prefix string, depth int) {
		for i, n := range nodes {
			if visited[n.SpanID] {
				continue
			}
			visited[n.SpanID] = true
			conn, cont := treeBranch, treeVertical
			if i == len(nodes)-1 {
				conn, cont = treeCorner, treeSpace
			}
			lane := r.lane(depth)
			graph := prefix + lane.Render(conn)
			rows = append(rows, row{
				spanID:       n.SpanID,
				left:         r.spanLeft(n, graph, nameW),
				tail:         r.spanTail(n),
				node:         n,
				detailPrefix: prefix + lane.Render(cont) + "  ",
			})
			walk(n.Children, prefix+lane.Render(cont), depth+1)
		}
	}
	for _, rt := range t.Roots {
		if visited[rt.SpanID] {
			continue
		}
		visited[rt.SpanID] = true
		rows = append(rows, row{
			spanID:       rt.SpanID,
			left:         r.spanLeft(rt, "", nameW),
			tail:         r.spanTail(rt),
			node:         rt,
			detailPrefix: "  ",
		})
		walk(rt.Children, "", 0)
	}

	width := 0
	for _, rw := range rows {
		if w := lipgloss.Width(rw.left); w > width {
			width = w
		}
	}
	out := make([]Line, 0, len(rows))
	for _, rw := range rows {
		text := rw.left
		if rw.tail != "" {
			text += strings.Repeat(" ", width-lipgloss.Width(rw.left)) + "  " + rw.tail
		}
		out = append(out, Line{Kind: KindSpan, SpanID: rw.spanID, Text: text})
		if opts.Level >= LevelAttributes {
			// Indent detail past the leading time column so it nests under the graph.
			out = append(out, r.detailLines(rw.node, timeBlank+rw.detailPrefix, opts.Width, opts.Level)...)
		}
	}
	return out
}

// spanLeft renders a span row's left side: a leading start-time column (a
// fixed column the eye can scan while following), the graph guides, the status
// node, and the name fitted to nameW cells so the metadata tail keeps a stable
// column as spans of varying name length stream in.
func (r *Renderer) spanLeft(n *Node, graph string, nameW int) string {
	ts := r.st.dim.Render(n.Start.Format(detailTimeLayout))
	return ts + " " + graph + r.glyph(n) + " " + r.st.name.Render(capName(n.Name, nameW))
}

// spanTail renders the metadata chips after a span's name: an HTTP status
// (or the span status when there is no HTTP code) colored by class, the
// duration, the emitting component, and an event count. A non-empty error
// message trails in red.
func (r *Renderer) spanTail(n *Node) string {
	var parts []string
	switch {
	case n.HTTPStatus != "":
		if code, err := strconv.Atoi(n.HTTPStatus); err == nil {
			parts = append(parts, r.statusCodeStyle(code).Render(n.HTTPStatus))
		} else {
			parts = append(parts, r.st.dim.Render(n.HTTPStatus))
		}
	case n.Status == StatusError:
		parts = append(parts, r.st.bad.Render("ERR"))
	case n.Status == StatusOk:
		parts = append(parts, r.st.ok.Render("OK"))
	}
	parts = append(parts, r.st.dim.Render(humanDuration(n.Duration())))
	if n.Component != "" {
		parts = append(parts, r.st.dim.Render(n.Component))
	}
	if len(n.Events) > 0 {
		parts = append(parts, r.st.dim.Render(fmt.Sprintf("%dev", len(n.Events))))
	}
	if n.Status == StatusError && n.StatusMsg != "" {
		parts = append(parts, r.st.bad.Render(n.StatusMsg))
	}
	return strings.Join(parts, "  ")
}

// detailLines renders an expanded span's detail beneath its row, indented
// by prefix (the span's lane). At LevelAttributes it shows the span summary
// and the span's attributes; at LevelBodies and above it also shows each
// event -- the egress sink's captured request/response headers and bodies --
// with bodies soft-wrapped to width and, at LevelBodies, capped at
// bodyLineCap lines (LevelBodiesFull lifts the cap). Long non-body values
// always soft-wrap so a multi-KiB payload stays readable instead of running
// off the pane.
func (r *Renderer) detailLines(n *Node, prefix string, width int, level ExpandLevel) []Line {
	var out []Line
	add := func(s string) { out = append(out, Line{Kind: KindDetail, Text: prefix + s}) }
	avail := width - lipgloss.Width(prefix)
	if avail < minDetailWidth {
		avail = minDetailWidth
	}

	add(r.st.dim.Render(fmt.Sprintf("span %s  %s  %s",
		shortID(n.SpanID, 8), n.Start.Format(detailTimeLayout), humanDuration(n.Duration()))))
	for _, kv := range n.Attributes {
		r.appendKV(&out, prefix, kv.Key, kv.Value, avail, false, 0)
	}
	if level < LevelBodies {
		return out
	}
	bodyCap := bodyLineCap
	if level >= LevelBodiesFull {
		bodyCap = 0 // no limit
	}
	for _, ev := range n.Events {
		add(r.st.dim.Render("event ") + r.st.name.Render(ev.Name) + r.st.dim.Render("  "+ev.Time.Format(detailTimeLayout)))
		for _, kv := range ev.Attributes {
			body := kv.Key == "body"
			maxLines := 0
			if body {
				maxLines = bodyCap
			}
			r.appendKV(&out, prefix+"  ", kv.Key, kv.Value, avail-2, body, maxLines)
		}
	}
	return out
}

// appendKV emits a "key: value" detail line, wrapping to a value block when
// the value is long or multi-line (bodies always take the block form so JSON
// keeps its own line breaks). When maxLines > 0 the value block is truncated
// to maxLines wrapped lines, with a dim marker noting how many were elided.
func (r *Renderer) appendKV(out *[]Line, prefix, key, value string, avail int, block bool, maxLines int) {
	if avail < minDetailWidth {
		avail = minDetailWidth
	}
	label := key + ": "
	if !block && !strings.ContainsRune(value, '\n') && lipgloss.Width(value) <= avail-len(label) {
		*out = append(*out, Line{Kind: KindDetail, Text: prefix + r.st.detail.Render(label) + value})
		return
	}
	*out = append(*out, Line{Kind: KindDetail, Text: prefix + r.st.detail.Render(key+":")})
	wrapped := hardWrap(value, avail-2)
	elided := 0
	if maxLines > 0 && len(wrapped) > maxLines {
		elided = len(wrapped) - maxLines
		wrapped = wrapped[:maxLines]
	}
	for _, vl := range wrapped {
		*out = append(*out, Line{Kind: KindDetail, Text: prefix + "  " + vl})
	}
	if elided > 0 {
		noun := "lines"
		if elided == 1 {
			noun = "line"
		}
		*out = append(*out, Line{Kind: KindDetail,
			Text: prefix + "  " + r.st.dim.Render(fmt.Sprintf("... %d more %s (+ to expand)", elided, noun))})
	}
}

func (r *Renderer) headerText(t *Tree) string {
	noun := "spans"
	if t.Count == 1 {
		noun = "span"
	}
	dur := t.End.Sub(t.Start)
	if dur < 0 {
		dur = 0
	}
	return fmt.Sprintf("trace %s  %d %s  %s", shortID(t.ID, 16), t.Count, noun, humanDuration(dur))
}

// glyph colors a span's node by its HTTP status class when it has one (2xx
// green, 3xx cyan, 4xx yellow, 5xx red) and otherwise by its span status
// (green Ok, red Error, faint Unset). HTTP class wins so a 4xx -- which the
// instrumentation often also marks as an Error span -- shows yellow, not red.
func (r *Renderer) glyph(n *Node) string {
	return r.nodeStyle(n).Render(glyphNode)
}

func (r *Renderer) nodeStyle(n *Node) lipgloss.Style {
	if n.HTTPStatus != "" {
		if code, err := strconv.Atoi(n.HTTPStatus); err == nil {
			return r.statusCodeStyle(code)
		}
	}
	switch n.Status {
	case StatusError:
		return r.st.bad
	case StatusOk:
		return r.st.ok
	default:
		return r.st.unset
	}
}

// statusCodeStyle colors an HTTP status code by class.
func (r *Renderer) statusCodeStyle(code int) lipgloss.Style {
	switch {
	case code >= 500:
		return r.st.bad
	case code >= 400:
		return r.st.warn
	case code >= 300:
		return r.st.info
	case code >= 200:
		return r.st.ok
	default:
		return r.st.dim
	}
}

const (
	detailTimeLayout = "15:04:05.000"
	minDetailWidth   = 20
	timeColWidth     = len(detailTimeLayout) + 1 // leading start-time column + a space
	nameColMin       = 16
	nameColMax       = 48
	nameColReserve   = 46 // cells kept for the time column, graph, and tail beside the name
)

// humanDuration renders a duration compactly (ASCII "us" for microseconds).
func humanDuration(d time.Duration) string {
	switch {
	case d >= time.Second:
		return fmt.Sprintf("%.2fs", d.Seconds())
	case d >= time.Millisecond:
		return fmt.Sprintf("%dms", d.Milliseconds())
	case d >= time.Microsecond:
		return fmt.Sprintf("%dus", d.Microseconds())
	default:
		return fmt.Sprintf("%dns", d.Nanoseconds())
	}
}

// shortID truncates a hex id to n characters for display.
func shortID(id string, n int) string {
	if len(id) > n {
		return id[:n]
	}
	return id
}

// nameColWidth is the fixed cell width of the span-name column for a given pane
// width. It is stable for a terminal size (so the tail does not jump as names
// stream in) while still using more of a wide pane; capName fits each name to
// exactly this width.
func nameColWidth(total int) int {
	w := total - nameColReserve
	if w < nameColMin {
		w = nameColMin
	}
	if w > nameColMax {
		w = nameColMax
	}
	return w
}

// capName fits a span name into exactly width display cells: a short name is
// right-padded with spaces (keeping the metadata tail in a fixed column), and a
// long one (e.g. a long URL) is truncated with a "...(+N)" suffix noting how
// many characters were hidden.
func capName(name string, width int) string {
	if width < 1 {
		width = 1
	}
	runes := []rune(name)
	if len(runes) <= width {
		return name + strings.Repeat(" ", width-len(runes))
	}
	// Truncate, leaving room for a "...(+N)" suffix. N depends on how much is
	// cut, which changes its digit count, so size the suffix over two passes.
	keep := width
	for pass := 0; pass < 2; pass++ {
		hidden := len(runes) - keep
		keep = width - len(fmt.Sprintf("...(+%d)", hidden))
		if keep < 0 {
			keep = 0
		}
	}
	s := string(runes[:keep]) + fmt.Sprintf("...(+%d)", len(runes)-keep)
	// Clamp to exactly width (guards the rare digit-count edge).
	if rs := []rune(s); len(rs) > width {
		s = string(rs[:width])
	} else if len(rs) < width {
		s += strings.Repeat(" ", width-len(rs))
	}
	return s
}

// hardWrap splits s on newlines, then chops any line wider than width into
// width-sized pieces (bodies are often unbroken JSON, so a space-aware
// wrap would not bound the width).
func hardWrap(s string, width int) []string {
	if width <= 0 {
		return strings.Split(s, "\n")
	}
	var out []string
	for _, line := range strings.Split(s, "\n") {
		runes := []rune(line)
		if len(runes) == 0 {
			out = append(out, "")
			continue
		}
		for len(runes) > width {
			out = append(out, string(runes[:width]))
			runes = runes[width:]
		}
		out = append(out, string(runes))
	}
	return out
}
