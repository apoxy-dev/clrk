package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/apoxy-dev/clrk/internal/cmd/spangraph"
	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
)

// maxTracesSpans caps how many spans the TUI retains while following so a
// long-lived tail stays bounded. When exceeded the oldest spans are
// dropped (an old trace may then render as a partial tree).
const maxTracesSpans = 5000

// runTracesTUI runs the interactive spans graph. A pump goroutine reads
// the backlog and (with follow) streams chunks, decoding each into neutral
// spans and posting them to the program; the model renders them through
// the shared spangraph renderer. A child context, cancelled when Run
// returns, stops the pump's follow loop on quit.
func runTracesTUI(parent context.Context, src *traceSource, follow bool, tailSpans int, colorOn bool) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	m := newTracesModel(follow, colorOn)
	// WithMouseCellMotion turns on terminal mouse reporting so the viewport's
	// MouseWheelEnabled flag actually receives wheel events; without it the
	// flag is inert and only keyboard scrolling works.
	prog := tea.NewProgram(m, tea.WithAltScreen(), tea.WithMouseCellMotion())

	go func() {
		send := func(msg tea.Msg) {
			if ctx.Err() == nil {
				prog.Send(msg)
			}
		}
		chunk, err := backlogTraceChunk(ctx, src, tailSpans)
		if err != nil {
			send(tracesErrMsg{err: err})
			return
		}
		spans, watermark, derr := decodeTraceSpans(chunk)
		if derr != nil {
			send(tracesErrMsg{err: fmt.Errorf("decoding traces (%d bytes): %w", len(chunk), derr)})
			return
		}
		send(tracesSpansMsg{spans: spans})
		if !follow {
			send(tracesDoneMsg{})
			return
		}
		ferr := followTraceChunks(ctx, src, watermark, func(c []byte) (time.Time, error) {
			sp, mx, e := decodeTraceSpans(c)
			if e != nil {
				return time.Time{}, e
			}
			send(tracesSpansMsg{spans: sp})
			return mx, nil
		})
		if ferr != nil {
			send(tracesErrMsg{err: ferr})
			return
		}
		send(tracesDoneMsg{})
	}()
	go func() {
		<-ctx.Done()
		prog.Quit()
	}()

	_, err := prog.Run()
	return err
}

// Messages the pump posts into the model.
type (
	tracesSpansMsg struct{ spans []spangraph.Span }
	tracesDoneMsg  struct{}
	tracesErrMsg   struct{ err error }
)

// maxExpandLevel is the deepest spangraph.ExpandLevel (bodies in full), so
// the +/- keys clamp the global expand level to [0, maxExpandLevel].
const maxExpandLevel = int(spangraph.LevelBodiesFull)

// levelNames labels each expand level for the footer, indexed by level.
var levelNames = []string{"graph", "attrs", "bodies", "bodies+"}

// tracesKeyMap is the model's own bindings (the dev TUI's keys live in a
// different package). +/- step the global expand level (the whole graph
// expands or collapses together); scrolling (j/k, arrows, page keys, mouse
// wheel) is left to the viewport; g/G jump to the ends; q/esc/ctrl+c quit.
type tracesKeyMap struct {
	expandMore key.Binding
	expandLess key.Binding
	top        key.Binding
	bottom     key.Binding
	quit       key.Binding
}

func defaultTracesKeys() tracesKeyMap {
	return tracesKeyMap{
		// "=" is the unshifted "+" key, so accept both; likewise "_" for "-".
		expandMore: key.NewBinding(key.WithKeys("+", "=")),
		expandLess: key.NewBinding(key.WithKeys("-", "_")),
		top:        key.NewBinding(key.WithKeys("g", "home")),
		bottom:     key.NewBinding(key.WithKeys("G", "end")),
		quit:       key.NewBinding(key.WithKeys("q", "esc", "ctrl+c")),
	}
}

// tracesModel is the Bubble Tea model for `agents traces`. It accumulates
// streamed spans (deduped by SpanID so a re-emitted span refreshes in
// place), renders them through the shared graph renderer into a viewport,
// and tracks a single global expand level so +/- open every span's
// attributes and event bodies at once. The view pins to the bottom while
// the user is at the bottom (auto-tail) and stays put once they scroll up.
type tracesModel struct {
	renderer    *spangraph.Renderer
	footerStyle lipgloss.Style
	follow      bool

	spans []spangraph.Span
	index map[string]int // SpanID -> position in spans (dedup / refresh)

	level int // global expand level in [0, maxExpandLevel]

	traceCount int
	content    string
	lines      []spangraph.Line // last render, for mapping scroll position to spans

	viewport viewport.Model
	keys     tracesKeyMap
	width    int
	height   int
	ready    bool
	done     bool
	err      error
}

func newTracesModel(follow, colorOn bool) *tracesModel {
	vp := viewport.New(0, 0)
	vp.MouseWheelEnabled = true

	lr := lipgloss.NewRenderer(os.Stdout)
	if colorOn {
		lr.SetColorProfile(termenv.ANSI)
	} else {
		lr.SetColorProfile(termenv.Ascii)
	}
	return &tracesModel{
		renderer:    spangraph.NewRenderer(os.Stdout, colorOn),
		footerStyle: lr.NewStyle().Faint(true),
		follow:      follow,
		index:       map[string]int{},
		viewport:    vp,
		keys:        defaultTracesKeys(),
	}
}

func (m *tracesModel) Init() tea.Cmd { return nil }

func (m *tracesModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		// Width changes re-wrap bodies, so re-render; keep pinned to the bottom
		// only if the user was already there.
		pin := m.viewport.AtBottom()
		m.width = msg.Width
		m.height = msg.Height
		vh := msg.Height - 1 // reserve one line for the footer
		if vh < 1 {
			vh = 1
		}
		m.viewport.Width = msg.Width
		m.viewport.Height = vh
		m.ready = true
		m.rebuild(pin)
		return m, nil
	case tracesSpansMsg:
		// Ride the tail only when the view was already at the bottom; a
		// scrolled-up reader is left where they are.
		pin := m.viewport.AtBottom()
		m.ingest(msg.spans)
		m.rebuild(pin)
		return m, nil
	case tracesDoneMsg:
		m.done = true
		m.rebuild(m.viewport.AtBottom())
		return m, nil
	case tracesErrMsg:
		m.err = msg.err
		m.done = true
		m.rebuild(false)
		return m, nil
	case tea.KeyMsg:
		return m.handleKey(msg)
	}
	var cmd tea.Cmd
	m.viewport, cmd = m.viewport.Update(msg)
	return m, cmd
}

func (m *tracesModel) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch {
	case key.Matches(msg, m.keys.quit):
		return m, tea.Quit
	case key.Matches(msg, m.keys.expandMore):
		m.setLevel(1)
		return m, nil
	case key.Matches(msg, m.keys.expandLess):
		m.setLevel(-1)
		return m, nil
	case key.Matches(msg, m.keys.top):
		m.viewport.GotoTop()
		return m, nil
	case key.Matches(msg, m.keys.bottom):
		m.viewport.GotoBottom()
		return m, nil
	}
	// Everything else (up/down, j/k, page keys, mouse wheel) scrolls.
	var cmd tea.Cmd
	m.viewport, cmd = m.viewport.Update(msg)
	return m, cmd
}

// ingest merges a chunk of spans, refreshing a span seen before (it may be
// re-emitted as it completes) and appending new ones. It trims the oldest
// spans past the retention cap, rebuilding the index.
func (m *tracesModel) ingest(spans []spangraph.Span) {
	for _, s := range spans {
		if i, ok := m.index[s.SpanID]; ok {
			m.spans[i] = s
			continue
		}
		m.index[s.SpanID] = len(m.spans)
		m.spans = append(m.spans, s)
	}
	if len(m.spans) > maxTracesSpans {
		drop := len(m.spans) - maxTracesSpans
		m.spans = append([]spangraph.Span(nil), m.spans[drop:]...)
		m.index = make(map[string]int, len(m.spans))
		for i, s := range m.spans {
			m.index[s.SpanID] = i
		}
	}
}

// setLevel steps the global expand level by delta, clamped to the valid
// range, and re-renders. The span owning the BOTTOM line of the viewport is
// held fixed on screen across the re-render, so the content at the bottom
// (the newest spans when tailing) stays put and the new detail grows upward
// off the top, rather than the bottom content being shoved off-screen.
func (m *tracesModel) setLevel(delta int) {
	lv := m.level + delta
	if lv < 0 {
		lv = 0
	}
	if lv > maxExpandLevel {
		lv = maxExpandLevel
	}
	if lv == m.level {
		return
	}
	anchorID, anchorOff := m.bottomAnchor()
	m.level = lv
	m.rebuild(false)
	if anchorID == "" {
		return
	}
	if idx := m.spanLineIndex(anchorID); idx >= 0 {
		// Restore the anchor span to the same row offset from the viewport top it
		// had before; SetYOffset clamps to the new content bounds.
		m.viewport.SetYOffset(idx - anchorOff)
	}
}

// bottomAnchor picks the span to hold fixed across a re-render and its current
// offset (the anchor's row relative to the viewport top). It is the span that
// OWNS the bottom line -- the nearest span row at or above the bottom of the
// viewport. Keeping the bottom fixed is what stops the text from jumping: a
// level change only appends (or trims) detail BELOW each span's rows, so the
// lines from the owning span's row down to the bottom line do not move
// relative to that row; pinning it keeps the bottom line exactly put while the
// extra detail grows upward off the top.
//
// Anchoring the top span instead would jump: when an upper span's attribute
// list or body is long, expanding it inserts lines ABOVE the bottom span and
// pushes the content at the bottom (the newest spans when tailing) off-screen.
func (m *tracesModel) bottomAnchor() (string, int) {
	top := m.viewport.YOffset
	bottom := top + m.viewport.Height - 1
	if bottom > len(m.lines)-1 {
		bottom = len(m.lines) - 1
	}
	for i := bottom; i >= 0; i-- {
		if m.lines[i].Kind == spangraph.KindSpan {
			return m.lines[i].SpanID, i - top
		}
	}
	// No span row at or above the bottom (the view sits above every span row):
	// fall back to the first span below it.
	for i := bottom + 1; i < len(m.lines); i++ {
		if i >= 0 && m.lines[i].Kind == spangraph.KindSpan {
			return m.lines[i].SpanID, i - top
		}
	}
	return "", 0
}

func (m *tracesModel) spanLineIndex(spanID string) int {
	for i, ln := range m.lines {
		if ln.Kind == spangraph.KindSpan && ln.SpanID == spanID {
			return i
		}
	}
	return -1
}

// rebuild re-renders the span forest into the viewport at the current
// expand level. When pinBottom is set (the view was at the bottom, or new
// data arrived while following) the view snaps back to the bottom after the
// content grows or shrinks; otherwise the scroll position is left alone and
// the viewport clamps it to the new content height.
func (m *tracesModel) rebuild(pinBottom bool) {
	if !m.ready {
		return
	}
	trees := spangraph.BuildTrees(m.spans)
	m.traceCount = len(trees)
	m.lines = m.renderer.Render(trees, m.renderOpts())

	var b strings.Builder
	for i, ln := range m.lines {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(ln.Text)
	}
	m.content = b.String()
	if len(m.spans) == 0 {
		m.content = m.emptyMessage()
	}

	m.viewport.SetContent(m.content)
	if pinBottom {
		m.viewport.GotoBottom()
	}
}

func (m *tracesModel) renderOpts() spangraph.Options {
	return spangraph.Options{Level: spangraph.ExpandLevel(m.level), Width: m.viewport.Width}
}

func (m *tracesModel) emptyMessage() string {
	switch {
	case m.err != nil:
		return "No spans. " + m.err.Error()
	case m.done:
		return "No spans for this agent."
	case m.follow:
		return "Waiting for spans..."
	default:
		return "Loading spans..."
	}
}

func (m *tracesModel) View() string {
	if !m.ready {
		return "Loading traces..."
	}
	return m.viewport.View() + "\n" + m.footer()
}

func (m *tracesModel) footer() string {
	help := "+/- expand | j/k scroll | g/G top/bottom | q quit"
	level := "graph"
	if m.level >= 0 && m.level < len(levelNames) {
		level = levelNames[m.level]
	}
	var state string
	switch {
	case m.err != nil:
		state = "error: " + m.err.Error()
	case m.follow && !m.done:
		state = "following"
	case m.done:
		state = "end"
	}
	noun := "spans"
	if len(m.spans) == 1 {
		noun = "span"
	}
	right := fmt.Sprintf("%d %s, %d traces | %s", len(m.spans), noun, m.traceCount, level)
	if state != "" {
		right += " | " + state
	}
	return m.footerStyle.Render(truncateLine(help+"    "+right, m.width))
}

// truncateLine hard-cuts s to width runes so the footer never wraps onto a
// second line (which would push the viewport off the alt-screen).
func truncateLine(s string, width int) string {
	if width <= 0 {
		return s
	}
	r := []rune(s)
	if len(r) <= width {
		return s
	}
	return string(r[:width])
}
