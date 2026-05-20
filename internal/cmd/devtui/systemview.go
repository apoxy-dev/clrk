package devtui

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// systemView renders the per-component sidebar + log pane that was the
// only screen before the redesign. It's now reachable via the [s] key
// from the agents screen.
type systemView struct {
	components []*component
	byName     map[string]*component
	selected   int

	viewport viewport.Model
	watcher  watcherState
	forwards map[string]int // key = "<ns>/<name>", value = host port
}

func newSystemView(componentNames []string) *systemView {
	names := append([]string{ClrkSource}, componentNames...)
	comps := make([]*component, 0, len(names))
	byName := make(map[string]*component, len(names))
	for _, n := range names {
		c := newComponent(n)
		comps = append(comps, c)
		byName[n] = c
	}
	// The clrk pseudo-component is always "ready" — it represents the dev
	// command itself, which is by definition running while the TUI is up.
	comps[0].status = StatusReady
	comps[0].startedAt = time.Now()

	vp := viewport.New(0, 0)
	vp.MouseWheelEnabled = true

	return &systemView{
		components: comps,
		byName:     byName,
		viewport:   vp,
	}
}

// applyStatus flips a component's glyph. No-op if the component name
// isn't registered (e.g. a component the orchestrator added late).
func (v *systemView) applyStatus(name string, status Status) {
	c, ok := v.byName[name]
	if !ok {
		return
	}
	c.status = status
	if status != StatusPending && c.startedAt.IsZero() {
		c.startedAt = time.Now()
	}
}

// applyLog appends a styled line to the matching component's ring buffer
// and refreshes the viewport when the log targets the selected pane.
func (v *systemView) applyLog(source, line string, stream LogStream, focused bool) {
	c, ok := v.byName[source]
	if !ok {
		return
	}
	c.append(styleLine(line, stream))
	if focused && c == v.current() {
		v.refreshViewport(true)
	}
}

// applyForwards replaces the rendered "Exposed Services" block with the
// supplied snapshot. The reconciler sends a fresh map on every change.
func (v *systemView) applyForwards(m map[string]int) {
	v.forwards = m
}

func (v *systemView) applyWatcher(msg WatcherMsg) {
	v.watcher = watcherState{
		event:  msg.Event,
		prefix: msg.Prefix,
		dur:    msg.Duration,
		err:    msg.Err,
		seen:   true,
	}
}

func (v *systemView) selectDelta(d int) {
	n := len(v.components)
	v.selected = (v.selected + d + n) % n
	v.refreshViewport(true)
}

func (v *systemView) clearCurrent() {
	v.current().clear()
	v.refreshViewport(true)
}

func (v *systemView) gotoTop()    { v.viewport.GotoTop() }
func (v *systemView) gotoBottom() { v.viewport.GotoBottom() }

func (v *systemView) updateViewport(msg tea.Msg) tea.Cmd {
	var cmd tea.Cmd
	v.viewport, cmd = v.viewport.Update(msg)
	return cmd
}

func (v *systemView) current() *component {
	return v.components[v.selected]
}

func (v *systemView) refreshViewport(autoFollow bool) {
	c := v.current()
	follow := autoFollow && v.viewport.AtBottom()
	v.viewport.SetContent(c.joined())
	if follow {
		v.viewport.GotoBottom()
	}
}

func (v *systemView) relayout(width, bodyHeight int) {
	logWidth := width - sidebarWidth - 4
	if logWidth < 10 {
		logWidth = 10
	}
	logHeight := bodyHeight - 2
	if logHeight < 1 {
		logHeight = 1
	}
	if v.viewport.Width == logWidth && v.viewport.Height == logHeight {
		return
	}
	v.viewport.Width = logWidth
	v.viewport.Height = logHeight
	v.refreshViewport(true)
}

// render returns the system-screen body — the sidebar + log pane —
// sized to width × bodyHeight. Caller stacks header / footer above
// and below.
func (v *systemView) render(width, bodyHeight int) string {
	sidebar := sidebarStyle.
		Width(sidebarWidth).
		Height(bodyHeight).
		Render(v.renderSidebar(bodyHeight - 2))

	logTitle := titleStyle.Render("Logs: " + v.current().display)
	logBody := v.viewport.View()
	logPane := logPaneStyle.
		Width(width - sidebarWidth - 2).
		Height(bodyHeight).
		Render(logTitle + "\n" + logBody)

	return lipgloss.JoinHorizontal(lipgloss.Top, sidebar, logPane)
}

// renderSidebar paints the component list and any trailing status blocks
// (exposed services, watcher) stacked at the bottom of the sidebar.
func (v *systemView) renderSidebar(budget int) string {
	now := time.Now()
	rows := make([]string, 0, len(v.components))
	for i, c := range v.components {
		row := renderComponentRow(c, now)
		if i == v.selected {
			row = sidebarSelectedStyle.Render("▌ " + row)
		} else {
			row = "  " + row
		}
		rows = append(rows, row)
	}
	listBlock := strings.Join(rows, "\n")

	trailing := make([]string, 0, 2)
	if fwd := renderForwardsBlock(v.forwards); fwd != "" {
		trailing = append(trailing, fwd)
	}
	if w := renderWatcherBlock(v.watcher); w != "" {
		trailing = append(trailing, w)
	}
	if len(trailing) == 0 {
		return listBlock
	}
	tail := strings.Join(trailing, "\n\n")
	used := lipgloss.Height(listBlock) + lipgloss.Height(tail)
	pad := budget - used - 1
	if pad < 1 {
		pad = 1
	}
	return listBlock + strings.Repeat("\n", pad) + tail
}

func renderComponentRow(c *component, now time.Time) string {
	st := statusStyle(c.status)
	glyph := st.Render(c.glyph())
	name := c.display
	uptime := mutedStyle.Render(c.uptime(now))

	avail := sidebarWidth - 2 - 2 - lipgloss.Width(glyph) - lipgloss.Width(uptime) - 2
	if avail < 0 {
		avail = 0
	}
	if lipgloss.Width(name) > avail {
		if avail > 1 {
			name = name[:avail-1] + "…"
		} else {
			name = ""
		}
	}
	pad := avail - lipgloss.Width(name)
	if pad < 0 {
		pad = 0
	}
	return glyph + " " + name + strings.Repeat(" ", pad) + " " + uptime
}

// renderForwardsBlock paints "ns/name → :PORT" lines for every auto-
// exposed Service. Returns "" when nothing is forwarded so the caller
// can skip the section entirely.
func renderForwardsBlock(m map[string]int) string {
	if len(m) == 0 {
		return ""
	}
	header := mutedStyle.Render("─ exposed ─")
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	lines := make([]string, 0, len(keys)+1)
	lines = append(lines, header)
	for _, k := range keys {
		line := fmt.Sprintf("%s → :%d", k, m[k])
		if lipgloss.Width(line) > sidebarWidth-2 {
			line = line[:sidebarWidth-3] + "…"
		}
		lines = append(lines, statusReadyStyle.Render(line))
	}
	return strings.Join(lines, "\n")
}

func renderWatcherBlock(w watcherState) string {
	if !w.seen {
		return ""
	}
	header := mutedStyle.Render("─ watcher ─")
	switch w.event {
	case WatcherBuilding:
		return header + "\n" + statusStartingStyle.Render("building…")
	case WatcherReloaded:
		line := fmt.Sprintf("idle · %s @%s", w.prefix, w.dur.Round(time.Millisecond))
		return header + "\n" + statusReadyStyle.Render(line)
	case WatcherFailed:
		line := "error"
		if w.prefix != "" {
			line = "error · " + w.prefix
		}
		return header + "\n" + statusErrorStyle.Render(line)
	default:
		return header + "\n" + mutedStyle.Render("idle")
	}
}

// styleLine applies stream-specific coloring. We render once on append so
// the viewport can stay zero-allocation on every redraw.
func styleLine(line string, stream LogStream) string {
	switch stream {
	case StreamStderr:
		return streamStderrStyle.Render(line)
	case StreamClrk:
		return streamClrkStyle.Render(line)
	default:
		return line
	}
}
