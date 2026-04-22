package devtui

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/help"
	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

const (
	sidebarWidth = 30
	tickInterval = time.Second
)

// tickMsg fires once per second to refresh uptime counters.
type tickMsg time.Time

// model is the root tea.Model. It owns the per-component state and the
// viewport that renders the selected component's log buffer.
type model struct {
	width, height int

	components []*component
	byName     map[string]*component
	selected   int

	viewport viewport.Model
	keys     keyMap
	help     help.Model
	showHelp bool
	quitting bool

	watcher watcherState
}

// watcherState consolidates the sidebar's rebuild-status block. seen is true
// once the watcher has emitted at least one event; until then the block is
// hidden entirely (no point displaying "idle" before --watch ever fires).
type watcherState struct {
	event  WatcherEvent
	prefix string
	dur    time.Duration
	err    string
	seen   bool
}

func newModel(componentNames []string) *model {
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

	h := help.New()
	h.ShowAll = false

	vp := viewport.New(0, 0)
	vp.MouseWheelEnabled = true

	return &model{
		components: comps,
		byName:     byName,
		viewport:   vp,
		keys:       defaultKeys,
		help:       h,
	}
}

func (m *model) Init() tea.Cmd {
	return tickCmd()
}

func (m *model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		m.relayout()
		return m, nil

	case tea.KeyMsg:
		return m.handleKey(msg)

	case ComponentStatusMsg:
		if c, ok := m.byName[msg.Name]; ok {
			c.status = msg.Status
			if msg.Status != StatusPending && c.startedAt.IsZero() {
				c.startedAt = time.Now()
			}
		}
		return m, nil

	case LogLineMsg:
		c, ok := m.byName[msg.Source]
		if !ok {
			return m, nil
		}
		styled := styleLine(msg.Line, msg.Stream)
		c.append(styled)
		if c == m.current() {
			m.refreshViewport(true)
		}
		return m, nil

	case WatcherMsg:
		m.watcher = watcherState{
			event:  msg.Event,
			prefix: msg.Prefix,
			dur:    msg.Duration,
			err:    msg.Err,
			seen:   true,
		}
		return m, nil

	case tickMsg:
		return m, tickCmd()
	}

	var cmd tea.Cmd
	m.viewport, cmd = m.viewport.Update(msg)
	return m, cmd
}

func (m *model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch {
	case key.Matches(msg, m.keys.Quit):
		m.quitting = true
		return m, tea.Quit
	case key.Matches(msg, m.keys.Help):
		m.showHelp = !m.showHelp
		m.help.ShowAll = m.showHelp
		m.relayout()
		return m, nil
	case key.Matches(msg, m.keys.Up):
		m.selectDelta(-1)
		return m, nil
	case key.Matches(msg, m.keys.Down), key.Matches(msg, m.keys.Tab):
		m.selectDelta(+1)
		return m, nil
	case key.Matches(msg, m.keys.Clear):
		m.current().clear()
		m.refreshViewport(true)
		return m, nil
	case key.Matches(msg, m.keys.Top):
		m.viewport.GotoTop()
		return m, nil
	case key.Matches(msg, m.keys.Bottom):
		m.viewport.GotoBottom()
		return m, nil
	}
	var cmd tea.Cmd
	m.viewport, cmd = m.viewport.Update(msg)
	return m, cmd
}

func (m *model) View() string {
	if m.quitting {
		return ""
	}
	if m.width == 0 || m.height == 0 {
		return "Initializing..."
	}

	footer := footerStyle.Width(m.width - 2).Render(m.help.View(m.keys))
	footerHeight := lipgloss.Height(footer)

	bodyHeight := m.height - footerHeight - 1
	if bodyHeight < 1 {
		bodyHeight = 1
	}

	sidebar := sidebarStyle.
		Width(sidebarWidth).
		Height(bodyHeight).
		Render(m.renderSidebar(bodyHeight - 2))

	logTitle := titleStyle.Render("Logs: " + m.current().display)
	logBody := m.viewport.View()
	logPane := logPaneStyle.
		Width(m.width - sidebarWidth - 2).
		Height(bodyHeight).
		Render(logTitle + "\n" + logBody)

	body := lipgloss.JoinHorizontal(lipgloss.Top, sidebar, logPane)
	header := titleStyle.Render("clrk dev")

	return lipgloss.JoinVertical(lipgloss.Left, header, body, footer)
}

// renderSidebar paints the component list and the watcher status block. The
// budget is the available height in lines so the watcher block can be padded
// to the bottom.
func (m *model) renderSidebar(budget int) string {
	now := time.Now()
	var rows []string
	for i, c := range m.components {
		row := m.renderComponentRow(c, now)
		if i == m.selected {
			row = sidebarSelectedStyle.Render("▌ " + row)
		} else {
			row = "  " + row
		}
		rows = append(rows, row)
	}

	listBlock := strings.Join(rows, "\n")

	watcher := m.renderWatcherBlock()
	if watcher == "" {
		return listBlock
	}

	used := lipgloss.Height(listBlock) + lipgloss.Height(watcher)
	pad := budget - used - 1
	if pad < 1 {
		pad = 1
	}
	return listBlock + strings.Repeat("\n", pad) + watcher
}

func (m *model) renderComponentRow(c *component, now time.Time) string {
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

func (m *model) renderWatcherBlock() string {
	if !m.watcher.seen {
		return ""
	}
	header := mutedStyle.Render("─ watcher ─")
	switch m.watcher.event {
	case WatcherBuilding:
		return header + "\n" + statusStartingStyle.Render("building…")
	case WatcherReloaded:
		line := fmt.Sprintf("idle · %s @%s", m.watcher.prefix, m.watcher.dur.Round(time.Millisecond))
		return header + "\n" + statusReadyStyle.Render(line)
	case WatcherFailed:
		line := "error"
		if m.watcher.prefix != "" {
			line = "error · " + m.watcher.prefix
		}
		return header + "\n" + statusErrorStyle.Render(line)
	default:
		return header + "\n" + mutedStyle.Render("idle")
	}
}

func (m *model) current() *component {
	return m.components[m.selected]
}

func (m *model) selectDelta(d int) {
	n := len(m.components)
	m.selected = (m.selected + d + n) % n
	m.refreshViewport(true)
}

// refreshViewport rebuilds the viewport contents from the current component's
// ring buffer. If autoFollow is true and the user was already pinned to the
// bottom (or just selected this component), auto-scroll to the tail.
func (m *model) refreshViewport(autoFollow bool) {
	c := m.current()
	follow := autoFollow && m.viewport.AtBottom()
	m.viewport.SetContent(c.joined())
	if follow {
		m.viewport.GotoBottom()
	}
}

func (m *model) relayout() {
	footerHeight := lipgloss.Height(footerStyle.Render(m.help.View(m.keys)))
	bodyHeight := m.height - footerHeight - 1
	if bodyHeight < 1 {
		bodyHeight = 1
	}
	logWidth := m.width - sidebarWidth - 4
	if logWidth < 10 {
		logWidth = 10
	}
	logHeight := bodyHeight - 2
	if logHeight < 1 {
		logHeight = 1
	}
	if m.viewport.Width == logWidth && m.viewport.Height == logHeight {
		return
	}
	m.viewport.Width = logWidth
	m.viewport.Height = logHeight
	m.refreshViewport(true)
}

func tickCmd() tea.Cmd {
	return tea.Tick(tickInterval, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

// styleLine applies stream-specific coloring. We render once on append so the
// viewport can stay zero-allocation on every redraw.
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
