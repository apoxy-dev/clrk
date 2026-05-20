package devtui

import (
	"time"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/apoxy-dev/clrk/internal/cmd/devagents"
)

const (
	sidebarWidth = 30
	tickInterval = time.Second
)

// tickMsg fires once per second to refresh uptime counters and re-pull
// store snapshots. Both the agents and detail screens render directly
// from the store, so the tick is also the pace at which their numbers
// update.
type tickMsg time.Time

// screen identifies which view rootModel is currently rendering.
type screen int

const (
	screenAgents screen = iota
	screenAgentDetail
	screenSystem
)

// watcherState is shared between the systemView and the orchestrator's
// SendWatcher push. Kept at package scope so messages.go's WatcherMsg
// can populate it without round-tripping through systemView.
type watcherState struct {
	event  WatcherEvent
	prefix string
	dur    time.Duration
	err    string
	seen   bool
}

// rootModel is the screen router. It owns shared chrome (header) and
// dispatches input to the active view.
type rootModel struct {
	width, height int

	current  screen
	keys     keyMap
	quitting bool

	store *devagents.Store

	agents *agentsView
	detail *agentDetailView
	system *systemView
}

func newRootModel(componentNames []string, store *devagents.Store) *rootModel {
	if store == nil {
		store = devagents.New()
	}
	return &rootModel{
		current: screenAgents,
		keys:    defaultKeys,
		store:   store,
		agents:  newAgentsView(store),
		system:  newSystemView(componentNames),
	}
}

func (m *rootModel) Init() tea.Cmd {
	return tickCmd()
}

func (m *rootModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		m.relayout()
		return m, nil

	case tea.KeyMsg:
		return m.handleKey(msg)

	case ComponentStatusMsg:
		m.system.applyStatus(msg.Name, msg.Status)
		return m, nil

	case LogLineMsg:
		// Only refresh the viewport when the system screen is active
		// AND the message targets the visible component — saves redraw
		// work while the user is on the agents screen.
		focused := m.current == screenSystem
		m.system.applyLog(msg.Source, msg.Line, msg.Stream, focused)
		return m, nil

	case WatcherMsg:
		m.system.applyWatcher(msg)
		return m, nil

	case ExposedForwardsMsg:
		m.system.applyForwards(msg)
		return m, nil

	case tickMsg:
		return m, tickCmd()
	}

	if m.current == screenSystem {
		cmd := m.system.updateViewport(msg)
		return m, cmd
	}
	if m.current == screenAgentDetail && m.detail != nil {
		cmd := m.detail.updateViewport(msg)
		return m, cmd
	}
	return m, nil
}

func (m *rootModel) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch {
	case key.Matches(msg, m.keys.Quit):
		m.quitting = true
		return m, tea.Quit
	case key.Matches(msg, m.keys.AgentsScreen):
		m.current = screenAgents
		return m, nil
	case key.Matches(msg, m.keys.SystemScreen):
		m.current = screenSystem
		return m, nil
	case key.Matches(msg, m.keys.Back):
		if m.current == screenAgentDetail {
			if m.detail != nil && m.detail.showSpec {
				m.detail.showSpec = false
				return m, nil
			}
			m.current = screenAgents
		}
		return m, nil
	}

	switch m.current {
	case screenAgents:
		return m.handleAgentsKey(msg)
	case screenAgentDetail:
		return m.handleDetailKey(msg)
	case screenSystem:
		return m.handleSystemKey(msg)
	}
	return m, nil
}

func (m *rootModel) handleAgentsKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch {
	case key.Matches(msg, m.keys.Up):
		m.agents.selectDelta(-1)
	case key.Matches(msg, m.keys.Down), key.Matches(msg, m.keys.Tab):
		m.agents.selectDelta(+1)
	case key.Matches(msg, m.keys.Open):
		if id, ok := m.agents.selectedID(); ok {
			m.detail = newAgentDetailView(m.store, id)
			m.detail.relayout(m.width, m.bodyHeight())
			m.current = screenAgentDetail
		}
	}
	return m, nil
}

func (m *rootModel) handleDetailKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if m.detail == nil {
		m.current = screenAgents
		return m, nil
	}
	switch {
	case key.Matches(msg, m.keys.Spec):
		m.detail.toggleSpec()
	case key.Matches(msg, m.keys.Tab):
		if !m.detail.showSpec {
			m.detail.cycleTab()
		}
	case key.Matches(msg, m.keys.Top):
		m.detail.gotoTop()
	case key.Matches(msg, m.keys.Bottom):
		m.detail.gotoBottom()
	default:
		cmd := m.detail.updateViewport(msg)
		return m, cmd
	}
	return m, nil
}

func (m *rootModel) handleSystemKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch {
	case key.Matches(msg, m.keys.Up):
		m.system.selectDelta(-1)
	case key.Matches(msg, m.keys.Down), key.Matches(msg, m.keys.Tab):
		m.system.selectDelta(+1)
	case key.Matches(msg, m.keys.Clear):
		m.system.clearCurrent()
	case key.Matches(msg, m.keys.Top):
		m.system.gotoTop()
	case key.Matches(msg, m.keys.Bottom):
		m.system.gotoBottom()
	default:
		cmd := m.system.updateViewport(msg)
		return m, cmd
	}
	return m, nil
}

func (m *rootModel) View() string {
	if m.quitting {
		return ""
	}
	if m.width == 0 || m.height == 0 {
		return "Initializing..."
	}

	header := m.renderHeader()
	footer := m.renderFooter()
	headerH := lipgloss.Height(header)
	footerH := lipgloss.Height(footer)

	bodyHeight := m.height - headerH - footerH
	if bodyHeight < 1 {
		bodyHeight = 1
	}

	var body string
	switch m.current {
	case screenAgents:
		body = m.agents.render(m.width, bodyHeight)
	case screenAgentDetail:
		if m.detail == nil {
			m.current = screenAgents
			body = m.agents.render(m.width, bodyHeight)
		} else {
			body = m.detail.render(m.width, bodyHeight)
		}
	case screenSystem:
		body = m.system.render(m.width, bodyHeight)
	}

	// Pad body to bodyHeight so the footer stays glued to the
	// terminal's bottom edge regardless of the screen's rendered
	// content height.
	bodyBlock := lipgloss.NewStyle().Height(bodyHeight).Width(m.width).Render(body)
	return lipgloss.JoinVertical(lipgloss.Left, header, bodyBlock, footer)
}

func (m *rootModel) renderHeader() string {
	stats := m.collectStats()
	title := m.screenTitle()
	return renderHeader(m.width, title, stats)
}

func (m *rootModel) renderFooter() string {
	return renderFooter(m.width, m.screenNav())
}

// screenTitle returns the per-screen subtitle that pairs with the
// brand mark on header line 1.
func (m *rootModel) screenTitle() string {
	switch m.current {
	case screenAgents:
		return "agents"
	case screenAgentDetail:
		if m.detail != nil {
			if m.detail.showSpec {
				return string(m.detail.id.Kind) + "/" + m.detail.id.Namespace + "/" + m.detail.id.Name + " · spec"
			}
			return string(m.detail.id.Kind) + "/" + m.detail.id.Namespace + "/" + m.detail.id.Name
		}
		return "agent"
	case screenSystem:
		return "system view"
	}
	return ""
}

// screenNav returns the keybinding strip rendered in the bottom
// footer. Keys are screen-specific so the strip never shows bindings
// the user can't act on.
func (m *rootModel) screenNav() string {
	switch m.current {
	case screenAgents:
		return "↑↓ select   ⏎ open   s system   q quit"
	case screenAgentDetail:
		if m.detail != nil && m.detail.showSpec {
			return "esc back   y close spec   a agents   s system   q quit"
		}
		return "tab logs/traces   y spec   esc back   a agents   s system   q quit"
	case screenSystem:
		return "↑↓ select   c clear   g/G top/bottom   a agents   q quit"
	}
	return ""
}

// collectStats sweeps the current snapshot for the header counters. We
// don't cache — Snapshot() under RLock is cheap and the tick rate is
// 1Hz, so a per-tick recount is fine.
func (m *rootModel) collectStats() headerStats {
	snaps := m.store.Snapshot()
	pools := map[string]struct{}{}
	stats := headerStats{Agents: len(snaps)}
	for _, s := range snaps {
		if s.Pool != "" {
			pools[s.Pool] = struct{}{}
		}
		stats.ReqsPerM += s.Reqs1m
		stats.InTokPM += s.TokensIn1m
		stats.OutTokPM += s.TokensOut1m
	}
	stats.Pools = len(pools)
	return stats
}

func (m *rootModel) bodyHeight() int {
	header := m.renderHeader()
	footer := m.renderFooter()
	h := m.height - lipgloss.Height(header) - lipgloss.Height(footer)
	if h < 1 {
		h = 1
	}
	return h
}

func (m *rootModel) relayout() {
	bodyHeight := m.bodyHeight()
	m.system.relayout(m.width, bodyHeight)
	if m.detail != nil {
		m.detail.relayout(m.width, bodyHeight)
	}
}

func tickCmd() tea.Cmd {
	return tea.Tick(tickInterval, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}
