package devtui

import (
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// statusLogTail is how many recent lines of the in-flight step the toggleable
// log peek shows.
const statusLogTail = 14

// statusModel is the compact, inline (no alt-screen) install/upgrade progress
// view: an ordered step checklist with a spinner on the in-flight step and its
// latest log line shown beneath it, plus a per-step log tail a curious operator
// can toggle with "l". It deliberately avoids the full systemView (sidebar +
// panes + alt-screen) — `clrk install` wants a status line you can expand, not a
// dashboard. It consumes the same ComponentStatusMsg / LogLineMsg the rest of
// the package emits, so Program.SetStatus / SendLog drive it unchanged.
type statusModel struct {
	title    string
	steps    []*component
	byName   map[string]*component
	current  string            // name of the in-flight step (last StatusStarting)
	lastLine map[string]string // step name -> latest raw log line (for the inline peek)

	spin     spinner.Model
	showLogs bool
	width    int
}

func newStatusModel(stepNames []string, title string) *statusModel {
	sp := spinner.New(spinner.WithSpinner(spinner.Dot))
	sp.Style = lipgloss.NewStyle().Foreground(colorAccent)

	steps := make([]*component, 0, len(stepNames))
	byName := make(map[string]*component, len(stepNames))
	for _, n := range stepNames {
		c := newComponent(n)
		steps = append(steps, c)
		byName[n] = c
	}
	return &statusModel{
		title:    title,
		steps:    steps,
		byName:   byName,
		lastLine: make(map[string]string),
		spin:     sp,
	}
}

func (m *statusModel) Init() tea.Cmd { return m.spin.Tick }

func (m *statusModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		return m, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c":
			// Always available to cancel an in-flight install / dismiss the view.
			return m, tea.Quit
		case "enter":
			// Enter exits only once the run has finished (succeeded or failed) —
			// mid-run it's a no-op so an accidental press can't cancel the install.
			if m.finished() {
				return m, tea.Quit
			}
		case "l", "tab", " ":
			m.showLogs = !m.showLogs
		}
		return m, nil

	case ComponentStatusMsg:
		if c, ok := m.byName[msg.Name]; ok {
			c.status = msg.Status
			if msg.Status != StatusPending && c.startedAt.IsZero() {
				c.startedAt = time.Now()
			}
			if msg.Status == StatusStarting {
				m.current = msg.Name
			}
		}
		return m, nil

	case LogLineMsg:
		m.applyLog(msg.Source, msg.Line, msg.Stream)
		return m, nil

	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spin, cmd = m.spin.Update(msg)
		return m, cmd
	}
	return m, nil
}

// applyLog appends a line to the named step's ring (for the toggled tail) and
// records it as that step's latest line (for the inline peek). Logs from the
// synthetic "cli" source — slog routed through the TUI, e.g. crds.Install
// progress — have no step of their own, so they're attributed to the in-flight
// step rather than dropped.
func (m *statusModel) applyLog(source, line string, stream LogStream) {
	name := source
	if source == ClrkSource {
		name = m.current
	}
	if name == "" {
		return
	}
	c, ok := m.byName[name]
	if !ok {
		return
	}
	c.append(styleLine(line, stream))
	m.lastLine[name] = line
}

func (m *statusModel) View() string {
	// No early-return on quit: this is an inline (non-alt-screen) view, so the
	// final frame is what stays in the operator's scrollback after they exit.
	width := m.width
	if width <= 0 {
		width = 80
	}
	now := time.Now()

	var b strings.Builder
	b.WriteString(headerTitleStyle.Render(m.title) + "\n\n")

	failed, allReady := false, true
	for _, c := range m.steps {
		switch c.status {
		case StatusError:
			failed = true
			allReady = false
		case StatusReady:
			// counts toward allReady
		default:
			allReady = false
		}
		b.WriteString("  " + m.renderStepRow(c, now) + "\n")
		// Show the active step's latest line inline so the operator sees what's
		// happening — crucially, a pod failure reason like "ImagePullBackOff" —
		// without toggling the full log. The active step is the in-flight one, or
		// the last/failed one once the run ends (m.current never moves off it), so
		// this also surfaces the terminal "complete"/"failed" summary line.
		if c.name == m.current || c.status == StatusError {
			if last := m.lastLine[c.name]; last != "" {
				b.WriteString("    " + mutedStyle.Render(truncate("↳ "+last, width-6)) + "\n")
			}
		}
	}

	if m.showLogs && m.current != "" {
		if c, ok := m.byName[m.current]; ok {
			b.WriteString("\n" + titleStyle.Render("logs · "+c.display) + "\n")
			if tail := c.tail(statusLogTail); tail != "" {
				b.WriteString(tail + "\n")
			} else {
				b.WriteString(mutedStyle.Render("  (no output yet)") + "\n")
			}
		}
	}

	b.WriteString("\n" + m.footer(failed, allReady))
	return b.String()
}

// (truncate lives in agentsview.go — rune-based clip with an ellipsis, applied
// to raw text before styling so ANSI codes aren't sliced.)

func (m *statusModel) renderStepRow(c *component, now time.Time) string {
	var glyph, name string
	switch c.status {
	case StatusReady:
		glyph = statusReadyStyle.Render("●")
		name = c.display
	case StatusStarting:
		glyph = m.spin.View()
		name = lipgloss.NewStyle().Bold(true).Render(c.display)
	case StatusError:
		glyph = statusErrorStyle.Render("✕")
		name = statusErrorStyle.Render(c.display)
	default:
		glyph = statusPendingStyle.Render("◌")
		name = statusPendingStyle.Render(c.display)
	}
	row := glyph + " " + name
	if !c.startedAt.IsZero() && c.status != StatusReady {
		row += mutedStyle.Render("  " + c.uptime(now))
	}
	return row
}

// finished reports whether the run reached a terminal state: every step Ready
// (success), or some step Error (RunSteps stops on the first failure, leaving
// the later steps Pending). Drives both the footer and the Enter-to-exit key.
func (m *statusModel) finished() bool {
	allReady := true
	for _, c := range m.steps {
		if c.status == StatusError {
			return true
		}
		if c.status != StatusReady {
			allReady = false
		}
	}
	return allReady
}

func (m *statusModel) footer(failed, allReady bool) string {
	logHint := "l logs"
	if m.showLogs {
		logHint = "l hide logs"
	}
	switch {
	case failed:
		return statusErrorStyle.Render("✕ failed") + mutedStyle.Render("   "+logHint+" · enter to exit")
	case allReady:
		return statusReadyStyle.Render("✓ done") + mutedStyle.Render("   "+logHint+" · enter to exit")
	default:
		return mutedStyle.Render(logHint + " · ctrl-c cancel")
	}
}
