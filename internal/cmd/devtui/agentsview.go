package devtui

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"

	"github.com/apoxy-dev/clrk/internal/cmd/devagents"
)

// agentsView renders one row per TaskAgent + DaemonAgent. Rows are
// rebuilt on every render from a Store snapshot, so an agent
// disappearing from the cluster drops out of the list immediately.
type agentsView struct {
	store *devagents.Store

	// snapshot is the most recently fetched row set. Rebuilt at the
	// top of render so input handlers (selectDelta, current) always
	// reflect current data without needing to re-fetch under the
	// store's lock.
	snapshot []devagents.Snapshot
	selected int
}

func newAgentsView(store *devagents.Store) *agentsView {
	return &agentsView{store: store}
}

func (v *agentsView) selectDelta(d int) {
	n := len(v.snapshot)
	if n == 0 {
		return
	}
	v.selected = (v.selected + d + n) % n
}

// selectedID returns the agent the cursor is on, or zero ID + false
// when the list is empty.
func (v *agentsView) selectedID() (devagents.ID, bool) {
	if len(v.snapshot) == 0 || v.selected >= len(v.snapshot) {
		return devagents.ID{}, false
	}
	return v.snapshot[v.selected].ID, true
}

// refresh pulls the latest snapshot from the store and clamps the
// selection. Called on every render — cheap enough at the row counts
// a dev session deals with.
func (v *agentsView) refresh() {
	v.snapshot = v.store.Snapshot()
	if v.selected >= len(v.snapshot) {
		v.selected = len(v.snapshot) - 1
	}
	if v.selected < 0 {
		v.selected = 0
	}
}

func (v *agentsView) render(width, bodyHeight int) string {
	v.refresh()
	if width <= 0 {
		width = 80
	}

	cols := columnsForWidth(width - 2)
	header := agentRowHeader(width, cols)
	if len(v.snapshot) == 0 {
		empty := mutedStyle.Render(
			"  No agents yet. Apply a TaskAgent or DaemonAgent manifest with `clrk dev --apply ...`.",
		)
		return lipgloss.JoinVertical(lipgloss.Left, header, empty)
	}

	rows := make([]string, 0, len(v.snapshot)+2)
	rows = append(rows, header)
	now := time.Now()
	for i, snap := range v.snapshot {
		marker := "  "
		if i == v.selected {
			marker = sidebarSelectedStyle.Render("▌ ")
		}
		rows = append(rows, marker+renderAgentRow(snap, cols, now))
		if len(rows)-1 >= bodyHeight {
			break
		}
	}
	return strings.Join(rows, "\n")
}

// Column widths are fixed; tiered visibility (drop columns under
// narrow widths) keeps the layout readable instead of squishing every
// column proportionally.
const (
	colKindWidth   = 7
	colNameWidth   = 28
	colPoolWidth   = 18
	colStatusWidth = 18
	colTokenWidth  = 16
	colLatWidth    = 10
)

// agentColumn is one column in the agents grid. Both header and row
// rendering walk the same column list so they can never disagree.
type agentColumn struct {
	title  string
	width  int
	render func(s devagents.Snapshot, now time.Time) string
}

// columnsForWidth returns the columns that fit within `width` cells.
// KIND/NAME/STATUS are always present; p95, TOKENS, and POOL are
// added in order of importance as room becomes available. The exact
// thresholds are derived from the column widths plus inter-column
// spaces so we never over-claim horizontal space.
func columnsForWidth(width int) []agentColumn {
	kind := agentColumn{
		title: " KIND", width: colKindWidth,
		render: func(s devagents.Snapshot, _ time.Time) string {
			return " " + padTo(agentKindGlyph(s.ID.Kind), colKindWidth-1)
		},
	}
	name := agentColumn{
		title: "NAME", width: colNameWidth,
		render: func(s devagents.Snapshot, _ time.Time) string {
			return padTo(truncate(s.ID.Namespace+"/"+s.ID.Name, colNameWidth), colNameWidth)
		},
	}
	status := agentColumn{
		title: "STATUS", width: colStatusWidth,
		render: func(s devagents.Snapshot, now time.Time) string {
			return padTo(truncate(agentStatusBadge(s, now), colStatusWidth), colStatusWidth)
		},
	}
	p95 := agentColumn{
		title: "p95", width: colLatWidth,
		render: func(s devagents.Snapshot, _ time.Time) string {
			v := "—"
			if s.P95 > 0 {
				v = s.P95.Round(time.Millisecond).String()
			}
			return padTo(truncate(v, colLatWidth), colLatWidth)
		},
	}
	tokens := agentColumn{
		title: "TOKENS IN/OUT", width: colTokenWidth,
		render: func(s devagents.Snapshot, _ time.Time) string {
			v := fmt.Sprintf("%s / %s", compactNumber(s.TokensInTotal), compactNumber(s.TokensOutTotal))
			return padTo(truncate(v, colTokenWidth), colTokenWidth)
		},
	}
	pool := agentColumn{
		title: "POOL", width: colPoolWidth,
		render: func(s devagents.Snapshot, _ time.Time) string {
			v := "-"
			if s.Pool != "" {
				v = s.Pool
			}
			return padTo(truncate(v, colPoolWidth), colPoolWidth)
		},
	}

	cols := []agentColumn{kind, name, status}
	tryAdd := func(c agentColumn) {
		need := c.width + 1 // joining space
		used := 0
		for i, x := range cols {
			used += x.width
			if i > 0 {
				used++
			}
		}
		if used+need <= width {
			cols = append(cols, c)
		}
	}
	tryAdd(p95)
	tryAdd(tokens)
	tryAdd(pool)
	return cols
}

// agentRowHeader paints the column legend in muted style.
func agentRowHeader(width int, cols []agentColumn) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = padTo(c.title, c.width)
	}
	line := mutedStyle.Render(strings.Join(parts, " "))
	return tabStripStyle.Width(width).Render(line)
}

func renderAgentRow(s devagents.Snapshot, cols []agentColumn, now time.Time) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = c.render(s, now)
	}
	return strings.Join(parts, " ")
}

// agentKindGlyph returns a stylised single-token label for the agent
// kind (used as the first column).
func agentKindGlyph(k devagents.Kind) string {
	switch k {
	case devagents.KindDaemonAgent:
		return agentKindDaemonStyle.Render("⬢ dmn")
	case devagents.KindTaskAgent:
		return agentKindTaskStyle.Render("◇ tsk")
	default:
		return mutedStyle.Render("? unk")
	}
}

// agentStatusBadge produces "●up 4m12s" / "⊘pending" / "✕crash" from
// the snapshot's K8s status fields. Returns "—" when no status is
// available yet (agent only seen on the wire, not via the apiserver).
func agentStatusBadge(s devagents.Snapshot, now time.Time) string {
	switch s.ID.Kind {
	case devagents.KindDaemonAgent:
		switch s.Phase {
		case "Running":
			up := "?"
			if !s.UpSince.IsZero() {
				up = shortDuration(now.Sub(s.UpSince))
			}
			restarts := ""
			if s.RestartCount > 0 {
				restarts = fmt.Sprintf(" ↻%d", s.RestartCount)
			}
			return statusReadyStyle.Render("●") + " up " + up + restarts
		case "Stopped":
			return statusPendingStyle.Render("⊘") + " stopped"
		case "CrashLoopBackOff":
			return statusErrorStyle.Render("✕") + fmt.Sprintf(" crashloop ↻%d", s.RestartCount)
		default:
			if s.Phase != "" {
				return statusStartingStyle.Render("◐") + " " + strings.ToLower(s.Phase)
			}
			return statusPendingStyle.Render("◌") + " pending"
		}
	case devagents.KindTaskAgent:
		if s.ActiveExecutions > 0 {
			return statusReadyStyle.Render("●") + fmt.Sprintf(" active ×%d", s.ActiveExecutions)
		}
		return statusPendingStyle.Render("◌") + " idle"
	}
	return "—"
}

// shortDuration trims a Duration to a small "4m12s" / "1h03m" form.
func shortDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm%02ds", int(d.Minutes()), int(d.Seconds())%60)
	default:
		return fmt.Sprintf("%dh%02dm", int(d.Hours()), int(d.Minutes())%60)
	}
}

// padTo right-pads s with spaces so its visible width equals n. If s
// already exceeds n it's returned untouched (caller should pre-truncate).
func padTo(s string, n int) string {
	w := lipgloss.Width(s)
	if w >= n {
		return s
	}
	return s + strings.Repeat(" ", n-w)
}

// truncate cuts s to a max visible width of n, appending "…" when
// trimmed. Operates on bytes for non-ANSI strings — we don't currently
// truncate styled text in this path.
func truncate(s string, n int) string {
	if lipgloss.Width(s) <= n {
		return s
	}
	if n <= 1 {
		return "…"
	}
	r := []rune(s)
	if len(r) <= n-1 {
		return s
	}
	return string(r[:n-1]) + "…"
}
