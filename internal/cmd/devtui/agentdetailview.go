package devtui

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	sigsyaml "sigs.k8s.io/yaml"

	"github.com/apoxy-dev/clrk/internal/cmd/devagents"
)

// detailTab enumerates the panes inside the per-agent detail view.
// Spec used to be a third tab — it's now a full-screen overlay
// reachable via the `y` hotkey, which is what the operator actually
// wants when they pop open a spec dump.
type detailTab int

const (
	tabLogs detailTab = iota
	tabTraces
)

func (t detailTab) String() string {
	switch t {
	case tabLogs:
		return "logs"
	case tabTraces:
		return "traces"
	}
	return "?"
}

// agentDetailView renders the per-agent header (immutable spec on the
// left, runtime status+stats on the right) and one of two tabs:
// `logs` or `traces`, cycled with `tab`. Pressing `y` swaps the entire
// body for a YAML render of the agent's spec; `y` again or `esc`
// returns.
type agentDetailView struct {
	store *devagents.Store
	id    devagents.ID

	tab      detailTab
	showSpec bool

	viewport viewport.Model
}

func newAgentDetailView(store *devagents.Store, id devagents.ID) *agentDetailView {
	vp := viewport.New(0, 0)
	vp.MouseWheelEnabled = true
	return &agentDetailView{
		store:    store,
		id:       id,
		viewport: vp,
	}
}

func (v *agentDetailView) cycleTab() {
	v.tab = (v.tab + 1) % 2
	v.viewport.GotoBottom()
}

func (v *agentDetailView) toggleSpec() {
	v.showSpec = !v.showSpec
	if v.showSpec {
		v.viewport.GotoTop()
	} else {
		v.viewport.GotoBottom()
	}
}

func (v *agentDetailView) gotoTop()    { v.viewport.GotoTop() }
func (v *agentDetailView) gotoBottom() { v.viewport.GotoBottom() }

func (v *agentDetailView) updateViewport(msg tea.Msg) tea.Cmd {
	var cmd tea.Cmd
	v.viewport, cmd = v.viewport.Update(msg)
	return cmd
}

func (v *agentDetailView) relayout(width, bodyHeight int) {
	contentH := bodyHeight
	contentW := width - 2
	if contentW < 20 {
		contentW = 20
	}
	if !v.showSpec {
		// Header (4 status lines + tab strip) takes ~6 lines.
		contentH = bodyHeight - 7
		if contentH < 3 {
			contentH = 3
		}
	}
	if v.viewport.Width != contentW || v.viewport.Height != contentH {
		v.viewport.Width = contentW
		v.viewport.Height = contentH
	}
}

func (v *agentDetailView) render(width, bodyHeight int) string {
	v.relayout(width, bodyHeight)

	snap, obj, ok := v.store.Get(v.id)
	if !ok {
		return mutedStyle.Render("  Agent " + v.id.String() + " no longer exists.")
	}

	if v.showSpec {
		v.viewport.SetContent(renderSpecYAML(obj))
		return v.viewport.View()
	}

	headerBlock := renderDetailHeader(snap, width-2)
	tabs := renderTabStrip(v.tab)

	switch v.tab {
	case tabLogs:
		v.viewport.SetContent(renderLogsPane(v.store.LogsFor(v.id)))
	case tabTraces:
		v.viewport.SetContent(renderSpansPane(v.store.SpansFor(v.id)))
	}

	body := v.viewport.View()
	return lipgloss.JoinVertical(lipgloss.Left, headerBlock, tabs, body)
}

// renderDetailHeader paints the per-agent header as two columns —
// immutable spec data on the left, live runtime + stats on the right.
// The split makes a quick visual scan possible: image/pool/restart
// stay put across renders, and only the right column ticks.
func renderDetailHeader(s devagents.Snapshot, width int) string {
	now := time.Now()

	left := []string{
		labelKV("kind", string(s.ID.Kind)),
		labelKV("pool", fallback(s.Pool, "-")),
		labelKV("image", truncate(fallback(s.Image, "-"), 60)),
		labelKV("restart", fallback(s.RestartPolicy, "-")),
	}

	statusBadge := agentStatusBadge(s, now)
	right := []string{
		labelKV("status", statusBadge),
		labelKV("reqs/m", fmt.Sprintf("%d", s.Reqs1m)),
		labelKV("p50/p95", fmt.Sprintf("%s / %s",
			fallbackDuration(s.P50), fallbackDuration(s.P95))),
		labelKV("tokens",
			fmt.Sprintf("%s / %s",
				compactNumber(s.TokensInTotal),
				compactNumber(s.TokensOutTotal))),
	}
	if s.LastCondition != "" {
		right = append(right, labelKV("issue", statusErrorStyle.Render(truncate(s.LastCondition, 60))))
	}

	leftCol := strings.Join(left, "\n")
	rightCol := strings.Join(right, "\n")

	leftWidth := width / 2
	if leftWidth < 30 {
		leftWidth = 30
	}
	row := lipgloss.JoinHorizontal(lipgloss.Top,
		lipgloss.NewStyle().Width(leftWidth).Render(leftCol),
		rightCol,
	)
	return tabStripStyle.Render(row)
}

func renderTabStrip(active detailTab) string {
	tabs := []string{tabLogs.String(), tabTraces.String()}
	parts := make([]string, len(tabs))
	for i, t := range tabs {
		if detailTab(i) == active {
			parts[i] = tabActiveStyle.Render(t)
		} else {
			parts[i] = tabInactiveStyle.Render(t)
		}
	}
	return strings.Join(parts, " ")
}

// renderLogsPane draws each per-agent log event on one line with a
// short time prefix. Body is the summaryLine extproc emits (HTTP
// method/path/status/duration + provider/model/tokens).
func renderLogsPane(logs []devagents.LogEvent) string {
	if len(logs) == 0 {
		return mutedStyle.Render("  No log records yet for this agent.")
	}
	lines := make([]string, 0, len(logs))
	for _, e := range logs {
		ts := mutedStyle.Render(e.Time.Format("15:04:05.000"))
		body := e.Body
		if body == "" {
			body = "<no body>"
		}
		lines = append(lines, ts+" "+body)
	}
	return strings.Join(lines, "\n")
}

// renderSpansPane draws each span on one line: name, status, duration,
// trace prefix.
func renderSpansPane(spans []devagents.SpanEvent) string {
	if len(spans) == 0 {
		return mutedStyle.Render("  No spans yet for this agent.")
	}
	lines := make([]string, 0, len(spans))
	for _, sp := range spans {
		ts := mutedStyle.Render(sp.Time.Format("15:04:05.000"))
		dur := "?"
		if sp.Duration > 0 {
			dur = sp.Duration.Round(time.Millisecond).String()
		}
		status := sp.Status
		if status == "" {
			status = "UNSET"
		}
		statusStyled := statusStyleForSpan(status).Render(status)
		trace := ""
		if sp.TraceID != "" {
			trace = " " + mutedStyle.Render("trace="+shortTraceID(sp.TraceID))
		}
		lines = append(lines, fmt.Sprintf("%s %s [%s] %s%s",
			ts, sp.Name, statusStyled, dur, trace))
	}
	return strings.Join(lines, "\n")
}

func statusStyleForSpan(s string) lipgloss.Style {
	switch s {
	case "STATUS_CODE_OK", "OK", "Ok":
		return statusReadyStyle
	case "STATUS_CODE_ERROR", "ERROR", "Error":
		return statusErrorStyle
	default:
		return mutedStyle
	}
}

// renderSpecYAML pretty-prints the spec sub-object as YAML — what
// `kubectl get -o yaml` would show. We strip away managedFields and
// other large server-side metadata that obscures the user-authored
// shape, but keep top-level metadata so resourceVersion / uid are
// still legible for debugging.
func renderSpecYAML(obj interface{}) string {
	type unstructuredLike interface {
		UnstructuredContent() map[string]interface{}
	}
	if obj == nil {
		return mutedStyle.Render("  No spec available (agent only seen on the wire).")
	}
	var content map[string]interface{}
	if u, ok := obj.(unstructuredLike); ok {
		content = u.UnstructuredContent()
	} else if m, ok := obj.(map[string]interface{}); ok {
		content = m
	} else {
		return mutedStyle.Render("  Object is not unstructured.")
	}
	// Drop noise that would push the spec off the screen.
	if md, ok := content["metadata"].(map[string]interface{}); ok {
		delete(md, "managedFields")
		delete(md, "annotations")
	}

	out, err := sigsyaml.Marshal(content)
	if err != nil {
		return statusErrorStyle.Render("  spec marshal error: " + err.Error())
	}
	return string(out)
}

func labelKV(label, val string) string {
	return mutedStyle.Render(label+":") + " " + val
}

func fallback(s, alt string) string {
	if s == "" {
		return alt
	}
	return s
}

func fallbackDuration(d time.Duration) string {
	if d <= 0 {
		return "—"
	}
	return d.Round(time.Millisecond).String()
}

func shortTraceID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
