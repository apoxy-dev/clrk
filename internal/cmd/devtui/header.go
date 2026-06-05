package devtui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// renderHeader paints the always-visible top bar as two stacked lines:
// the brand + screen title on line 1, the live stats strip on line 2.
// Nav hints render at the bottom of the screen via renderFooter, not
// here — the user explicitly asked for a single legend, not two.
func renderHeader(width int, title string, stats headerStats) string {
	if width <= 0 {
		width = 80
	}
	brand := headerLogoStyle.Render("apoxy://clrk")
	titleBlock := title
	if titleBlock != "" {
		titleBlock = " " + headerSubtitleStyle.Render("· "+title)
	}
	line1 := brand + titleBlock
	line2 := stats.render(width - 2)

	body := lipgloss.JoinVertical(lipgloss.Left, line1, line2)
	return headerBoxStyle.Width(width).Render(body)
}

// renderFooter paints the always-visible bottom legend with the
// screen-specific keybindings. Single line, muted text, top border so
// it visually closes the body.
func renderFooter(width int, nav string) string {
	if width <= 0 {
		width = 80
	}
	return footerStyle.Width(width).Render(headerNavStyle.Render(nav))
}

// headerStats holds the live counters rendered on the right-hand side.
// All fields are best-effort — missing values render as "-".
type headerStats struct {
	Agents   int
	Pools    int
	ReqsPerM int
	InTokPM  int64
	OutTokPM int64
}

// render builds the stats strip, dropping less-important tags as the
// available width shrinks. agents+pools always render; reqs/m and
// tokens/m peel off when they don't fit.
func (h headerStats) render(width int) string {
	tags := []string{
		statTag("agents", fmt.Sprintf("%d", h.Agents)),
		statTag("pools", fmt.Sprintf("%d", h.Pools)),
		statTag("reqs/m", fmt.Sprintf("%d", h.ReqsPerM)),
		statTag("tokens/m", fmt.Sprintf("%s/%s", compactNumber(h.InTokPM), compactNumber(h.OutTokPM))),
	}
	if width <= 0 {
		return strings.Join(tags, "  ")
	}
	for n := len(tags); n >= 1; n-- {
		s := strings.Join(tags[:n], "  ")
		if lipgloss.Width(s) <= width {
			return s
		}
	}
	return tags[0]
}

func statTag(name, val string) string {
	return headerStatLabelStyle.Render(name+":") + " " + headerStatValueStyle.Render(val)
}

// compactNumber formats large numbers as 1.2k / 3.4M for the header
// strip. Negative values are passed through verbatim.
func compactNumber(n int64) string {
	if n < 0 {
		return fmt.Sprintf("%d", n)
	}
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	case n >= 10_000:
		return fmt.Sprintf("%dk", n/1_000)
	case n >= 1_000:
		return fmt.Sprintf("%.1fk", float64(n)/1_000)
	default:
		return fmt.Sprintf("%d", n)
	}
}
