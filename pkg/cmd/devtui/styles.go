package devtui

import "github.com/charmbracelet/lipgloss"

var (
	colorAccent  = lipgloss.AdaptiveColor{Light: "#5C2E91", Dark: "#A78BFA"}
	colorMuted   = lipgloss.AdaptiveColor{Light: "#6B7280", Dark: "#9CA3AF"}
	colorReady   = lipgloss.AdaptiveColor{Light: "#15803D", Dark: "#34D399"}
	colorWarn    = lipgloss.AdaptiveColor{Light: "#B45309", Dark: "#FBBF24"}
	colorError   = lipgloss.AdaptiveColor{Light: "#B91C1C", Dark: "#F87171"}
	colorBorder  = lipgloss.AdaptiveColor{Light: "#D4D4D8", Dark: "#3F3F46"}
	colorPanelBg = lipgloss.AdaptiveColor{Light: "#FAFAFA", Dark: "#18181B"}
)

var (
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(colorAccent).
			Padding(0, 1)

	mutedStyle = lipgloss.NewStyle().Foreground(colorMuted)

	statusReadyStyle    = lipgloss.NewStyle().Foreground(colorReady)
	statusStartingStyle = lipgloss.NewStyle().Foreground(colorWarn)
	statusErrorStyle    = lipgloss.NewStyle().Foreground(colorError)
	statusPendingStyle  = lipgloss.NewStyle().Foreground(colorMuted)

	sidebarStyle = lipgloss.NewStyle().
			Border(lipgloss.NormalBorder(), false, true, false, false).
			BorderForeground(colorBorder).
			Padding(0, 1)

	sidebarSelectedStyle = lipgloss.NewStyle().
				Foreground(colorAccent).
				Bold(true)

	logPaneStyle = lipgloss.NewStyle().
			Padding(0, 1)

	footerStyle = lipgloss.NewStyle().
			Border(lipgloss.NormalBorder(), true, false, false, false).
			BorderForeground(colorBorder).
			Foreground(colorMuted).
			Padding(0, 1)

	streamStderrStyle = lipgloss.NewStyle().Foreground(colorWarn)
	streamClrkStyle   = lipgloss.NewStyle().Foreground(colorAccent)
)

// statusStyle picks the lipgloss style for a given status. Centralizes the
// pendant-color decisions so glyph and label render in the same hue.
func statusStyle(s Status) lipgloss.Style {
	switch s {
	case StatusReady:
		return statusReadyStyle
	case StatusStarting:
		return statusStartingStyle
	case StatusError:
		return statusErrorStyle
	default:
		return statusPendingStyle
	}
}
