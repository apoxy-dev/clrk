package devtui

import "github.com/charmbracelet/bubbles/key"

type keyMap struct {
	Up           key.Binding
	Down         key.Binding
	Tab          key.Binding
	Open         key.Binding
	Back         key.Binding
	Spec         key.Binding
	AgentsScreen key.Binding
	SystemScreen key.Binding
	Clear        key.Binding
	Top          key.Binding
	Bottom       key.Binding
	Help         key.Binding
	Quit         key.Binding
}

var defaultKeys = keyMap{
	Up: key.NewBinding(
		key.WithKeys("up", "k"),
		key.WithHelp("↑/k", "prev"),
	),
	Down: key.NewBinding(
		key.WithKeys("down", "j"),
		key.WithHelp("↓/j", "next"),
	),
	Tab: key.NewBinding(
		key.WithKeys("tab"),
		key.WithHelp("tab", "next/cycle"),
	),
	Open: key.NewBinding(
		key.WithKeys("enter"),
		key.WithHelp("⏎", "open"),
	),
	Back: key.NewBinding(
		key.WithKeys("esc"),
		key.WithHelp("esc", "back"),
	),
	Spec: key.NewBinding(
		key.WithKeys("y"),
		key.WithHelp("y", "spec yaml"),
	),
	AgentsScreen: key.NewBinding(
		key.WithKeys("a"),
		key.WithHelp("a", "agents"),
	),
	SystemScreen: key.NewBinding(
		key.WithKeys("s"),
		key.WithHelp("s", "system"),
	),
	Clear: key.NewBinding(
		key.WithKeys("c"),
		key.WithHelp("c", "clear"),
	),
	Top: key.NewBinding(
		key.WithKeys("g", "home"),
		key.WithHelp("g", "top"),
	),
	Bottom: key.NewBinding(
		key.WithKeys("G", "end"),
		key.WithHelp("G", "bottom"),
	),
	Help: key.NewBinding(
		key.WithKeys("?"),
		key.WithHelp("?", "help"),
	),
	Quit: key.NewBinding(
		key.WithKeys("q", "ctrl+c"),
		key.WithHelp("q", "quit"),
	),
}

// ShortHelp implements help.KeyMap: the single-line hint row.
func (k keyMap) ShortHelp() []key.Binding {
	return []key.Binding{k.AgentsScreen, k.SystemScreen, k.Open, k.Back, k.Help, k.Quit}
}

// FullHelp implements help.KeyMap: the multi-column expanded help overlay.
func (k keyMap) FullHelp() [][]key.Binding {
	return [][]key.Binding{
		{k.Up, k.Down, k.Tab, k.Open, k.Back},
		{k.AgentsScreen, k.SystemScreen, k.Clear},
		{k.Top, k.Bottom, k.Help, k.Quit},
	}
}
