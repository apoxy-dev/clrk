// Package devtui renders the live status of `clrk dev` (k3s,
// controller-manager, workers) and their per-component logs in a Bubble Tea
// TUI. Orchestration code stays in pkg/cmd; this package is a passive
// renderer driven by Send* helpers.
package devtui

import (
	"context"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

// Program wraps a tea.Program with typed senders so callers don't need to
// import bubbletea directly.
type Program struct {
	p *tea.Program
}

// New constructs a TUI program for the given component names. The clrk
// pseudo-source is added implicitly as the first sidebar entry.
func New(componentNames []string) *Program {
	m := newModel(componentNames)
	p := tea.NewProgram(
		m,
		tea.WithAltScreen(),
	)
	return &Program{p: p}
}

// Run blocks until the user quits or ctx is cancelled. It's safe to call
// Send* from any goroutine before, during, and after Run, but messages sent
// after Run returns are dropped.
func (p *Program) Run(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		p.p.Quit()
	}()
	_, err := p.p.Run()
	return err
}

// Quit asks the program to exit gracefully. Idempotent.
func (p *Program) Quit() { p.p.Quit() }

// SetStatus updates a component's status glyph. The error string is not
// rendered — failures are surfaced via slog into the cli pane.
func (p *Program) SetStatus(name string, status Status) {
	p.p.Send(ComponentStatusMsg{Name: name, Status: status})
}

// SendLog appends a line to a component's buffer.
func (p *Program) SendLog(source, line string, stream LogStream) {
	p.p.Send(LogLineMsg{Source: source, Line: line, Stream: stream})
}

// SendWatcher reports a watcher lifecycle event.
func (p *Program) SendWatcher(event WatcherEvent, prefix string, dur time.Duration, err error) {
	msg := WatcherMsg{Event: event, Prefix: prefix, Duration: dur}
	if err != nil {
		msg.Err = err.Error()
	}
	p.p.Send(msg)
}

// MarkSyntheticReady flips a synthetic component (e.g. otel-logs) to
// StatusReady so its sidebar glyph shows green from the start. Use
// this for components that have no driver lifecycle to track —
// they're "ready" the moment the TUI is up.
func MarkSyntheticReady(p *Program, name string) {
	if p == nil {
		return
	}
	p.SetStatus(name, StatusReady)
}
