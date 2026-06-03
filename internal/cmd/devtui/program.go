// Package devtui renders the live status of `clrk dev` (agents, k3s,
// controller-manager, workers) and their per-component logs in a Bubble Tea
// TUI. Orchestration code stays in pkg/cmd; this package is a passive
// renderer driven by Send* helpers and the devagents.Store.
package devtui

import (
	"context"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/apoxy-dev/clrk/internal/cmd/devagents"
)

// preRunQueueSize bounds the number of messages buffered before Run()
// starts draining. Sized to absorb the bring-up burst (a couple of
// status flips per component plus early slog records) without dropping;
// once Run is up the forwarder keeps pace with tea.Program.
const preRunQueueSize = 1024

// Program wraps a tea.Program with typed senders so callers don't need to
// import bubbletea directly.
//
// All Send* methods are non-blocking. They post to a buffered queue that
// a single forwarder drains into tea.Program once Run() starts its event
// loop. tea.Program.Send is itself blocking on an unbuffered channel
// until Run() begins reading — funneling through the buffer here keeps
// callers (slog handler, orchestrator goroutines) from deadlocking each
// other during bring-up.
type Program struct {
	p     *tea.Program
	queue chan tea.Msg
}

// New constructs a TUI program for the given component names backed
// by the supplied agents store. The clrk pseudo-source is added
// implicitly as the first sidebar entry on the system screen. Pass
// nil for store to start with an empty agents view (the store will
// be created internally) — useful for tests.
func New(componentNames []string, store *devagents.Store) *Program {
	m := newRootModel(componentNames, store)
	p := tea.NewProgram(
		m,
		tea.WithAltScreen(),
	)
	return &Program{
		p:     p,
		queue: make(chan tea.Msg, preRunQueueSize),
	}
}

// NewSystem constructs a TUI program that renders only the system view (the
// step-list sidebar + per-step log pane) with no agents store or agent screens,
// for `clrk install`/`upgrade` progress. componentNames are the install steps;
// title is the header subtitle (e.g. "install · my-context"). The clrk pseudo-
// source is still the implicit first sidebar entry, so slog routed via
// NewSlogHandler lands in its pane.
func NewSystem(componentNames []string, title string) *Program {
	m := newSystemRootModel(componentNames, title)
	p := tea.NewProgram(
		m,
		tea.WithAltScreen(),
	)
	return &Program{
		p:     p,
		queue: make(chan tea.Msg, preRunQueueSize),
	}
}

// Run blocks until the user quits or ctx is cancelled. Send* calls made
// before Run are buffered and replayed once the event loop is up.
func (p *Program) Run(ctx context.Context) error {
	go p.forward(ctx)
	go func() {
		<-ctx.Done()
		p.p.Quit()
	}()
	_, err := p.p.Run()
	return err
}

// forward drains the pre-Run buffer (and the steady-state stream) into
// tea.Program. tea.Program.Send blocks on its unbuffered msgs channel
// until the event loop is reading; serializing through this single
// goroutine isolates that blocking from every Send* caller.
func (p *Program) forward(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-p.queue:
			p.p.Send(msg)
		}
	}
}

// send is the single chokepoint every public Send* helper funnels
// through. Drops on a full queue rather than block — under normal load
// the 1024-slot buffer is more than enough; if a pathological burst
// fills it the dropped message is preferable to wedging the caller.
func (p *Program) send(msg tea.Msg) {
	select {
	case p.queue <- msg:
	default:
	}
}

// Quit asks the program to exit gracefully. Idempotent.
func (p *Program) Quit() { p.p.Quit() }

// SetStatus updates a component's status glyph. The error string is not
// rendered — failures are surfaced via slog into the cli pane.
func (p *Program) SetStatus(name string, status Status) {
	p.send(ComponentStatusMsg{Name: name, Status: status})
}

// SendLog appends a line to a component's buffer.
func (p *Program) SendLog(source, line string, stream LogStream) {
	p.send(LogLineMsg{Source: source, Line: line, Stream: stream})
}

// SendWatcher reports a watcher lifecycle event.
func (p *Program) SendWatcher(event WatcherEvent, prefix string, dur time.Duration, err error) {
	msg := WatcherMsg{Event: event, Prefix: prefix, Duration: dur}
	if err != nil {
		msg.Err = err.Error()
	}
	p.send(msg)
}

// SendExposedForwards replaces the sidebar's "Exposed Services" block
// with the supplied snapshot. Caller transfers ownership of the map —
// don't mutate it after sending.
func (p *Program) SendExposedForwards(m map[string]int) {
	p.send(ExposedForwardsMsg(m))
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
