package cmd

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/lipgloss"
	"golang.org/x/term"
)

// spinnerStyle colors the pre-bring-up phase spinner in the brand accent so it
// reads as one tool with the install status view (which uses the same
// bubbles spinner.Dot).
var spinnerStyle = lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#5C2E91", Dark: "#A78BFA"})

// withSpinner runs fn while animating bubbles' spinner.Dot labeled `label` on
// stderr, then clears the line. It enforces minVisible so a fast phase
// (preflight, plan) doesn't flash past unread — the operator sees a deliberate
// "working..." beat rather than results appearing instantly. On a non-TTY
// stderr it prints the label once and runs fn with no animation and no
// artificial delay (CI / redirected output shouldn't be padded). Frames go to
// stderr so a redirected stdout (`-o yaml`) stays clean.
//
// It drives spinner.Dot's frames directly rather than running the spinner.Model
// component in a tea.Program: these two phases run before any bubbletea program
// exists, and standing one up for a ~1.5s pause would grab the terminal into raw
// mode, capture stdin, and install a SIGINT handler that swallows ctrl-c — so a
// plain \r writer keeps ctrl-c aborting the install. The in-bring-up status view
// (devtui) does use spinner.Model proper, since it owns the screen.
func withSpinner(label string, minVisible time.Duration, fn func()) {
	if !term.IsTerminal(int(os.Stderr.Fd())) {
		fmt.Fprintf(os.Stderr, "%s...\n", label)
		fn()
		return
	}

	frames := spinner.Dot.Frames
	interval := spinner.Dot.FPS
	if interval <= 0 {
		interval = time.Second / 10
	}

	start := time.Now()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for i := 0; ; i++ {
			frame := spinnerStyle.Render(frames[i%len(frames)])
			fmt.Fprintf(os.Stderr, "\r%s %s", frame, mutedSt.Render(label))
			select {
			case <-stop:
				return
			case <-ticker.C:
			}
		}
	}()

	fn()
	if d := time.Since(start); d < minVisible {
		time.Sleep(minVisible - d)
	}
	close(stop)
	wg.Wait()
	// Clear the spinner line so the caller's output starts clean.
	fmt.Fprint(os.Stderr, "\r\033[K")
}
