package cmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/term"
)

// errNoTTY is returned by confirm when stdin isn't a terminal. Callers
// turn this into a user-visible refusal with a hint to pass
// --force-recreate, instead of silently defaulting to "no" and
// confusing the operator.
var errNoTTY = errors.New("stdin is not a TTY")

// confirm prints prompt to stderr and reads a single line of input. It
// returns true only when the user types "y" or "yes" (case-insensitive,
// whitespace-trimmed). Returns errNoTTY when stdin isn't a terminal so
// the caller can produce a context-appropriate error rather than
// auto-rejecting in CI. EOF before any input is treated as "no" (the
// safe default for a destructive prompt). The blocking read is driven
// from a goroutine so ctx cancellation (Ctrl-C / SIGTERM via the
// caller's signal.NotifyContext) returns promptly instead of waiting
// for stdin to receive a newline; the goroutine leaks until stdin
// closes, which is fine because the process is on its way out.
func confirm(ctx context.Context, prompt string) (bool, error) {
	if !term.IsTerminal(int(os.Stdin.Fd())) {
		return false, errNoTTY
	}
	fmt.Fprint(os.Stderr, prompt)

	type scanResult struct {
		answer string
		err    error
	}
	resultCh := make(chan scanResult, 1)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
				resultCh <- scanResult{err: fmt.Errorf("reading confirmation: %w", err)}
				return
			}
			resultCh <- scanResult{}
			return
		}
		resultCh <- scanResult{answer: scanner.Text()}
	}()

	select {
	case <-ctx.Done():
		fmt.Fprintln(os.Stderr)
		return false, ctx.Err()
	case r := <-resultCh:
		if r.err != nil {
			return false, r.err
		}
		answer := strings.ToLower(strings.TrimSpace(r.answer))
		return answer == "y" || answer == "yes", nil
	}
}
