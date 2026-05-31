package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/apoxy-dev/clrk/internal/cmd"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		// A command may request a specific process exit code (e.g.
		// run-task mirrors the run's terminal phase: 124 timeout, 77
		// rejected). The message is printed only when non-empty.
		var ec interface{ ExitCode() int }
		if errors.As(err, &ec) {
			if msg := err.Error(); msg != "" {
				fmt.Fprintln(os.Stderr, msg)
			}
			os.Exit(ec.ExitCode())
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
