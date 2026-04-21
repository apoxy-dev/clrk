// Package cmd implements the `clrk` CLI built on top of cobra.
package cmd

import (
	"log/slog"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

var (
	logLevel string
	clrkDir  string
)

// RootCmd is the top-level `clrk` command.
var RootCmd = &cobra.Command{
	Use:           "clrk",
	Short:         "CLRK agent sandbox runtime CLI",
	SilenceUsage:  true,
	SilenceErrors: true,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return configureLogging()
	},
}

func init() {
	RootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "Log level (debug, info, warn, error).")
	RootCmd.PersistentFlags().StringVar(&clrkDir, "clrk-dir", defaultClrkDir(), "State directory for clrk.")

	RootCmd.AddCommand(newDevCmd())
}

func defaultClrkDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "/var/lib/clrk"
	}
	return filepath.Join(home, ".clrk")
}

func configureLogging() error {
	var lvl slog.Level
	switch logLevel {
	case "debug":
		lvl = slog.LevelDebug
	case "info":
		lvl = slog.LevelInfo
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})))
	return nil
}
