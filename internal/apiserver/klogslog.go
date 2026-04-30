package apiserver

import (
	"log/slog"

	"github.com/go-logr/logr"
)

// klogSlogAdapter bridges logr (used by klog and controller-runtime) to the
// process-wide slog.Default logger so everything ends up in one sink.
type klogSlogAdapter struct {
	name   string
	values []any
}

var _ logr.LogSink = klogSlogAdapter{}

func (k klogSlogAdapter) Init(logr.RuntimeInfo) {}

func (k klogSlogAdapter) Enabled(level int) bool {
	return slog.Default().Enabled(nil, slog.LevelInfo)
}

func (k klogSlogAdapter) Info(level int, msg string, keysAndValues ...any) {
	slog.Default().Info(msg, append(k.values, keysAndValues...)...)
}

func (k klogSlogAdapter) Error(err error, msg string, keysAndValues ...any) {
	args := append([]any{"error", err}, k.values...)
	args = append(args, keysAndValues...)
	slog.Default().Error(msg, args...)
}

func (k klogSlogAdapter) WithValues(keysAndValues ...any) logr.LogSink {
	return klogSlogAdapter{name: k.name, values: append(append([]any{}, k.values...), keysAndValues...)}
}

func (k klogSlogAdapter) WithName(name string) logr.LogSink {
	if k.name == "" {
		k.name = name
	} else {
		k.name = k.name + "." + name
	}
	return k
}
