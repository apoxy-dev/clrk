package devtui

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
)

// SlogHandler is a slog.Handler that forwards each record to the TUI as a
// LogLineMsg under the synthetic "clrk" source. It exists so the global
// slog default can be redirected for the lifetime of a TUI session without
// racing the TUI's screen updates.
type SlogHandler struct {
	prog  *Program
	level slog.Level
	attrs []slog.Attr
	group string
}

// NewSlogHandler returns a handler bound to prog. Records at or above
// minLevel are forwarded; everything else is dropped.
func NewSlogHandler(prog *Program, minLevel slog.Level) *SlogHandler {
	return &SlogHandler{prog: prog, level: minLevel}
}

func (h *SlogHandler) Enabled(_ context.Context, l slog.Level) bool { return l >= h.level }

func (h *SlogHandler) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder
	fmt.Fprintf(&b, "%s %s %s", r.Time.Format("15:04:05"), r.Level.String(), r.Message)
	for _, a := range h.attrs {
		writeAttr(&b, h.group, a)
	}
	r.Attrs(func(a slog.Attr) bool {
		writeAttr(&b, h.group, a)
		return true
	})
	h.prog.SendLog(ClrkSource, b.String(), StreamClrk)
	return nil
}

func (h *SlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	clone := *h
	clone.attrs = append(append([]slog.Attr{}, h.attrs...), attrs...)
	return &clone
}

func (h *SlogHandler) WithGroup(name string) slog.Handler {
	clone := *h
	if h.group == "" {
		clone.group = name
	} else {
		clone.group = h.group + "." + name
	}
	return &clone
}

func writeAttr(b *strings.Builder, group string, a slog.Attr) {
	if a.Equal(slog.Attr{}) {
		return
	}
	key := a.Key
	if group != "" {
		key = group + "." + key
	}
	fmt.Fprintf(b, " %s=%v", key, a.Value.Any())
}
