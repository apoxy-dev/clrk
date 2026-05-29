//go:build linux

package sandbox

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"

	"github.com/apoxy-dev/clrk/internal/egress/proxyproto"
	"github.com/apoxy-dev/clrk/internal/workerlog"
)

// sandboxLineWriter is an io.Writer that splits incoming bytes on '\n'
// and emits one slog record per complete line, decorated with the
// sandbox's identity. Bytes without a trailing newline are buffered
// until either a newline arrives or Flush is called on shutdown.
//
// Used to fan the libcontainer process's stdout / stderr into the
// worker's structured log pipeline so agent output is attributable and
// shaped like the rest of our logs.
//
// When fileSink is non-nil each line is also appended (with a `[stdout]`
// / `[stderr]` prefix and a trailing newline) so `clrk agents logs` can
// `tail -F` the file from outside the worker process.
type sandboxLineWriter struct {
	logger   *slog.Logger
	level    slog.Level
	stream   string // "stdout" or "stderr".
	fileSink *os.File

	mu  sync.Mutex
	buf bytes.Buffer
}

const sandboxLineMaxBytes = 64 * 1024

func newSandboxLineWriter(base *slog.Logger, level slog.Level, stream string, fileSink *os.File) *sandboxLineWriter {
	return &sandboxLineWriter{
		logger:   base.With(slog.String("stream", stream)),
		level:    level,
		stream:   stream,
		fileSink: fileSink,
	}
}

var _ io.Writer = (*sandboxLineWriter)(nil)

func (w *sandboxLineWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.buf.Write(p)
	for {
		raw := w.buf.Bytes()
		i := bytes.IndexByte(raw, '\n')
		if i < 0 {
			// No complete line yet; cap the buffer so a misbehaving
			// process spewing without newlines can't OOM the worker.
			if w.buf.Len() > sandboxLineMaxBytes {
				w.emit(string(w.buf.Bytes()[:sandboxLineMaxBytes]) + " ...[truncated]")
				w.buf.Reset()
			}
			return len(p), nil
		}
		line := strings.TrimRight(string(raw[:i]), "\r")
		w.buf.Next(i + 1)
		if line != "" {
			w.emit(line)
		}
	}
}

// Flush emits any buffered tail bytes that arrived without a trailing
// newline. Call on sandbox teardown.
func (w *sandboxLineWriter) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.buf.Len() == 0 {
		return
	}
	line := strings.TrimRight(w.buf.String(), "\r")
	w.buf.Reset()
	if line != "" {
		w.emit(line)
	}
}

func (w *sandboxLineWriter) emit(line string) {
	w.logger.LogAttrs(context.Background(), w.level, "sandbox output",
		slog.String("line", line),
	)
	if w.fileSink != nil {
		// Best-effort: a failed write should never break sandbox stdio.
		_, _ = fmt.Fprintf(w.fileSink, "[%s] %s\n", w.stream, line)
	}
}

func openAgentLogFile(rootDir, namespace, name string) (*os.File, error) {
	if rootDir == "" {
		return nil, fmt.Errorf("logs dir not configured")
	}
	return os.OpenFile(workerlog.AgentPath(rootDir, namespace, name),
		os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
}

// identityLogFields returns slog key/value args for the populated
// fields of an AgentIdentity, ready to be spread into slog.With /
// logger.Info. Single source of truth for the agent.kind /
// agent.namespace / agent.name / agent.uid / agent.revision /
// invocation.id field names across the sandbox package — sites used
// to rebuild this attr set inline and drifted from each other.
//
// Empty UID / Revision / InvocationID are omitted so a "no UID"
// reading is distinguishable from a daemon that never had one.
func identityLogFields(id proxyproto.AgentIdentity) []any {
	fields := []any{
		slog.String("agent.kind", id.Kind.String()),
		slog.String("agent.namespace", id.Namespace),
		slog.String("agent.name", id.Name),
	}
	if id.UID != "" {
		fields = append(fields, slog.String("agent.uid", id.UID))
	}
	if id.Revision != "" {
		fields = append(fields, slog.String("agent.revision", id.Revision))
	}
	if id.InvocationID != "" {
		fields = append(fields, slog.String("invocation.id", id.InvocationID))
	}
	return fields
}
