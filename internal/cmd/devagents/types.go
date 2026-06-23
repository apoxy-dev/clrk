// Package devagents tracks the live state of TaskAgent and DaemonAgent
// objects in the dev cluster and rolls up per-agent telemetry from the
// in-process OTLP receiver. The dev TUI consumes a Snapshot to render
// the API-object-centric main screen.
package devagents

import (
	"time"

	"github.com/apoxy-dev/clrk/internal/cmd/devotel"
)

// Kind enumerates the agent CRDs the store watches.
type Kind string

const (
	KindTaskAgent   Kind = "TaskAgent"
	KindDaemonAgent Kind = "DaemonAgent"
)

// ID uniquely names an agent within the dev cluster.
type ID struct {
	Kind      Kind
	Namespace string
	Name      string
}

func (id ID) String() string {
	return string(id.Kind) + "/" + id.Namespace + "/" + id.Name
}

// Snapshot is one row in the agents view. It blends the K8s object's
// spec/status with rolling stats derived from OTel records.
type Snapshot struct {
	ID            ID
	Pool          string
	Image         string
	RestartPolicy string

	// Status fields. Phase is populated for DaemonAgent; for TaskAgent
	// we synthesise it from conditions.
	Phase            string
	UpSince          time.Time
	RestartCount     int32
	ActiveExecutions int32
	WarmSandboxes    int32
	LastCondition    string

	// Stats (rolling 60s window unless noted).
	Reqs1m       int
	TokensIn1m   int64
	TokensOut1m  int64
	TokensInTotal  int64
	TokensOutTotal int64
	P50           time.Duration
	P95           time.Duration
	LastStatus    int

	// LastSeen is the timestamp of the most recent OTel record we
	// attributed to this agent. Zero when the agent has never been
	// seen on the wire.
	LastSeen time.Time
}

// LogEvent is a per-agent log record retained for the detail view.
type LogEvent struct {
	Time time.Time
	Body string
	// Severity is the OTLP severity name (e.g. "SEVERITY_NUMBER_INFO").
	Severity string
	// Attributes is the flattened attribute map from the LogRecord.
	Attributes map[string]string
}

// SpanEvent is a per-agent span retained for the detail view.
type SpanEvent struct {
	Time     time.Time
	Duration time.Duration
	Name     string
	Status   string
	TraceID  string
	SpanID   string
	// Attributes is the flattened attribute map from the Span.
	Attributes map[string]string
}

// fromLog returns the agent ID an OTLP log record is tagged with, or
// an empty ID + false when the record is unattributed.
func fromLog(rec devotel.LogRecord) (ID, bool) {
	return idFromAttrs(rec.Attributes)
}

func fromSpan(sp devotel.Span) (ID, bool) {
	return idFromAttrs(sp.Attributes)
}

func idFromAttrs(a map[string]string) (ID, bool) {
	name := a["agent.name"]
	if name == "" {
		return ID{}, false
	}
	kind := Kind(a["agent.kind"])
	switch kind {
	case KindTaskAgent, KindDaemonAgent:
	default:
		// Unknown agent.kind — drop. We still expect a value so the
		// detail view can disambiguate two agents with the same name
		// across different kinds.
		return ID{}, false
	}
	return ID{
		Kind:      kind,
		Namespace: a["agent.namespace"],
		Name:      name,
	}, true
}
