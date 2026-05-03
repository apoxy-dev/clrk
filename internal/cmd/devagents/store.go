package devagents

import (
	"context"
	"sort"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/apoxy-dev/clrk/internal/cmd/devotel"
)

// Store holds the live set of TaskAgents and DaemonAgents plus rolling
// per-agent telemetry derived from the in-process OTLP receiver.
//
// The dev TUI reads from Store.Snapshot() once per render tick. OTel
// records push into the store via AddLog/AddSpan as the receiver
// decodes them.
//
// Store is safe for concurrent use.
type Store struct {
	mu      sync.RWMutex
	entries map[ID]*entry
}

// entry is the per-agent record kept inside the store. obj is the most
// recently observed K8s object (kept as unstructured so the watcher
// stays decoupled from typed clientsets); stats accumulates OTel data.
type entry struct {
	id    ID
	obj   *unstructured.Unstructured
	stats *agentStats
}

// New returns an empty store. Call Run to begin watching the cluster.
func New() *Store {
	return &Store{entries: make(map[ID]*entry)}
}

// Run drives the K8s watchers on the configured kubeconfig. It blocks
// until ctx cancels. Failures inside the watcher loops back off and
// retry — Run only returns nil after ctx is done, or a fatal client
// construction error.
func (s *Store) Run(ctx context.Context, kubeconfig string) error {
	return runWatchers(ctx, kubeconfig,
		func(k Kind, u *unstructured.Unstructured) { s.upsert(k, u) },
		func(k Kind, ns, name string) { s.delete(ID{Kind: k, Namespace: ns, Name: name}) },
	)
}

// AddLog folds one OTLP log record into the matching agent's stats.
// Records that don't carry agent.{kind,name,namespace} are ignored —
// they don't have an owner row to attribute to.
func (s *Store) AddLog(rec devotel.LogRecord) {
	id, ok := fromLog(rec)
	if !ok {
		return
	}
	e := s.ensureStats(id)
	e.stats.addLog(rec)
}

// AddSpan folds one OTLP span into the matching agent's stats.
func (s *Store) AddSpan(sp devotel.Span) {
	id, ok := fromSpan(sp)
	if !ok {
		return
	}
	e := s.ensureStats(id)
	e.stats.addSpan(sp)
}

// Snapshot returns one row per known agent, sorted by (kind, namespace,
// name). The returned slice is the caller's to keep.
func (s *Store) Snapshot() []Snapshot {
	now := time.Now()
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Snapshot, 0, len(s.entries))
	for _, e := range s.entries {
		snap := buildSnapshot(e.id, e.obj)
		e.stats.snapshotInto(&snap, now)
		out = append(out, snap)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].ID.Kind != out[j].ID.Kind {
			return out[i].ID.Kind < out[j].ID.Kind
		}
		if out[i].ID.Namespace != out[j].ID.Namespace {
			return out[i].ID.Namespace < out[j].ID.Namespace
		}
		return out[i].ID.Name < out[j].ID.Name
	})
	return out
}

// Get returns the snapshot + raw object for a single agent. ok is false
// when the agent has been deleted.
func (s *Store) Get(id ID) (Snapshot, *unstructured.Unstructured, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[id]
	if !ok {
		return Snapshot{}, nil, false
	}
	snap := buildSnapshot(e.id, e.obj)
	e.stats.snapshotInto(&snap, time.Now())
	return snap, e.obj, true
}

// LogsFor returns a chronological copy of the per-agent log ring.
func (s *Store) LogsFor(id ID) []LogEvent {
	s.mu.RLock()
	e, ok := s.entries[id]
	s.mu.RUnlock()
	if !ok {
		return nil
	}
	return e.stats.copyLogs()
}

// SpansFor returns a chronological copy of the per-agent span ring.
func (s *Store) SpansFor(id ID) []SpanEvent {
	s.mu.RLock()
	e, ok := s.entries[id]
	s.mu.RUnlock()
	if !ok {
		return nil
	}
	return e.stats.copySpans()
}

// upsert handles an Add/Modified watch event by replacing the stored
// object. Stats are preserved across spec changes — restarts shouldn't
// reset the rolling counters.
func (s *Store) upsert(k Kind, u *unstructured.Unstructured) {
	id := ID{Kind: k, Namespace: u.GetNamespace(), Name: u.GetName()}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[id]
	if !ok {
		e = &entry{id: id, stats: &agentStats{}}
		s.entries[id] = e
	}
	e.obj = u
}

func (s *Store) delete(id ID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, id)
}

// ensureStats returns the entry for id, creating a stats-only entry if
// the K8s watcher hasn't seen the object yet. This handles the order
// where OTel records arrive before the apiserver List has caught up.
func (s *Store) ensureStats(id ID) *entry {
	s.mu.RLock()
	e, ok := s.entries[id]
	s.mu.RUnlock()
	if ok {
		return e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok = s.entries[id]; ok {
		return e
	}
	e = &entry{id: id, stats: &agentStats{}}
	s.entries[id] = e
	return e
}

// buildSnapshot extracts the spec/status fields the row renderer
// needs from the unstructured object. Returns a zero-valued Snapshot
// (with just the ID populated) when obj is nil — that's the case for
// agents we've only seen via OTel records.
func buildSnapshot(id ID, obj *unstructured.Unstructured) Snapshot {
	snap := Snapshot{ID: id}
	if obj == nil {
		return snap
	}
	snap.Pool = nestedString(obj.Object, "spec", "workerPoolRef")
	snap.Image = nestedString(obj.Object, "spec", "template", "spec", "image")
	snap.RestartPolicy = nestedString(obj.Object, "spec", "restartPolicy")
	snap.Phase = nestedString(obj.Object, "status", "phase")
	snap.RestartCount = nestedInt32(obj.Object, "status", "restartCount")
	snap.ActiveExecutions = nestedInt32(obj.Object, "status", "activeExecutions")
	if t, ok := nestedTime(obj.Object, "status", "upSince"); ok {
		snap.UpSince = t
	}
	snap.LastCondition = lastConditionMessage(obj.Object)
	return snap
}

func nestedString(o map[string]interface{}, fields ...string) string {
	cur := interface{}(o)
	for _, f := range fields {
		m, ok := cur.(map[string]interface{})
		if !ok {
			return ""
		}
		cur = m[f]
	}
	if s, ok := cur.(string); ok {
		return s
	}
	return ""
}

func nestedInt32(o map[string]interface{}, fields ...string) int32 {
	cur := interface{}(o)
	for _, f := range fields {
		m, ok := cur.(map[string]interface{})
		if !ok {
			return 0
		}
		cur = m[f]
	}
	switch v := cur.(type) {
	case int64:
		return int32(v)
	case float64:
		return int32(v)
	case int:
		return int32(v)
	}
	return 0
}

func nestedTime(o map[string]interface{}, fields ...string) (time.Time, bool) {
	s := nestedString(o, fields...)
	if s == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// lastConditionMessage returns the message of a False condition if
// one exists. Healthy conditions ("Ready=True", "Spec valid") aren't
// noteworthy — their message just clutters the detail header. Only
// surface problems the operator should react to.
func lastConditionMessage(o map[string]interface{}) string {
	status, _ := o["status"].(map[string]interface{})
	if status == nil {
		return ""
	}
	conds, _ := status["conditions"].([]interface{})
	if len(conds) == 0 {
		return ""
	}
	for _, raw := range conds {
		c, _ := raw.(map[string]interface{})
		if c == nil {
			continue
		}
		st, _ := c["status"].(string)
		if st != "False" {
			continue
		}
		if msg, _ := c["message"].(string); msg != "" {
			return msg
		}
	}
	return ""
}
