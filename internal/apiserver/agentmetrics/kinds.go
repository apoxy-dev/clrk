package agentmetrics

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
	metricsv1 "github.com/apoxy-dev/clrk/api/metrics/v1alpha1"
)

// agentRef is the per-agent identity + status the snapshot needs from the
// agent CR: name/namespace/labels are stamped onto the metrics object (so
// label selectors and display work), and gauge carries the kind's
// point-in-time status gauge read from the CR — warm sandbox count for a
// TaskAgent, running-liveness (0/1) for a DaemonAgent. Its usage-key name
// comes from the kindAdapter's gaugeKey.
type agentRef struct {
	name      string
	namespace string
	labels    map[string]string
	gauge     int32
}

// kindAdapter bridges the two near-identical snapshot kinds. They differ
// only in Go type, the agent kind they scope to, the resource/discovery
// name, the parent CR they enumerate, and which usage keys they carry
// (a long-lived DaemonAgent has no request boundary, so no latency
// percentiles; and its status gauge is `running`, not the TaskAgent's
// `warm`). One Storage serves both behind this adapter.
type kindAdapter interface {
	// agentKind is the SpanAttributes['agent.kind'] value this kind scopes
	// to ("TaskAgent" / "DaemonAgent").
	agentKind() string
	// resource is the GVR resource / discovery name
	// ("taskagentmetrics" / "daemonagentmetrics").
	resource() string
	// includeLatency reports whether the latency percentile keys are part
	// of this kind's snapshot.
	includeLatency() bool
	// gaugeKey is the usage-key name for this kind's point-in-time status
	// gauge (agentRef.gauge): "warm" for TaskAgent, "running" for
	// DaemonAgent.
	gaugeKey() string
	newObject() runtime.Object
	newList() runtime.Object
	// listAgents enumerates the parent agent CRs in scope (the namespace,
	// "" for all-namespaces) matching sel.
	listAgents(ctx context.Context, c client.Client, namespace string, sel labels.Selector) ([]agentRef, error)
	// getAgent fetches one parent agent CR; a NotFound error propagates so
	// Get can 404 on a nonexistent agent.
	getAgent(ctx context.Context, c client.Client, namespace, name string) (agentRef, error)
	// object builds one snapshot for the agent.
	object(ref agentRef, ts metav1.Time, window metav1.Duration, usage metricsv1.UsageList) runtime.Object
	// appendTo appends obj (built by object) into list (built by newList).
	appendTo(list, obj runtime.Object)
}

// taskKind serves TaskAgentMetrics.
type taskKind struct{}

func (taskKind) agentKind() string         { return clrkv1alpha1.AgentKindTask }
func (taskKind) resource() string          { return "taskagentmetrics" }
func (taskKind) includeLatency() bool      { return true }
func (taskKind) gaugeKey() string          { return metricsv1.UsageWarm }
func (taskKind) newObject() runtime.Object { return &metricsv1.TaskAgentMetrics{} }
func (taskKind) newList() runtime.Object   { return &metricsv1.TaskAgentMetricsList{} }

func (taskKind) listAgents(ctx context.Context, c client.Client, namespace string, sel labels.Selector) ([]agentRef, error) {
	var list clrkv1alpha1.TaskAgentList
	if err := c.List(ctx, &list, listOptions(namespace, sel)...); err != nil {
		return nil, err
	}
	return collectRefs(list.Items, func(ta *clrkv1alpha1.TaskAgent) agentRef {
		return agentRef{
			name:      ta.Name,
			namespace: ta.Namespace,
			labels:    ta.Labels,
			gauge:     ta.Status.WarmSandboxes,
		}
	}), nil
}

func (taskKind) getAgent(ctx context.Context, c client.Client, namespace, name string) (agentRef, error) {
	var ta clrkv1alpha1.TaskAgent
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &ta); err != nil {
		return agentRef{}, err
	}
	return agentRef{name: ta.Name, namespace: ta.Namespace, labels: ta.Labels, gauge: ta.Status.WarmSandboxes}, nil
}

func (taskKind) object(ref agentRef, ts metav1.Time, window metav1.Duration, usage metricsv1.UsageList) runtime.Object {
	return &metricsv1.TaskAgentMetrics{
		ObjectMeta: metav1.ObjectMeta{Name: ref.name, Namespace: ref.namespace, Labels: ref.labels, CreationTimestamp: ts},
		Timestamp:  ts,
		Window:     window,
		Usage:      usage,
	}
}

func (taskKind) appendTo(list, obj runtime.Object) {
	l := list.(*metricsv1.TaskAgentMetricsList)
	l.Items = append(l.Items, *obj.(*metricsv1.TaskAgentMetrics))
}

// daemonKind serves DaemonAgentMetrics. DaemonAgents are long-lived and
// not request-invoked, so a snapshot omits the latency percentiles; its
// status gauge is `running` (process liveness, 0/1), not the TaskAgent's
// `warm`.
type daemonKind struct{}

func (daemonKind) agentKind() string         { return clrkv1alpha1.AgentKindDaemon }
func (daemonKind) resource() string          { return "daemonagentmetrics" }
func (daemonKind) includeLatency() bool      { return false }
func (daemonKind) gaugeKey() string          { return metricsv1.UsageRunning }
func (daemonKind) newObject() runtime.Object { return &metricsv1.DaemonAgentMetrics{} }
func (daemonKind) newList() runtime.Object   { return &metricsv1.DaemonAgentMetricsList{} }

func (daemonKind) listAgents(ctx context.Context, c client.Client, namespace string, sel labels.Selector) ([]agentRef, error) {
	var list clrkv1alpha1.DaemonAgentList
	if err := c.List(ctx, &list, listOptions(namespace, sel)...); err != nil {
		return nil, err
	}
	return collectRefs(list.Items, func(da *clrkv1alpha1.DaemonAgent) agentRef {
		return agentRef{name: da.Name, namespace: da.Namespace, labels: da.Labels, gauge: daemonRunning(da.Status.Phase)}
	}), nil
}

func (daemonKind) getAgent(ctx context.Context, c client.Client, namespace, name string) (agentRef, error) {
	var da clrkv1alpha1.DaemonAgent
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &da); err != nil {
		return agentRef{}, err
	}
	return agentRef{name: da.Name, namespace: da.Namespace, labels: da.Labels, gauge: daemonRunning(da.Status.Phase)}, nil
}

// daemonRunning projects a DaemonAgent's lifecycle phase onto the 0/1
// `running` liveness gauge: 1 only while the single daemon process is
// Running, 0 for Stopped / CrashLoopBackOff / unset. DaemonAgents are
// single-instance by definition, so this never exceeds 1.
func daemonRunning(phase clrkv1alpha1.DaemonPhase) int32 {
	if phase == clrkv1alpha1.DaemonPhaseRunning {
		return 1
	}
	return 0
}

func (daemonKind) object(ref agentRef, ts metav1.Time, window metav1.Duration, usage metricsv1.UsageList) runtime.Object {
	return &metricsv1.DaemonAgentMetrics{
		ObjectMeta: metav1.ObjectMeta{Name: ref.name, Namespace: ref.namespace, Labels: ref.labels, CreationTimestamp: ts},
		Timestamp:  ts,
		Window:     window,
		Usage:      usage,
	}
}

func (daemonKind) appendTo(list, obj runtime.Object) {
	l := list.(*metricsv1.DaemonAgentMetricsList)
	l.Items = append(l.Items, *obj.(*metricsv1.DaemonAgentMetrics))
}

// collectRefs projects a CR list's items into []agentRef. The per-kind
// List/Get/object plumbing stays explicit (controller-runtime's typed
// client needs the concrete CR/list types, and a full generic adapter
// over them would cost more pointer-constraint machinery than the two
// short structs it replaced); only this projection loop, the one piece
// that was genuinely copy-pasted, is shared.
func collectRefs[T any](items []T, project func(*T) agentRef) []agentRef {
	refs := make([]agentRef, 0, len(items))
	for i := range items {
		refs = append(refs, project(&items[i]))
	}
	return refs
}

// listOptions builds the controller-runtime list options shared by both
// kinds: namespace scope (empty lists all namespaces) plus an optional
// label selector.
func listOptions(namespace string, sel labels.Selector) []client.ListOption {
	opts := []client.ListOption{client.InNamespace(namespace)}
	if sel != nil && !sel.Empty() {
		opts = append(opts, client.MatchingLabelsSelector{Selector: sel})
	}
	return opts
}
