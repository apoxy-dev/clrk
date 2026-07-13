/*
Copyright 2026 Apoxy, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// UsageList is a flat map of named rollup values, faithful to the
// metrics.k8s.io PodMetrics.Containers[].Usage (ResourceList) shape:
// every value is a resource.Quantity so counts, byte totals, token
// totals, and latency-as-milliseconds all share one wire type.
// Dimensional breakdowns (tokens-by-model, requests-by-status) do NOT
// live here — those are the Tier-2 range-query surface; a flat scalar
// map is deliberately all this tier carries.
type UsageList map[string]resource.Quantity

// Usage keys emitted on agent snapshots. These are the Tier-1
// materialization of the same recipes the Tier-2 catalog names, so the
// two tiers never drift. Latency keys are present only on TaskAgent
// snapshots (a long-lived DaemonAgent has no request boundary, so a
// p50/p99 over its spans is meaningless).
const (
	// UsageInvocations is the count of distinct invocations observed in
	// the window (uniq InvocationId).
	UsageInvocations = "invocations"
	// UsageErrors is the count of spans whose status is Error in the
	// window.
	UsageErrors = "errors"
	// UsageWarm is a TaskAgent's current pre-warmed sandbox count: sandboxes
	// Created and held Ready (rootfs mounted, TAP+netns provisioned, process
	// not yet started) across its workers, ready to absorb a request without
	// cold-start. Unlike the other keys it is a point-in-time gauge read from
	// the agent's CR status (TaskAgent.Status.WarmSandboxes), not a window
	// aggregate over ClickHouse. TaskAgent only.
	UsageWarm = "warm"
	// UsageRunning is a DaemonAgent's current liveness gauge: 1 when its
	// single long-lived process is Running, 0 otherwise (Stopped /
	// CrashLoopBackOff). Like UsageWarm it is a point-in-time read from the
	// agent's CR status (DaemonAgent.Status.Phase), not a window aggregate.
	// DaemonAgent only.
	UsageRunning = "running"
	// UsageInputTokens / UsageOutputTokens are the summed GenAI token
	// counts (gen_ai.usage.input_tokens / output_tokens) over the window.
	UsageInputTokens  = "input_tokens"
	UsageOutputTokens = "output_tokens"
	// UsageToolCalls is the count of MCP tool-call spans (spans carrying an
	// mcp.method attribute) in the window.
	UsageToolCalls = "tool_calls"
	// UsageLatencyP50Ms / UsageLatencyP99Ms are percentiles of the
	// per-invocation end-to-end latency, in milliseconds: each
	// invocation's trace wall-clock (first span start to last span end
	// across everything sharing its invocation.id), NOT the duration of
	// the short ingress.dispatch routing span. TaskAgent only.
	UsageLatencyP50Ms = "latency_p50_ms"
	UsageLatencyP99Ms = "latency_p99_ms"
)

// Usage keys emitted on EgressGateway snapshots. A gateway snapshot
// counts proxied L7 HTTP exchanges (egress ext_proc records carrying a
// response status), not agent invocations, so it has its own key set.
const (
	// UsageRequests is the count of L7 HTTP requests the gateway proxied
	// in the window.
	UsageRequests = "requests"
	// UsageStatus2xx / UsageStatus4xx / UsageStatus5xx bucket those
	// requests by response status class. The three buckets need not sum
	// to `requests`: 1xx/3xx responses and exchanges that never produced
	// a response status fall outside all of them.
	UsageStatus2xx = "status_2xx"
	UsageStatus4xx = "status_4xx"
	UsageStatus5xx = "status_5xx"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TaskAgentMetrics is a point-in-time rollup of one TaskAgent's activity
// over Window, derived by aggregating the agent's otel_traces spans. Its
// name and namespace match the TaskAgent it summarizes, so a list of
// these is the agents page in a single call.
type TaskAgentMetrics struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Timestamp is when the rollup was computed (the query time).
	Timestamp metav1.Time `json:"timestamp"`
	// Window is the look-back the usage values cover, e.g. 24h. Making the
	// window self-describing on every object resolves the telemetry read
	// model's standing ambiguity, where an unset since/until means "the
	// latest N records" rather than a stated span.
	Window metav1.Duration `json:"window"`
	// Usage is the scalar rollup. See the Usage* key constants.
	Usage UsageList `json:"usage"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TaskAgentMetricsList is a list of TaskAgentMetrics.
type TaskAgentMetricsList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []TaskAgentMetrics `json:"items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DaemonAgentMetrics is a point-in-time rollup of one DaemonAgent's
// activity over Window. Unlike TaskAgentMetrics it omits the latency
// percentiles (a long-lived daemon has no request boundary to measure),
// leaning on token / tool-call / error totals instead.
type DaemonAgentMetrics struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Timestamp metav1.Time     `json:"timestamp"`
	Window    metav1.Duration `json:"window"`
	Usage     UsageList       `json:"usage"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DaemonAgentMetricsList is a list of DaemonAgentMetrics.
type DaemonAgentMetricsList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []DaemonAgentMetrics `json:"items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// EgressGatewayMetrics is a point-in-time rollup of one EgressGateway's
// proxied traffic over Window, derived by aggregating the gateway's
// egress ext_proc spans (EGRef-scoped otel_traces rows). Its name and
// namespace match the EgressGateway it summarizes. Usage carries the
// gateway-wide totals; Listeners nests a usage per listener and per
// attached route, the way PodMetrics carries a usage per container.
type EgressGatewayMetrics struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Timestamp metav1.Time     `json:"timestamp"`
	Window    metav1.Duration `json:"window"`
	// Usage is the gateway-wide rollup, including traffic that matched no
	// attached route (default-policy traffic), which no listener entry
	// can carry.
	Usage UsageList `json:"usage"`
	// Listeners breaks the rollup down by the gateway's declared
	// listeners and the routes attached to each.
	Listeners []EgressListenerMetrics `json:"listeners,omitempty"`
}

// EgressListenerMetrics is the rollup for one declared listener: the sum
// of its attached routes' usage. Egress spans carry route identity, not
// listener identity, so a route attached to several listeners (a
// parentRef with no sectionName) contributes its full usage to each --
// per-listener rows may overlap; the top-level Usage never double-counts.
type EgressListenerMetrics struct {
	// Name is the listener name from the EgressGateway spec.
	Name string `json:"name"`
	// Usage is the sum over Routes.
	Usage UsageList `json:"usage"`
	// Routes is the per-attached-route breakdown.
	Routes []EgressRouteMetrics `json:"routes,omitempty"`
}

// EgressRouteMetrics is the rollup for one route attached to a listener.
type EgressRouteMetrics struct {
	// Kind is the route CRD kind ("AIProviderRoute" / "MCPRoute").
	Kind string `json:"kind"`
	// Name is the route's object name. Its namespace is not carried: a
	// route attaches from its own namespace and the listener entry is
	// already scoped to one gateway.
	Name string `json:"name"`
	// Usage is the route's windowed rollup.
	Usage UsageList `json:"usage"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// EgressGatewayMetricsList is a list of EgressGatewayMetrics.
type EgressGatewayMetricsList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []EgressGatewayMetrics `json:"items"`
}

// MetricType is the catalog type of a metric, telling the console how to render
// it and which query options apply.
type MetricType string

const (
	// MetricTypeCounter is a monotonic count/sum aggregate.
	MetricTypeCounter MetricType = "Counter"
	// MetricTypeGauge is a point-in-time value (reserved; the v1 catalog has none).
	MetricTypeGauge MetricType = "Gauge"
	// MetricTypeHistogram is a duration distribution queried as quantile series;
	// its measures are the requested ?quantiles, not a fixed set.
	MetricTypeHistogram MetricType = "Histogram"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Metric is one entry of the Tier-2 catalog: a named aggregation recipe over
// the otel_traces / otel_logs spans, with the dimensions it may be grouped by.
// Its Name is the stable metric id (e.g. "gen_ai.tokens") used as the path
// element of the series subresource. The catalog is the LIST of this resource,
// so the console renders its metric menus, units, and legends from a typed
// object instead of hardcoded JS, and `kubectl get metrics` prints it.
type Metric struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Type is Counter, Gauge, or Histogram. A histogram is queried with
	// ?quantiles and returns one series per (group x quantile).
	Type MetricType `json:"type"`
	// Unit is the value unit ("tokens", "requests", "bytes", "ms", ...).
	Unit string `json:"unit"`
	// Source is the backing table the recipe scans: "traces" or "logs".
	Source string `json:"source"`
	// GroupBy is the set of dimension keys this metric may be grouped by (the
	// chart legends). One is passed as ?groupBy on the series subresource.
	GroupBy []string `json:"groupBy,omitempty"`
	// DefaultGroupBy is the dimension the console selects by default.
	DefaultGroupBy string `json:"defaultGroupBy,omitempty"`
	// Legend is a short human label for the default dimension.
	Legend string `json:"legend,omitempty"`
	// Measures are the named sub-values a single point carries when a metric
	// reports more than one (e.g. gen_ai.tokens -> input/output). Each becomes a
	// "measure" label on its series. Empty for single-valued metrics.
	Measures []string `json:"measures,omitempty"`
	// Description is a one-line human summary.
	Description string `json:"description,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MetricList is the catalog: the full set of queryable metrics.
type MetricList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Metric `json:"items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MetricSeriesSet is the result of a metric series query: one labeled series per
// (group value x measure/quantile), each carrying one point (scalar) or a point
// per time bucket (range). It is returned through content negotiation by the
// `series` connect subresource, so a client gets a typed object in json, yaml,
// or protobuf per its Accept header. Its Name echoes the metric id.
type MetricSeriesSet struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Metric echoes the queried metric id.
	Metric string `json:"metric"`
	// Type echoes the metric type.
	Type MetricType `json:"type"`
	// Unit echoes the metric unit.
	Unit string `json:"unit"`
	// Since / Until are the resolved half-open [since, until) window bounds.
	Since metav1.Time `json:"since"`
	Until metav1.Time `json:"until"`
	// Step is the bucket width on a range query; nil on a scalar query.
	Step *metav1.Duration `json:"step,omitempty"`
	// GroupBy echoes the grouping dimension, when one was requested.
	GroupBy string `json:"groupBy,omitempty"`
	// Truncated is set when the distinct group count exceeded the per-query
	// series cap and only the top groups (by total value) were returned.
	Truncated bool `json:"truncated,omitempty"`
	// Series is the labeled lines. A query with no groupBy and a single measure
	// returns exactly one series with empty labels.
	Series []MetricSeries `json:"series"`
}

// MetricSeries is one labeled line/area: the group value (under the groupBy key)
// plus a "measure" or "quantile" label when the metric carries several.
type MetricSeries struct {
	Labels map[string]string `json:"labels,omitempty"`
	Points []MetricPoint     `json:"points"`
}

// MetricPoint is one sample. Timestamp is the bucket start on a range query and
// the window end on a scalar query. Value is a resource.Quantity (the same exact
// decimal wire type the Tier-1 UsageList uses), so an integer counter total
// stays exact regardless of the client's JSON number precision.
type MetricPoint struct {
	Timestamp metav1.Time       `json:"timestamp"`
	Value     resource.Quantity `json:"value"`
}
