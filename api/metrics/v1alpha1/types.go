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
	// UsageActive is the agent's current in-flight execution count. Unlike
	// the other keys it is a point-in-time gauge read from the agent's CR
	// status (ActiveExecutions), not a window aggregate over ClickHouse.
	UsageActive = "active"
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
