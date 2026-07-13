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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Usage keys emitted on WorkerPool snapshots. A pool snapshot mixes two
// kinds of value: point-in-time gauges read from the WorkerPool CR
// (replica counts and execution-slot capacity, so the metrics agree with
// `kubectl get workerpool`) and window aggregates over the pool's
// ingress.dispatch spans (the per-request routing span the ingress
// ext_proc emits, which is the only span carrying clrk.worker.pool). The
// snapshot reuses UsageInvocations / UsageErrors for the dispatch
// counters; the keys below are the ones unique to the pool.
const (
	// UsageReadyReplicas is the number of worker pods currently Ready
	// (WorkerPool.Status.ReadyReplicas). Gauge, not a window aggregate.
	UsageReadyReplicas = "ready_replicas"
	// UsageDesiredReplicas is the pool's desired worker count
	// (WorkerPool.Spec.Replicas, default 1). Gauge.
	UsageDesiredReplicas = "desired_replicas"
	// UsageActive is the count of in-flight (non-terminal) executions
	// across the pool (WorkerPool.Status.ActiveExecutions). Gauge.
	UsageActive = "active"
	// UsageMaxExecutions is the pool's total execution slots across ready
	// workers (WorkerPool.Status.Capacity.MaxExecutions). Gauge.
	UsageMaxExecutions = "max_executions"
	// UsageAvailableExecutions is the pool's free execution slots
	// (WorkerPool.Status.Capacity.AvailableExecutions). Gauge.
	UsageAvailableExecutions = "available_executions"
	// UsageDispatchP50Ms / UsageDispatchP99Ms are percentiles of the
	// ingress.dispatch span wall time (worker selection plus request
	// ingestion), in milliseconds. This is the pool's dispatch overhead,
	// deliberately NOT the agent end-to-end latency the TaskAgent snapshot
	// reports: the dispatch span EOFs once the ext_proc has picked a
	// worker and rewritten :authority, while the agent run and the
	// streamed response flow back through Envoy and never re-enter it.
	UsageDispatchP50Ms = "dispatch_p50_ms"
	UsageDispatchP99Ms = "dispatch_p99_ms"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// WorkerPoolMetrics is a point-in-time rollup of one WorkerPool over
// Window, backing the console pools list. Its name and namespace match
// the WorkerPool it summarizes. Usage carries both CR-status gauges
// (replica counts, execution-slot capacity, in-flight executions) and
// window aggregates over the pool's ingress.dispatch spans (invocations
// dispatched, dispatch errors, dispatch-latency percentiles).
//
// The dispatch counters count only requests that resolved a ready
// revision and reached worker selection, because clrk.worker.pool is
// stamped on the dispatch span only past that point: a request rejected
// earlier (unknown TaskAgent, no ready revision, malformed) carries no
// pool and belongs to no pool snapshot. Within that set, UsageInvocations
// is the successfully-dispatched count and UsageErrors is the
// pool-attributable failures (at cluster MaxConcurrent, no ready worker).
type WorkerPoolMetrics struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Timestamp is when the rollup was computed (the query time).
	Timestamp metav1.Time `json:"timestamp"`
	// Window is the look-back the aggregate usage values cover, e.g. 24h.
	// The gauge keys are point-in-time reads and do not depend on it.
	Window metav1.Duration `json:"window"`
	// Usage is the scalar rollup. See the Usage* key constants.
	Usage UsageList `json:"usage"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// WorkerPoolMetricsList is a list of WorkerPoolMetrics.
type WorkerPoolMetricsList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []WorkerPoolMetrics `json:"items"`
}
