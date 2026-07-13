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
