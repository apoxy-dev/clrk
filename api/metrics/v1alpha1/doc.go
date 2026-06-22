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

// +k8s:openapi-gen=true
// +kubebuilder:object:generate=true
// +groupName=metrics.clrk.apoxy.dev
// +k8s:deepcopy-gen=package,register

// Package v1alpha1 contains the API schema for the metrics.clrk.apoxy.dev
// v1alpha1 group: the Tier-1 "snapshot" surface of the clrk metrics API.
//
// Modeled on metrics.k8s.io (PodMetrics / NodeMetrics), each kind is a
// listable, point-in-time rollup — {timestamp, window, usage} — of an
// agent's activity over a stated window, derived at read time by
// aggregating the otel_traces table the agent's spans already populate.
// There is no separate metrics store: the usage values are named
// aggregation recipes over those spans. See
// apoxy-cloud/docs/clrk-metrics-api.md.
package v1alpha1
