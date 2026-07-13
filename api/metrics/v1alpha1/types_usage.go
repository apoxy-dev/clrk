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
)

// UsageList is a flat map of named rollup values, faithful to the
// metrics.k8s.io PodMetrics.Containers[].Usage (ResourceList) shape:
// every value is a resource.Quantity so counts, byte totals, token
// totals, and latency-as-milliseconds all share one wire type.
// Dimensional breakdowns (tokens-by-model, requests-by-status) do NOT
// live here — those are the Tier-2 range-query surface; a flat scalar
// map is deliberately all this tier carries.
//
// The key vocabulary is per-kind: the agent snapshots' keys live in
// types_agentmetrics.go, the gateway snapshot's in
// types_egressgatewaymetrics.go.
type UsageList map[string]resource.Quantity
