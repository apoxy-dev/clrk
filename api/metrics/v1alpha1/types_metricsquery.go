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
