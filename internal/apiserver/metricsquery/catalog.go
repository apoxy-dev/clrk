package metricsquery

import (
	"fmt"
	"slices"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	metricsv1 "github.com/apoxy-dev/clrk/api/metrics/v1alpha1"
	"github.com/apoxy-dev/clrk/internal/chwriter"
)

// metricType is the catalog "type" of a metric, which tells the UI how to
// render it and which query options apply.
type metricType string

const (
	// typeCounter is a monotonic count/sum aggregate (count(), sum(...)).
	typeCounter metricType = "counter"
	// typeGauge is a point-in-time value (unused by the v1 catalog, reserved
	// so the type vocabulary matches the design doc).
	typeGauge metricType = "gauge"
	// typeHistogram is a duration distribution queried as quantile series; its
	// measures are the requested ?quantiles=, not a fixed set.
	typeHistogram metricType = "histogram"
)

// sourceTable selects the backing ClickHouse table and the per-record
// attribute-map column scope clauses index into.
type sourceTable struct {
	name    string // chwriter table name
	attrCol string // the attribute Map column (SpanAttributes / LogAttributes)
	label   string // catalog "source" value
}

var (
	tracesSource = sourceTable{name: chwriter.TracesTable, attrCol: "SpanAttributes", label: "traces"}
	logsSource   = sourceTable{name: chwriter.LogsTable, attrCol: "LogAttributes", label: "logs"}
)

// measure is one named sub-value of a counter/gauge metric. A metric with a
// single unnamed measure (name == "") yields one series per group with no
// extra label; a multi-measure metric (gen_ai.tokens, egress.bytes) yields one
// series per (group x measure), the measure name carried as a "measure" label.
// expr is the ClickHouse aggregate over the scoped rows.
type measure struct {
	name string
	expr string
}

// metricDef is one catalog entry: a named aggregation recipe over otel_traces
// or otel_logs. The dimensions are the REAL attribute keys clrk emits today
// (see apoxy-cloud/docs/clrk-egress-telemetry.md), resolved to SQL via dimExpr.
type metricDef struct {
	id          string
	typ         metricType
	unit        string
	source      sourceTable
	filter      string    // extra WHERE predicate selecting the metric's rows, or ""
	measures    []measure // counter/gauge values; empty for histograms
	histoExpr   string    // histogram: the value column to quantile (e.g. "Duration / 1e6")
	dims        []string  // allowed groupBy dimension keys (must exist in dimExpr)
	defaultDim  string
	legend      string
	description string
}

// rankExpr is the aggregate used to pick the top-N groups when a high-
// cardinality groupBy is capped: histograms rank by call volume, everything
// else by its first (and usually only) measure.
func (m metricDef) rankExpr() string {
	if m.typ == typeHistogram {
		return "count()"
	}
	return m.measures[0].expr
}

// dimExpr resolves a group-by dimension key to its ClickHouse expression. The
// trace dimensions read the SpanAttributes map (or the materialized Agent /
// Component columns, which are cheaper than a map probe); the log dimensions
// read the materialized SeverityText / Component columns. A metric only lists
// dimensions valid for its own source, so the SpanAttributes references here
// are never evaluated against otel_logs.
var dimExpr = map[string]string{
	"gen_ai.request.model":              "SpanAttributes['gen_ai.request.model']",
	"gen_ai.system":                     "SpanAttributes['gen_ai.system']",
	"gen_ai.operation.name":             "SpanAttributes['gen_ai.operation.name']",
	"mcp.method":                        "SpanAttributes['mcp.method']",
	"mcp.tool.name":                     "SpanAttributes['mcp.tool.name']",
	"clrk.mcproute.name":                "SpanAttributes['clrk.mcproute.name']",
	"clrk.mcproute.toolpolicy.decision": "SpanAttributes['clrk.mcproute.toolpolicy.decision']",
	"clrk.aiproviderroute.name":         "SpanAttributes['clrk.aiproviderroute.name']",
	"http.response.status_code":         "SpanAttributes['http.response.status_code']",
	// Status class folds 2xx/4xx/5xx for the egress request-rate-by-class
	// chart; the status code is stored as a stringified int, so the first
	// digit + "xx" yields the class without parsing. egress.requests gates only
	// on Component, which also matches L4 / connect-failure spans that carry no
	// status code, so map the empty code to an explicit 'none' class rather than
	// the meaningless 'xx' an empty substring would produce.
	"http.response.status_class": "if(empty(SpanAttributes['http.response.status_code']), 'none', concat(substring(SpanAttributes['http.response.status_code'], 1, 1), 'xx'))",
	"agent.name":                 "Agent",
	"agent.kind":                 "SpanAttributes['agent.kind']",
	"severity":                   "SeverityText",
	"component":                  "Component",
}

// catalog is the v1 metric registry: the named recipes Tier-2 serves. Each is
// a query-time aggregation over spans/logs clrk already writes (no Meter
// provider, no second copy of the dimensions). Tier-1's snapshot usage keys
// are the same recipes materialized as scalars, so the two tiers never drift.
var catalog = []metricDef{
	{
		id: "gen_ai.tokens", typ: typeCounter, unit: "tokens", source: tracesSource,
		// Only AI-provider spans carry gen_ai.*; gate on the system tag so a
		// non-AI egress call doesn't dilute the per-model breakdown.
		filter: "mapContains(SpanAttributes, 'gen_ai.system')",
		measures: []measure{
			{name: "input", expr: "sum(toUInt64OrZero(SpanAttributes['gen_ai.usage.input_tokens']))"},
			{name: "output", expr: "sum(toUInt64OrZero(SpanAttributes['gen_ai.usage.output_tokens']))"},
		},
		dims:       []string{"gen_ai.request.model", "gen_ai.system", "agent.name"},
		defaultDim: "gen_ai.request.model", legend: "model",
		description: "GenAI token usage (input/output), summed from gen_ai.usage.*.",
	},
	{
		id: "gen_ai.duration", typ: typeHistogram, unit: "ms", source: tracesSource,
		filter:     "mapContains(SpanAttributes, 'gen_ai.request.model')",
		histoExpr:  "Duration / 1e6",
		dims:       []string{"gen_ai.request.model", "gen_ai.operation.name"},
		defaultDim: "gen_ai.request.model", legend: "model",
		description: "GenAI call wall-clock duration (span Duration), as quantile series.",
	},
	{
		id: "mcp.calls", typ: typeCounter, unit: "calls", source: tracesSource,
		filter:     "mapContains(SpanAttributes, 'mcp.method')",
		measures:   []measure{{name: "", expr: "count()"}},
		dims:       []string{"mcp.tool.name", "clrk.mcproute.name", "clrk.mcproute.toolpolicy.decision", "mcp.method"},
		defaultDim: "mcp.tool.name", legend: "tool",
		description: "MCP JSON-RPC calls (spans carrying mcp.method).",
	},
	{
		id: "mcp.duration", typ: typeHistogram, unit: "ms", source: tracesSource,
		filter:     "mapContains(SpanAttributes, 'mcp.method')",
		histoExpr:  "Duration / 1e6",
		dims:       []string{"clrk.mcproute.name", "mcp.tool.name"},
		defaultDim: "clrk.mcproute.name", legend: "route",
		description: "MCP call duration (span Duration), as quantile series.",
	},
	{
		id: "egress.requests", typ: typeCounter, unit: "requests", source: tracesSource,
		// One row per egress HTTP transaction (the egress ext_proc span);
		// exclude ingress.dispatch / L4 spans that share the trace.
		filter:     "Component = 'egress-extproc'",
		measures:   []measure{{name: "", expr: "count()"}},
		dims:       []string{"http.response.status_class", "http.response.status_code", "clrk.aiproviderroute.name", "clrk.mcproute.name", "agent.name"},
		defaultDim: "http.response.status_class", legend: "status",
		description: "Egress HTTP transactions through the MITM proxy.",
	},
	{
		id: "egress.bytes", typ: typeCounter, unit: "bytes", source: tracesSource,
		filter: "Component = 'egress-extproc'",
		measures: []measure{
			{name: "request", expr: "sum(toUInt64OrZero(SpanAttributes['clrk.req.bytes']))"},
			{name: "response", expr: "sum(toUInt64OrZero(SpanAttributes['clrk.resp.bytes']))"},
		},
		dims:       []string{"agent.name", "clrk.aiproviderroute.name", "clrk.mcproute.name"},
		defaultDim: "agent.name", legend: "agent",
		description: "Egress request/response bytes captured (post-truncation).",
	},
	{
		id: "agent.invocations", typ: typeCounter, unit: "invocations", source: tracesSource,
		// Count distinct invocations, not spans: a single invocation fans out
		// into many egress spans sharing its invocation.id.
		filter:     "InvocationId != ''",
		measures:   []measure{{name: "", expr: "uniqExact(InvocationId)"}},
		dims:       []string{"agent.name", "agent.kind"},
		defaultDim: "agent.name", legend: "agent",
		description: "Distinct invocations observed (uniq invocation.id).",
	},
	{
		id: "agent.errors", typ: typeCounter, unit: "errors", source: tracesSource,
		// Healthy egress spans stay Unset (not Ok), so error == StatusCode
		// Error, never "!= Ok" (which would count every success).
		filter:     "StatusCode = 'Error'",
		measures:   []measure{{name: "", expr: "count()"}},
		dims:       []string{"agent.name", "agent.kind"},
		defaultDim: "agent.name", legend: "agent",
		description: "Error-status spans (StatusCode = Error).",
	},
	{
		id: "budget.denied", typ: typeCounter, unit: "denials", source: tracesSource,
		filter:     "SpanAttributes['clrk.budget.denied'] = 'true'",
		measures:   []measure{{name: "", expr: "count()"}},
		dims:       []string{"clrk.aiproviderroute.name"},
		defaultDim: "clrk.aiproviderroute.name", legend: "route",
		description: "TokenBudget 429 denials (clrk.budget.denied).",
	},
	{
		id: "log.severity", typ: typeCounter, unit: "records", source: logsSource,
		measures:   []measure{{name: "", expr: "count()"}},
		dims:       []string{"severity", "component"},
		defaultDim: "severity", legend: "severity",
		description: "Log records by severity (otel_logs).",
	},
}

// catalogByID indexes the registry for metric lookup.
var catalogByID = func() map[string]metricDef {
	m := make(map[string]metricDef, len(catalog))
	for _, def := range catalog {
		m[def.id] = def
	}
	return m
}()

// catalogMetrics is the catalog projected to typed Metric objects once at init
// (the registry is immutable), in registry order; catalogByName indexes them
// for the per-id Get. These back the metrics resource's List and Get.
var (
	catalogMetrics = func() []metricsv1.Metric {
		out := make([]metricsv1.Metric, 0, len(catalog))
		for _, def := range catalog {
			out = append(out, buildMetric(def))
		}
		return out
	}()
	catalogByName = func() map[string]metricsv1.Metric {
		m := make(map[string]metricsv1.Metric, len(catalogMetrics))
		for _, cm := range catalogMetrics {
			m[cm.Name] = cm
		}
		return m
	}()
)

// apiMetricType maps the internal metricType to the registered API enum.
func apiMetricType(t metricType) metricsv1.MetricType {
	switch t {
	case typeHistogram:
		return metricsv1.MetricTypeHistogram
	case typeGauge:
		return metricsv1.MetricTypeGauge
	default:
		return metricsv1.MetricTypeCounter
	}
}

// buildMetric projects one registry entry into its typed catalog object.
// Histograms advertise no fixed measures (the measures are the requested
// quantiles); single-unnamed-measure counters advertise none either.
func buildMetric(def metricDef) metricsv1.Metric {
	m := metricsv1.Metric{
		ObjectMeta:     metav1.ObjectMeta{Name: def.id},
		Type:           apiMetricType(def.typ),
		Unit:           def.unit,
		Source:         def.source.label,
		GroupBy:        def.dims,
		DefaultGroupBy: def.defaultDim,
		Legend:         def.legend,
		Description:    def.description,
	}
	for _, ms := range def.measures {
		if ms.name != "" {
			m.Measures = append(m.Measures, ms.name)
		}
	}
	return m
}

// init enforces the catalog's structural invariants the query path relies on,
// turning a malformed registry entry into a deterministic startup panic rather
// than a per-request panic or a silently wrong query. Specifically: rankExpr
// and the counter measure-loop index measures[0], so a non-histogram needs at
// least one measure; a histogram needs a histoExpr to quantile; every declared
// dimension (and the default) must resolve through dimExpr.
func init() {
	seen := make(map[string]bool, len(catalog))
	for _, def := range catalog {
		if def.id == "" {
			panic("metricsquery: catalog entry with empty id")
		}
		if seen[def.id] {
			panic(fmt.Sprintf("metricsquery: duplicate catalog id %q", def.id))
		}
		seen[def.id] = true
		if def.typ == typeHistogram {
			if def.histoExpr == "" {
				panic(fmt.Sprintf("metricsquery: histogram %q missing histoExpr", def.id))
			}
		} else if len(def.measures) == 0 {
			panic(fmt.Sprintf("metricsquery: %s %q has no measures", def.typ, def.id))
		}
		if len(def.dims) == 0 {
			panic(fmt.Sprintf("metricsquery: %q declares no group-by dimensions", def.id))
		}
		for _, d := range def.dims {
			expr, ok := dimExpr[d]
			if !ok {
				panic(fmt.Sprintf("metricsquery: %q dimension %q not in dimExpr", def.id, d))
			}
			// A dim's SQL must reference only columns present on the metric's
			// source table, or the query 500s at request time ("Unknown
			// identifier") rather than at startup. SpanAttributes is traces-only;
			// SeverityText is logs-only (Agent / Component / EGRef are on both).
			if def.source.name == logsSource.name && strings.Contains(expr, "SpanAttributes") {
				panic(fmt.Sprintf("metricsquery: %q (logs source) dimension %q references SpanAttributes", def.id, d))
			}
			if def.source.name == tracesSource.name && strings.Contains(expr, "SeverityText") {
				panic(fmt.Sprintf("metricsquery: %q (traces source) dimension %q references SeverityText", def.id, d))
			}
		}
		if def.defaultDim == "" || !slices.Contains(def.dims, def.defaultDim) {
			panic(fmt.Sprintf("metricsquery: %q defaultDim %q not among its dims", def.id, def.defaultDim))
		}
	}
}

// lookupMetric returns the metric definition for id, or false if unknown.
func lookupMetric(id string) (metricDef, bool) {
	def, ok := catalogByID[id]
	return def, ok
}

// allowsDim reports whether dim is a valid group-by for this metric.
func (m metricDef) allowsDim(dim string) bool {
	return slices.Contains(m.dims, dim)
}
