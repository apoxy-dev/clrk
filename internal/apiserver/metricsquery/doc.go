// Package metricsquery serves the Tier-2 time-series surface of the clrk metrics
// API: a metric catalog plus a series query, for the charts the Tier-1
// metrics-server-shaped snapshots (internal/apiserver/agentmetrics) structurally
// cannot express -- labeled series, histogram percentiles, and free-dimension
// group-by. Everything is a typed kind in the metrics.clrk.apoxy.dev group
// (api/metrics/v1alpha1), so the responses round-trip the apiserver codec with
// OpenAPI and content negotiation, exactly like Tier-1.
//
// Like Tier-1 and the telemetry read API, every value is a query-time
// aggregation over the otel_traces / otel_logs ClickHouse tables the agent's
// spans already populate (no separate metrics store, no MeterProvider). Each
// catalog entry is a named recipe -- count / sum / quantile, optionally grouped
// by a real emitted attribute and bucketed by toStartOfInterval -- over those
// spans. See apoxy-cloud/docs/clrk-metrics-api.md.
//
// Resources (all metrics.clrk.apoxy.dev/v1alpha1):
//
//   - metrics (Lister/Getter/TableConvertor) -- the catalog. The LIST is the set
//     of queryable Metric descriptors (type, unit, source, group-by dims); a GET
//     returns one. `kubectl get metrics` prints it.
//   - metrics/{id}/series (Connecter -> MetricSeriesSet) -- the fleet query,
//     scoped to the namespace with an optional scopeKind / scopeName refinement.
//   - taskagentmetrics/{name}/series, daemonagentmetrics/{name}/series
//     (Connecter -> MetricSeriesSet) -- the per-agent query, scope fixed and
//     server-enforced by the path agent (the metric id is ?metric=).
//
// The series connecter follows the pods/log pattern -- a GET connect subresource
// that returns a typed MetricSeriesSet via responder.Object (content-negotiated)
// -- with one deviation: NewConnectOptions returns nil and the handler parses the
// URL query directly, because the apoxy apiserver builder installs connect
// handlers with the metav1-only ParameterCodec, so a registered custom options
// object cannot decode (it 400s "no kind is registered"). The telemetry read
// connecter parses its query the same way. The URL parameters (metric, groupBy,
// since, until, step, quantiles, scopeKind, scopeName) are unchanged. An empty
// step selects a scalar query (one point per series, for cards); a set step
// selects a bucketed range query (for charts); a histogram returns one series per
// (group, quantile). Values are resource.Quantity, the same exact decimal wire
// type the Tier-1 UsageList uses.
//
// ch-go has no parameter binding for the chpool.Pool.Do raw-SQL access pattern,
// so the SELECTs are built by hand and every request-derived value is escaped
// via internal/apiserver/chsql.
package metricsquery
