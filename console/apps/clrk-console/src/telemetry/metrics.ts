// The Tier-2 metrics time-series client (APO-813). Where the Tier-1 rollup
// (telemetry/client.ts) is a flat point-in-time snapshot per agent, this reads
// the `metrics.clrk.apoxy.dev` catalog's `series` connect subresource: a
// query-time aggregation over otel_traces/otel_logs returning a labeled
// MetricSeriesSet (one line per group x measure/quantile, each with one point
// per time bucket on a range query). It backs the Overview's KPI shapes and the
// traffic chart.
//
// The fleet surface (`metrics/{id}/series`) is namespace-scoped: its window is
// pinned to one `agent.namespace`, so a cluster-wide read isn't possible through
// it (the console scopes the Overview's time-shaped reads to the namespace its
// agents live in). Like fetchAgentTraces, it's a decorated raw GET because the
// connecter returns a typed object, not a k8s List — the shared RequestDecorator
// still applies project scoping + auth headers.

import type { ConsoleClient } from "@apoxy/console-core";

const METRICS_GROUP = "metrics.clrk.apoxy.dev";
const VERSION = "v1alpha1";

/** One sample. `timestamp` is the bucket start (range) or window end (scalar);
 *  `value` is a resource.Quantity decimal STRING (exact past 2^53). */
export interface MetricPoint {
  timestamp: string;
  value: string;
}

/** One labeled line: the group value (under the groupBy key) plus a `measure`
 *  or `quantile` label when the metric carries several. Empty for an ungrouped
 *  single-measure query. */
export interface MetricSeries {
  labels?: Record<string, string>;
  points: MetricPoint[];
}

/** The result of a metric series query (the MetricSeriesSet kind on the wire). */
export interface MetricSeriesSet {
  metric: string;
  type: string;
  unit: string;
  /** Resolved half-open window bounds (RFC3339). */
  since: string;
  until: string;
  /** Bucket width (a Go duration like "1h0m0s") on a range query; absent on scalar. */
  step?: string;
  groupBy?: string;
  /** Set when the distinct-group count exceeded the cap and only the top were kept. */
  truncated?: boolean;
  series: MetricSeries[];
}

export interface MetricSeriesQuery {
  /** Catalog metric id, e.g. `agent.invocations`, `gen_ai.tokens`. */
  metric: string;
  /** The namespace the fleet surface pins `agent.namespace` to. */
  namespace: string;
  /** RFC3339 half-open window. */
  since: string;
  until: string;
  /** A Go duration (`1h`, `6h`, `24h`) selecting a bucketed range query; omit
   *  for a single-point scalar query. */
  step?: string;
  /** Optional grouping dimension (a chart legend); omit for a single line. */
  groupBy?: string;
  /** Optional scope refinement (TaskAgent | DaemonAgent | EgressGateway + name). */
  scopeKind?: string;
  scopeName?: string;
}

/** Parse a MetricPoint's value (a decimal string) to a number; 0 when absent or
 *  unparseable. Use only where the value is known to fit a float (counts/tokens
 *  on the Overview are well within range). */
export function pointValue(p: MetricPoint | undefined): number {
  if (p == null) return 0;
  const n = Number(p.value);
  return Number.isFinite(n) ? n : 0;
}

/**
 * GET a metric's `series` subresource as a typed MetricSeriesSet. The fleet path
 * the apiserver mounts is namespaced; scoping/auth ride the shared
 * RequestDecorator, the same one the GVRClient uses for every k8s call.
 */
export async function fetchMetricSeries(
  client: ConsoleClient,
  q: MetricSeriesQuery,
): Promise<MetricSeriesSet> {
  const params = new URLSearchParams();
  params.set("since", q.since);
  params.set("until", q.until);
  if (q.step) params.set("step", q.step);
  if (q.groupBy) params.set("groupBy", q.groupBy);
  if (q.scopeKind) params.set("scopeKind", q.scopeKind);
  if (q.scopeName) params.set("scopeName", q.scopeName);

  const path =
    `/apis/${METRICS_GROUP}/${VERSION}/namespaces/${q.namespace}` +
    `/metrics/${encodeURIComponent(q.metric)}/series?${params.toString()}`;

  const { url, headers } = client.gvr.decorator.decorate({
    path,
    method: "GET",
    headers: new Headers({ Accept: "application/json" }),
  });
  const res = await fetch(url, { method: "GET", headers });
  if (!res.ok) {
    throw new Error(
      `metric series request failed: ${res.status} ${res.statusText}`,
    );
  }
  return (await res.json()) as MetricSeriesSet;
}
