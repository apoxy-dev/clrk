// Overview view-model: the pure transforms that turn the live inputs (agent
// rows, gateway rows, worker-pool tallies, and the Tier-2 metric series) into
// the numbers, sparkline arrays, deltas, traffic series, and health banner the
// landing page renders. Kept side-effect free and unit-tested (overview-data.
// test.ts); the route container does the I/O, the view does the SVG.
//
// Two scopes meet here, deliberately (mirroring the design's own split of a
// printed total from a sparkline shape): the KPI numbers + shapes come from the
// range-scoped Tier-2 series (so the range selector drives them and the chart),
// while "active now", the agent/gateway tables, and the health banner come from
// the cluster-wide CR lists + Tier-1 rollup the route already loads. The Tier-2
// fleet series is namespace-scoped, so its window is pinned to the namespace the
// agents live in (mostCommonNamespace) — a known single-namespace simplification
// noted in apoxy-cloud//docs/clrk-improvements.mdx.

import type { MetricSeriesSet } from "../telemetry/metrics";
import { pointValue } from "../telemetry/metrics";
import type { AgentRow } from "./agents-data";
import type { EgGatewayRow } from "./egress-data";

// Catalog metric ids the Overview reads. They mirror the Tier-1 usage keys, so a
// KPI's range sum and an agent row's 24h snapshot tell the same story.
export const M_INVOCATIONS = "agent.invocations";
export const M_TOKENS = "gen_ai.tokens";
export const M_TOOLS = "mcp.calls";
export const M_ERRORS = "agent.errors";

/** The metric ids fetched for the Overview, in query order. */
export const OVERVIEW_METRICS = [
  M_INVOCATIONS,
  M_TOKENS,
  M_TOOLS,
  M_ERRORS,
] as const;

export type OvwRange = "24h" | "7d" | "30d";
export const OVW_RANGES: OvwRange[] = ["24h", "7d", "30d"];

const HOUR = 3_600_000;
const DAY = 86_400_000;

interface RangeMeta {
  /** Total look-back. */
  windowMs: number;
  /** Bucket width in ms and as the Go duration the series query takes. */
  stepMs: number;
  step: string;
  /** Human window label and the chart's x-axis ticks. */
  label: string;
  xticks: string[];
}

// Bucket counts: 24h/1h = 24, 7d/6h = 28, 30d/24h = 30 — all under the server's
// 1500-bucket cap, and dense enough to read as a trend without overplotting.
export const RANGE_META: Record<OvwRange, RangeMeta> = {
  "24h": {
    windowMs: 24 * HOUR,
    stepMs: HOUR,
    step: "1h",
    label: "last 24 hours",
    xticks: ["24h", "18h", "12h", "6h", "now"],
  },
  "7d": {
    windowMs: 7 * DAY,
    stepMs: 6 * HOUR,
    step: "6h",
    label: "last 7 days",
    xticks: ["7d", "5d", "3d", "1d", "now"],
  },
  "30d": {
    windowMs: 30 * DAY,
    stepMs: DAY,
    step: "24h",
    label: "last 30 days",
    xticks: ["30d", "22d", "15d", "7d", "now"],
  },
};

export interface RangeWindow {
  sinceMs: number;
  untilMs: number;
  sinceISO: string;
  untilISO: string;
  stepMs: number;
  step: string;
  /** Number of buckets the dense arrays carry. */
  buckets: number;
}

/** Resolve a range to its [since, until) window + bucket geometry, anchored at now. */
export function rangeWindow(range: OvwRange, nowMs: number): RangeWindow {
  const meta = RANGE_META[range];
  const untilMs = nowMs;
  const sinceMs = nowMs - meta.windowMs;
  return {
    sinceMs,
    untilMs,
    sinceISO: new Date(sinceMs).toISOString(),
    untilISO: new Date(untilMs).toISOString(),
    stepMs: meta.stepMs,
    step: meta.step,
    buckets: Math.max(1, Math.round(meta.windowMs / meta.stepMs)),
  };
}

/** Build the four series queries (in OVERVIEW_METRICS order) for a range + namespace. */
export function overviewQueries(
  namespace: string,
  range: OvwRange,
  nowMs: number,
) {
  const w = rangeWindow(range, nowMs);
  return OVERVIEW_METRICS.map((metric) => ({
    metric,
    namespace,
    since: w.sinceISO,
    until: w.untilISO,
    step: w.step,
  }));
}

/** Number of buckets on the UTC step grid that intersect [sinceMs, untilMs). The
 *  grid origin is the UTC epoch (ClickHouse toStartOfInterval aligns there), so a
 *  bucket start is floor(t / step) * step regardless of how `since` is offset. */
export function bucketCount(
  sinceMs: number,
  untilMs: number,
  stepMs: number,
): number {
  if (
    stepMs <= 0 ||
    !Number.isFinite(sinceMs) ||
    !Number.isFinite(untilMs) ||
    untilMs <= sinceMs
  ) {
    return 1;
  }
  const originIdx = Math.floor(sinceMs / stepMs);
  const lastIdx = Math.ceil(untilMs / stepMs) - 1;
  return Math.max(1, lastIdx - originIdx + 1);
}

/** UTC step-grid start (epoch ms) of each bucket densify produces — same origin
 *  and length — so the chart can label a hovered bucket with its real time. */
export function bucketStarts(
  sinceMs: number,
  untilMs: number,
  stepMs: number,
): number[] {
  const buckets = bucketCount(sinceMs, untilMs, stepMs);
  if (stepMs <= 0 || !Number.isFinite(sinceMs)) {
    return new Array<number>(buckets).fill(NaN);
  }
  const originIdx = Math.floor(sinceMs / stepMs);
  const out = new Array<number>(buckets);
  for (let i = 0; i < buckets; i++) out[i] = (originIdx + i) * stepMs;
  return out;
}

/**
 * Densify a series set onto the UTC step grid: returns one value per bucket
 * (0 for buckets the server omitted because they were empty). When `measure` is
 * given only the series carrying that measure label is summed; otherwise every
 * series is summed per bucket (so the two-measure gen_ai.tokens collapses to a
 * single in+out shape).
 *
 * The grid origin is aligned to the UTC step grid the server buckets to
 * (toStartOfInterval(Timestamp, step, 'UTC')), so each server bucket maps 1:1 to
 * one slot. A grid keyed off the (unaligned) request `since` would squeeze the
 * server's N+1 partial-edge buckets into N slots and double the leading point —
 * so the bucket index is floor(t / step) − floor(since / step), an exact integer,
 * not a round-to-nearest over an offset origin.
 */
export function densify(
  set: MetricSeriesSet | undefined,
  sinceMs: number,
  untilMs: number,
  stepMs: number,
  measure?: string,
): number[] {
  const buckets = bucketCount(sinceMs, untilMs, stepMs);
  const out = new Array<number>(buckets).fill(0);
  if (!set || stepMs <= 0 || !Number.isFinite(sinceMs)) return out;
  const originIdx = Math.floor(sinceMs / stepMs);
  for (const s of set.series) {
    if (measure != null && s.labels?.measure !== measure) continue;
    for (const p of s.points) {
      const t = Date.parse(p.timestamp);
      if (Number.isNaN(t)) continue;
      let idx = Math.floor(t / stepMs) - originIdx;
      if (idx < 0) idx = 0;
      else if (idx >= buckets) idx = buckets - 1;
      out[idx] = (out[idx] ?? 0) + pointValue(p);
    }
  }
  return out;
}

/** Sum of a dense array (the KPI total over the window). */
export function sum(values: number[]): number {
  let s = 0;
  for (const v of values) s += v;
  return s;
}

/**
 * Percent change between the first and last third of a series (the KPI delta),
 * matching the design's ovwDelta. 0 when the head is empty (no baseline to
 * compare against) or the series is too short.
 */
export function deltaPct(values: number[]): number {
  const k = Math.max(1, Math.floor(values.length / 3));
  if (values.length < 2) return 0;
  let head = 0;
  for (let i = 0; i < k; i++) head += values[i] ?? 0;
  head /= k;
  let tail = 0;
  for (let i = values.length - k; i < values.length; i++)
    tail += values[i] ?? 0;
  tail /= k;
  if (head === 0) return 0;
  return Math.round(((tail - head) / head) * 100);
}

/** The namespace the Tier-2 (namespace-scoped) reads target: where the agents
 *  live. Most-common wins; falls back to `default` when there are no agents. */
export function mostCommonNamespace(agentRows: AgentRow[]): string {
  const counts = new Map<string, number>();
  for (const r of agentRows)
    counts.set(r.namespace, (counts.get(r.namespace) ?? 0) + 1);
  let best = "default";
  let bestN = 0;
  for (const [ns, n] of counts) {
    if (n > bestN) {
      best = ns;
      bestN = n;
    }
  }
  return best;
}

export interface OvwKpis {
  invocations: number;
  tokensIn: number;
  tokensOut: number;
  tools: number;
  errors: number;
  /** Failure share as a percent: errors / (invocations + errors). */
  errorRate: number;
  /** Cluster-wide in-flight executions (CR gauge), and how many agents are busy. */
  active: number;
  activeAgents: number;
}

export interface OvwSparks {
  inv: number[];
  tok: number[];
  tool: number[];
  err: number[];
}

export type Severity = "ok" | "warn" | "crit";

export interface AttnItem {
  sev: "crit" | "warn";
  title: string;
  sub: string;
  /** What the chip opens, when it points at a resource. */
  target?: { kind: "agent" | "gateway"; id: string };
}

export interface HealthModel {
  severity: Severity;
  headline: string;
  sub: string;
  attn: AttnItem[];
}

export interface PoolTally {
  ready: number;
  total: number;
}

export interface OverviewModel {
  range: OvwRange;
  rangeLabel: string;
  xticks: string[];
  kpis: OvwKpis;
  spark: OvwSparks;
  /** Traffic chart series (dense): invocations area + errors line, with each
   *  bucket's UTC start (epoch ms) for hover labels and the step width. */
  traffic: {
    inv: number[];
    err: number[];
    bucketStartMs: number[];
    stepMs: number;
  };
  delta: { inv: number; tok: number; tool: number; err: number };
  health: HealthModel;
  topAgents: AgentRow[];
  topGateways: EgGatewayRow[];
}

export interface BuildOverviewInput {
  range: OvwRange;
  /** The four Tier-2 series, indexed by metric id (see useSeriesByMetric). */
  series: Record<string, MetricSeriesSet>;
  agentRows: AgentRow[];
  gatewayRows: EgGatewayRow[];
  pools: PoolTally;
}

/** Parse an RFC3339 instant to epoch ms, or NaN. */
function parseMs(s: string | undefined): number {
  return s ? Date.parse(s) : NaN;
}

/** Top-N agents by traffic (invocations, with errors weighted heavier so a
 *  failing agent surfaces). */
function topAgents(rows: AgentRow[], n: number): AgentRow[] {
  return [...rows]
    .sort(
      (a, b) =>
        (b.invocations24h ?? 0) +
        (b.errors24h ?? 0) * 5 -
        ((a.invocations24h ?? 0) + (a.errors24h ?? 0) * 5),
    )
    .slice(0, n);
}

function buildHealth(
  agentRows: AgentRow[],
  gwRows: EgGatewayRow[],
  pools: PoolTally,
): HealthModel {
  const badAgents = agentRows.filter((a) => !a.ready);
  const badGws = gwRows.filter((g) => !g.ready);
  const attn: AttnItem[] = [
    ...badAgents.map(
      (a): AttnItem => ({
        sev: "crit",
        title: a.name,
        sub: a.readyReason ?? "not ready",
        target: { kind: "agent", id: a.id },
      }),
    ),
    ...badGws.map(
      (g): AttnItem => ({
        sev: "crit",
        title: g.name,
        sub: g.statusReason ?? "degraded",
        target: { kind: "gateway", id: g.id },
      }),
    ),
  ];
  const crit = badAgents.length + badGws.length;
  const poolsDegraded = pools.total > 0 && pools.ready < pools.total;
  if (crit === 0 && poolsDegraded) {
    attn.push({
      sev: "warn",
      title: `${pools.total - pools.ready} workers down`,
      sub: "pool below desired replicas",
    });
  }
  const severity: Severity = crit > 0 ? "crit" : poolsDegraded ? "warn" : "ok";
  const headline =
    severity === "ok"
      ? "All systems operational"
      : `${attn.length} ${attn.length === 1 ? "item needs" : "items need"} attention`;
  const sub =
    `${agentRows.length - badAgents.length}/${agentRows.length} agents ready · ` +
    `${gwRows.length - badGws.length}/${gwRows.length} gateways healthy · ` +
    `${pools.ready}/${pools.total} workers up`;
  return { severity, headline, sub, attn };
}

/** Assemble the full Overview view-model from the loaded inputs. */
export function buildOverview(input: BuildOverviewInput): OverviewModel {
  const { range, series, agentRows, gatewayRows, pools } = input;
  const meta = RANGE_META[range];
  const stepMs = meta.stepMs;

  // Densify against the server's RESOLVED window (echoed on every MetricSeriesSet),
  // so the dense grid lines up exactly with the buckets the server returned. Before
  // any series load, fall back to a nominal zero grid [0, windowMs) — the arrays are
  // all-zero then anyway, so only their length (the range's nominal bucket count)
  // matters. This keeps buildOverview a pure function of its inputs (no clock read).
  const anySet =
    series[M_INVOCATIONS] ??
    series[M_TOKENS] ??
    series[M_TOOLS] ??
    series[M_ERRORS];
  const rawSince = parseMs(anySet?.since);
  const rawUntil = parseMs(anySet?.until);
  const haveWindow =
    Number.isFinite(rawSince) &&
    Number.isFinite(rawUntil) &&
    rawUntil > rawSince;
  const sinceMs = haveWindow ? rawSince : 0;
  const untilMs = haveWindow ? rawUntil : meta.windowMs;

  const bucketStartMs = bucketStarts(sinceMs, untilMs, stepMs);
  const inv = densify(series[M_INVOCATIONS], sinceMs, untilMs, stepMs);
  const tokIn = densify(series[M_TOKENS], sinceMs, untilMs, stepMs, "input");
  const tokOut = densify(series[M_TOKENS], sinceMs, untilMs, stepMs, "output");
  const tok = tokIn.map((v, i) => v + (tokOut[i] ?? 0));
  const tool = densify(series[M_TOOLS], sinceMs, untilMs, stepMs);
  const err = densify(series[M_ERRORS], sinceMs, untilMs, stepMs);

  const invocations = sum(inv);
  const tokensIn = sum(tokIn);
  const tokensOut = sum(tokOut);
  const tools = sum(tool);
  const errors = sum(err);
  const denom = invocations + errors;
  const errorRate = denom > 0 ? (errors / denom) * 100 : 0;

  const active = agentRows.reduce((s, a) => s + (a.active ?? 0), 0);
  const activeAgents = agentRows.filter((a) => (a.active ?? 0) > 0).length;

  return {
    range,
    rangeLabel: meta.label,
    xticks: meta.xticks,
    kpis: {
      invocations,
      tokensIn,
      tokensOut,
      tools,
      errors,
      errorRate,
      active,
      activeAgents,
    },
    spark: { inv, tok, tool, err },
    traffic: { inv, err, bucketStartMs, stepMs },
    delta: {
      inv: deltaPct(inv),
      tok: deltaPct(tok),
      tool: deltaPct(tool),
      err: deltaPct(err),
    },
    health: buildHealth(agentRows, gatewayRows, pools),
    topAgents: topAgents(agentRows, 5),
    topGateways: [...gatewayRows].sort((a, b) => (b.rps ?? 0) - (a.rps ?? 0)),
  };
}
