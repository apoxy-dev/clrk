// OVERVIEW — the CLRK landing page, built to the design (`view-overview.jsx`):
// a range selector that drives every number, a fleet-health banner, a five-cell
// KPI strip with sparklines, a traffic area chart, and the two surfaces that
// matter most (top agents + egress gateways). Presentational only — it renders
// the OverviewModel the route container assembles (overview-data.ts) from live
// CRs, the Tier-1 rollup, and the Tier-2 metric series; the charts are the shared
// console-core primitives (TimeSeriesChart, Sparkline), fed real dense buckets.

import { ArrowRight } from "@carbon/icons-react";
import { Sparkline, TimeSeriesChart } from "@apoxy/console-core";
import { fmtK } from "../telemetry/format";
import type { AttnItem, OverviewModel, OvwRange } from "./overview-data";
import { OVW_RANGES } from "./overview-data";

const DAY_MS = 86_400_000;

// Label a hovered bucket with its real time: a bare date for day-wide buckets, a
// short date + 24h clock for finer ones (so a midnight bucket reads unambiguously).
function fmtBucketTime(ms: number, stepMs: number): string {
  if (!Number.isFinite(ms)) return "";
  const d = new Date(ms);
  if (stepMs >= DAY_MS)
    return d.toLocaleDateString(undefined, { month: "short", day: "numeric" });
  return d.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
}

export interface OverviewViewProps {
  model: OverviewModel;
  range: OvwRange;
  onRange: (r: OvwRange) => void;
  agentCount: number;
  gatewayCount: number;
  /** Loading the CR lists (the tables + header counts). */
  isLoading?: boolean;
  /** The Tier-2 series have loaded at least once (KPIs/charts settle from 0). */
  metricsReady?: boolean;
  onOpenAgent?: (id: string) => void;
  onOpenGateway?: (id: string) => void;
  onAll?: (path: string) => void;
}

function DeltaTag({ pct, invert }: { pct: number; invert?: boolean }) {
  // invert = a rise is bad (errors): show rises in coral.
  const dir = pct > 1 ? "up" : pct < -1 ? "down" : "flat";
  const good = invert ? dir === "down" : dir === "up";
  const cls = dir === "flat" ? "flat" : good ? "up" : "down";
  const arrow = dir === "up" ? "▲" : dir === "down" ? "▼" : "■";
  return (
    <span className={"ovw-kpi-delta " + cls}>
      {arrow} {Math.abs(pct)}%
    </span>
  );
}

function SecHead({
  label,
  sub,
  onAll,
  allLabel,
}: {
  label: string;
  sub?: string;
  onAll?: () => void;
  allLabel?: string;
}) {
  return (
    <div className="ovw-sec-head">
      <div className="ovw-sec-label">
        {label}
        {sub && <span className="ovw-sec-sub">{sub}</span>}
      </div>
      {onAll && (
        <button type="button" className="ovw-link" onClick={onAll}>
          {allLabel || "View all"}
          <ArrowRight size={12} />
        </button>
      )}
    </div>
  );
}

function Kpi({
  lab,
  val,
  unit,
  spark,
  color = "var(--apx-graphite)",
  sparkColor,
  crit,
  invert,
  delta,
  sub,
  noDelta,
  pending,
}: {
  lab: string;
  val: string;
  unit?: string;
  spark?: number[];
  color?: string;
  sparkColor?: string;
  crit?: boolean;
  invert?: boolean;
  delta?: number;
  sub?: string;
  noDelta?: boolean;
  /** The Tier-2 series haven't loaded yet — show a dash, not a fabricated 0. */
  pending?: boolean;
}) {
  if (pending) {
    return (
      <div className="ovw-kpi">
        <div className="ovw-kpi-lab">{lab}</div>
        <div className="ovw-kpi-val">—</div>
        <div className="ovw-kpi-foot" />
      </div>
    );
  }
  return (
    <div className="ovw-kpi">
      <div className="ovw-kpi-lab">{lab}</div>
      <div className={"ovw-kpi-val" + (crit ? " is-crit" : "")}>
        {val}
        {unit && <span className="unit">{unit}</span>}
      </div>
      <div className="ovw-kpi-foot">
        {noDelta ? (
          <span className="ovw-kpi-delta flat">{sub}</span>
        ) : sub ? (
          <span
            className="ovw-kpi-delta"
            style={{ color: "var(--text-muted)" }}
          >
            {sub}
          </span>
        ) : (
          <DeltaTag pct={delta ?? 0} invert={invert} />
        )}
        {spark && (
          <Sparkline
            values={spark}
            color={sparkColor ?? (crit ? "var(--apx-coral)" : color)}
            fill
            className="ovw-spark"
          />
        )}
      </div>
    </div>
  );
}

export function OverviewView({
  model,
  range,
  onRange,
  agentCount,
  gatewayCount,
  isLoading,
  metricsReady,
  onOpenAgent,
  onOpenGateway,
  onAll,
}: OverviewViewProps) {
  const { kpis, spark, delta, traffic, health, topAgents, topGateways } = model;

  // The KPI numbers + charts come from the Tier-2 series; until they load, show a
  // dash instead of the from-zero render that reads as a real but idle fleet.
  // Undefined means "ungated" (e.g. a standalone render), so only an explicit
  // false suppresses. "Active now" is CR-sourced and not gated by this.
  const showMetrics = metricsReady !== false;
  const sev = health.severity;
  // Only return a handler when the target exists AND the parent wired one, so a
  // chip with no reachable handler renders disabled rather than as a no-op click.
  const attnTarget = (it: AttnItem) => {
    if (!it.target) return undefined;
    if (it.target.kind === "agent") {
      return onOpenAgent ? () => onOpenAgent(it.target!.id) : undefined;
    }
    return onOpenGateway ? () => onOpenGateway(it.target!.id) : undefined;
  };

  return (
    <div className="ovw">
      <div className="page-head">
        <div className="page-head-l">
          <div className="page-head-titles">
            <div className="page-h1-row">
              <h1 className="page-h1">Overview</h1>
            </div>
            <div className="meta">
              <span>
                {agentCount} agents · {gatewayCount} gateways
              </span>
              <span className="dot-sep">·</span>
              <span>{model.rangeLabel}</span>
            </div>
          </div>
        </div>
        <div className="page-head-r">
          <div className="ovw-range" role="group" aria-label="Time range">
            {OVW_RANGES.map((r) => (
              <button
                key={r}
                type="button"
                className={range === r ? "is-on" : ""}
                onClick={() => onRange(r)}
              >
                {r}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Health banner */}
      <div
        className={
          "ovw-health" +
          (sev === "crit" ? " is-crit" : sev === "warn" ? " is-warn" : "")
        }
      >
        <div className="ovw-health-accent" />
        <div className="ovw-health-body">
          <span className="ovw-health-dot" />
          <div className="ovw-health-text">
            <div className="ovw-health-h">{health.headline}</div>
            <div className="ovw-health-sub">{health.sub}</div>
          </div>
          {health.attn.length > 0 && (
            <div className="ovw-attn">
              {health.attn.map((it, i) => {
                const go = attnTarget(it);
                return (
                  <button
                    key={i}
                    type="button"
                    className="ovw-attn-chip"
                    onClick={go}
                    disabled={!go}
                  >
                    <span className={"pip pip--" + it.sev} />
                    <span>
                      <b>{it.title}</b>
                      <br />
                      <span className="sub">
                        {it.sub.length > 42
                          ? it.sub.slice(0, 42) + "…"
                          : it.sub}
                      </span>
                    </span>
                  </button>
                );
              })}
            </div>
          )}
        </div>
      </div>

      {/* KPI strip */}
      <div className="ovw-kpis">
        <Kpi
          lab="Invocations"
          pending={!showMetrics}
          val={fmtK(kpis.invocations)}
          spark={spark.inv}
          color="var(--apx-blue)"
          delta={delta.inv}
        />
        <Kpi
          lab="Tokens in / out"
          pending={!showMetrics}
          val={fmtK(kpis.tokensIn)}
          unit={"/ " + fmtK(kpis.tokensOut)}
          spark={spark.tok}
          delta={delta.tok}
        />
        <Kpi
          lab="Tool calls"
          pending={!showMetrics}
          val={fmtK(kpis.tools)}
          spark={spark.tool}
          delta={delta.tool}
        />
        <Kpi
          lab="Error rate"
          pending={!showMetrics}
          val={kpis.errorRate.toFixed(2) + "%"}
          crit={kpis.errorRate > 0.5}
          spark={spark.err}
          invert
          sub={fmtK(kpis.errors) + " failed"}
          sparkColor="var(--apx-coral)"
        />
        <Kpi
          lab="Active now"
          val={String(kpis.active)}
          sub={"across " + kpis.activeAgents + " agents"}
          noDelta
        />
      </div>

      {/* Traffic */}
      <div>
        <div className="ovw-panel ovw-traffic">
          <div className="ovw-traffic-head">
            <div className="ovw-traffic-title">
              Traffic · {model.rangeLabel}
            </div>
            <div className="ovw-traffic-legend">
              <span className="ovw-leg">
                <span
                  className="sw sw--area"
                  style={{ background: "var(--apx-ink)" }}
                />
                invocations
              </span>
              <span className="ovw-leg">
                <span
                  className="sw"
                  style={{ background: "var(--apx-coral)" }}
                />
                errors
              </span>
            </div>
          </div>
          {showMetrics ? (
            <TimeSeriesChart
              height={240}
              series={[
                {
                  key: "invocations",
                  values: traffic.inv,
                  color: "var(--apx-ink)",
                  area: true,
                },
                {
                  key: "errors",
                  values: traffic.err,
                  color: "var(--apx-coral)",
                  axis: "secondary",
                },
              ]}
              xTicks={model.xticks}
              formatValue={fmtK}
              formatPoint={(i) =>
                fmtBucketTime(traffic.bucketStartMs[i] ?? NaN, traffic.stepMs)
              }
            />
          ) : (
            <div className="ovw-chart-empty">Loading metrics…</div>
          )}
        </div>
      </div>

      {/* Agents + Gateways */}
      <div className="ovw-grid-2">
        <div>
          <SecHead
            label="Agents"
            sub={`top ${topAgents.length}`}
            onAll={onAll ? () => onAll("agents") : undefined}
          />
          <div className="viz-frame">
            <table className="ovw-table">
              <thead>
                <tr>
                  <th style={{ width: 24 }} />
                  <th>Name</th>
                  <th>24h calls</th>
                  <th>Tokens in/out</th>
                  <th>Errors</th>
                </tr>
              </thead>
              <tbody>
                {topAgents.map((a) => (
                  <tr
                    key={a.id}
                    className={onOpenAgent ? "gws-row" : undefined}
                    onClick={onOpenAgent ? () => onOpenAgent(a.id) : undefined}
                  >
                    <td>
                      <span
                        className={"gws-pip" + (a.ready ? "" : " err")}
                        title={a.ready ? "Ready" : a.readyReason}
                      />
                    </td>
                    <td>
                      <div style={{ fontWeight: 500 }}>
                        {a.name}{" "}
                        <span
                          className={
                            "kind-tag" +
                            (a.kind === "DaemonAgent"
                              ? " kind-tag--daemon"
                              : "")
                          }
                        >
                          {a.kind === "DaemonAgent" ? "DA" : "TA"}
                        </span>
                      </div>
                      <div
                        style={{
                          fontSize: 12,
                          color: "var(--text-muted)",
                          fontFamily: "var(--font-mono)",
                          marginTop: 2,
                        }}
                      >
                        {a.namespace} · {a.pool}
                      </div>
                    </td>
                    <td style={{ fontFamily: "var(--font-mono)" }}>
                      {a.invocations24h == null ? "—" : fmtK(a.invocations24h)}
                    </td>
                    <td
                      style={{ fontFamily: "var(--font-mono)", fontSize: 14 }}
                    >
                      {a.inTokens24h == null ? "—" : fmtK(a.inTokens24h)}
                      <span style={{ color: "var(--text-muted)" }}>
                        {" "}
                        / {a.outTokens24h == null ? "—" : fmtK(a.outTokens24h)}
                      </span>
                    </td>
                    <td
                      style={{
                        fontFamily: "var(--font-mono)",
                        color: a.errors24h
                          ? "var(--apx-coral)"
                          : "var(--text-muted)",
                      }}
                    >
                      {a.errors24h == null ? "—" : a.errors24h}
                    </td>
                  </tr>
                ))}
                {topAgents.length === 0 && (
                  <tr>
                    <td colSpan={5} className="viz-empty-state">
                      {isLoading ? "Loading agents…" : "No agents yet."}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>

        <div>
          <SecHead
            label="Egress gateways"
            sub={`${topGateways.reduce((s, g) => s + (g.rps ?? 0), 0)} rps`}
            onAll={onAll ? () => onAll("egress") : undefined}
          />
          <div className="viz-frame">
            <table className="ovw-table">
              <thead>
                <tr>
                  <th style={{ width: 24 }} />
                  <th>Gateway</th>
                  <th>Routes</th>
                  <th>rps</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {topGateways.map((g) => (
                  <tr
                    key={g.id}
                    className={onOpenGateway ? "gws-row" : undefined}
                    onClick={
                      onOpenGateway ? () => onOpenGateway(g.id) : undefined
                    }
                  >
                    <td>
                      <span
                        className={"gws-pip" + (g.ready ? "" : " err")}
                        title={g.ready ? "Ready" : g.statusReason}
                      />
                    </td>
                    <td>
                      <div style={{ fontWeight: 500 }}>{g.name}</div>
                      <div
                        style={{
                          fontSize: 12,
                          color: "var(--text-muted)",
                          fontFamily: "var(--font-mono)",
                          marginTop: 2,
                        }}
                      >
                        {g.namespace}
                      </div>
                    </td>
                    <td style={{ fontFamily: "var(--font-mono)" }}>
                      {g.routesCount}
                    </td>
                    <td style={{ fontFamily: "var(--font-mono)" }}>
                      {g.rps == null ? "—" : fmtK(g.rps)}
                    </td>
                    <td>
                      <span
                        className={
                          g.ready ? "chip chip--leaf" : "chip chip--coral"
                        }
                      >
                        {g.ready ? "Ready" : "Degraded"}
                      </span>
                    </td>
                  </tr>
                ))}
                {topGateways.length === 0 && (
                  <tr>
                    <td colSpan={5} className="viz-empty-state">
                      {isLoading
                        ? "Loading gateways…"
                        : "No egress gateways yet."}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}
