// AGENT DETAIL — the per-agent panel, built to the CLRK Dashboard design
// (`view-agent.jsx` / `view-daemon.jsx`). A TaskAgent's headline tab is the
// per-invocation swimlane (Inbound → LLM → MCP → Network) with a span inspector;
// a DaemonAgent — never invoked — shows the same lanes on a wall clock with the
// long-running process as the top "session" lane. Both read real OTLP spans from
// the traces subresource (telemetry/*); Revisions, Egress, and Events are backed
// by the AgentSandboxRevision CRs, the agent's egressRefs, and its status
// conditions. There are no fabricated request/response bodies — the inspector
// shows the captured span attributes.

import {
  useEffect,
  useMemo,
  useState,
  type CSSProperties,
  type ReactNode,
} from "react";
import {
  Activity,
  Archive,
  ChartLine,
  ChevronDown,
  ChevronRight,
  ChevronUp,
  Close,
  ListBulleted,
  Network_3 as NetworkIcon,
  Version,
  type CarbonIconType,
} from "@carbon/icons-react";
import {
  BodyBox,
  YamlMenu,
  YamlTray,
  type BodyView,
  type K8sObject,
  type ResourceEntry,
} from "@apoxy/console-core";
import { fmtK, fmtMs, fmtAgo } from "../telemetry/format";
import {
  flattenSpans,
  toDaemonCalls,
  toInvocations,
  type DaemonCall,
  type Invocation,
  type Lane,
  type RelSpan,
  type Span,
} from "../telemetry/spans";
import type { OtlpTracesData } from "../telemetry/otlp";
import { shortImage, type AgentRow } from "./agents-data";
import type {
  AgentEgressRow,
  AgentEventRow,
  RevisionRow,
} from "./agent-detail-data";

export interface AgentDetailViewProps {
  row: AgentRow;
  object: K8sObject;
  entry: ResourceEntry;
  revisions: RevisionRow[];
  egress: AgentEgressRow[];
  events: AgentEventRow[];
  traces?: OtlpTracesData;
  tracesLoading?: boolean;
  onOpenGateway?: (name: string) => void;
  /** Invocation to pre-select in the Interaction tab (deep-link from an
   *  Invocation row, `/agents/<name>?inv=<id>`). */
  initialInvocationId?: string;
}

type TabId = "interaction" | "activity" | "revisions" | "egress" | "events";

export function AgentDetailView({
  row,
  object,
  entry,
  revisions,
  egress,
  events,
  traces,
  tracesLoading,
  onOpenGateway,
  initialInvocationId,
}: AgentDetailViewProps) {
  const isTask = row.kind === "TaskAgent";
  const spans = useMemo(() => flattenSpans(traces), [traces]);
  const [tab, setTab] = useState<TabId>(isTask ? "interaction" : "activity");
  const [editingRaw, setEditingRaw] = useState(false);

  const headTab: { id: TabId; label: string; icon: CarbonIconType } = isTask
    ? { id: "interaction", label: "Interaction", icon: ChartLine }
    : { id: "activity", label: "Activity", icon: Activity };
  const tabs: Array<{
    id: TabId;
    label: string;
    icon: CarbonIconType;
    count?: number;
  }> = [
    headTab,
    {
      id: "revisions",
      label: "Revisions",
      icon: Version,
      count: revisions.length,
    },
    { id: "egress", label: "Egress", icon: NetworkIcon, count: egress.length },
    { id: "events", label: "Events", icon: Archive, count: events.length },
  ];

  const status = row.ready
    ? isTask
      ? "Ready"
      : row.phase || "Running"
    : row.readyReason || "Not ready";

  return (
    <div className="agent-detail">
      <div className="page-head">
        <div className="page-head-l">
          <div className="page-head-titles">
            <div className="page-h1-row">
              <h1 className="page-h1">{row.name}</h1>
              {row.ready ? (
                <span className="gw-status">
                  <span className="pulse" />
                  {status}
                </span>
              ) : (
                <span className="chip chip--coral">{status}</span>
              )}
            </div>
            <div className="meta">
              <span>{row.kind}</span>
              <span className="dot-sep">·</span>
              <span>{row.namespace}</span>
              <span className="dot-sep">·</span>
              <span style={{ fontFamily: "var(--font-mono)" }}>
                {shortImage(row.image)}
              </span>
              <span className="dot-sep">·</span>
              <span>
                pool <b style={{ color: "var(--text-primary)" }}>{row.pool}</b>
              </span>
              {!isTask && row.upSince && (
                <>
                  <span className="dot-sep">·</span>
                  <span>
                    up{" "}
                    <b style={{ color: "var(--text-primary)" }}>
                      {row.upSince}
                    </b>
                  </span>
                  <span className="dot-sep">·</span>
                  <span>
                    {row.restarts} restart{row.restarts === 1 ? "" : "s"}
                  </span>
                </>
              )}
              <span className="dot-sep">·</span>
              <span>{row.age}</span>
            </div>
          </div>
        </div>
        <div className="page-head-r">
          <YamlMenu
            entry={entry}
            object={object}
            onEditRaw={() => setEditingRaw(true)}
          />
        </div>
      </div>

      <YamlTray
        entry={entry}
        object={object}
        open={editingRaw}
        onClose={() => setEditingRaw(false)}
      />

      <div className="tabs">
        {tabs.map((t) => {
          const Icon = t.icon;
          return (
            <div
              key={t.id}
              className={"tab" + (tab === t.id ? " is-active" : "")}
              onClick={() => setTab(t.id)}
            >
              <Icon size={16} />
              {t.label}
              {t.count != null && <span className="tab-ct">{t.count}</span>}
            </div>
          );
        })}
      </div>

      {tab === "interaction" && (
        <InteractionTab
          spans={spans}
          loading={tracesLoading}
          initialId={initialInvocationId}
        />
      )}
      {tab === "activity" && (
        <ActivityTab row={row} spans={spans} loading={tracesLoading} />
      )}
      {tab === "revisions" && <RevisionsTab revisions={revisions} />}
      {tab === "egress" && (
        <EgressTab egress={egress} onOpenGateway={onOpenGateway} />
      )}
      {tab === "events" && <EventsTab events={events} />}
    </div>
  );
}

// ── TaskAgent: invocation swimlane ───────────────────────────────────────────

const LANES_TASK: LaneDef[] = [
  { id: "inbound", label: "Inbound HTTP", sub: "the trigger" },
  { id: "llm", label: "LLM", sub: "AIProviderRoute" },
  { id: "mcp", label: "MCP tools", sub: "MCPRoute" },
  { id: "net", label: "Network", sub: "HTTP/L4 Route" },
];

function InteractionTab({
  spans,
  loading,
  initialId,
}: {
  spans: Span[];
  loading?: boolean;
  initialId?: string;
}) {
  const invs = useMemo(() => toInvocations(spans), [spans]);
  // Seed the selection from a deep-link (`?inv=<id>`); it persists through the
  // async traces load, and falls back to the newest invocation if that id isn't
  // in the captured window.
  const [invId, setInvId] = useState<string | null>(initialId ?? null);
  const inv = invs.find((i) => i.id === invId) ?? invs[0];
  const [spanId, setSpanId] = useState<string | null>(null);
  const span = inv?.spans.find((s) => s.id === spanId) ?? inv?.spans[0];

  if (!inv) {
    return (
      <div className="viz-frame viz-empty-state">
        {loading
          ? "Loading invocations…"
          : "No invocations recorded for this agent yet."}
      </div>
    );
  }

  const llm = inv.spans.filter((s) => s.lane === "llm");
  const mcp = inv.spans.filter((s) => s.lane === "mcp");
  const net = inv.spans.filter((s) => s.lane === "net");

  return (
    <>
      <InvocationStrip
        invocations={invs}
        selectedId={inv.id}
        onSelect={(id) => {
          setInvId(id);
          setSpanId(null);
        }}
      />
      <div className="stat-strip" style={strip(6)}>
        <Stat
          lab="Status"
          val={String(inv.statusCode)}
          unit={inv.ok ? "OK" : "ERR"}
          tone={inv.ok ? "ok" : "err"}
        />
        <Stat lab="Duration" val={fmtMs(inv.durMs)} />
        <Stat
          lab="LLM tokens in / out"
          val={fmtK(inv.tokIn)}
          unit={`/ ${fmtK(inv.tokOut)}`}
        />
        <Stat lab="LLM calls" val={String(llm.length)} />
        <Stat lab="MCP tool calls" val={String(mcp.length)} />
        <Stat lab="Network calls" val={String(net.length)} />
      </div>
      <div className="viz-frame">
        <LaneChart
          lanes={LANES_TASK}
          totalMs={Math.max(1, inv.durMs)}
          items={inv.spans
            .filter((s) => s.lane !== "inbound")
            .map((s) => chartItem(s, s.t0Ms))}
          rootBar={
            inv.inbound
              ? {
                  id: inv.inbound.id,
                  laneId: "inbound",
                  x0Ms: 0,
                  x1Ms: Math.max(1, inv.durMs),
                  label: `${inv.inbound.label} → ${inv.inbound.statusCode}`,
                  error: !inv.inbound.ok,
                }
              : undefined
          }
          selectedId={span?.id ?? null}
          onSelect={setSpanId}
          onSelectRoot={
            inv.inbound ? () => setSpanId(inv.inbound!.id) : undefined
          }
          fmtTick={fmtTickMs}
          resetKey={inv.id}
        />
        <Legend kind="task" />
      </div>
      {span && (
        <SpanInspector
          span={span}
          traceId={inv.traceId}
          invocationId={inv.id}
        />
      )}
    </>
  );
}

function InvocationStrip({
  invocations,
  selectedId,
  onSelect,
}: {
  invocations: Invocation[];
  selectedId: string;
  onSelect: (id: string) => void;
}) {
  const now = Date.now();
  return (
    <div className="inv-strip">
      <div className="inv-strip-head">
        <span className="inv-strip-lab">Recent invocations</span>
      </div>
      <div className="inv-strip-track">
        <div className="inv-strip-list">
          {invocations.map((i) => (
            <button
              key={i.id}
              type="button"
              className={"inv-pill" + (i.id === selectedId ? " is-active" : "")}
              onClick={() => onSelect(i.id)}
            >
              <span className={"gws-pip" + (i.ok ? "" : " err")} />
              <div className="inv-pill-body">
                <div className="inv-pill-id">{i.id.slice(0, 8)}…</div>
                <div className="inv-pill-meta">
                  {fmtMs(i.durMs)} · {i.statusCode} · {fmtK(i.tokIn + i.tokOut)}{" "}
                  tok
                </div>
              </div>
              <span className="inv-pill-when">
                {fmtAgo(now - i.startNano / 1e6)}
              </span>
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}

// ── DaemonAgent: wall-clock activity ─────────────────────────────────────────

const LANES_DAEMON: LaneDef[] = [
  { id: "session", label: "Session", sub: "DaemonAgent" },
  { id: "llm", label: "LLM", sub: "AIProviderRoute" },
  { id: "mcp", label: "MCP tools", sub: "MCPRoute" },
  { id: "net", label: "Network", sub: "HTTP/L4 Route" },
];

const DAEMON_WINDOWS = [
  { id: "live", label: "Live" },
  { id: "5m", label: "5m", sec: 300 },
  { id: "15m", label: "15m", sec: 900 },
  { id: "1h", label: "1h", sec: 3600 },
] as const;

function ActivityTab({
  row,
  spans,
  loading,
}: {
  row: AgentRow;
  spans: Span[];
  loading?: boolean;
}) {
  const [mode, setMode] = useState<string>("live");
  const [selId, setSelId] = useState<string | null>("session");
  // In live mode the wall clock must keep advancing or the chart freezes at the
  // mount instant; a fixed window (5m/15m/1h) is anchored to its preset, so it
  // only needs `now` sampled once.
  const now = useNowMs(mode === "live");
  const calls = useMemo(() => toDaemonCalls(spans, now), [spans, now]);

  const preset = DAEMON_WINDOWS.find((w) => w.id === mode);
  const maxAgo = calls.length ? calls[calls.length - 1]!.agoSec : 60;
  const windowSec =
    preset && "sec" in preset ? preset.sec : Math.max(60, maxAgo * 1.04);
  const visible = calls.filter((c) => c.agoSec <= windowSec);
  const windowMs = windowSec * 1000;

  // Keep the inspector anchored: if the selected call scrolls out of the chosen
  // window, fall back to the session row instead of silently showing nothing.
  useEffect(() => {
    if (selId !== "session" && !visible.some((c) => c.id === selId))
      setSelId("session");
  }, [selId, visible]);

  const selected =
    selId === "session"
      ? "session"
      : (visible.find((c) => c.id === selId) ?? "session");

  return (
    <>
      <div className="stat-strip" style={strip(6)}>
        <Stat
          lab="Phase"
          val={row.phase || "Running"}
          tone={row.ready ? "ok" : "err"}
        />
        <Stat lab="Uptime" val={row.upSince ?? "—"} />
        <Stat lab="Restarts" val={String(row.restarts ?? 0)} />
        <Stat
          lab="LLM tokens in / out · 24h"
          val={metricStr(row.inTokens24h)}
          unit={`/ ${metricStr(row.outTokens24h)}`}
        />
        <Stat lab="Tool calls · 24h" val={metricStr(row.tools24h)} />
        <Stat
          lab="Errors · 24h"
          val={metricStr(row.errors24h)}
          tone={row.errors24h ? "err" : undefined}
        />
      </div>
      <div className="viz-frame">
        <div className="dtrace-head">
          <span className="dtrace-head-lab">Captured egress</span>
          <div className="met-range">
            {DAEMON_WINDOWS.map((w) => (
              <button
                key={w.id}
                type="button"
                className={mode === w.id ? "is-on" : ""}
                onClick={() => setMode(w.id)}
              >
                {w.label}
              </button>
            ))}
          </div>
          <span className="dtrace-now">
            {visible.length} {visible.length === 1 ? "trace" : "traces"}
            {preset && "sec" in preset ? ` · last ${preset.label}` : ""}
          </span>
        </div>
        {visible.length === 0 ? (
          <div className="dtrace-empty">
            {loading
              ? "Loading captured egress…"
              : "No calls captured in this window."}
          </div>
        ) : (
          <LaneChart
            lanes={LANES_DAEMON}
            totalMs={Math.max(1, windowMs)}
            items={visible.map((c) => chartItem(c, windowMs - c.agoSec * 1000))}
            rootBar={{
              id: "session",
              laneId: "session",
              x0Ms: 0,
              x1Ms: windowMs,
              label: `process running · up ${row.upSince ?? "—"} · ${row.restarts ?? 0} restart${row.restarts === 1 ? "" : "s"}`,
              error: false,
            }}
            selectedId={selId}
            onSelect={setSelId}
            fmtTick={(ms) => `${Math.round((windowMs - ms) / 1000)}s`}
            onSelectRoot={() => setSelId("session")}
            resetKey={mode}
          />
        )}
        <Legend kind="daemon" />
      </div>
      {selected === "session" ? (
        <DaemonSessionInspector row={row} />
      ) : selected ? (
        <SpanInspector span={selected} traceId={selected.traceId} />
      ) : null}
    </>
  );
}

function DaemonSessionInspector({ row }: { row: AgentRow }) {
  return (
    <div className="span-inspector">
      <div className="span-inspector-hd">
        <div className="span-kind">
          <span className="span-kind-pip span-kind-pip--inbound" />
          <span className="span-kind-lab">SESSION</span>
        </div>
        <div className="span-title">{row.name} · long-running process</div>
        <div className="span-status">
          <span className="chip chip--leaf">{row.phase || "Running"}</span>
          <span className="chip">up {row.upSince ?? "—"}</span>
        </div>
      </div>
      <div className="span-inspector-bd">
        <div className="span-main">
          <KvBlock
            rows={[
              ["kind", row.kind],
              ["image", shortImage(row.image)],
              ["pool", row.pool],
              ["revision", row.revision],
              ["phase", row.phase || "Running"],
              ["up_since", row.upSince ?? "—"],
              ["restarts", String(row.restarts ?? 0)],
            ]}
          />
          <div className="span-section-lab">Why there is no invocation</div>
          <div
            style={{
              fontSize: 14,
              color: "var(--text-secondary)",
              lineHeight: 1.55,
              maxWidth: 640,
            }}
          >
            A DaemonAgent has no request boundary — it is not triggered, it just
            runs. Egress is still fully captured: each LLM, MCP, and network
            call above is its own span on the wall clock, with its own trace id,
            rather than nested under an invocation.
          </div>
        </div>
      </div>
    </div>
  );
}

// ── Shared chart ─────────────────────────────────────────────────────────────

interface LaneDef {
  id: string;
  label: string;
  sub: string;
}
interface ChartItem {
  id: string;
  laneId: string;
  t0Ms: number;
  durMs: number;
  lane: Lane;
  ok: boolean;
  label: string;
  host: string;
  statusCode: number;
}
interface RootBar {
  id: string;
  laneId: string;
  x0Ms: number;
  x1Ms: number;
  label: string;
  error: boolean;
}

function chartItem(s: Span | RelSpan | DaemonCall, t0Ms: number): ChartItem {
  return {
    id: s.id,
    laneId: s.lane,
    t0Ms,
    durMs: s.durMs,
    lane: s.lane,
    ok: s.ok,
    label: s.label,
    host: s.host,
    statusCode: s.statusCode,
  };
}

// The chart is a DOM-row Gantt, not one scaling <svg>: a CSS grid pins a fixed
// label gutter beside a flexible timeline, and each span is an absolutely-placed
// <button> sized by percent-of-window (left/width). Only the horizontal time
// axis stretches with the panel; lane heights and label text stay at fixed px,
// so the chart never balloons on a wide monitor the way a viewBox-scaled SVG did.
function LaneChart({
  lanes,
  totalMs,
  items,
  rootBar,
  selectedId,
  onSelect,
  onSelectRoot,
  fmtTick,
  resetKey,
}: {
  lanes: LaneDef[];
  totalMs: number;
  items: ChartItem[];
  rootBar?: RootBar;
  selectedId: string | null;
  onSelect: (id: string) => void;
  onSelectRoot?: () => void;
  fmtTick: (ms: number) => string;
  /** Changes when the inspected invocation / time window changes, so per-lane
   *  expand state doesn't leak across runs. */
  resetKey: string;
}) {
  // Lanes the user has opened past the concurrency fold. Reset on run change so
  // a deep fan-out in one invocation doesn't start the next one pre-expanded.
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  useEffect(() => {
    setExpanded({});
  }, [resetKey]);
  const toggleLane = (id: string) =>
    setExpanded((e) => ({ ...e, [id]: !e[id] }));

  const ticks = niceTicks(0, totalMs, 8);
  const pct = (ms: number) => (clamp(ms, 0, totalMs) / totalMs) * 100;
  // Anchor the first/last labels by position, not list index: with no forced
  // edge tick the rightmost label is usually mid-track (centered), and only a
  // tick that truly lands on 0%/100% needs to be tucked inside the box.
  const tickClass = (t: number) => {
    const p = pct(t);
    return (
      "swim-tick" +
      (p <= 0.5 ? " swim-tick--first" : p >= 99.5 ? " swim-tick--last" : "")
    );
  };

  return (
    <div className="swim">
      <div className="swim-axis">
        <div className="swim-axis-gutter" />
        <div className="swim-axis-track">
          {ticks.map((t) => (
            <span
              key={t}
              className={tickClass(t)}
              style={{ left: `${pct(t)}%` }}
            >
              {fmtTick(t)}
            </span>
          ))}
        </div>
      </div>
      <div className="swim-body">
        {lanes.map((lane) => {
          const root = rootBar?.laneId === lane.id ? rootBar : undefined;
          const laneItems = items.filter((it) => it.laneId === lane.id);
          const isExpanded = !!expanded[lane.id];
          const pack = packLanes(laneItems, isExpanded);
          // The selected span is pinned visible (rendered as a full bar, never a
          // dim tick) so collapsing a lane can't strand the selection behind the
          // fold — and it's excluded from the hidden tally the pill advertises.
          const selItem =
            selectedId != null
              ? laneItems.find((it) => it.id === selectedId)
              : undefined;
          const selHidden = selItem != null && pack.overflow.has(selItem.id);
          const hiddenCount = pack.overflowCount - (selHidden ? 1 : 0);
          const hiddenErr =
            pack.overflowErr - (selHidden && !selItem.ok ? 1 : 0);
          return (
            <div
              key={lane.id}
              className="swim-lane"
              style={{ height: laneHeight(pack.rows) }}
            >
              <div className="swim-lane-label">
                <div className="swim-lane-name">{lane.label}</div>
                <div className="swim-lane-sub">{lane.sub}</div>
                {pack.trueRows > 1 && (
                  <div className="swim-lane-conc">
                    {pack.trueRows}× concurrent
                  </div>
                )}
                {pack.expandable && (isExpanded || hiddenCount > 0) && (
                  <button
                    type="button"
                    className={
                      "swim-expand" +
                      (!isExpanded && hiddenErr ? " has-err" : "")
                    }
                    aria-expanded={isExpanded}
                    aria-label={
                      isExpanded
                        ? `Collapse ${lane.label} lane`
                        : `Show ${hiddenCount} more ${lane.label} spans${hiddenErr ? `, ${hiddenErr} errored` : ""}`
                    }
                    onClick={() => toggleLane(lane.id)}
                  >
                    {isExpanded ? (
                      <ChevronUp size={14} />
                    ) : (
                      <ChevronDown size={14} />
                    )}
                    {isExpanded
                      ? "Collapse"
                      : `+${hiddenCount} more${hiddenErr ? ` · ${hiddenErr} err` : ""}`}
                  </button>
                )}
              </div>
              <div className="swim-lane-track">
                {ticks.map((t) => (
                  <span
                    key={`g${t}`}
                    className="swim-grid"
                    style={{ left: `${pct(t)}%` }}
                  />
                ))}
                {root && (
                  <SwimRootBar
                    bar={root}
                    pct={pct}
                    selected={selectedId === root.id}
                    onSelect={onSelectRoot}
                  />
                )}
                {laneItems.map((it) => {
                  const top = barTop(pack.rowOf.get(it.id) ?? 0);
                  return pack.overflow.has(it.id) && it.id !== selectedId ? (
                    <SwimOverflowTick
                      key={it.id}
                      item={it}
                      pct={pct}
                      top={top}
                    />
                  ) : (
                    <SwimSpanBar
                      key={it.id}
                      item={it}
                      pct={pct}
                      top={top}
                      selected={it.id === selectedId}
                      onSelect={onSelect}
                    />
                  );
                })}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// Swimlane vertical metrics. A lane is top/bottom padding plus N stacked bar
// rows; spans whose time intervals overlap are packed onto separate rows
// (greedy first-fit) so a later call can't hide an earlier one behind it. One
// row reproduces the original fixed 56px lane.
const BAR_H = 32;
const ROW_GAP = 6;
const LANE_PAD = 12;
const OVERFLOW_H = 8;
// Centers the 8px density tick inside its 32px row slot.
const OVERFLOW_INSET = (BAR_H - OVERFLOW_H) / 2;
// How many sub-rows a lane stacks before it folds. The last visible row becomes
// a density strip; deeper concurrency collapses into it (so 2 bar rows stay
// readable and a pathological fan-out can't blow the lane's height up). Past 3
// parallel calls a lane folds — the review-bot invocation (six MCP calls fired
// in parallel) lands here.
const SWIM_MAX_ROWS = 3;

function laneHeight(rows: number): number {
  return LANE_PAD * 2 + rows * BAR_H + Math.max(0, rows - 1) * ROW_GAP;
}
function barTop(row: number): number {
  return LANE_PAD + row * (BAR_H + ROW_GAP);
}

interface LanePack {
  /** Visible row index per item (folded items resolve to the overflow row). */
  rowOf: Map<string, number>;
  /** Items rendered as a density tick in the overflow strip, not a full bar. */
  overflow: Set<string>;
  /** Rows the lane actually draws (folded → capped at SWIM_MAX_ROWS). */
  rows: number;
  /** Rows the lane would need with nothing folded. */
  trueRows: number;
  /** True once the lane stacks past the fold and offers an expand toggle. */
  expandable: boolean;
  overflowCount: number;
  overflowErr: number;
}

function packLanes(items: ChartItem[], expanded: boolean): LanePack {
  const sorted = [...items].sort(
    (a, b) => a.t0Ms - b.t0Ms || a.id.localeCompare(b.id),
  );
  const rowEnd: number[] = []; // running end-ms of each open row
  const trueRowOf = new Map<string, number>();
  for (const it of sorted) {
    const end = it.t0Ms + Math.max(0, it.durMs);
    let row = rowEnd.findIndex((e) => it.t0Ms >= e);
    if (row === -1) {
      row = rowEnd.length;
      rowEnd.push(end);
    } else {
      rowEnd[row] = end;
    }
    trueRowOf.set(it.id, row);
  }
  const trueRows = Math.max(1, rowEnd.length);
  const expandable = trueRows > SWIM_MAX_ROWS;
  const capped = expandable && !expanded;
  const overflowRow = SWIM_MAX_ROWS - 1; // last visible row holds the folded spans
  const rowOf = new Map<string, number>();
  const overflow = new Set<string>();
  let overflowCount = 0;
  let overflowErr = 0;
  for (const it of sorted) {
    let row = trueRowOf.get(it.id) ?? 0;
    if (capped && row >= overflowRow) {
      overflow.add(it.id);
      overflowCount++;
      if (!it.ok) overflowErr++;
      row = overflowRow;
    }
    rowOf.set(it.id, row);
  }
  return {
    rowOf,
    overflow,
    rows: capped ? SWIM_MAX_ROWS : trueRows,
    trueRows,
    expandable,
    overflowCount,
    overflowErr,
  };
}

function SwimRootBar({
  bar,
  pct,
  selected,
  onSelect,
}: {
  bar: RootBar;
  pct: (ms: number) => number;
  selected: boolean;
  onSelect?: () => void;
}) {
  const left = pct(bar.x0Ms);
  const width = Math.max(0, pct(bar.x1Ms) - left);
  return (
    <button
      type="button"
      className={
        "swim-bar swim-bar--root" +
        (bar.error ? " is-err" : "") +
        (selected ? " is-sel" : "")
      }
      style={{
        left: `${left}%`,
        width: `${width}%`,
        cursor: onSelect ? "pointer" : "default",
      }}
      aria-pressed={selected}
      title={bar.label}
      onClick={onSelect}
    >
      <span className="swim-bar-lab swim-bar-lab--root">{bar.label}</span>
    </button>
  );
}

function SwimSpanBar({
  item,
  pct,
  top,
  selected,
  onSelect,
}: {
  item: ChartItem;
  pct: (ms: number) => number;
  top: number;
  selected: boolean;
  onSelect: (id: string) => void;
}) {
  const left = pct(item.t0Ms);
  const width = Math.max(0, pct(item.t0Ms + item.durMs) - left);
  // Errored spans are filled coral, so they take the same light text every
  // other filled bar uses; only the light network fill wants dark text.
  const fg =
    item.ok && item.lane === "net" ? "var(--text-primary)" : "var(--apx-bone)";
  return (
    <button
      type="button"
      className={"swim-bar swim-bar--span" + (selected ? " is-sel" : "")}
      style={{
        left: `${left}%`,
        top: `${top}px`,
        width: `${width}%`,
        background: laneFill(item.lane, item.ok),
        borderColor: laneStroke(item.lane, item.ok),
        color: fg,
      }}
      aria-pressed={selected}
      onClick={() => onSelect(item.id)}
      title={`${item.label} · ${item.host} · ${fmtMs(item.durMs)} · ${item.statusCode}`}
    >
      <span className="swim-bar-lab">{item.label}</span>
    </button>
  );
}

// A folded span. Past the lane's concurrency cap there's no room to draw every
// parallel call as its own bar, so the deepest ones collapse into this dim
// density tick (coral and more opaque when errored, so a hidden failure still
// reads). Expanding the lane promotes them back to full, selectable bars.
function SwimOverflowTick({
  item,
  pct,
  top,
}: {
  item: ChartItem;
  pct: (ms: number) => number;
  top: number;
}) {
  const left = pct(item.t0Ms);
  const width = Math.max(0, pct(item.t0Ms + item.durMs) - left);
  return (
    <div
      className={"swim-overflow" + (item.ok ? "" : " is-err")}
      style={{
        left: `${left}%`,
        top: `${top + OVERFLOW_INSET}px`,
        width: `${width}%`,
        background: laneFill(item.lane, item.ok),
      }}
      title={`${item.label} · ${item.host} · ${fmtMs(item.durMs)} · ${item.statusCode}`}
    />
  );
}

function Legend({ kind }: { kind: "task" | "daemon" }) {
  return (
    <div className="swim-legend">
      <span className="lg-item">
        <span className="lg-sw lg-sw--inbound" />
        {kind === "task" ? "Inbound (200)" : "Session (up)"}
      </span>
      <span className="lg-item">
        <span className="lg-sw lg-sw--llm" />
        LLM call
      </span>
      <span className="lg-item">
        <span className="lg-sw lg-sw--mcp" />
        MCP tool
      </span>
      <span className="lg-item">
        <span className="lg-sw lg-sw--net" />
        Network
      </span>
      <span className="lg-item">
        <span className="lg-sw lg-sw--err" />
        Error / 5xx
      </span>
      <span
        style={{
          marginLeft: "auto",
          color: "var(--text-muted)",
          fontFamily: "var(--font-mono)",
          fontSize: 12,
        }}
      >
        tap a span to inspect
      </span>
    </div>
  );
}

// ── Span inspector ───────────────────────────────────────────────────────────

function SpanInspector({
  span,
  traceId,
  invocationId,
}: {
  span: Span;
  traceId: string;
  invocationId?: string;
}) {
  // The full raw attribute list lives in a slide-out tray rather than inline, so
  // the main panel stays a short curated read and the dense dotted-key dump is
  // on demand. Close it when the inspected span changes, or on Esc.
  const [attrsOpen, setAttrsOpen] = useState(false);
  useEffect(() => {
    setAttrsOpen(false);
  }, [span.id]);
  useEffect(() => {
    if (!attrsOpen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setAttrsOpen(false);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [attrsOpen]);

  // Pretty-print / assemble the body views once per span. SpanInspector
  // re-renders on the Attributes toggle, and prettyBody re-parses + re-serializes
  // the whole captured body (tens of KiB) each time without this memo.
  const reqViews = useMemo<BodyView[] | null>(
    () =>
      span.reqBody
        ? [
            {
              id: "request",
              text: prettyBody(span.reqBody.text),
              bytes: span.reqBody.bytes,
              truncated: span.reqBody.truncated,
              note: "captured at span open",
            },
          ]
        : null,
    [span.reqBody],
  );
  const respViews = useMemo(
    () => responseViews(span),
    [span.respContent, span.respBody, span.stream],
  );

  return (
    <div className="span-inspector">
      <SpanInspectorHeader span={span} />
      <div className="span-inspector-bd">
        <div className="span-main">
          <RequestSection
            span={span}
            reqViews={reqViews}
            attrsOpen={attrsOpen}
            onOpenAttrs={() => setAttrsOpen(true)}
          />
          <ResponseSection span={span} respViews={respViews} />
        </div>
      </div>
      {attrsOpen && (
        <SpanAttrsTray
          span={span}
          traceId={traceId}
          invocationId={invocationId}
          onClose={() => setAttrsOpen(false)}
        />
      )}
    </div>
  );
}

// The inspector header: lane pip + the span's request line + the duration and
// route chips. The HTTP status is deliberately absent here -- it is a response
// fact and lives in the Response section.
function SpanInspectorHeader({ span }: { span: Span }) {
  return (
    <div className="span-inspector-hd">
      <div className="span-kind">
        <span className={`span-kind-pip span-kind-pip--${span.lane}`} />
        <span className="span-kind-lab">{span.lane.toUpperCase()}</span>
      </div>
      <div className="span-title">{span.label}</div>
      <div className="span-status">
        <span className="chip">{fmtMs(span.durMs)}</span>
        {span.route && <span className="chip">via {span.route}</span>}
      </div>
    </div>
  );
}

// The request half of a span: the call parameters (method/url + lane-specific
// dimensions), the LLM token rhythm, the captured request headers, and the
// captured request body. Carries the Attributes-tray trigger on its heading.
function RequestSection({
  span,
  reqViews,
  attrsOpen,
  onOpenAttrs,
}: {
  span: Span;
  reqViews: BodyView[] | null;
  attrsOpen: boolean;
  onOpenAttrs: () => void;
}) {
  return (
    <>
      <div className="span-section-head">
        <div className="span-section-lab">{laneHeading(span.lane)}</div>
        <button
          type="button"
          className="span-attrs-btn"
          onClick={onOpenAttrs}
          aria-haspopup="dialog"
          aria-expanded={attrsOpen}
        >
          <ListBulleted size={16} />
          Attributes
          <ChevronRight />
        </button>
      </div>
      <KvBlock rows={requestRows(span)} />
      {span.lane === "llm" && (span.tokIn != null || span.tokOut != null) && (
        <>
          <div className="span-section-lab">Token rhythm</div>
          <TokenBar tokIn={span.tokIn ?? 0} tokOut={span.tokOut ?? 0} />
        </>
      )}
      <HeadersSection label="Request headers" headers={span.reqHeaders} />
      {reqViews && (
        <>
          <div className="span-section-lab">Request body</div>
          <div className="span-body">
            {/* key per span so the box resets (view tab, wrap, fullscreen)
                when a different span is selected. */}
            <BodyBox
              key={span.id}
              title={contentTypeOf(span.reqHeaders)}
              views={reqViews}
            />
          </div>
        </>
      )}
    </>
  );
}

// The response half of a span: the status chip on the heading, captured
// response headers, and the captured/decoded response body. Always renders so
// the status is shown even when no response payload was captured.
function ResponseSection({
  span,
  respViews,
}: {
  span: Span;
  respViews: BodyView[];
}) {
  return (
    <>
      <div className="span-section-head span-section-head--response">
        <div className="span-section-lab">Response</div>
        <StatusChip ok={span.ok} statusCode={span.statusCode} />
      </div>
      <HeadersSection label="Response headers" headers={span.respHeaders} />
      {respViews.length > 0 && (
        <>
          <div className="span-section-lab">Response body</div>
          <div className="span-body">
            <BodyBox
              key={span.id}
              title={contentTypeOf(span.respHeaders)}
              views={respViews}
            />
          </div>
        </>
      )}
    </>
  );
}

function StatusChip({ ok, statusCode }: { ok: boolean; statusCode: number }) {
  return (
    <span className={ok ? "chip chip--leaf" : "chip chip--coral"}>
      {statusCode || "—"}
    </span>
  );
}

// A labelled key/value block of captured headers, hidden entirely when none are
// displayable. The full set (including the x-/pseudo headers filtered out here)
// stays available in the attributes tray.
function HeadersSection({
  label,
  headers,
}: {
  label: string;
  headers?: Record<string, string>;
}) {
  const rows = headers ? headerRows(headers) : [];
  if (rows.length === 0) return null;
  return (
    <>
      <div className="span-section-lab">{label}</div>
      <div className="kvblock">
        {rows.map(([k, v]) => (
          <div key={k} className="kvrow">
            <div className="kvk">{k}</div>
            <div className="kvv">{headerCellValue(v)}</div>
          </div>
        ))}
      </div>
    </>
  );
}

// A captured header value, coral when the egress sink withheld it (it strips
// authorization/x-api-key/set-cookie to `[redacted]`) so it reads as withheld
// rather than as a literal payload.
function headerCellValue(v: string): ReactNode {
  return v === "[redacted]" ? (
    <span style={{ color: "var(--apx-coral)" }}>{v}</span>
  ) : (
    v
  );
}

// Title for a body box: the captured content-type sans charset/boundary params
// (e.g. "text/event-stream"), or a neutral "body" when none was captured.
function contentTypeOf(headers?: Record<string, string>): string {
  const ct = headers?.["content-type"];
  if (!ct) return "body";
  return ct.split(";")[0]?.trim() || "body";
}

// The slide-out tray with the full raw attribute dump, kept out of the main
// panel so the inspector stays a short curated read.
function SpanAttrsTray({
  span,
  traceId,
  invocationId,
  onClose,
}: {
  span: Span;
  traceId: string;
  invocationId?: string;
  onClose: () => void;
}) {
  return (
    <>
      <div className="attrs-tray-scrim" onClick={onClose} />
      <aside className="attrs-tray" role="dialog" aria-label="Span attributes">
        <div className="attrs-tray-hd">
          <span className="lab">Span attributes</span>
          <span className="name" title={span.label}>
            {span.label}
          </span>
          <button
            type="button"
            className="attrs-tray-close"
            onClick={onClose}
            aria-label="Close"
            title="Close (Esc)"
          >
            <Close size={16} />
          </button>
        </div>
        <div className="attrs-tray-bd">
          <KvBlock rows={attrRows(span, traceId, invocationId)} />
        </div>
      </aside>
    </>
  );
}

function laneHeading(lane: Lane): string {
  return lane === "inbound"
    ? "Request"
    : lane === "llm"
      ? "GenAI call"
      : lane === "mcp"
        ? "MCP tool call"
        : "HTTP call";
}

// The request parameters shown above the request headers/body: the HTTP basics
// (method + url, or host/path when no full url was captured) followed by the
// lane-specific dimensions. Status, latency, and output tokens are response
// facts and are surfaced in the Response section / header instead.
function requestRows(s: Span): Array<[string, ReactNode]> {
  const rows: Array<[string, ReactNode]> = [];
  if (s.httpMethod) rows.push(["method", s.httpMethod]);
  if (s.url) rows.push(["url", s.url]);
  else if (s.host) rows.push(["host", s.host]);
  if (s.path && !s.url) rows.push(["path", s.path]);
  if (s.lane === "llm") {
    rows.push(["gen_ai.system", s.provider ?? "—"]);
    rows.push(["gen_ai.request.model", s.model ?? "—"]);
    rows.push(["gen_ai.response.stream", s.stream ? "true" : "false"]);
  } else if (s.lane === "mcp") {
    rows.push(["mcp.server", s.server ?? "—"]);
    rows.push(["mcp.tool.name", s.tool ?? "—"]);
    rows.push(["mcp.method", s.method ?? "—"]);
  }
  if (s.route) rows.push(["route", s.route]);
  // Fall back to the request line so the block is never empty (e.g. an inbound
  // span with neither method nor url captured).
  if (rows.length === 0) rows.push(["request", s.label]);
  return rows;
}

// Captured headers as sorted rows for the curated Headers sections. Drops the
// HTTP/2 pseudo-headers (:authority/:method/:path/:scheme/:status -- already
// shown as url/method/status) and the noisy x-* hop/forwarding/vendor headers
// (x-forwarded-*, x-envoy-*, x-request-id, ...). The full set, x-* and pseudo
// included, stays available in the attributes tray.
function headerRows(headers: Record<string, string>): Array<[string, string]> {
  return Object.keys(headers)
    .filter((k) => !k.startsWith(":") && !k.startsWith("x-"))
    .sort()
    .map((k) => [k, headers[k] ?? ""]);
}

function attrRows(
  s: Span,
  traceId: string,
  invocationId?: string,
): Array<[string, ReactNode]> {
  const rows: Array<[string, ReactNode]> = [];
  for (const k of Object.keys(s.attrs)) {
    if (k === "invocation.id") continue;
    rows.push([k, s.attrs[k] ?? ""]);
  }
  // Captured headers ride span events, not the attribute map, so surface the
  // complete set here under their original OTLP keys -- this is where the
  // x-/pseudo headers hidden from the curated Headers sections remain visible.
  if (s.reqHeaders) {
    for (const k of Object.keys(s.reqHeaders)) {
      rows.push([
        "http.request.header." + k,
        headerCellValue(s.reqHeaders[k] ?? ""),
      ]);
    }
  }
  if (s.respHeaders) {
    for (const k of Object.keys(s.respHeaders)) {
      rows.push([
        "http.response.header." + k,
        headerCellValue(s.respHeaders[k] ?? ""),
      ]);
    }
  }
  rows.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
  if (traceId) rows.push(["trace.id", traceId]);
  rows.push([
    "invocation.id",
    invocationId ? (
      invocationId
    ) : (
      <span style={{ color: "var(--text-muted)" }}>none · daemon span</span>
    ),
  ]);
  return rows;
}

function TokenBar({ tokIn, tokOut }: { tokIn: number; tokOut: number }) {
  const total = Math.max(1, tokIn + tokOut);
  return (
    <div className="token-bar">
      <div className="token-bar-row">
        <div className="token-bar-bg">
          <div
            className="token-bar-fill"
            style={{ width: `${(tokIn / total) * 100}%` }}
          />
        </div>
        <div className="token-bar-lab">
          in <b>{fmtK(tokIn)}</b>
        </div>
      </div>
      <div className="token-bar-row">
        <div className="token-bar-bg">
          <div
            className="token-bar-fill token-bar-fill--out"
            style={{ width: `${(tokOut / total) * 100}%` }}
          />
        </div>
        <div className="token-bar-lab">
          out <b>{fmtK(tokOut)}</b>
        </div>
      </div>
    </div>
  );
}

// ── Secondary tabs ───────────────────────────────────────────────────────────

function RevisionsTab({ revisions }: { revisions: RevisionRow[] }) {
  if (revisions.length === 0) {
    return (
      <div className="viz-frame viz-empty-state">
        No sandbox revisions for this agent yet.
      </div>
    );
  }
  return (
    <div className="viz-frame">
      <table className="ovw-table">
        <thead>
          <tr>
            <th style={{ width: 28 }} />
            <th>Revision</th>
            <th>Image</th>
            <th>Ready</th>
            <th>Workers</th>
            <th>Age</th>
          </tr>
        </thead>
        <tbody>
          {revisions.map((r) => (
            <tr key={r.name}>
              <td>
                <span className={"gws-pip" + (r.ready ? "" : " err")} />
              </td>
              <td style={{ fontFamily: "var(--font-mono)" }}>
                {r.name}
                {r.active && (
                  <span className="chip chip--ink" style={{ marginLeft: 8 }}>
                    active
                  </span>
                )}
              </td>
              <td style={{ fontFamily: "var(--font-mono)", fontSize: 14 }}>
                {shortImage(r.image)}
              </td>
              <td>
                <span
                  className={r.ready ? "chip chip--leaf" : "chip chip--coral"}
                >
                  {r.ready ? "Ready" : "Not ready"}
                </span>
              </td>
              <td style={{ fontFamily: "var(--font-mono)" }}>
                {r.readyWorkers}
              </td>
              <td
                style={{
                  fontFamily: "var(--font-mono)",
                  color: "var(--text-muted)",
                }}
              >
                {r.age}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function EgressTab({
  egress,
  onOpenGateway,
}: {
  egress: AgentEgressRow[];
  onOpenGateway?: (name: string) => void;
}) {
  if (egress.length === 0) {
    return (
      <div className="viz-frame viz-empty-state">
        This agent references no egress gateways.
      </div>
    );
  }
  return (
    <div className="viz-frame">
      <table className="ovw-table">
        <thead>
          <tr>
            <th style={{ width: 28 }} />
            <th>EgressGateway</th>
            <th>Namespace</th>
            <th>Listeners</th>
            <th>Status</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {egress.map((g) => (
            <tr
              key={g.name}
              className={g.exists && onOpenGateway ? "gws-row" : undefined}
              onClick={
                g.exists && onOpenGateway
                  ? () => onOpenGateway(g.name)
                  : undefined
              }
            >
              <td>
                <span className={"gws-pip" + (g.ready ? "" : " err")} />
              </td>
              <td
                style={{
                  fontFamily: "var(--font-mono)",
                  fontWeight: 500,
                  color: "var(--apx-blue-deep)",
                }}
              >
                {g.name}
              </td>
              <td
                style={{
                  fontFamily: "var(--font-mono)",
                  color: "var(--text-muted)",
                }}
              >
                {g.namespace}
              </td>
              <td style={{ fontFamily: "var(--font-mono)" }}>
                {g.exists ? g.listeners : "—"}
              </td>
              <td>
                <span
                  className={g.ready ? "chip chip--leaf" : "chip chip--coral"}
                >
                  {g.exists
                    ? g.ready
                      ? "Ready"
                      : g.statusReason || "Degraded"
                    : "Not found"}
                </span>
              </td>
              <td
                style={{
                  textAlign: "right",
                  color: "var(--text-muted)",
                  fontFamily: "var(--font-mono)",
                }}
              >
                {g.exists && onOpenGateway ? "→" : ""}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function EventsTab({ events }: { events: AgentEventRow[] }) {
  if (events.length === 0) {
    return <div className="viz-frame viz-empty-state">No recent events.</div>;
  }
  return (
    <div className="viz-frame ovw-events">
      {events.map((e, i) => (
        <div key={i} className="ovw-event">
          <div className="ovw-event-time">{relTime(e.time)}</div>
          <div
            className={
              "ovw-event-rail" +
              (e.tone === "error"
                ? " ovw-event-rail--error"
                : e.tone === "warn"
                  ? " ovw-event-rail--warn"
                  : "")
            }
          />
          <div className="ovw-event-body">
            <div className="ovw-event-line">
              <span
                className={
                  e.tone === "error"
                    ? "chip chip--coral"
                    : e.tone === "warn"
                      ? "chip chip--amber"
                      : "chip"
                }
              >
                {e.type}
              </span>
            </div>
            <div className="ovw-event-detail">{e.message}</div>
          </div>
        </div>
      ))}
    </div>
  );
}

// ── Small shared bits ────────────────────────────────────────────────────────

function Stat({
  lab,
  val,
  unit,
  tone,
}: {
  lab: string;
  val: string;
  unit?: string;
  tone?: "ok" | "err";
}) {
  const color =
    tone === "ok"
      ? "var(--apx-leaf)"
      : tone === "err"
        ? "var(--apx-coral)"
        : undefined;
  return (
    <div className="stat">
      <div className="lab">{lab}</div>
      <div className="val" style={color ? { color } : undefined}>
        {val}
        {unit && <span className="unit">{unit}</span>}
      </div>
    </div>
  );
}

function KvBlock({ rows }: { rows: Array<[string, ReactNode]> }) {
  return (
    <div className="kvblock">
      {rows.map(([k, v], i) => (
        <div key={i} className="kvrow">
          <div className="kvk">{k}</div>
          <div className="kvv">{v}</div>
        </div>
      ))}
    </div>
  );
}

// Pretty-print a single JSON document so a captured body is readable. NDJSON,
// SSE, and non-JSON payloads aren't one parseable value — pass them through
// verbatim rather than corrupting them.
function prettyBody(text: string): string {
  const t = text.trim();
  if (t.startsWith("{") || t.startsWith("[")) {
    try {
      return JSON.stringify(JSON.parse(t), null, 2);
    } catch {
      /* not a single JSON doc — show as captured */
    }
  }
  return text;
}

// Build the BodyBox views for a span's response. A streamed LLM response carries
// both the assembled message (gen_ai.response.content) and the raw captured
// frames, so it gets Decoded / Raw views; everything else is just the captured
// body.
function responseViews(s: Span): BodyView[] {
  const views: BodyView[] = [];
  // `!= null` (not truthy): a legitimately empty assembled message ('') still
  // gets a Decoded view rather than silently degrading to Raw-only.
  if (s.respContent != null) {
    views.push({
      id: "decoded",
      label: "Decoded",
      text: s.respContent,
      note: "assembled response",
    });
  }
  if (s.respBody) {
    views.push({
      id: "raw",
      label: s.respContent ? "Raw" : undefined,
      text: prettyBody(s.respBody.text),
      bytes: s.respBody.bytes,
      truncated: s.respBody.truncated,
      note: s.stream ? "raw event stream" : "captured at span open",
    });
  }
  return views;
}

function strip(cols: number): CSSProperties {
  return { ["--strip-cols" as string]: cols } as CSSProperties;
}

function metricStr(v: number | null): string {
  return v == null ? "—" : fmtK(v);
}

function laneFill(lane: Lane, ok: boolean): string {
  if (!ok) return "var(--apx-coral)";
  return lane === "inbound"
    ? "var(--apx-ink)"
    : lane === "llm"
      ? "var(--apx-blue)"
      : lane === "mcp"
        ? "var(--apx-graphite)"
        : "var(--apx-stone)";
}
function laneStroke(lane: Lane, ok: boolean): string {
  if (!ok) return "var(--apx-coral)";
  return lane === "inbound"
    ? "var(--apx-ink)"
    : lane === "llm"
      ? "var(--apx-blue-deep)"
      : lane === "mcp"
        ? "var(--apx-ink-2)"
        : "var(--apx-graphite)";
}

function clamp(v: number, lo: number, hi: number): number {
  return Math.max(lo, Math.min(hi, v));
}

// A wall clock that re-renders the caller on an interval while `active`, so a
// live view keeps advancing. When inactive it holds its last sample, which is
// all a preset (anchored) window needs.
function useNowMs(active: boolean, intervalMs = 1000): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!active) return;
    const id = setInterval(() => setNow(Date.now()), intervalMs);
    return () => clearInterval(id);
  }, [active, intervalMs]);
  return now;
}

function niceTicks(min: number, max: number, count: number): number[] {
  if (!(max > min)) return [min];
  const step = niceStep((max - min) / count);
  const ticks: number[] = [];
  // Walk nice multiples up to the window end; don't force-append the raw `max`.
  // A non-nice end (e.g. 10.9s sitting next to 10s) crowds two labels at the
  // edge, and the panel's right border already marks where the window ends.
  for (let v = Math.ceil(min / step) * step; v <= max + step * 1e-6; v += step)
    ticks.push(v);
  return ticks.length ? ticks : [min];
}
function niceStep(rough: number): number {
  if (rough <= 0) return 1;
  const exp = Math.floor(Math.log10(rough));
  const base = rough / Math.pow(10, exp);
  const nice = base <= 1 ? 1 : base <= 2 ? 2 : base <= 5 ? 5 : 10;
  return nice * Math.pow(10, exp);
}
function fmtTickMs(ms: number): string {
  if (ms === 0) return "0";
  if (ms >= 1000) return (ms / 1000).toFixed(ms % 1000 === 0 ? 0 : 1) + "s";
  return Math.round(ms) + "ms";
}
function relTime(iso: string): string {
  if (!iso) return "—";
  const ms = Date.now() - Date.parse(iso);
  if (Number.isNaN(ms)) return iso;
  return fmtAgo(ms);
}
