import { describe, expect, it } from "vitest";
import type { MetricSeriesSet } from "../telemetry/metrics";
import type { AgentRow } from "./agents-data";
import type { EgGatewayRow } from "./egress-data";
import {
  buildOverview,
  bucketCount,
  bucketStarts,
  deltaPct,
  densify,
  mostCommonNamespace,
  overviewQueries,
  rangeWindow,
  sum,
  M_ERRORS,
  M_INVOCATIONS,
  M_TOKENS,
  M_TOOLS,
} from "./overview-data";

// --- fixtures ---------------------------------------------------------------

function agent(p: Partial<AgentRow>): AgentRow {
  return {
    id: p.name ?? "a",
    kind: "TaskAgent",
    name: "a",
    namespace: "default",
    image: "img",
    pool: "default",
    revision: "a-1",
    ready: true,
    active: 0,
    warm: 0,
    invocations24h: 0,
    p50ms: null,
    p99ms: null,
    inTokens24h: 0,
    outTokens24h: 0,
    tools24h: 0,
    errors24h: 0,
    schedule: null,
    age: "1d",
    egressRefs: [],
    ...p,
  };
}

function gateway(p: Partial<EgGatewayRow>): EgGatewayRow {
  return {
    id: p.name ?? "g",
    name: "g",
    namespace: "default",
    defaultPolicy: "deny-all",
    ready: true,
    address: "g.default.eg.clrk.local",
    age: "1d",
    rps: 0,
    listenersCount: 1,
    routesCount: 1,
    routeKinds: [],
    ...p,
  };
}

/** A single-series (ungrouped, single-measure) set with points at given (ms, value).
 *  densify reads only the points (it grids off its own args), so the window is set
 *  only where buildOverview needs it (it derives the grid from since/until). */
function single(
  metric: string,
  pts: Array<[number, number]>,
  win: { since: string; until: string } = { since: "", until: "" },
): MetricSeriesSet {
  return {
    metric,
    type: "Counter",
    unit: "",
    since: win.since,
    until: win.until,
    series: [
      {
        points: pts.map(([ms, v]) => ({
          timestamp: new Date(ms).toISOString(),
          value: String(v),
        })),
      },
    ],
  };
}

// --- bucketCount / densify --------------------------------------------------

describe("bucketCount", () => {
  it("counts the UTC-grid buckets that intersect the window", () => {
    expect(bucketCount(0, 4000, 1000)).toBe(4); // aligned
    expect(bucketCount(1620, 5620, 1000)).toBe(5); // misaligned: 5 distinct UTC buckets
  });
  it("is 1 for a degenerate window", () => {
    expect(bucketCount(0, 0, 1000)).toBe(1);
    expect(bucketCount(NaN, 1000, 1000)).toBe(1);
  });
});

describe("densify", () => {
  it("fills empty buckets with 0 and lands points on their UTC bucket index", () => {
    const set = single("m", [
      [0, 5],
      [2400, 7], // floor(2400/1000) = bucket 2
    ]);
    expect(densify(set, 0, 4000, 1000)).toEqual([5, 0, 7, 0]);
  });

  it("clamps out-of-range points into the first/last bucket", () => {
    const set = single("m", [
      [-100, 3], // before since -> bucket 0
      [9000, 4], // past the last bucket -> clamped to bucket 3
    ]);
    expect(densify(set, 0, 4000, 1000)).toEqual([3, 0, 0, 4]);
  });

  it("maps UTC-aligned server buckets 1:1 even when `since` is not step-aligned", () => {
    // since=1620 is offset from the 1000ms UTC grid. The server emits 5 distinct
    // bucket starts (1000..5000) intersecting [1620, 5620); each must land in its
    // own slot (the round-to-`since` approach collided two into index 0).
    const set = single("m", [
      [1000, 10],
      [2000, 10],
      [3000, 10],
      [4000, 10],
      [5000, 10],
    ]);
    expect(densify(set, 1620, 5620, 1000)).toEqual([10, 10, 10, 10, 10]);
  });

  it("sums every series per bucket when no measure is given", () => {
    const set: MetricSeriesSet = {
      metric: M_TOKENS,
      type: "Counter",
      unit: "tokens",
      since: "",
      until: "",
      series: [
        {
          labels: { measure: "input" },
          points: [{ timestamp: new Date(0).toISOString(), value: "100" }],
        },
        {
          labels: { measure: "output" },
          points: [{ timestamp: new Date(0).toISOString(), value: "40" }],
        },
      ],
    };
    expect(densify(set, 0, 2000, 1000)).toEqual([140, 0]);
  });

  it("selects only the named measure when given", () => {
    const set: MetricSeriesSet = {
      metric: M_TOKENS,
      type: "Counter",
      unit: "tokens",
      since: "",
      until: "",
      series: [
        {
          labels: { measure: "input" },
          points: [{ timestamp: new Date(0).toISOString(), value: "100" }],
        },
        {
          labels: { measure: "output" },
          points: [{ timestamp: new Date(0).toISOString(), value: "40" }],
        },
      ],
    };
    expect(densify(set, 0, 2000, 1000, "input")).toEqual([100, 0]);
    expect(densify(set, 0, 2000, 1000, "output")).toEqual([40, 0]);
  });

  it("returns an all-zero array of the grid length for an absent set", () => {
    expect(densify(undefined, 0, 3000, 1000)).toEqual([0, 0, 0]);
  });
});

// --- deltaPct + sum ---------------------------------------------------------

describe("bucketStarts", () => {
  it("aligns each bucket start to the UTC step grid", () => {
    // since=1620 floors to origin bucket start 1000; 5 buckets of width 1000,
    // matching the densify misalignment case (1:1 with the server buckets).
    expect(bucketStarts(1620, 5620, 1000)).toEqual([
      1000, 2000, 3000, 4000, 5000,
    ]);
  });
  it("matches the densify bucket count and origin", () => {
    const starts = bucketStarts(1620, 5620, 1000);
    expect(starts.length).toBe(bucketCount(1620, 5620, 1000));
    expect(starts[0]).toBe(Math.floor(1620 / 1000) * 1000);
  });
  it("returns NaN starts for a non-finite or zero step", () => {
    expect(bucketStarts(0, 1000, 0).every(Number.isNaN)).toBe(true);
    expect(bucketStarts(NaN, 1000, 1000).every(Number.isNaN)).toBe(true);
  });
});

describe("deltaPct", () => {
  it("compares the first third to the last third", () => {
    // len 6, k=2: head avg = (10+10)/2 = 10, tail avg = (20+20)/2 = 20 -> +100%.
    expect(deltaPct([10, 10, 0, 0, 20, 20])).toBe(100);
  });
  it("is 0 when the baseline is empty", () => {
    expect(deltaPct([0, 0, 0, 5, 5, 5])).toBe(0);
  });
  it("is 0 for a degenerate series", () => {
    expect(deltaPct([5])).toBe(0);
  });
});

describe("sum", () => {
  it("adds the array", () => {
    expect(sum([1, 2, 3])).toBe(6);
    expect(sum([])).toBe(0);
  });
});

// --- namespace + window -----------------------------------------------------

describe("mostCommonNamespace", () => {
  it("picks the most frequent namespace", () => {
    const rows = [
      agent({ name: "a", namespace: "platform" }),
      agent({ name: "b", namespace: "platform" }),
      agent({ name: "c", namespace: "research" }),
    ];
    expect(mostCommonNamespace(rows)).toBe("platform");
  });
  it("falls back to default with no agents", () => {
    expect(mostCommonNamespace([])).toBe("default");
  });
});

describe("rangeWindow / overviewQueries", () => {
  const now = Date.parse("2026-06-25T12:00:00Z");

  it("derives buckets + step per range", () => {
    expect(rangeWindow("24h", now).buckets).toBe(24);
    expect(rangeWindow("7d", now).buckets).toBe(28);
    expect(rangeWindow("30d", now).buckets).toBe(30);
    expect(rangeWindow("24h", now).step).toBe("1h");
  });

  it("builds one query per Overview metric, with the resolved window", () => {
    const qs = overviewQueries("platform", "24h", now);
    expect(qs.map((q) => q.metric)).toEqual([
      M_INVOCATIONS,
      M_TOKENS,
      M_TOOLS,
      M_ERRORS,
    ]);
    expect(qs.every((q) => q.namespace === "platform" && q.step === "1h")).toBe(
      true,
    );
    expect(qs[0]?.until).toBe(new Date(now).toISOString());
    expect(qs[0]?.since).toBe(new Date(now - 24 * 3_600_000).toISOString());
  });
});

// --- buildOverview ----------------------------------------------------------

describe("buildOverview", () => {
  const now = Date.parse("2026-06-25T12:00:00Z");
  const h = (n: number) => now - n * 3_600_000; // n hours ago, inside the 24h window
  // The server-resolved window buildOverview grids against (hour-aligned).
  const win = {
    since: new Date(now - 24 * 3_600_000).toISOString(),
    until: new Date(now).toISOString(),
  };

  const series = {
    [M_INVOCATIONS]: single(
      M_INVOCATIONS,
      [
        [h(1), 100],
        [h(2), 50],
      ],
      win,
    ),
    [M_TOKENS]: {
      metric: M_TOKENS,
      type: "Counter",
      unit: "tokens",
      since: win.since,
      until: win.until,
      series: [
        {
          labels: { measure: "input" },
          points: [{ timestamp: new Date(h(1)).toISOString(), value: "900" }],
        },
        {
          labels: { measure: "output" },
          points: [{ timestamp: new Date(h(1)).toISOString(), value: "100" }],
        },
      ],
    } as MetricSeriesSet,
    [M_TOOLS]: single(M_TOOLS, [[h(1), 42]], win),
    [M_ERRORS]: single(M_ERRORS, [[h(1), 50]], win),
  };

  it("sums the range series into the KPI numbers", () => {
    const m = buildOverview({
      range: "24h",
      series,
      agentRows: [
        agent({ name: "a", active: 2 }),
        agent({ name: "b", active: 0 }),
      ],
      gatewayRows: [],
      pools: { ready: 4, total: 4 },
    });
    expect(m.kpis.invocations).toBe(150);
    expect(m.kpis.tokensIn).toBe(900);
    expect(m.kpis.tokensOut).toBe(100);
    expect(m.kpis.tools).toBe(42);
    expect(m.kpis.errors).toBe(50);
    // 50 / (150 + 50) = 25%.
    expect(m.kpis.errorRate).toBeCloseTo(25);
    // active is the cluster-wide CR gauge, not from the series.
    expect(m.kpis.active).toBe(2);
    expect(m.kpis.activeAgents).toBe(1);
  });

  it("produces all-zero shapes (no NaN) before any series load", () => {
    const m = buildOverview({
      range: "24h",
      series: {},
      agentRows: [agent({ name: "a" })],
      gatewayRows: [],
      pools: { ready: 1, total: 1 },
    });
    expect(m.kpis.invocations).toBe(0);
    expect(m.kpis.errorRate).toBe(0);
    expect(m.spark.inv).toHaveLength(24); // nominal grid length for 24h
    expect(m.spark.inv.every((v) => v === 0)).toBe(true);
    expect(m.delta.inv).toBe(0);
  });

  it("reports healthy when everything is ready", () => {
    const m = buildOverview({
      range: "24h",
      series,
      agentRows: [agent({ name: "a" })],
      gatewayRows: [gateway({ name: "g" })],
      pools: { ready: 2, total: 2 },
    });
    expect(m.health.severity).toBe("ok");
    expect(m.health.headline).toBe("All systems operational");
    expect(m.health.attn).toHaveLength(0);
  });

  it("escalates to crit with a failed agent and surfaces an attn chip", () => {
    const m = buildOverview({
      range: "24h",
      series,
      agentRows: [
        agent({ name: "a", ready: false, readyReason: "image pull error" }),
      ],
      gatewayRows: [
        gateway({ name: "g", ready: false, statusReason: "CA missing" }),
      ],
      pools: { ready: 2, total: 2 },
    });
    expect(m.health.severity).toBe("crit");
    expect(m.health.attn).toHaveLength(2);
    expect(m.health.attn[0]).toMatchObject({
      sev: "crit",
      title: "a",
      target: { kind: "agent", id: "a" },
    });
    expect(m.health.attn[1]).toMatchObject({
      sev: "crit",
      title: "g",
      target: { kind: "gateway", id: "g" },
    });
  });

  it("warns (not crit) when only the worker pool is degraded", () => {
    const m = buildOverview({
      range: "24h",
      series,
      agentRows: [agent({ name: "a" })],
      gatewayRows: [gateway({ name: "g" })],
      pools: { ready: 1, total: 3 },
    });
    expect(m.health.severity).toBe("warn");
    expect(m.health.attn).toHaveLength(1);
    expect(m.health.attn[0]).toMatchObject({ sev: "warn" });
    expect(m.health.sub).toContain("1/3 workers up");
  });

  it("ranks top agents by traffic (errors weighted) and gateways by rps", () => {
    const m = buildOverview({
      range: "24h",
      series,
      agentRows: [
        agent({ name: "quiet", invocations24h: 10, errors24h: 0 }),
        agent({ name: "busy", invocations24h: 100, errors24h: 0 }),
        agent({ name: "flaky", invocations24h: 20, errors24h: 30 }), // 20 + 30*5 = 170, beats busy.
      ],
      gatewayRows: [
        gateway({ name: "slow", rps: 5 }),
        gateway({ name: "fast", rps: 200 }),
      ],
      pools: { ready: 1, total: 1 },
    });
    expect(m.topAgents.map((a) => a.name)).toEqual(["flaky", "busy", "quiet"]);
    expect(m.topGateways.map((g) => g.name)).toEqual(["fast", "slow"]);
  });
});
