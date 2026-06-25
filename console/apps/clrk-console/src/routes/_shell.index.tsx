// Overview (`/`): the CLRK landing page. It joins three reads the design's
// dashboard needs: the live CRs (TaskAgents / DaemonAgents / EgressGateways /
// WorkerPools, cluster-wide), the Tier-1 per-agent rollup (the tables' 24h
// columns), and the Tier-2 metric series (the range-driven KPI shapes + traffic
// chart). The KPI numbers + charts come from the range-scoped Tier-2 series; the
// tables, "active now", and health banner come from the cluster-wide CRs + the
// Tier-1 snapshot. See overview-data.ts for the transforms.

import { useEffect, useMemo, useState } from "react";
import { createFileRoute, useRouter } from "@tanstack/react-router";
import { useK8sList, type GVR, type K8sObject } from "@apoxy/console-core";
import { OverviewView } from "../views/overview";
import { mapAgents } from "../views/agents-data";
import { mapEgress } from "../views/egress-data";
import {
  buildOverview,
  mostCommonNamespace,
  overviewQueries,
  OVW_RANGES,
  type OvwRange,
  type PoolTally,
} from "../views/overview-data";
import {
  useAgentMetrics,
  useMetricSeriesBatch,
  useSeriesByMetric,
} from "../telemetry/hooks";

const gvr = (resource: string): GVR => ({
  group: "clrk.apoxy.dev",
  version: "v1alpha1",
  resource,
});
const TASK_GVR = gvr("taskagents");
const DAEMON_GVR = gvr("daemonagents");
const EG_GVR = gvr("egressgateways");
const WP_GVR = gvr("workerpools");

const RANGE_KEY = "clrk.ovwRange";

export const Route = createFileRoute("/_shell/")({ component: Overview });

interface WorkerPoolObj extends K8sObject {
  spec?: { replicas?: number };
  status?: { readyReplicas?: number };
}

function Overview() {
  const router = useRouter();
  const [range, setRange] = useState<OvwRange>(() => {
    const saved =
      typeof localStorage !== "undefined"
        ? localStorage.getItem(RANGE_KEY)
        : null;
    return saved && (OVW_RANGES as string[]).includes(saved)
      ? (saved as OvwRange)
      : "24h";
  });
  useEffect(() => {
    if (typeof localStorage !== "undefined")
      localStorage.setItem(RANGE_KEY, range);
  }, [range]);

  const task = useK8sList(TASK_GVR);
  const daemon = useK8sList(DAEMON_GVR);
  const gateways = useK8sList(EG_GVR);
  const pools = useK8sList(WP_GVR);
  const taskMetrics = useAgentMetrics("TaskAgent");
  const daemonMetrics = useAgentMetrics("DaemonAgent");

  const { rows: agentRows } = useMemo(
    () =>
      mapAgents(
        task.data?.items ?? [],
        daemon.data?.items ?? [],
        taskMetrics.data ?? [],
        daemonMetrics.data ?? [],
        Date.now(),
      ),
    [task.data, daemon.data, taskMetrics.data, daemonMetrics.data],
  );

  const { rows: gatewayRows } = useMemo(
    () => mapEgress(gateways.data?.items ?? [], []),
    [gateways.data],
  );

  const poolTally = useMemo<PoolTally>(() => {
    const items = (pools.data?.items ?? []) as WorkerPoolObj[];
    return items.reduce<PoolTally>(
      (s, p) => {
        s.ready += p.status?.readyReplicas ?? 0;
        s.total += p.spec?.replicas ?? 1;
        return s;
      },
      { ready: 0, total: 0 },
    );
  }, [pools.data]);

  // The Tier-2 fleet series is namespace-scoped; pin it to where the agents live
  // once the lists have loaded (so we don't fire a throwaway `default` query
  // first). `identity` is the scope+range the data is valid for — when it changes
  // the hook resets to loading; `build` is re-invoked each poll to read a fresh
  // window from the clock, so the advancing window never churns the poll.
  const agentsLoaded = task.isSuccess && daemon.isSuccess;
  const namespace = agentsLoaded ? mostCommonNamespace(agentRows) : null;
  const identity = namespace ? `${namespace}|${range}` : null;
  const build = useMemo(
    () =>
      namespace ? () => overviewQueries(namespace, range, Date.now()) : null,
    [namespace, range],
  );
  const seriesResult = useMetricSeriesBatch(build, identity);
  const series = useSeriesByMetric(seriesResult);

  const model = useMemo(
    () =>
      buildOverview({
        range,
        series,
        agentRows,
        gatewayRows,
        pools: poolTally,
      }),
    [range, series, agentRows, gatewayRows, poolTally],
  );

  return (
    <OverviewView
      model={model}
      range={range}
      onRange={setRange}
      agentCount={agentRows.length}
      gatewayCount={gatewayRows.length}
      isLoading={task.isLoading || daemon.isLoading || gateways.isLoading}
      metricsReady={seriesResult.isSuccess}
      onOpenAgent={(id) =>
        void router.navigate({ to: `/agents/${id}` as never })
      }
      onOpenGateway={(id) =>
        void router.navigate({ to: `/egress/${id}` as never })
      }
      onAll={(path) => void router.navigate({ to: `/${path}` as never })}
    />
  );
}
