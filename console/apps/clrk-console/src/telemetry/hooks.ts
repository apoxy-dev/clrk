// React hooks over the telemetry data layer (telemetry/client.ts). These reads
// are polled, not watched, so they don't go through console-core's WatchManager.
// They also deliberately avoid `@tanstack/react-query`: the QueryClientProvider
// is owned by console-core's bundled copy, so the app's own react-query would
// see a null context. Plain useEffect fetching sidesteps that and works the same
// in dev and prod.

import { useEffect, useMemo, useRef, useState } from "react";
import { useConsoleClient } from "@apoxy/console-core";
import {
  fetchAgentTraces,
  listAgentMetrics,
  type AgentKind,
  type AgentMetrics,
  type TraceQuery,
} from "./client";
import {
  fetchMetricSeries,
  type MetricSeriesQuery,
  type MetricSeriesSet,
} from "./metrics";
import type { OtlpTracesData } from "./otlp";

/** Minimal async result, mirroring the react-query fields the views read. */
export interface AsyncResult<T> {
  data?: T;
  isLoading: boolean;
  isSuccess: boolean;
  error?: Error;
}

const METRICS_POLL_MS = 30_000;

/** The 24h per-agent rollup for a kind, refreshed on an interval. */
export function useAgentMetrics(
  kind: AgentKind,
  enabled = true,
): AsyncResult<AgentMetrics[]> {
  const client = useConsoleClient();
  const [state, setState] = useState<AsyncResult<AgentMetrics[]>>({
    isLoading: enabled,
    isSuccess: false,
  });

  useEffect(() => {
    if (!enabled) {
      setState({ isLoading: false, isSuccess: false });
      return;
    }
    let cancelled = false;
    const run = () => {
      listAgentMetrics(client, kind).then(
        (data) => {
          if (!cancelled) setState({ data, isLoading: false, isSuccess: true });
        },
        (error: Error) => {
          if (!cancelled)
            setState({ isLoading: false, isSuccess: false, error });
        },
      );
    };
    setState((s) => ({ ...s, isLoading: s.data == null }));
    run();
    const id = setInterval(run, METRICS_POLL_MS);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [client, kind, enabled]);

  return state;
}

/**
 * Poll a batch of metric series queries together: one 30s loop, one combined
 * loading/error state, the results returned in query order. Used by the Overview
 * to read its KPI shapes + traffic chart in a single refresh.
 *
 * `build` produces the current-window queries; it is re-invoked on every poll so
 * the [since, until) window advances with real time WITHOUT the timestamps having
 * to flow through a re-render key. `identity` is the scope/geometry the result is
 * valid for (namespace + range) — when it changes the prior window's data must
 * not be shown against the new geometry, so the hook resets to a loading state;
 * a same-identity poll refreshes the data in place (no flicker). `build`/
 * `identity` null (e.g. the scope namespace isn't known yet) disables the poll.
 */
export function useMetricSeriesBatch(
  build: (() => MetricSeriesQuery[]) | null,
  identity: string | null,
): AsyncResult<MetricSeriesSet[]> {
  const client = useConsoleClient();
  const [state, setState] = useState<AsyncResult<MetricSeriesSet[]>>({
    isLoading: false,
    isSuccess: false,
  });
  // Hold the latest builder in a ref so the poll always reads the current window
  // without `build`'s changing identity (a fresh closure each render) re-running
  // the effect — only `identity` drives re-runs.
  const buildRef = useRef(build);
  buildRef.current = build;

  useEffect(() => {
    if (identity == null) {
      setState({ isLoading: false, isSuccess: false });
      return;
    }
    let cancelled = false;
    const run = () => {
      const qs = buildRef.current?.() ?? [];
      Promise.all(qs.map((q) => fetchMetricSeries(client, q))).then(
        (data) => {
          if (!cancelled) setState({ data, isLoading: false, isSuccess: true });
        },
        (error: Error) => {
          if (!cancelled)
            setState({ isLoading: false, isSuccess: false, error });
        },
      );
    };
    // A scope/geometry change: drop the prior window's data so the view shows a
    // loading state instead of stale series binned into the new grid.
    setState({ isLoading: true, isSuccess: false });
    run();
    const id = setInterval(run, METRICS_POLL_MS);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [client, identity]);

  return state;
}

/** Convenience: index a batch result back by metric id (the last write wins on a
 *  duplicate id, which the Overview never issues). */
export function useSeriesByMetric(
  result: AsyncResult<MetricSeriesSet[]>,
): Record<string, MetricSeriesSet> {
  return useMemo(() => {
    const by: Record<string, MetricSeriesSet> = {};
    for (const s of result.data ?? []) by[s.metric] = s;
    return by;
  }, [result.data]);
}

/** One agent's OTLP traces (raw TracesData), filtered by the given query. */
export function useAgentTraces(
  kind: AgentKind,
  namespace: string | undefined,
  name: string | undefined,
  q: TraceQuery = {},
  enabled = true,
): AsyncResult<OtlpTracesData> {
  const client = useConsoleClient();
  const [state, setState] = useState<AsyncResult<OtlpTracesData>>({
    isLoading: false,
    isSuccess: false,
  });
  const qKey = JSON.stringify(q);

  useEffect(() => {
    if (!enabled || !namespace || !name) {
      setState({ isLoading: false, isSuccess: false });
      return;
    }
    let cancelled = false;
    setState((s) => ({ ...s, isLoading: true }));
    fetchAgentTraces(
      client,
      kind,
      namespace,
      name,
      JSON.parse(qKey) as TraceQuery,
    ).then(
      (data) => {
        if (!cancelled) setState({ data, isLoading: false, isSuccess: true });
      },
      (error: Error) => {
        if (!cancelled) setState({ isLoading: false, isSuccess: false, error });
      },
    );
    return () => {
      cancelled = true;
    };
  }, [client, kind, namespace, name, qKey, enabled]);

  return state;
}
