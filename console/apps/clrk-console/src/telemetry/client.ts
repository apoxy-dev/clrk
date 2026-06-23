// The clrk telemetry data layer. Two read paths the generic watch-backed
// `useK8sList` can't serve:
//   - the metrics rollup (`metrics.clrk.apoxy.dev` taskagentmetrics /
//     daemonagentmetrics) is a fresh ClickHouse aggregation per read and is not
//     watchable, so it's a plain LIST (no WatchManager subscription);
//   - the OTLP traces subresource (`taskagents/{name}/traces` etc.) returns raw
//     protojson TracesData, not a k8s List, so it's a decorated raw GET.
// Both reuse the ConsoleClient's GVRClient + RequestDecorator so project scoping
// and auth headers are applied exactly as for every other request.

import type { ConsoleClient, GVR, K8sObject } from '@apoxy/console-core'
import type { OtlpTracesData } from './otlp'

export type AgentKind = 'TaskAgent' | 'DaemonAgent'

const CLRK_GROUP = 'clrk.apoxy.dev'
const VERSION = 'v1alpha1'
const METRICS_GROUP = 'metrics.clrk.apoxy.dev'

/** A per-agent metrics snapshot: name/namespace match the agent it summarizes,
 *  `usage` is a flat map of resource.Quantity strings (see api/metrics/v1alpha1
 *  UsageList — invocations, errors, active, input_tokens, output_tokens,
 *  tool_calls, latency_p50_ms, latency_p99_ms). */
export interface AgentMetrics extends K8sObject {
  timestamp?: string
  window?: string
  usage?: Record<string, string>
}

const METRICS_GVR: Record<AgentKind, GVR> = {
  TaskAgent: { group: METRICS_GROUP, version: VERSION, resource: 'taskagentmetrics' },
  DaemonAgent: { group: METRICS_GROUP, version: VERSION, resource: 'daemonagentmetrics' },
}

const AGENT_RESOURCE: Record<AgentKind, string> = {
  TaskAgent: 'taskagents',
  DaemonAgent: 'daemonagents',
}

/** List the per-agent rollup for a kind, cluster-wide (one object per agent). */
export async function listAgentMetrics(
  client: ConsoleClient,
  kind: AgentKind,
): Promise<AgentMetrics[]> {
  const list = await client.gvr.list<AgentMetrics>(METRICS_GVR[kind])
  return list.items
}

/** Read a usage key off a metrics snapshot as a number (0 when absent/unparseable). */
export function usage(m: AgentMetrics | undefined, key: string): number {
  const v = m?.usage?.[key]
  if (v == null) return 0
  const n = Number(v)
  return Number.isFinite(n) ? n : 0
}

export interface TraceQuery {
  /** Scope to one invocation id. */
  invocation?: string
  /** RFC3339 lower/upper time bounds. */
  since?: string
  until?: string
  /** Newest-first row cap (server clamps to [1, 1000], default 100). */
  limit?: number
}

/**
 * GET an agent's OTLP traces subresource as raw protojson TracesData. The path is
 * the namespaced agent subresource the apiserver mounts; scoping/auth ride the
 * shared RequestDecorator, the same one the GVRClient uses for every k8s call.
 */
export async function fetchAgentTraces(
  client: ConsoleClient,
  kind: AgentKind,
  namespace: string,
  name: string,
  q: TraceQuery = {},
): Promise<OtlpTracesData> {
  const params = new URLSearchParams()
  if (q.invocation) params.set('invocation', q.invocation)
  if (q.since) params.set('since', q.since)
  if (q.until) params.set('until', q.until)
  if (q.limit) params.set('limit', String(q.limit))
  const qs = params.toString()
  const base = `/apis/${CLRK_GROUP}/${VERSION}/namespaces/${namespace}/${AGENT_RESOURCE[kind]}/${name}/traces`
  const path = qs ? `${base}?${qs}` : base

  const { url, headers } = client.gvr.decorator.decorate({
    path,
    method: 'GET',
    headers: new Headers({ Accept: 'application/json' }),
  })
  const res = await fetch(url, { method: 'GET', headers })
  if (!res.ok) {
    throw new Error(`traces request failed: ${res.status} ${res.statusText}`)
  }
  return (await res.json()) as OtlpTracesData
}
