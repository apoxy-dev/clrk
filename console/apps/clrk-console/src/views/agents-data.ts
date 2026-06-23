// Maps live TaskAgent + DaemonAgent objects (and their metrics rollup) into the
// combined rows + header totals the Agents list renders. Structural fields come
// from the CRs (api/clrk/v1alpha1 TaskAgent/DaemonAgent); the 24h activity
// columns (invocations, tokens, tool calls, errors, p50/p99) come from the
// matching metrics.clrk.apoxy.dev snapshot, joined by namespace/name. A row's
// metric field is null until its snapshot loads, and the view renders null as
// "—" rather than a misleading 0.

import type { K8sObject } from '@apoxy/console-core'
import { usage, type AgentKind, type AgentMetrics } from '../telemetry/client'

const DESC_ANNOTATION = 'clrk.apoxy.dev/description'

/** The agent fields the mapper reads (everything else stays untyped). */
interface AgentObject extends K8sObject {
  spec?: {
    template?: { spec?: { image?: string } }
    workerPoolRef?: string
    schedule?: string
    egressRefs?: Array<{ gatewayRef?: string }>
  }
  status?: {
    phase?: string
    conditions?: Array<{ type: string; status: string; reason?: string; message?: string }>
    activeExecutions?: number
    restartCount?: number
    upSince?: string
    latestReadyRevisionName?: string
    latestCreatedRevisionName?: string
  }
}

export interface AgentRow {
  /** Stable id used in the URL — the agent name (unique within the rail). */
  id: string
  kind: AgentKind
  name: string
  namespace: string
  image: string
  pool: string
  revision: string
  ready: boolean
  readyReason?: string
  description?: string
  /** In-flight executions (TaskAgent); null for DaemonAgents (single-instance). */
  active: number | null
  invocations24h: number | null
  p50ms: number | null
  p99ms: number | null
  inTokens24h: number | null
  outTokens24h: number | null
  tools24h: number | null
  errors24h: number | null
  /** Cron schedule (TaskAgent) or null. */
  schedule: string | null
  /** DaemonAgent lifecycle phase. */
  phase?: string
  restarts?: number
  /** Humanized uptime for a DaemonAgent (e.g. "14d"). */
  upSince?: string
  age: string
  creationTimestamp?: string
  egressRefs: string[]
}

export interface AgentTotals {
  agents: number
  ready: number
  degraded: number
  invocations: number
  tokensIn: number
  tokensOut: number
  tools: number
  errors: number
  active: number
}

/** A short age like "23d" / "4h" / "12s" from an ISO timestamp. */
export function ageShort(iso?: string, nowMs: number = Date.now()): string {
  if (!iso) return '—'
  const t = Date.parse(iso)
  if (Number.isNaN(t)) return '—'
  const s = Math.max(0, Math.floor((nowMs - t) / 1000))
  if (s < 60) return `${s}s`
  const m = Math.floor(s / 60)
  if (m < 60) return `${m}m`
  const h = Math.floor(m / 60)
  if (h < 24) return `${h}h`
  return `${Math.floor(h / 24)}d`
}

function findCondition(a: AgentObject, type: string) {
  return (a.status?.conditions ?? []).find((c) => c.type === type)
}

function num(m: AgentMetrics | undefined, key: string): number | null {
  return m ? usage(m, key) : null
}

function mapKind(
  agents: K8sObject[],
  metrics: AgentMetrics[],
  kind: AgentKind,
  nowMs: number,
): AgentRow[] {
  const byKey = new Map(
    metrics.map((m) => [`${m.metadata.namespace ?? ''}/${m.metadata.name ?? ''}`, m]),
  )
  const isTask = kind === 'TaskAgent'
  return agents.map((obj) => {
    const a = obj as AgentObject
    const namespace = a.metadata.namespace ?? 'default'
    const name = a.metadata.name ?? ''
    const m = byKey.get(`${namespace}/${name}`)

    // Readiness: a TaskAgent's lives in its Ready condition (assume healthy when
    // unstated); a DaemonAgent's is its lifecycle phase.
    let ready: boolean
    let readyReason: string | undefined
    if (isTask) {
      const cond = findCondition(a, 'Ready') ?? findCondition(a, 'Available')
      ready = cond ? cond.status === 'True' : true
      if (!ready) readyReason = cond?.message ?? cond?.reason
    } else {
      const phase = a.status?.phase
      ready = phase === 'Running'
      if (!ready) readyReason = phase ? `Phase ${phase}` : 'Not running'
    }

    const activeFromCr = a.status?.activeExecutions ?? null
    return {
      id: name,
      kind,
      name,
      namespace,
      image: a.spec?.template?.spec?.image ?? '—',
      pool: a.spec?.workerPoolRef ?? '—',
      revision:
        a.status?.latestReadyRevisionName ?? a.status?.latestCreatedRevisionName ?? '—',
      ready,
      readyReason,
      description: a.metadata.annotations?.[DESC_ANNOTATION],
      active: isTask ? (m ? usage(m, 'active') : activeFromCr) : null,
      invocations24h: num(m, 'invocations'),
      p50ms: isTask ? num(m, 'latency_p50_ms') : null,
      p99ms: isTask ? num(m, 'latency_p99_ms') : null,
      inTokens24h: num(m, 'input_tokens'),
      outTokens24h: num(m, 'output_tokens'),
      tools24h: num(m, 'tool_calls'),
      errors24h: num(m, 'errors'),
      schedule: isTask ? (a.spec?.schedule ?? null) : null,
      phase: isTask ? undefined : a.status?.phase,
      restarts: isTask ? undefined : (a.status?.restartCount ?? 0),
      upSince: isTask ? undefined : ageShort(a.status?.upSince, nowMs),
      age: ageShort(a.metadata.creationTimestamp, nowMs),
      creationTimestamp: a.metadata.creationTimestamp,
      egressRefs: (a.spec?.egressRefs ?? [])
        .map((r) => r.gatewayRef)
        .filter((g): g is string => Boolean(g)),
    }
  })
}

/** Build the combined, sorted rows + header totals from both kinds. */
export function mapAgents(
  taskAgents: K8sObject[],
  daemonAgents: K8sObject[],
  taskMetrics: AgentMetrics[],
  daemonMetrics: AgentMetrics[],
  nowMs: number = Date.now(),
): { rows: AgentRow[]; totals: AgentTotals } {
  const rows = [
    ...mapKind(taskAgents, taskMetrics, 'TaskAgent', nowMs),
    ...mapKind(daemonAgents, daemonMetrics, 'DaemonAgent', nowMs),
  ].sort((a, b) => a.name.localeCompare(b.name))

  const totals = rows.reduce<AgentTotals>(
    (s, r) => {
      s.agents += 1
      if (r.ready) s.ready += 1
      else s.degraded += 1
      s.invocations += r.invocations24h ?? 0
      s.tokensIn += r.inTokens24h ?? 0
      s.tokensOut += r.outTokens24h ?? 0
      s.tools += r.tools24h ?? 0
      s.errors += r.errors24h ?? 0
      s.active += r.active ?? 0
      return s
    },
    { agents: 0, ready: 0, degraded: 0, invocations: 0, tokensIn: 0, tokensOut: 0, tools: 0, errors: 0, active: 0 },
  )
  return { rows, totals }
}

/** `ghcr.io/acme/review-bot:c41a7e` → `review-bot:c41a7e`. */
export function shortImage(image: string): string {
  const slash = image.lastIndexOf('/')
  return slash >= 0 ? image.slice(slash + 1) : image
}
