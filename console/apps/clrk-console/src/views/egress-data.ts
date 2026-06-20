// Egress Gateways list — mapping from live k8s objects to the design's table
// rows. The console reads EgressGateways (clrk.apoxy.dev/v1alpha1) plus the
// routes that attach to them (MCPRoute / AIProviderRoute / EgressL4Route, and
// gateway-api HTTPRoute) and joins them by `parentRefs[].name`. Structural
// columns come from the objects; per-gateway RPS is telemetry the CRD doesn't
// carry, so it reads a demo annotation when present (the mock seeds it) and
// renders "—" otherwise, pending the OTLP/ClickHouse wiring (APO-785).

import type { K8sObject } from '@apoxy/console-core'

export interface EgGatewayRow {
  id: string
  name: string
  namespace: string
  description?: string
  defaultPolicy: string
  ready: boolean
  statusReason?: string
  address: string
  age: string
  /** Requests/sec from telemetry; null when no source is wired. */
  rps: number | null
  listenersCount: number
  routesCount: number
  /** Distinct short route-kind labels (e.g. `['AI', 'HTTP', 'L4']`). */
  routeKinds: string[]
}

export interface EgressTotals {
  gateways: number
  listeners: number
  mcp: number
  ai: number
  l4: number
  degraded: number
  routes: number
}

/** Loosely-typed views over the k8s objects (the registry erases to K8sObject). */
interface ListenerSpec {
  name?: string
}
interface ListenerStatus {
  attachedRoutes?: number
}
interface GatewayObj extends K8sObject {
  spec?: { defaultPolicy?: string; listeners?: ListenerSpec[] }
  status?: {
    conditions?: Array<{ type?: string; status?: string; message?: string }>
    listeners?: ListenerStatus[]
  }
}
interface ParentRef {
  name?: string
}
interface RouteObj extends K8sObject {
  kind: string
  spec?: { parentRefs?: ParentRef[] }
}

const KIND_SHORT: Record<string, string> = {
  MCPRoute: 'MCP',
  AIProviderRoute: 'AI',
  HTTPRoute: 'HTTP',
  EgressL4Route: 'L4',
}

export function kindShort(kind: string): string {
  return KIND_SHORT[kind] ?? kind
}

/** Compact a request rate to a short label (`1.2k`); small values pass through. */
export function fmtK(n: number): string {
  if (n >= 1000) {
    const v = n / 1000
    return `${v >= 10 ? Math.round(v) : v.toFixed(1).replace(/\.0$/, '')}k`
  }
  return String(n)
}

/** Read an EgressGateway's readiness from its conditions (Programmed, then Accepted). */
export function readiness(gw: {
  status?: {
    conditions?: Array<{ type?: string; status?: string; message?: string }>
  }
}): {
  ready: boolean
  reason?: string
} {
  const conds = gw.status?.conditions ?? []
  const programmed = conds.find((c) => c.type === 'Programmed')
  if (programmed)
    return {
      ready: programmed.status === 'True',
      reason: programmed.status === 'True' ? undefined : programmed.message,
    }
  const accepted = conds.find((c) => c.type === 'Accepted')
  return {
    ready: accepted?.status === 'True',
    reason: accepted?.status === 'True' ? undefined : accepted?.message,
  }
}

export function fmtAge(creationTimestamp?: string): string {
  if (!creationTimestamp) return '—'
  const ms = Date.now() - Date.parse(creationTimestamp)
  if (Number.isNaN(ms)) return '—'
  const days = Math.floor(ms / 86_400_000)
  if (days >= 1) return `${days}d`
  const hours = Math.floor(ms / 3_600_000)
  if (hours >= 1) return `${hours}h`
  const mins = Math.max(1, Math.floor(ms / 60_000))
  return `${mins}m`
}

/** Map live EgressGateways + their attaching routes to table rows + strip totals. */
export function mapEgress(
  gateways: K8sObject[],
  routes: K8sObject[],
): { rows: EgGatewayRow[]; totals: EgressTotals } {
  const byGateway = new Map<string, RouteObj[]>()
  for (const r of routes as RouteObj[]) {
    for (const p of r.spec?.parentRefs ?? []) {
      if (!p.name) continue
      const arr = byGateway.get(p.name) ?? []
      arr.push(r)
      byGateway.set(p.name, arr)
    }
  }

  const rows = (gateways as GatewayObj[]).map((gw): EgGatewayRow => {
    const name = gw.metadata.name ?? ''
    const namespace = gw.metadata.namespace ?? 'default'
    const attached = byGateway.get(name) ?? []
    const statusListeners = gw.status?.listeners ?? []
    const routesCount =
      statusListeners.reduce((s, l) => s + (l.attachedRoutes ?? 0), 0) ||
      attached.length
    const r = readiness(gw)
    const rpsRaw = gw.metadata.annotations?.['clrk.apoxy.dev/demo-rps']
    return {
      id: name,
      name,
      namespace,
      description: gw.metadata.annotations?.['clrk.apoxy.dev/description'],
      defaultPolicy: gw.spec?.defaultPolicy ?? '—',
      ready: r.ready,
      statusReason: r.reason,
      address: `${name}.${namespace}.eg.clrk.local`,
      age: fmtAge(gw.metadata.creationTimestamp),
      rps: rpsRaw != null ? Number(rpsRaw) : null,
      listenersCount: gw.spec?.listeners?.length ?? 0,
      routesCount,
      routeKinds: Array.from(new Set(attached.map((rt) => kindShort(rt.kind)))),
    }
  })

  const kindCount = (k: string) =>
    (routes as RouteObj[]).filter((r) => r.kind === k).length
  const totals: EgressTotals = {
    gateways: rows.length,
    listeners: rows.reduce((s, g) => s + g.listenersCount, 0),
    routes: rows.reduce((s, g) => s + g.routesCount, 0),
    mcp: kindCount('MCPRoute'),
    ai: kindCount('AIProviderRoute'),
    l4: kindCount('EgressL4Route'),
    degraded: rows.filter((g) => !g.ready).length,
  }
  return { rows, totals }
}
