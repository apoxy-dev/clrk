// EGRESS GATEWAY DETAIL — mapping live k8s objects to the Miller-columns
// hierarchy of the CLRK Dashboard design (`view-egress-detail.jsx`):
//
//   EgressGateway
//   └─ Listener            (spec.listeners[] ⋈ status.listeners[])
//      └─ Route            (MCPRoute · AIProviderRoute · EgressL4Route · HTTPRoute,
//         └─ Rule           attached by parentRefs[].sectionName)
//            └─ Filters     (inline TokenBudget/ToolPolicy + attached policy CRDs)
//            └─ Targets     (backendRefs, or passthrough)
//
// Inline filters come straight off the route's `spec.rules[].filters` (the real
// CRD shape: TokenBudget on AIProviderRoute, ToolPolicy on MCPRoute). The
// remaining policy chips (CredentialInjection, RateLimit, Logging, Deny) are
// separate CRDs that attach to a route via parentRef/targetRef — the mapper
// resolves them by route name and shows them on the route's rules. Per-gateway
// RPS is telemetry the CRDs don't carry, so it rides a demo annotation (the mock
// seeds it) and is null otherwise, pending the OTLP/ClickHouse wiring.

import type { K8sObject } from '@apoxy/console-core'
import { fmtAge, kindShort, readiness } from './egress-data'

export type ChipVariant = '' | 'blue' | 'amber' | 'coral' | 'leaf'

export interface EgFilter {
  type: string
  detail: string
  variant: ChipVariant
}
export interface EgBackend {
  name: string
  port: number
  weight: number
  health: 'ok' | 'warn'
}
export interface EgMatch {
  kind: string
  value: string
  models?: string[]
  endpoints?: string[]
}
export interface EgRule {
  id: string
  match: EgMatch
  filters: EgFilter[]
  backends: EgBackend[]
}
export interface EgRoute {
  id: string
  kind: string
  listener: string
  name: string
  hostnames: string[]
  rules: EgRule[]
}
export interface EgListener {
  id: string
  name: string
  protocol: string
  port: number
  tlsMode: string | null
  attachedRoutes: number
  backendAddress: string
  ready: boolean
}
export interface EgEvent {
  time: string
  type: string
  message: string
  tone: 'ok' | 'warn' | 'error'
}
export interface EgDetail {
  name: string
  namespace: string
  defaultPolicy: string
  ready: boolean
  status: string
  statusReason?: string
  address: string
  age: string
  rps: number | null
  otlpEndpoint?: string
  listeners: EgListener[]
  routes: EgRoute[]
  events: EgEvent[]
}

// ── Loosely-typed views over the erased k8s objects ──────────────────────────
interface ParentRef {
  group?: string
  kind?: string
  name?: string
  sectionName?: string
}
interface BackendRef {
  name?: string
  port?: number
  weight?: number
}
interface RawRule {
  matches?: Array<Record<string, unknown>>
  filters?: Array<Record<string, unknown>>
  backendRefs?: BackendRef[]
}
interface GatewayObj extends K8sObject {
  spec?: {
    defaultPolicy?: string
    listeners?: Array<{
      name?: string
      protocol?: string
      port?: number
      tls?: { mode?: string }
    }>
    otlp?: { endpoint?: string }
  }
  status?: {
    conditions?: Array<{
      type?: string
      status?: string
      message?: string
      reason?: string
      lastTransitionTime?: string
    }>
    listeners?: Array<{
      name?: string
      port?: number
      backendAddress?: string
      attachedRoutes?: number
      conditions?: Array<{ type?: string; status?: string }>
    }>
  }
}
interface RouteObj extends K8sObject {
  kind: string
  spec?: { parentRefs?: ParentRef[]; hostnames?: string[]; rules?: RawRule[] }
}
interface PolicyObj extends K8sObject {
  kind: string
  spec?: Record<string, unknown>
}

// The canonical host an AIProviderRoute's provider dials — AIProviderRoute has no
// hostnames field (it matches by provider), so the host shown is display-derived
// from the provider, the way the llmcall provider registry resolves it.
const PROVIDER_HOST: Record<string, string> = {
  openai: 'api.openai.com',
  anthropic: 'api.anthropic.com',
  google: 'generativelanguage.googleapis.com',
  'azure-openai': 'azure-openai.openai.azure.com',
  bedrock: 'bedrock-runtime.amazonaws.com',
}

/** Compact a token count to a short label (`20M`, `1.2k`). */
export function fmtTokens(n: number): string {
  if (n >= 1e9) return `${trim(n / 1e9)}B`
  if (n >= 1e6) return `${trim(n / 1e6)}M`
  if (n >= 1e3) return `${trim(n / 1e3)}k`
  return String(n)
}
function trim(v: number): string {
  return (v >= 10 ? Math.round(v) : Number(v.toFixed(1))).toString()
}

function listenerReady(st?: {
  conditions?: Array<{ type?: string; status?: string }>
}): boolean {
  const conds = st?.conditions ?? []
  const programmed =
    conds.find((c) => c.type === 'Programmed') ??
    conds.find((c) => c.type === 'Accepted')
  return programmed ? programmed.status === 'True' : true
}

function str(v: unknown): string | undefined {
  return typeof v === 'string' ? v : undefined
}
function arr(v: unknown): string[] | undefined {
  return Array.isArray(v)
    ? (v.filter((x) => typeof x === 'string') as string[])
    : undefined
}

/** Build the match shown in the Rules column from the route's first match. */
function ruleMatch(kind: string, rule: RawRule): EgMatch {
  const matches = rule.matches ?? []
  const m = (matches[0] ?? {}) as Record<string, unknown>
  if (kind === 'AIProviderRoute') {
    return {
      kind: 'provider',
      value: str(m.provider) ?? 'custom',
      models: arr(m.models),
      endpoints: arr(m.endpoints),
    }
  }
  if (kind === 'MCPRoute') {
    const tools = arr(m.tools)
    if (tools) return { kind: 'tools', value: tools.join(', ') }
    const methods = arr(m.methods)
    if (methods) return { kind: 'method', value: methods.join(', ') }
    const servers = arr(m.servers)
    if (servers) return { kind: 'server', value: servers.join(', ') }
    return { kind: 'tools', value: '*' }
  }
  if (kind === 'EgressL4Route') {
    const hosts = arr(m.destinationHostnames)
    if (hosts) return { kind: 'sni', value: hosts.join(', ') }
    const cidrs = arr(m.destinationCIDRs)
    if (cidrs) return { kind: 'cidr', value: cidrs.join(', ') }
    return { kind: 'cidr', value: '0.0.0.0/0' }
  }
  // HTTPRoute: path match, else the union of method matches.
  for (const mm of matches) {
    const path = (mm as { path?: { value?: string } }).path
    if (path?.value) return { kind: 'path', value: path.value }
  }
  const methods = matches
    .map((mm) => str((mm as Record<string, unknown>).method))
    .filter(Boolean) as string[]
  if (methods.length) return { kind: 'method', value: methods.join(', ') }
  return { kind: 'path', value: '/*' }
}

/** Inline filters declared on the rule itself (TokenBudget, ToolPolicy). */
function inlineFilters(kind: string, rule: RawRule): EgFilter[] {
  const out: EgFilter[] = []
  for (const f of rule.filters ?? []) {
    const type = str(f.type)
    if (kind === 'AIProviderRoute' && type === 'TokenBudget') {
      const tb = (f.tokenBudget ?? {}) as {
        maxTokensPerDay?: number
        maxTokensPerExecution?: number
      }
      const parts: string[] = []
      if (tb.maxTokensPerDay)
        parts.push(`MaxTokensPerDay=${fmtTokens(tb.maxTokensPerDay)}`)
      if (tb.maxTokensPerExecution)
        parts.push(
          `MaxTokensPerExecution=${fmtTokens(tb.maxTokensPerExecution)}`,
        )
      out.push({
        type: 'TokenBudget',
        detail: parts.join(' · ') || 'token budget',
        variant: 'blue',
      })
    } else if (kind === 'MCPRoute' && type === 'ToolPolicy') {
      const tp = (f.toolPolicy ?? {}) as {
        allowedTools?: string[]
        deniedTools?: string[]
        requireConfirmation?: string[]
        maxCallsPerExecution?: number
        rateLimits?: Array<{ requests?: number; window?: string }>
      }
      const detail = tp.allowedTools?.length
        ? `allowedTools=${tp.allowedTools.join(',')}`
        : tp.requireConfirmation?.length
          ? `requireConfirmation=${tp.requireConfirmation.join(',')}`
          : tp.maxCallsPerExecution
            ? `maxCallsPerExecution=${tp.maxCallsPerExecution}`
            : 'tool policy'
      out.push({ type: 'ToolPolicy', detail, variant: 'blue' })
      const rl = tp.rateLimits?.[0]
      if (rl?.requests)
        out.push({
          type: 'RateLimit',
          detail: `${rl.requests} / ${rl.window ?? '1m'}`,
          variant: 'amber',
        })
    } else if (type === 'ExtensionRef') {
      const ref = (f.extensionRef ?? {}) as { name?: string }
      out.push({
        type: 'ExtensionRef',
        detail: ref.name ?? 'extension',
        variant: '',
      })
    }
  }
  return out
}

/** Policy CRDs that attach to `routeName` (of `routeKind`) via targetRefs → chips. */
function attachedFilters(
  routeName: string,
  routeKind: string,
  policies: PolicyObj[],
): EgFilter[] {
  const out: EgFilter[] = []
  // Match on kind too, not just name: an EgressGateway and an AIProviderRoute
  // can share a name (different kinds), and a gateway-scoped CIP must not chip
  // onto the same-named route's row. targetRefs always carry a kind; legacy
  // parentRefs / singular targetRef may omit it, so a missing kind is tolerated.
  const matches = (r: ParentRef): boolean =>
    r.name === routeName && (r.kind === undefined || r.kind === routeKind)
  const attachesTo = (p: PolicyObj): boolean => {
    // clrk Direct Policy Attachment policies (CredentialInjection, Fallback,
    // EgressDeny) attach via spec.targetRefs (GEP-2648). Tolerate the legacy
    // parentRefs and the pre-consolidation singular targetRef for objects or
    // mocks not yet migrated.
    const targetRefs = (p.spec?.targetRefs as ParentRef[] | undefined) ?? []
    if (targetRefs.some(matches)) return true
    const refs = (p.spec?.parentRefs as ParentRef[] | undefined) ?? []
    if (refs.some(matches)) return true
    const target = p.spec?.targetRef as ParentRef | undefined
    return target !== undefined && matches(target)
  }
  for (const p of policies) {
    if (!attachesTo(p)) continue
    const s = p.spec ?? {}
    if (p.kind === 'CredentialInjectionPolicy') {
      const secret =
        (s.secretRef as { name?: string } | undefined)?.name ?? 'secret'
      const target = str(s.target) ?? 'Header'
      const header = str(s.headerName)
      out.push({
        type: 'CredentialInjection',
        detail: `${secret} → ${target}${header ? `: ${header}` : ''}`,
        variant: 'blue',
      })
    } else if (p.kind === 'RateLimitPolicy') {
      out.push({
        type: 'RateLimit',
        detail: `${s.requests as number} / ${s.window as string}`,
        variant: 'amber',
      })
    } else if (p.kind === 'LoggingPolicy') {
      const summary = str(p.metadata.annotations?.['clrk.apoxy.dev/summary'])
      const cap = [
        s.captureRequest ? 'request' : '',
        s.captureResponse ? 'response' : '',
      ]
        .filter(Boolean)
        .join(' + ')
      out.push({
        type: 'LoggingPolicy',
        detail: summary ?? cap ?? 'logging',
        variant: '',
      })
    } else if (p.kind === 'EgressDenyPolicy') {
      const code =
        (s.denyResponse as { statusCode?: number } | undefined)?.statusCode ??
        403
      out.push({ type: 'Deny', detail: `deny ${code}`, variant: 'coral' })
    }
  }
  return out
}

function routeHosts(route: RouteObj): string[] {
  const declared = route.spec?.hostnames
  if (declared?.length) return declared
  const firstRule = route.spec?.rules?.[0]
  if (route.kind === 'AIProviderRoute') {
    const provider = str(
      (firstRule?.matches?.[0] as Record<string, unknown> | undefined)
        ?.provider,
    )
    return [
      (provider && PROVIDER_HOST[provider]) ||
        route.metadata.name ||
        'upstream',
    ]
  }
  if (route.kind === 'EgressL4Route') {
    const m = (firstRule?.matches?.[0] ?? {}) as Record<string, unknown>
    return [
      arr(m.destinationHostnames)?.[0] ?? arr(m.destinationCIDRs)?.[0] ?? '*',
    ]
  }
  return [route.metadata.name || 'route']
}

/** Resolve the listener (by name) a route attaches to on this gateway. */
function listenerFor(
  route: RouteObj,
  gatewayName: string,
  listenerNames: Set<string>,
): string | null {
  for (const p of route.spec?.parentRefs ?? []) {
    if (p.name !== gatewayName) continue
    const section = p.sectionName
    if (!section) return null
    if (listenerNames.has(section)) return section
    // Tolerate the controller's `<listener>--<shape>` section encoding.
    const head = section.split('--')[0] ?? section
    if (listenerNames.has(head)) return head
    return section
  }
  return null
}

function events(gw: GatewayObj): EgEvent[] {
  const conds = gw.status?.conditions ?? []
  return conds.map((c) => ({
    time: c.lastTransitionTime ?? '',
    type: c.reason || c.type || 'Event',
    message: c.message ?? '',
    tone: c.status === 'True' ? 'ok' : 'error',
  }))
}

/**
 * Map one EgressGateway plus the routes and policies in scope to the detail
 * hierarchy. `routes` and `policies` are the cluster-wide lists (each item tagged
 * with its `kind`); the mapper filters to those attached to this gateway.
 */
export function mapEgressDetail(
  gateway: K8sObject,
  routes: K8sObject[],
  policies: K8sObject[],
): EgDetail {
  const gw = gateway as GatewayObj
  const name = gw.metadata.name ?? ''
  const namespace = gw.metadata.namespace ?? 'default'
  const statusListeners = new Map(
    (gw.status?.listeners ?? []).map((l) => [l.name, l]),
  )

  const listeners: EgListener[] = (gw.spec?.listeners ?? []).map(
    (l): EgListener => {
      const st = statusListeners.get(l.name)
      return {
        id: l.name ?? '',
        name: l.name ?? '',
        protocol: l.protocol ?? 'TCP',
        port: l.port ?? 0,
        tlsMode: l.tls?.mode ?? null,
        attachedRoutes: st?.attachedRoutes ?? 0,
        backendAddress: st?.backendAddress ?? (st?.port ? `:${st.port}` : ''),
        ready: listenerReady(st),
      }
    },
  )
  const listenerNames = new Set(listeners.map((l) => l.name))

  const policyObjs = policies as PolicyObj[]
  const mine = (routes as RouteObj[]).filter((r) =>
    (r.spec?.parentRefs ?? []).some((p) => p.name === name),
  )

  const egRoutes: EgRoute[] = mine.map((route): EgRoute => {
    const attached = attachedFilters(
      route.metadata.name ?? '',
      route.kind,
      policyObjs,
    )
    const rawRules = route.spec?.rules ?? []
    return {
      id: route.metadata.name ?? '',
      kind: route.kind,
      listener:
        listenerFor(route, name, listenerNames) ?? listeners[0]?.name ?? '',
      name: route.metadata.name ?? '',
      hostnames: routeHosts(route),
      rules: rawRules.map((rule, i): EgRule => {
        const refs = rule.backendRefs ?? []
        return {
          id: `${route.metadata.name}-${i}`,
          match: ruleMatch(route.kind, rule),
          filters: [...inlineFilters(route.kind, rule), ...attached],
          backends: refs.map(
            (b): EgBackend => ({
              name: b.name ?? 'backend',
              port: b.port ?? 443,
              weight: b.weight ?? Math.round(100 / refs.length),
              health: 'ok',
            }),
          ),
        }
      }),
    }
  })

  const r = readiness(gw)
  const rpsRaw = gw.metadata.annotations?.['clrk.apoxy.dev/demo-rps']
  return {
    name,
    namespace,
    defaultPolicy: gw.spec?.defaultPolicy ?? '—',
    ready: r.ready,
    status: r.ready ? 'Ready' : 'Degraded',
    statusReason: r.reason,
    address: `${name}.${namespace}.eg.clrk.local`,
    age: fmtAge(gw.metadata.creationTimestamp),
    rps: rpsRaw != null ? Number(rpsRaw) : null,
    otlpEndpoint: gw.spec?.otlp?.endpoint,
    listeners,
    routes: egRoutes,
    events: events(gw),
  }
}

export { kindShort }
