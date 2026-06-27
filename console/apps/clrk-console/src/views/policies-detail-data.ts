// Policy DETAIL view-model: maps one live policy CR (plus the cluster's egress
// routes and gateways) into the shape behind the bespoke per-policy detail page
// (the CLRK Dashboard design's `view-policies.jsx` PolicyDetailPage). The list
// (policies-data.ts) is kind-uniform; the detail adds the two things a single
// policy needs but a row does not:
//
//   1. Where it lands — `effective[]` resolves the policy's spec.targetRefs
//      against the live routes/gateways: a route-kind targetRef is a `direct`
//      attachment; an EgressGateway targetRef is a `catch-all` whose effect is
//      `inherited` by every route on that gateway. Standalone kinds (rlp/lp)
//      carry no targetRefs, so they resolve to nothing — the panel says so
//      rather than inventing routes (the real API exposes no reverse index).
//   2. What it does — a status-aware headline + a per-kind structured spec, plus
//      a flat key/value list for the raw-paths toggle.
//
// Side-effect free and unit-tested (policies-detail-data.test.ts); the route
// container (`_shell.policies_.$kind.$name.tsx`) does the I/O and edit wiring.

import type { K8sObject } from '@apoxy/console-core'
import { fmtAge } from './egress-data'
import {
  POLICY_SHORT,
  attachSummary,
  deriveStatus,
  isAttachedKind,
  type AttachView,
  type PolicyKind,
  type PolicyObj,
  type PolicySpec,
  type PolicyStatus,
  type PolTone,
  type TargetRef,
} from './policies-data'

// clrk egress routes reference their gateway by this group/kind — a parentRef
// must match both, not just the name, so an inbound HTTPRoute bound to an
// ingress Gateway of the same name doesn't masquerade as an egress attachment.
const EGRESS_GROUP = 'clrk.apoxy.dev'
const EGRESS_KIND = 'EgressGateway'

/** How a policy reaches a route: a direct targetRef, gateway-wide inheritance,
 *  or a route-rule reference (standalone kinds). */
export type EffVia = 'direct' | 'inherited' | 'reference'

/** One route a policy is in effect on, resolved against the live objects. */
export interface PolicyEffectiveRoute {
  via: EffVia
  routeKind: string
  routeName: string
  gatewayName: string
  host: string
  /** The targetRef's sectionName, when it scopes attachment to one section. */
  section?: string
}

/** A declared targetRef, with whether it resolved to a live object — drives the
 *  attachment map (an unresolved target is the orphaned/pending case). */
export interface PolicyAttachTarget {
  kind: string
  name: string
  resolved: boolean
  catchAll: boolean
}

/** High-level, jargon-free placement counts for the status band. */
export interface PolicyVitals {
  scope: string
  routeCount: number
  gwCount: number
}

/** One raw `spec.<path> = value` row for the spec card's raw-paths toggle. */
export interface PolicySpecField {
  k: string
  v: string
}

export interface PolicyDetail {
  kind: PolicyKind
  short: string
  name: string
  namespace: string
  age: string
  author?: string
  status: string
  tone: PolTone
  statusReason?: string
  /** Status-aware, plain-language placement sentence. */
  headline: string
  /** One-line description of what the policy does. */
  summary: string
  attach: AttachView
  vitals: PolicyVitals
  effective: PolicyEffectiveRoute[]
  targets: PolicyAttachTarget[]
  /** Typed spec, read directly by the per-kind spec cards. */
  spec: PolicySpec
  /** Flat raw-path list for the spec card's raw toggle. */
  fields: PolicySpecField[]
}

// ── route/gateway resolution ─────────────────────────────────────────────────

interface RouteObj extends K8sObject {
  kind: string
  spec?: { parentRefs?: TargetRef[]; hostnames?: string[] }
}

function routeAttachesToGateway(route: RouteObj, gwName: string | undefined): boolean {
  if (!gwName) return false
  return (route.spec?.parentRefs ?? []).some(
    (p) =>
      p.name === gwName &&
      (p.group === undefined || p.group === EGRESS_GROUP) &&
      (p.kind === undefined || p.kind === EGRESS_KIND),
  )
}

/** The EgressGateway a route attaches to, if any of its parentRefs names one. */
function gatewayForRoute(route: RouteObj, gwNames: Set<string>): string | undefined {
  for (const p of route.spec?.parentRefs ?? []) {
    if (
      p.name &&
      gwNames.has(p.name) &&
      (p.group === undefined || p.group === EGRESS_GROUP) &&
      (p.kind === undefined || p.kind === EGRESS_KIND)
    ) {
      return p.name
    }
  }
  return undefined
}

/** Display host for a route: its first declared hostname, else its name (the
 *  AIProviderRoute case, which matches by provider and declares no hostname). */
function routeHost(route: RouteObj): string {
  const h = route.spec?.hostnames
  if (Array.isArray(h) && h.length > 0 && typeof h[0] === 'string') return h[0]
  return route.metadata.name ?? 'route'
}

function isCatchAll(spec: PolicySpec | undefined): boolean {
  return (spec?.targetRefs ?? []).some((t) => t.kind === EGRESS_KIND)
}

/**
 * Resolve the routes a policy is in effect on. Attached kinds walk their
 * targetRefs: a route-kind ref is a `direct` hit on that route; an EgressGateway
 * ref fans out to every route on that gateway as `inherited`. Standalone kinds
 * resolve to nothing — they are applied by a route rule that names them, which
 * the policy object itself gives no way to find.
 */
function resolveEffective(
  kind: PolicyKind,
  spec: PolicySpec | undefined,
  routes: RouteObj[],
  gwNames: Set<string>,
): PolicyEffectiveRoute[] {
  if (!isAttachedKind(kind)) return []
  const out: PolicyEffectiveRoute[] = []
  const seen = new Set<string>()
  const push = (e: PolicyEffectiveRoute) => {
    const key = `${e.routeKind}/${e.routeName}`
    if (seen.has(key)) return
    seen.add(key)
    out.push(e)
  }
  for (const t of spec?.targetRefs ?? []) {
    if (t.kind === EGRESS_KIND) {
      for (const r of routes) {
        if (routeAttachesToGateway(r, t.name)) {
          push({
            via: 'inherited',
            routeKind: r.kind,
            routeName: r.metadata.name ?? '',
            gatewayName: t.name ?? '',
            host: routeHost(r),
          })
        }
      }
    } else {
      const r = routes.find((x) => x.kind === t.kind && x.metadata.name === t.name)
      if (r) {
        push({
          via: 'direct',
          routeKind: r.kind,
          routeName: r.metadata.name ?? '',
          gatewayName: gatewayForRoute(r, gwNames) ?? '—',
          host: routeHost(r),
          section: t.sectionName,
        })
      }
    }
  }
  return out
}

/** The declared targets, with resolution, for the attachment map. */
function resolveTargets(
  kind: PolicyKind,
  spec: PolicySpec | undefined,
  routes: RouteObj[],
  gwNames: Set<string>,
): PolicyAttachTarget[] {
  if (!isAttachedKind(kind)) return []
  return (spec?.targetRefs ?? []).map((t): PolicyAttachTarget => {
    const catchAll = t.kind === EGRESS_KIND
    const resolved = catchAll
      ? gwNames.has(t.name ?? '')
      : routes.some((r) => r.kind === t.kind && r.metadata.name === t.name)
    return { kind: t.kind ?? '?', name: t.name ?? '?', resolved, catchAll }
  })
}

// ── status / prose ───────────────────────────────────────────────────────────

/** The message on the first non-accepted ancestor, surfaced as the reason a
 *  policy isn't in effect. */
function failureReason(status: PolicyStatus | undefined): string | undefined {
  for (const a of status?.ancestors ?? []) {
    const c = (a.conditions ?? []).find((x) => x.type === 'Accepted')
    if (c && c.status !== 'True') return c.message || c.reason || undefined
  }
  return undefined
}

function lowerFirst(s: string): string {
  return s ? s.charAt(0).toLowerCase() + s.slice(1) : s
}

const plural = (n: number, unit: string) => `${n} ${unit}${n === 1 ? '' : 's'}`

/** A status-aware sentence placing the policy: where it lands and whether it is
 *  actually in effect. Mirrors the design's `polSummary` headline, told in
 *  scope/route/gateway terms rather than attachment-mechanism jargon. */
function headlineFor(
  kind: PolicyKind,
  tone: PolTone,
  scope: string,
  routeCount: number,
  gwCount: number,
  reason: string | undefined,
): string {
  if (!isAttachedKind(kind)) {
    return 'Referenced by route rules — applied wherever a route rule names this policy.'
  }
  if (tone === 'ok' && routeCount > 0) {
    return scope === 'Gateway'
      ? `Enforced gateway-wide — inherited by ${plural(routeCount, 'route')} on ${plural(gwCount, 'gateway')}.`
      : `Enforced on ${plural(routeCount, 'route')} across ${plural(gwCount, 'gateway')}.`
  }
  if (tone === 'ok') {
    return 'Accepted, but it currently matches no routes — not in effect.'
  }
  return reason
    ? `Not in effect — ${lowerFirst(reason)}`
    : 'Not in effect — waiting on its target to resolve.'
}

/** A one-line description of what the policy does, derived from its spec (the
 *  annotation override lets an operator hand-write one). */
function summaryFor(kind: PolicyKind, spec: PolicySpec | undefined): string {
  switch (kind) {
    case 'CredentialInjectionPolicy': {
      const secret = spec?.secretRef?.name ?? 'a secret'
      if (spec?.target === 'Header')
        return `Injects ${secret} into the ${spec?.headerName ?? '?'} request header.`
      if (spec?.target === 'QueryParam')
        return `Appends ${secret} as the ?${spec?.queryParamName ?? '?'} query parameter.`
      if (spec?.target === 'ProviderAuth')
        return `Signs requests with ${spec?.providerAuth?.type ?? 'provider auth'} using ${secret}.`
      return `Injects ${secret} into matched requests.`
    }
    case 'EgressDenyPolicy':
      return `Rejects every matched request with ${spec?.denyResponse?.statusCode ?? 403} — no upstream call is made.`
    case 'RateLimitPolicy':
      return `Limits matched traffic to ${spec?.requests ?? '?'} requests per ${spec?.window ?? '?'}, scoped ${spec?.scope ?? 'PerAgent'}.`
    case 'LoggingPolicy': {
      const req = Boolean(spec?.captureRequest)
      const res = Boolean(spec?.captureResponse)
      const cap = req && res ? 'request and response bodies' : req ? 'request bodies' : res ? 'response bodies' : 'summary metadata only'
      return `Captures ${cap} for matched traffic.`
    }
    case 'FallbackRoutingPolicy': {
      const codes = spec?.retry?.retriableStatusCodes
      const retriable = codes && codes.length ? codes.join(', ') : '429, 503'
      return `Retries failed attempts against the next backend in order, on ${retriable}.`
    }
    default:
      return ''
  }
}

// ── raw spec fields (the "Raw paths" toggle) ────────────────────────────────

function targetRefRows(spec: PolicySpec | undefined): PolicySpecField[] {
  return (spec?.targetRefs ?? []).map((t, i) => ({
    k: `targetRefs[${i}]`,
    v: `${t.kind ?? '?'}/${t.name ?? '?'}${t.sectionName ? `#${t.sectionName}` : ''}`,
  }))
}

function specFields(kind: PolicyKind, spec: PolicySpec | undefined): PolicySpecField[] {
  const f: PolicySpecField[] = []
  const add = (k: string, v: unknown) => {
    if (v !== undefined && v !== null && v !== '') f.push({ k, v: String(v) })
  }
  switch (kind) {
    case 'CredentialInjectionPolicy':
      f.push(...targetRefRows(spec))
      add('secretRef.name', spec?.secretRef?.name)
      add('secretKey', spec?.secretKey ?? 'token')
      add('target', spec?.target)
      add('headerName', spec?.headerName)
      add('queryParamName', spec?.queryParamName)
      add('providerAuth.type', spec?.providerAuth?.type)
      add('providerAuth.service', spec?.providerAuth?.service)
      add('providerAuth.region', spec?.providerAuth?.region)
      break
    case 'EgressDenyPolicy':
      f.push(...targetRefRows(spec))
      add('denyResponse.statusCode', spec?.denyResponse?.statusCode ?? 403)
      add('denyResponse.message', spec?.denyResponse?.message)
      break
    case 'RateLimitPolicy':
      add('requests', spec?.requests)
      add('window', spec?.window)
      add('scope', spec?.scope ?? 'PerAgent')
      break
    case 'LoggingPolicy':
      add('captureRequest', Boolean(spec?.captureRequest))
      add('captureResponse', Boolean(spec?.captureResponse))
      ;(spec?.redactHeaders ?? []).forEach((h, i) => add(`redactHeaders[${i}]`, h))
      add('sinkRef', spec?.sinkRef)
      break
    case 'FallbackRoutingPolicy':
      f.push(...targetRefRows(spec))
      add('retry.numRetries', spec?.retry?.numRetries)
      if (spec?.retry?.retriableStatusCodes?.length)
        add('retry.retriableStatusCodes', spec.retry.retriableStatusCodes.join(', '))
      add('retry.perTryTimeout', spec?.retry?.perTryTimeout)
      add('ejection.maxEjectionTime', spec?.ejection?.maxEjectionTime)
      break
  }
  return f
}

// ── public mapper ────────────────────────────────────────────────────────────

/**
 * Map one policy CR (tagged with its `kind`) plus the cluster-wide egress routes
 * and gateways into the detail shape. `routes` items must each carry a `kind`
 * (the route container tags them by the list they came from); `gateways` are the
 * EgressGateway list.
 */
export function mapPolicyDetail(
  kind: PolicyKind,
  obj: PolicyObj,
  routes: K8sObject[],
  gateways: K8sObject[],
): PolicyDetail {
  const spec = obj.spec
  const gwNames = new Set(
    gateways.map((g) => g.metadata.name).filter((n): n is string => Boolean(n)),
  )
  const routeObjs = routes as RouteObj[]

  const effective = resolveEffective(kind, spec, routeObjs, gwNames)
  const targets = resolveTargets(kind, spec, routeObjs, gwNames)
  const st = deriveStatus(kind, obj.status)
  const reason = st.tone === 'warn' ? failureReason(obj.status) : undefined

  const scope = !isAttachedKind(kind) ? 'Route rule' : isCatchAll(spec) ? 'Gateway' : 'Route'
  const gwCount = new Set(
    effective.map((e) => e.gatewayName).filter((g) => g && g !== '—'),
  ).size
  const vitals: PolicyVitals = { scope, routeCount: effective.length, gwCount }

  const anno = obj.metadata.annotations ?? {}
  return {
    kind,
    short: POLICY_SHORT[kind],
    name: obj.metadata.name ?? '',
    namespace: obj.metadata.namespace ?? 'default',
    age: fmtAge(obj.metadata.creationTimestamp),
    author: anno['clrk.apoxy.dev/author'],
    status: st.label,
    tone: st.tone,
    statusReason: reason,
    headline: headlineFor(kind, st.tone, scope, vitals.routeCount, vitals.gwCount, reason),
    summary: anno['clrk.apoxy.dev/summary'] ?? summaryFor(kind, spec),
    attach: attachSummary(kind, spec),
    vitals,
    effective,
    targets,
    spec: spec ?? {},
    fields: specFields(kind, spec),
  }
}

// ── attachment graph payload ─────────────────────────────────────────────────

/** Edge relationship in the attachment graph. `unresolved` is a declared
 *  targetRef that matched no live object (the orphaned/pending case). */
export type AttachVia = 'direct' | 'inherited' | 'reference' | 'unresolved'

/** One downstream node of the attachment graph. */
export interface AttachTarget {
  kind: string
  name: string
  host: string
  via: AttachVia
  section?: string
}

/** The resolved graph the React Flow attachment view renders: the policy, an
 *  optional catch-all gateway hop, and the route nodes it reaches. */
export interface AttachPayload {
  name: string
  /** Eyebrow over the policy node, e.g. `CIP · POLICY`. */
  short: string
  mode: 'targetRefs' | 'referencedBy'
  catchAll: boolean
  gateway: string | null
  targets: AttachTarget[]
}

/**
 * Project a PolicyDetail into the attachment graph. Prefers the resolved
 * `effective` routes; for an attached policy that resolved nothing, falls back
 * to its declared targets drawn as `unresolved`; a standalone policy has no
 * concrete target, so it points at a single synthetic "route rules" node.
 */
export function attachPayload(detail: PolicyDetail): AttachPayload {
  const attached = isAttachedKind(detail.kind)
  const catchAll = detail.targets.some((t) => t.catchAll)
  const gateway = detail.targets.find((t) => t.catchAll)?.name ?? null

  let targets: AttachTarget[]
  if (detail.effective.length > 0) {
    targets = detail.effective.map((e) => ({
      kind: e.routeKind,
      name: e.routeName,
      host: e.host,
      via: e.via,
      section: e.section,
    }))
  } else if (attached) {
    targets = detail.targets
      .filter((t) => !t.catchAll)
      .map((t) => ({ kind: t.kind, name: t.name, host: t.name, via: 'unresolved' as const }))
  } else {
    targets = [{ kind: '', name: 'route rules', host: 'route rules', via: 'reference' }]
  }

  return {
    name: detail.name,
    short: `${detail.short.toUpperCase()} · POLICY`,
    mode: attached ? 'targetRefs' : 'referencedBy',
    catchAll,
    gateway,
    targets,
  }
}
