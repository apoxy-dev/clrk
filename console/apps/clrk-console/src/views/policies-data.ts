// Policies view-model: the pure transforms behind the bespoke Policies list
// (the CLRK Dashboard's `view-policies.jsx` PoliciesPage). The console reads the
// five egress policy CRDs from api/clrk/v1alpha1/types_egresspolicies.go and
// folds them into one uniform PolicyRow keyed by kind. Two attachment shapes,
// faithful to the Go types:
//   - Direct Policy Attachment (GEP-2648 targetRefs + GEP-2649 PolicyStatus):
//     CredentialInjectionPolicy, FallbackRoutingPolicy, EgressDenyPolicy.
//   - Standalone (no targetRefs, no status; referenced by route rules):
//     RateLimitPolicy, LoggingPolicy.
// Kept side-effect free and unit-tested (policies-data.test.ts); the route
// container (`_shell.policies.tsx`) does the I/O.

import type { K8sObject } from '@apoxy/console-core'
import { fmtAge } from './egress-data'

// Canonical kind order — the grouped table and filter tags both render in this
// order so the page layout is stable regardless of which kinds have objects.
export const POLICY_KINDS = [
  'CredentialInjectionPolicy',
  'FallbackRoutingPolicy',
  'EgressDenyPolicy',
  'RateLimitPolicy',
  'LoggingPolicy',
] as const

export type PolicyKind = (typeof POLICY_KINDS)[number]

/** Short tag rendered in the group header + kind-tag, matching the CRD
 *  `shortName` markers (cip/frp/edp/rlp/lp). */
export const POLICY_SHORT: Record<PolicyKind, string> = {
  CredentialInjectionPolicy: 'cip',
  FallbackRoutingPolicy: 'frp',
  EgressDenyPolicy: 'edp',
  RateLimitPolicy: 'rlp',
  LoggingPolicy: 'lp',
}

/** Plural GVR resource per kind (clrk.apoxy.dev/v1alpha1). */
export const POLICY_RESOURCE: Record<PolicyKind, string> = {
  CredentialInjectionPolicy: 'credentialinjectionpolicies',
  FallbackRoutingPolicy: 'fallbackroutingpolicies',
  EgressDenyPolicy: 'egressdenypolicies',
  RateLimitPolicy: 'ratelimitpolicies',
  LoggingPolicy: 'loggingpolicies',
}

// Kinds that attach via spec.targetRefs and report GEP-2649 PolicyStatus. The
// other two (rlp, lp) are standalone: they carry no targetRefs and no status
// subresource, so neither an attachment target nor an acceptance state can be
// read from the object itself.
const ATTACHED_KINDS = new Set<PolicyKind>([
  'CredentialInjectionPolicy',
  'FallbackRoutingPolicy',
  'EgressDenyPolicy',
])

/** Whether a kind attaches via targetRefs (and so carries a PolicyStatus). */
export function isAttachedKind(kind: PolicyKind): boolean {
  return ATTACHED_KINDS.has(kind)
}

// --- loosely-typed views over the k8s objects (the registry erases to
// K8sObject; list items frequently omit `kind`, so the route tags each item
// with the kind of the list it came from). ---

export interface TargetRef {
  group?: string
  kind?: string
  name?: string
  sectionName?: string
}
export interface PolicyCondition {
  type?: string
  status?: string
  reason?: string
  message?: string
}
export interface PolicyAncestor {
  conditions?: PolicyCondition[]
}
export interface PolicyStatus {
  ancestors?: PolicyAncestor[]
}
export interface ProviderAuthConfig {
  type?: string
  service?: string
  region?: string
}
export interface PolicySpec {
  targetRefs?: TargetRef[]
  // CredentialInjectionPolicy
  secretRef?: { name?: string }
  secretKey?: string
  target?: string
  headerName?: string
  queryParamName?: string
  providerAuth?: ProviderAuthConfig
  // EgressDenyPolicy
  denyResponse?: { statusCode?: number; message?: string }
  // RateLimitPolicy
  requests?: number
  window?: string
  scope?: string
  // LoggingPolicy
  captureRequest?: boolean
  captureResponse?: boolean
  redactHeaders?: string[]
  sinkRef?: string
  // FallbackRoutingPolicy
  retry?: { numRetries?: number; retriableStatusCodes?: number[]; perTryTimeout?: string }
  ejection?: { maxEjectionTime?: string }
}

export interface PolicyObj extends K8sObject {
  spec?: PolicySpec
  status?: PolicyStatus
}

/** Acceptance tone, shared by the row pip and the status chip so they never
 *  disagree: `ok` = Accepted, `warn` = not (yet) in effect, `none` = the kind
 *  reports no acceptance state at all (standalone rlp/lp). */
export type PolTone = 'ok' | 'warn' | 'none'

export interface PolicyStatusView {
  label: string
  tone: PolTone
}

/**
 * Derive a policy's acceptance from its GEP-2649 PolicyStatus. Standalone kinds
 * have no status, so they report `none`. An attached kind with no ancestors has
 * not been reconciled yet (Pending); otherwise it is Accepted only when every
 * ancestor carries Accepted=True, and Conflicted when a refusal cites a
 * conflict.
 */
export function deriveStatus(kind: PolicyKind, status: PolicyStatus | undefined): PolicyStatusView {
  if (!isAttachedKind(kind)) return { label: '—', tone: 'none' }
  const ancestors = status?.ancestors ?? []
  if (ancestors.length === 0) return { label: 'Pending', tone: 'warn' }
  let conflicted = false
  let accepted = true
  for (const a of ancestors) {
    const cond = (a.conditions ?? []).find((c) => c.type === 'Accepted')
    if (!cond || cond.status !== 'True') {
      accepted = false
      if ((cond?.reason ?? '').toLowerCase().includes('conflict')) conflicted = true
    }
  }
  if (accepted) return { label: 'Accepted', tone: 'ok' }
  return { label: conflicted ? 'Conflicted' : 'Pending', tone: 'warn' }
}

function targetRefLabel(t: TargetRef): string {
  return `${t.kind ?? '?'}/${t.name ?? '?'}`
}

/** A gateway-wide targetRef is a catch-all — it claims any traffic on the
 *  gateway no narrower policy matched. */
function catchAllTarget(refs: TargetRef[]): TargetRef | undefined {
  return refs.find((t) => t.kind === 'EgressGateway')
}

export interface AttachView {
  via: string
  target: string
}

/** One-line attachment descriptor for a list row. */
export function attachSummary(kind: PolicyKind, spec: PolicySpec | undefined): AttachView {
  if (!isAttachedKind(kind)) {
    // rlp + lp don't bind themselves — a route rule references them.
    return { via: 'referenced by rules', target: '—' }
  }
  const refs = spec?.targetRefs ?? []
  if (refs.length === 0) return { via: 'targetRefs', target: '—' }
  const gw = catchAllTarget(refs)
  if (gw) return { via: 'targetRef · catch-all', target: `EgressGateway/${gw.name ?? '?'}` }
  return {
    via: refs.length === 1 ? 'targetRef' : 'targetRefs',
    target: refs.length === 1 ? targetRefLabel(refs[0]!) : `${refs.length} targets`,
  }
}

/** Compact, kind-specific spec summary for a list row. */
export function specLine(kind: PolicyKind, spec: PolicySpec | undefined): string {
  switch (kind) {
    case 'CredentialInjectionPolicy': {
      const secret = spec?.secretRef?.name ?? '—'
      if (spec?.target === 'Header') return `${secret} → ${spec?.headerName ?? '?'}`
      if (spec?.target === 'QueryParam') return `${secret} → ?${spec?.queryParamName ?? '?'}`
      if (spec?.target === 'ProviderAuth') return `ProviderAuth · ${spec?.providerAuth?.type ?? '?'}`
      return spec?.target ?? ''
    }
    case 'EgressDenyPolicy':
      return `deny → ${spec?.denyResponse?.statusCode ?? 403}`
    case 'RateLimitPolicy':
      return `${spec?.requests ?? '?'} / ${spec?.window ?? '?'} · ${spec?.scope ?? 'PerAgent'}`
    case 'LoggingPolicy': {
      const req = Boolean(spec?.captureRequest)
      const res = Boolean(spec?.captureResponse)
      return req && res ? 'capture req+res' : req ? 'capture req' : res ? 'capture res' : 'summary only'
    }
    case 'FallbackRoutingPolicy': {
      const codes = spec?.retry?.retriableStatusCodes
      const retriable = codes && codes.length ? codes.join(',') : '429,503'
      const n = spec?.retry?.numRetries
      return n != null ? `fallback · ${n}× · ${retriable}` : `fallback · ${retriable}`
    }
    default:
      return ''
  }
}

/** One row of the Policies table — a kind-uniform projection of a policy CR. */
export interface PolicyRow {
  /** Stable key across kinds: `${kind}:${namespace}:${name}`. */
  id: string
  kind: PolicyKind
  name: string
  namespace: string
  /** How the policy binds (targetRef / targetRefs / catch-all / reference). */
  via: string
  /** The bound target, or `—` when standalone / unresolved. */
  target: string
  /** Kind-specific one-line spec summary. */
  spec: string
  /** Acceptance label (Accepted / Pending / Conflicted / —). */
  status: string
  tone: PolTone
  /** Number of declared targetRefs (0 for standalone kinds). */
  effective: number
  age: string
}

export interface PolicyGroup {
  kind: PolicyKind
  items: PolicyObj[]
}

/** Map the five live policy lists to table rows, in canonical kind order. */
export function mapPolicies(groups: PolicyGroup[]): PolicyRow[] {
  const byKind = new Map(groups.map((g) => [g.kind, g.items]))
  const rows: PolicyRow[] = []
  for (const kind of POLICY_KINDS) {
    for (const o of byKind.get(kind) ?? []) {
      const name = o.metadata?.name ?? ''
      const namespace = o.metadata?.namespace ?? 'default'
      const attach = attachSummary(kind, o.spec)
      const st = deriveStatus(kind, o.status)
      rows.push({
        id: `${kind}:${namespace}:${name}`,
        kind,
        name,
        namespace,
        via: attach.via,
        target: attach.target,
        spec: specLine(kind, o.spec),
        status: st.label,
        tone: st.tone,
        effective: o.spec?.targetRefs?.length ?? 0,
        age: fmtAge(o.metadata?.creationTimestamp),
      })
    }
  }
  return rows
}

/** Per-kind row counts, in canonical order, for the stat strip + filter tags. */
export function countByKind(rows: PolicyRow[]): Record<PolicyKind, number> {
  const counts = Object.fromEntries(POLICY_KINDS.map((k) => [k, 0])) as Record<PolicyKind, number>
  for (const r of rows) counts[r.kind] += 1
  return counts
}
