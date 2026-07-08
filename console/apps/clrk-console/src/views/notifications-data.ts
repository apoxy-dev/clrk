// Pure transforms for the Notification Center. Notifications are real
// events.k8s.io/v1 Event objects recorded by the clrk control plane; this module
// maps them to a view model, derives category/severity from the frozen reason
// vocabulary (kept in sync with internal/notify/reasons.go), and provides the
// grouping/filter/count helpers the views and the bell badge share. No React,
// no I/O -- unit-tested in notifications-data.test.ts.

import type { K8sObject } from '@apoxy/console-core'

/** An events.k8s.io/v1 Event, plus the deprecated core-v1 fields we fall back
 *  to so a legacy Event still renders. */
export interface EventObj extends K8sObject {
  eventTime?: string
  reason?: string
  action?: string
  note?: string
  type?: string // "Normal" | "Warning"
  reportingController?: string
  reportingInstance?: string
  regarding?: {
    kind?: string
    namespace?: string
    name?: string
    apiVersion?: string
    uid?: string
    resourceVersion?: string
    fieldPath?: string
  }
  series?: { count?: number; lastObservedTime?: string }
  // The recording component/host (events.k8s.io keeps it under deprecatedSource).
  deprecatedSource?: { component?: string; host?: string }
  // Deprecated core-v1 fallbacks.
  message?: string
  involvedObject?: {
    kind?: string
    namespace?: string
    name?: string
    apiVersion?: string
    uid?: string
    resourceVersion?: string
    fieldPath?: string
  }
  deprecatedCount?: number
  deprecatedFirstTimestamp?: string
  deprecatedLastTimestamp?: string
}

export type NotificationCategory =
  | 'security'
  | 'agent'
  | 'rollout'
  | 'fleet'
  | 'other'
export type NotificationSeverity = 'critical' | 'warning' | 'info' | 'success'

/** The full object reference an Event regards, as the detail tray surfaces it. */
export interface InvolvedRef {
  apiVersion: string
  kind: string
  name: string
  namespace: string
  uid: string
  resourceVersion: string
  fieldPath: string
}

export interface NotificationVM {
  id: string
  namespace: string
  category: NotificationCategory
  severity: NotificationSeverity
  reason: string
  action: string
  title: string
  message: string
  regarding: { kind: string; name: string; namespace: string } | null
  type: 'Normal' | 'Warning'
  count: number
  timeMs: number
  read: boolean
  // Detail-tray fields: the full Event surfaced by NotificationDetailTray. The
  // row/bell views ignore them; they are populated from the same Event object.
  involved: InvolvedRef | null
  meta: {
    name: string
    namespace: string
    uid: string
    resourceVersion: string
    creationTimestamp: string
  }
  reportingController: string
  reportingInstance: string
  source: { component: string; host: string }
  firstMs: number
  lastMs: number
  firstTimestamp: string
  lastTimestamp: string
}

// The frozen reason vocabulary (mirror of internal/notify/reasons.go). Grouped
// by category so categorize() is a lookup, not a regex.
const SECURITY_REASONS = new Set([
  'EgressDenied',
  'OrphanSandbox',
  'EgressUpstreamFailed',
  'CredentialInjectionFailed',
  'SecurityAdvisory',
])
const AGENT_REASONS = new Set([
  'InvocationFailed',
  'InvocationTimeout',
  'InvocationRejected',
])
const ROLLOUT_REASONS = new Set(['RevisionReady', 'RolloutStalled'])
const FLEET_REASONS = new Set(['WorkerPoolDegraded', 'WorkerPoolHealthy'])

/** reason -> one of the four locked buckets (else 'other'). */
export function categorize(reason: string): NotificationCategory {
  if (SECURITY_REASONS.has(reason)) return 'security'
  if (AGENT_REASONS.has(reason)) return 'agent'
  if (ROLLOUT_REASONS.has(reason)) return 'rollout'
  if (FLEET_REASONS.has(reason)) return 'fleet'
  return 'other'
}

const SUCCESS_REASONS = new Set([
  'RevisionReady',
  'WorkerPoolHealthy',
])

export function severityOf(
  reason: string,
  type: string,
  category: NotificationCategory,
): NotificationSeverity {
  if (SUCCESS_REASONS.has(reason)) return 'success'
  if (category === 'security' && type === 'Warning') return 'critical'
  if (type === 'Warning') return 'warning'
  return 'info'
}

function toMs(s?: string): number {
  if (!s) return 0
  const t = Date.parse(s)
  return Number.isNaN(t) ? 0 : t
}

/** Most-recent observed time, preferring series/eventTime then deprecated
 *  fields then creationTimestamp. */
export function eventTimeMs(o: EventObj): number {
  return (
    toMs(o.series?.lastObservedTime) ||
    toMs(o.eventTime) ||
    toMs(o.deprecatedLastTimestamp) ||
    toMs(o.metadata.creationTimestamp)
  )
}

/** First-observed time: eventTime is the first sighting under events.k8s.io/v1;
 *  fall back to the deprecated core-v1 timestamp then creationTimestamp. */
export function eventFirstMs(o: EventObj): number {
  return (
    toMs(o.eventTime) ||
    toMs(o.deprecatedFirstTimestamp) ||
    toMs(o.metadata.creationTimestamp)
  )
}

/** First non-empty string among the candidates ('' when none). */
function firstStr(...xs: (string | undefined)[]): string {
  for (const x of xs) if (x) return x
  return ''
}

/** The full object reference an Event regards, or null when it names none. */
export function involvedRef(o: EventObj): InvolvedRef | null {
  const r = o.regarding ?? o.involvedObject
  if (!r || !r.kind || !r.name) return null
  return {
    apiVersion: r.apiVersion ?? '',
    kind: r.kind,
    name: r.name,
    namespace: r.namespace ?? o.metadata.namespace ?? '',
    uid: r.uid ?? '',
    resourceVersion: r.resourceVersion ?? '',
    fieldPath: r.fieldPath ?? '',
  }
}

export function regardingRef(
  o: EventObj,
): { kind: string; name: string; namespace: string } | null {
  const r = o.regarding ?? o.involvedObject
  if (!r || !r.kind || !r.name) return null
  return { kind: r.kind, name: r.name, namespace: r.namespace ?? '' }
}

// A short human title from the reason (camelCase -> spaced words).
function titleFromReason(reason: string): string {
  return reason.replace(/([a-z0-9])([A-Z])/g, '$1 $2')
}

/** Map raw Events to view models, newest-first, flagging read against a
 *  last-seen watermark (ms). */
export function mapEvents(
  items: EventObj[],
  lastSeenMs: number,
): NotificationVM[] {
  const vms = items.map((o) => {
    const reason = o.reason ?? 'Unknown'
    const type = o.type === 'Warning' ? 'Warning' : 'Normal'
    const category = categorize(reason)
    const timeMs = eventTimeMs(o)
    const firstMs = eventFirstMs(o)
    // Resolve the "last seen" timestamp once so the numeric (lastMs) and string
    // (lastTimestamp) forms share one precedence and can never disagree in the
    // detail tray's "Last seen" fact.
    const lastStr = firstStr(
      o.series?.lastObservedTime,
      o.deprecatedLastTimestamp,
      o.eventTime,
      o.metadata.creationTimestamp,
    )
    return {
      id: o.metadata.uid ?? o.metadata.name ?? '',
      namespace: o.metadata.namespace ?? '',
      category,
      severity: severityOf(reason, type, category),
      reason,
      action: o.action ?? '',
      title: titleFromReason(reason),
      message: o.note ?? o.message ?? '',
      regarding: regardingRef(o),
      type: type as 'Normal' | 'Warning',
      count: o.series?.count ?? o.deprecatedCount ?? 1,
      timeMs,
      read: timeMs <= lastSeenMs,
      involved: involvedRef(o),
      meta: {
        name: o.metadata.name ?? '',
        namespace: o.metadata.namespace ?? '',
        uid: o.metadata.uid ?? '',
        resourceVersion: o.metadata.resourceVersion ?? '',
        creationTimestamp: o.metadata.creationTimestamp ?? '',
      },
      reportingController: o.reportingController ?? '',
      reportingInstance: o.reportingInstance ?? '',
      source: {
        component: o.deprecatedSource?.component ?? o.reportingController ?? '',
        host: o.deprecatedSource?.host ?? '',
      },
      firstMs,
      lastMs: toMs(lastStr) || timeMs,
      firstTimestamp: firstStr(
        o.eventTime,
        o.deprecatedFirstTimestamp,
        o.metadata.creationTimestamp,
      ),
      lastTimestamp: lastStr,
    } satisfies NotificationVM
  })
  vms.sort((a, b) => b.timeMs - a.timeMs)
  return vms
}

export function unreadCount(vms: NotificationVM[]): number {
  return vms.reduce((n, v) => (v.read ? n : n + 1), 0)
}

export const CATEGORY_ORDER: NotificationCategory[] = [
  'security',
  'agent',
  'rollout',
  'fleet',
  'other',
]

export const CATEGORY_META: Record<
  NotificationCategory,
  { label: string }
> = {
  security: { label: 'Security' },
  agent: { label: 'Agent runs' },
  rollout: { label: 'Rollouts' },
  fleet: { label: 'Fleet health' },
  other: { label: 'Other' },
}

export function countByCategory(
  vms: NotificationVM[],
): Record<NotificationCategory, number> {
  const out: Record<NotificationCategory, number> = {
    security: 0,
    agent: 0,
    rollout: 0,
    fleet: 0,
    other: 0,
  }
  for (const v of vms) out[v.category] += 1
  return out
}

export interface NotificationFilter {
  category?: NotificationCategory | 'all'
  type?: 'Normal' | 'Warning' | 'all'
  object?: string
}

export function filterNotifications(
  vms: NotificationVM[],
  f: NotificationFilter,
): NotificationVM[] {
  return vms.filter((v) => {
    if (f.category && f.category !== 'all' && v.category !== f.category)
      return false
    if (f.type && f.type !== 'all' && v.type !== f.type) return false
    if (f.object) {
      const needle = f.object.toLowerCase()
      const hay = `${v.regarding?.kind ?? ''}/${v.regarding?.name ?? ''}`.toLowerCase()
      if (!hay.includes(needle)) return false
    }
    return true
  })
}

// Severity -> existing chip/pip class vocabulary (styles.css), so rows are
// consistent with Invocations/Policies before any mock restyle.
export const SEVERITY_CHIP: Record<NotificationSeverity, string> = {
  critical: 'chip chip--coral',
  warning: 'chip chip--amber',
  success: 'chip chip--leaf',
  info: 'chip',
}

/** A sibling Event on the same object, as the tray's "Related events" list wants
 *  it. Derived client-side from the same watched Event set -- no extra query. */
export interface RelatedEvent {
  id: string
  type: 'Normal' | 'Warning'
  reason: string
  message: string
  count: number
  agoMs: number
}

// Identity of the object an Event regards, for grouping siblings.
function objectKey(vm: NotificationVM): string {
  return vm.involved
    ? `${vm.involved.kind}/${vm.involved.namespace}/${vm.involved.name}`
    : ''
}

/** Other Events (newest-first) that regard the same object as `vm`, excluding
 *  `vm` itself. `rows` is assumed already newest-first (mapEvents sorts it). */
export function relatedFor(
  rows: NotificationVM[],
  vm: NotificationVM | undefined,
  limit = 6,
): RelatedEvent[] {
  if (!vm || !vm.involved) return []
  const key = objectKey(vm)
  const out: RelatedEvent[] = []
  for (const v of rows) {
    if (v.id === vm.id || objectKey(v) !== key) continue
    out.push({
      id: v.id,
      type: v.type,
      reason: v.reason,
      message: v.message,
      count: v.count,
      agoMs: v.lastMs,
    })
    if (out.length >= limit) break
  }
  return out
}

// Drop empty-string / null / undefined leaves so the rendered YAML stays clean.
function pruneEmpty<T>(o: T): T {
  if (Array.isArray(o)) return o.map((x) => pruneEmpty(x)) as unknown as T
  if (o && typeof o === 'object') {
    const out: Record<string, unknown> = {}
    for (const [k, v] of Object.entries(o as Record<string, unknown>)) {
      if (v === undefined || v === null || v === '') continue
      out[k] = pruneEmpty(v)
    }
    return out as T
  }
  return o
}

/** Rebuild a faithful events.k8s.io/v1 Event object for the raw-YAML view. */
export function eventToYamlObject(vm: NotificationVM): Record<string, unknown> {
  const obj: Record<string, unknown> = {
    apiVersion: 'events.k8s.io/v1',
    kind: 'Event',
    metadata: {
      name: vm.meta.name,
      namespace: vm.meta.namespace,
      uid: vm.meta.uid,
      resourceVersion: vm.meta.resourceVersion,
      creationTimestamp: vm.meta.creationTimestamp,
    },
    eventTime: vm.firstTimestamp,
    reason: vm.reason,
    note: vm.message,
    type: vm.type,
    action: vm.action,
    reportingController: vm.reportingController,
    reportingInstance: vm.reportingInstance,
  }
  if (vm.involved) {
    obj.regarding = {
      apiVersion: vm.involved.apiVersion,
      kind: vm.involved.kind,
      name: vm.involved.name,
      namespace: vm.involved.namespace,
      uid: vm.involved.uid,
      resourceVersion: vm.involved.resourceVersion,
      fieldPath: vm.involved.fieldPath,
    }
  }
  if (vm.count > 1) {
    obj.series = { count: vm.count, lastObservedTime: vm.lastTimestamp }
  }
  if (vm.source.component || vm.source.host) {
    obj.deprecatedSource = { component: vm.source.component, host: vm.source.host }
  }
  return pruneEmpty(obj)
}

// NOTE: there is deliberately no `kubectl get event` helper. Notification Events
// are recorded to the controller-manager's own embedded apiserver (the loopback
// events.k8s.io/v1 surface the console proxies), not the main apiserver's event
// store, and events.k8s.io is not aggregated to the clrk apiserver -- so
// `kubectl get event <name>` (which hits the main apiserver) never finds them.
// The YAML tab shows the object directly instead. describeCmd is fine: it targets
// the involved clrk.apoxy.dev object, which IS reachable via API aggregation.

/** `kubectl describe` for the regarded object (empty when there is none). */
export function describeCmd(vm: NotificationVM): string {
  if (!vm.involved) return ''
  return `kubectl -n ${vm.involved.namespace} describe ${vm.involved.kind.toLowerCase()} ${vm.involved.name}`
}
