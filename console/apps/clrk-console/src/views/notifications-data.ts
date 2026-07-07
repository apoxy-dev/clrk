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
  regarding?: {
    kind?: string
    namespace?: string
    name?: string
    apiVersion?: string
    uid?: string
  }
  series?: { count?: number; lastObservedTime?: string }
  // Deprecated core-v1 fallbacks.
  message?: string
  involvedObject?: { kind?: string; namespace?: string; name?: string }
  deprecatedCount?: number
  deprecatedLastTimestamp?: string
}

export type NotificationCategory =
  | 'security'
  | 'agent'
  | 'rollout'
  | 'fleet'
  | 'other'
export type NotificationSeverity = 'critical' | 'warning' | 'info' | 'success'

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
