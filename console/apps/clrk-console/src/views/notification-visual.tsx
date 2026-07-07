// Presentational glue between the clrk notification data model and the design
// mock's visual vocabulary. Category picks the glyph + label; severity picks the
// tone (the mock's rule: "coral/amber/leaf reserved for severity"). No data-layer
// logic lives here -- it only maps view-model fields to icons, tone CSS vars, and
// display strings the nc-* markup expects.

import type { CSSProperties, ComponentType } from 'react'
import { Bot, Grid, Information, Rocket, Security } from '@carbon/icons-react'
import {
  CATEGORY_META,
  type NotificationCategory,
  type NotificationSeverity,
  type NotificationVM,
} from './notifications-data'

type IconCmp = ComponentType<{ size?: number }>

// Category -> Carbon glyph. A full record so lookups are total under strict TS.
export const CATEGORY_ICON: Record<NotificationCategory, IconCmp> = {
  security: Security,
  agent: Bot,
  rollout: Rocket,
  fleet: Grid,
  other: Information,
}

// Severity -> {--tone, --tone-tint}. These are the only saturated hues in the
// system; each resolves to an existing Apoxy token pair.
const SEVERITY_TONE: Record<NotificationSeverity, { tone: string; tint: string }> = {
  critical: { tone: 'var(--apx-coral)', tint: 'var(--apx-coral-tint)' },
  warning: { tone: 'var(--apx-amber)', tint: 'var(--apx-amber-tint)' },
  success: { tone: 'var(--apx-leaf)', tint: 'var(--apx-leaf-tint)' },
  info: { tone: 'var(--apx-blue)', tint: 'var(--apx-blue-tint)' },
}

// Inline style setting the --tone / --tone-tint custom properties an nc-item and
// its children read.
export function toneVars(severity: NotificationSeverity): CSSProperties {
  const t = SEVERITY_TONE[severity]
  return { '--tone': t.tone, '--tone-tint': t.tint } as CSSProperties
}

export function categoryLabel(category: NotificationCategory): string {
  return CATEGORY_META[category].label
}

// Compact relative time for a row's corner ("8m", "3h", "2d", then a date).
export function relTime(ms: number): string {
  if (!ms) return ''
  const s = Math.max(0, Math.floor((Date.now() - ms) / 1000))
  if (s < 60) return `${s}s`
  const m = Math.floor(s / 60)
  if (m < 60) return `${m}m`
  const h = Math.floor(m / 60)
  if (h < 24) return `${h}h`
  const d = Math.floor(h / 24)
  if (d < 7) return `${d}d`
  return new Date(ms).toLocaleDateString(undefined, { month: 'short', day: 'numeric' })
}

export interface DayGroup {
  key: string
  label: string
  rows: NotificationVM[]
}

// Split newest-first rows into Today / Earlier buckets, matching the mock's
// day grouping. Empty buckets are omitted.
export function groupByDay(rows: NotificationVM[]): DayGroup[] {
  const start = new Date()
  start.setHours(0, 0, 0, 0)
  const todayStart = start.getTime()

  const today: NotificationVM[] = []
  const earlier: NotificationVM[] = []
  for (const r of rows) {
    if (r.timeMs >= todayStart) today.push(r)
    else earlier.push(r)
  }
  const groups: DayGroup[] = []
  if (today.length) groups.push({ key: 'today', label: 'Today', rows: today })
  if (earlier.length) groups.push({ key: 'earlier', label: 'Earlier', rows: earlier })
  return groups
}
