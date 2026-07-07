import { describe, expect, it } from 'vitest'
import {
  categorize,
  countByCategory,
  filterNotifications,
  mapEvents,
  regardingRef,
  severityOf,
  unreadCount,
  type EventObj,
} from './notifications-data'

// --- fixtures ---------------------------------------------------------------

function ev(p: {
  uid?: string
  name?: string
  reason?: string
  type?: string
  eventTime?: string
  note?: string
  count?: number
  regarding?: { kind?: string; name?: string; namespace?: string }
  involvedObject?: { kind?: string; name?: string }
  deprecatedLastTimestamp?: string
}): EventObj {
  return {
    metadata: {
      uid: p.uid,
      name: p.name ?? 'e1',
      namespace: 'default',
      creationTimestamp: '2026-07-01T00:00:00Z',
    },
    reason: p.reason,
    type: p.type,
    eventTime: p.eventTime,
    note: p.note,
    regarding: p.regarding,
    involvedObject: p.involvedObject,
    deprecatedLastTimestamp: p.deprecatedLastTimestamp,
    series: p.count ? { count: p.count, lastObservedTime: p.eventTime } : undefined,
  } as EventObj
}

// --- tests ------------------------------------------------------------------

describe('categorize', () => {
  it('maps reasons to the four buckets, else other', () => {
    expect(categorize('EgressDenied')).toBe('security')
    expect(categorize('SecurityAdvisory')).toBe('security')
    expect(categorize('InvocationFailed')).toBe('agent')
    expect(categorize('RevisionReady')).toBe('rollout')
    expect(categorize('WorkerPoolDegraded')).toBe('fleet')
    expect(categorize('SomethingElse')).toBe('other')
  })
})

describe('severityOf', () => {
  it('security Warning is critical; rollout ready is success', () => {
    expect(severityOf('EgressDenied', 'Warning', 'security')).toBe('critical')
    expect(severityOf('InvocationFailed', 'Warning', 'agent')).toBe('warning')
    expect(severityOf('RevisionReady', 'Normal', 'rollout')).toBe('success')
    expect(severityOf('WorkerPoolHealthy', 'Normal', 'fleet')).toBe('success')
    expect(severityOf('X', 'Normal', 'other')).toBe('info')
  })
})

describe('regardingRef', () => {
  it('prefers regarding, falls back to involvedObject, null when absent', () => {
    expect(regardingRef(ev({ regarding: { kind: 'TaskAgent', name: 'a' } }))).toEqual({
      kind: 'TaskAgent',
      name: 'a',
      namespace: '',
    })
    expect(regardingRef(ev({ involvedObject: { kind: 'WorkerPool', name: 'wp' } }))).toEqual({
      kind: 'WorkerPool',
      name: 'wp',
      namespace: '',
    })
    expect(regardingRef(ev({}))).toBeNull()
  })
})

describe('mapEvents', () => {
  it('sorts newest-first and flags read against the watermark', () => {
    const older = ev({ uid: 'a', reason: 'EgressDenied', type: 'Warning', eventTime: '2026-07-01T00:00:00Z' })
    const newer = ev({ uid: 'b', reason: 'RevisionReady', type: 'Normal', eventTime: '2026-07-02T00:00:00Z' })
    const watermark = Date.parse('2026-07-01T12:00:00Z')
    const rows = mapEvents([older, newer], watermark)
    expect(rows.map((r) => r.id)).toEqual(['b', 'a'])
    // older is before the watermark => read; newer is after => unread.
    expect(rows.find((r) => r.id === 'a')?.read).toBe(true)
    expect(rows.find((r) => r.id === 'b')?.read).toBe(false)
    expect(unreadCount(rows)).toBe(1)
  })

  it('counts aggregation series occurrences', () => {
    const rows = mapEvents([ev({ uid: 'a', reason: 'EgressDenied', type: 'Warning', eventTime: '2026-07-02T00:00:00Z', count: 7 })], 0)
    expect(rows[0]?.count).toBe(7)
  })
})

describe('countByCategory + filterNotifications', () => {
  const rows = mapEvents(
    [
      ev({ uid: '1', reason: 'EgressDenied', type: 'Warning', eventTime: '2026-07-02T00:00:00Z', regarding: { kind: 'TaskAgent', name: 'bot' } }),
      ev({ uid: '2', reason: 'InvocationFailed', type: 'Warning', eventTime: '2026-07-02T00:00:01Z' }),
      ev({ uid: '3', reason: 'RevisionReady', type: 'Normal', eventTime: '2026-07-02T00:00:02Z' }),
    ],
    0,
  )

  it('counts per category', () => {
    const c = countByCategory(rows)
    expect(c.security).toBe(1)
    expect(c.agent).toBe(1)
    expect(c.rollout).toBe(1)
    expect(c.fleet).toBe(0)
  })

  it('filters by category, type, and object', () => {
    expect(filterNotifications(rows, { category: 'security' })).toHaveLength(1)
    expect(filterNotifications(rows, { type: 'Warning' })).toHaveLength(2)
    expect(filterNotifications(rows, { object: 'bot' })).toHaveLength(1)
    expect(filterNotifications(rows, { category: 'all', type: 'all' })).toHaveLength(3)
  })
})
