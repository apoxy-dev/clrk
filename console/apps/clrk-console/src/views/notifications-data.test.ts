import { describe, expect, it } from 'vitest'
import {
  categorize,
  countByCategory,
  describeCmd,
  eventToYamlObject,
  filterNotifications,
  mapEvents,
  regardingRef,
  relatedFor,
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
  regarding?: {
    kind?: string
    name?: string
    namespace?: string
    apiVersion?: string
    uid?: string
    resourceVersion?: string
    fieldPath?: string
  }
  involvedObject?: { kind?: string; name?: string }
  reportingController?: string
  reportingInstance?: string
  deprecatedSource?: { component?: string; host?: string }
  resourceVersion?: string
  deprecatedLastTimestamp?: string
}): EventObj {
  return {
    metadata: {
      uid: p.uid,
      name: p.name ?? 'e1',
      namespace: 'default',
      resourceVersion: p.resourceVersion,
      creationTimestamp: '2026-07-01T00:00:00Z',
    },
    reason: p.reason,
    type: p.type,
    eventTime: p.eventTime,
    note: p.note,
    regarding: p.regarding,
    involvedObject: p.involvedObject,
    reportingController: p.reportingController,
    reportingInstance: p.reportingInstance,
    deprecatedSource: p.deprecatedSource,
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

describe('mapEvents detail fields', () => {
  it('carries the full involved ref, reporting, source, meta, and timestamps', () => {
    const rows = mapEvents(
      [
        ev({
          uid: 'x',
          name: 'evt.abc',
          reason: 'WorkerPoolDegraded',
          type: 'Warning',
          eventTime: '2026-07-02T00:00:00Z',
          deprecatedLastTimestamp: '2026-07-02T01:00:00Z',
          count: 4,
          resourceVersion: '9911',
          regarding: {
            kind: 'WorkerPool',
            name: 'gpu-a10g',
            namespace: 'agents',
            apiVersion: 'clrk.apoxy.dev/v1alpha1',
            uid: 'wp-uid',
            resourceVersion: '4820',
            fieldPath: 'spec.replicas',
          },
          reportingController: 'clrk-autoscaler',
          reportingInstance: 'clrk-controller-manager-0',
          deprecatedSource: { component: 'clrk-autoscaler', host: 'node-3' },
        }),
      ],
      0,
    )
    const r = rows[0]!
    expect(r.involved).toEqual({
      apiVersion: 'clrk.apoxy.dev/v1alpha1',
      kind: 'WorkerPool',
      name: 'gpu-a10g',
      namespace: 'agents',
      uid: 'wp-uid',
      resourceVersion: '4820',
      fieldPath: 'spec.replicas',
    })
    expect(r.meta.name).toBe('evt.abc')
    expect(r.meta.resourceVersion).toBe('9911')
    expect(r.reportingController).toBe('clrk-autoscaler')
    expect(r.reportingInstance).toBe('clrk-controller-manager-0')
    expect(r.source).toEqual({ component: 'clrk-autoscaler', host: 'node-3' })
    expect(r.firstTimestamp).toBe('2026-07-02T00:00:00Z')
    // series.lastObservedTime wins over the deprecated last timestamp.
    expect(r.lastTimestamp).toBe('2026-07-02T00:00:00Z')
    expect(r.firstMs).toBe(Date.parse('2026-07-02T00:00:00Z'))
  })

  it('falls back to the recording controller for source.component', () => {
    const rows = mapEvents(
      [ev({ uid: 'y', reason: 'RevisionReady', reportingController: 'clrk-rollout' })],
      0,
    )
    expect(rows[0]!.source.component).toBe('clrk-rollout')
    expect(rows[0]!.involved).toBeNull()
  })
})

describe('relatedFor', () => {
  const rows = mapEvents(
    [
      ev({ uid: '1', reason: 'Unhealthy', type: 'Warning', eventTime: '2026-07-02T00:00:03Z', regarding: { kind: 'Pod', name: 'bot-1', namespace: 'agents' } }),
      ev({ uid: '2', reason: 'BackOff', type: 'Warning', eventTime: '2026-07-02T00:00:02Z', regarding: { kind: 'Pod', name: 'bot-1', namespace: 'agents' } }),
      ev({ uid: '3', reason: 'Pulling', type: 'Normal', eventTime: '2026-07-02T00:00:01Z', regarding: { kind: 'Pod', name: 'bot-1', namespace: 'agents' } }),
      ev({ uid: '4', reason: 'ScaledUp', type: 'Normal', eventTime: '2026-07-02T00:00:00Z', regarding: { kind: 'WorkerPool', name: 'gpu', namespace: 'agents' } }),
    ],
    0,
  )

  it('returns sibling events on the same object, newest-first, excluding self', () => {
    const self = rows.find((r) => r.id === '1')
    const rel = relatedFor(rows, self)
    expect(rel.map((r) => r.id)).toEqual(['2', '3'])
    expect(rel[0]).toMatchObject({ reason: 'BackOff', type: 'Warning' })
  })

  it('is empty for an object with no siblings and for a null selection', () => {
    expect(relatedFor(rows, rows.find((r) => r.id === '4'))).toHaveLength(0)
    expect(relatedFor(rows, undefined)).toHaveLength(0)
  })

  it('respects the limit', () => {
    const self = rows.find((r) => r.id === '1')
    expect(relatedFor(rows, self, 1)).toHaveLength(1)
  })
})

describe('eventToYamlObject + kubectl commands', () => {
  const [vm] = mapEvents(
    [
      ev({
        uid: 'z',
        name: 'gpu.evt',
        reason: 'ScaledUp',
        type: 'Normal',
        eventTime: '2026-07-02T00:00:00Z',
        note: 'Scaled worker pool up: 3 -> 5 replicas.',
        count: 2,
        regarding: { kind: 'WorkerPool', name: 'gpu-a10g', namespace: 'agents', apiVersion: 'clrk.apoxy.dev/v1alpha1', uid: 'wp' },
        reportingController: 'clrk-autoscaler',
      }),
    ],
    0,
  )

  it('rebuilds a pruned events.k8s.io/v1 Event object', () => {
    const obj = eventToYamlObject(vm!) as Record<string, any>
    expect(obj.apiVersion).toBe('events.k8s.io/v1')
    expect(obj.kind).toBe('Event')
    expect(obj.metadata.name).toBe('gpu.evt')
    expect(obj.regarding.kind).toBe('WorkerPool')
    expect(obj.series).toEqual({ count: 2, lastObservedTime: '2026-07-02T00:00:00Z' })
    // Empty leaves (e.g. action, resourceVersion) are pruned away.
    expect('action' in obj).toBe(false)
    expect('resourceVersion' in obj.metadata).toBe(false)
  })

  it('builds the kubectl describe command from the involved object', () => {
    expect(describeCmd(vm!)).toBe('kubectl -n agents describe workerpool gpu-a10g')
  })
})
