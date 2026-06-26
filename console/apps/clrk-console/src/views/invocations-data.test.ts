import { describe, expect, it } from 'vitest'
import type { InvocationRollup } from '../telemetry/spans'
import {
  chipClass,
  distinctAgentRefs,
  invTone,
  mapInvocations,
  PAGER_GAP,
  pagerWindow,
  pipClass,
  sortByNewest,
  type InvocationObj,
} from './invocations-data'

// --- fixtures ---------------------------------------------------------------

interface InvFixture {
  name?: string
  namespace?: string
  created?: string
  phase?: string
  parentName?: string
  parentKind?: string
  trigger?: string
}

function inv(p: InvFixture): InvocationObj {
  return {
    metadata: {
      name: p.name ?? 'i-1',
      namespace: p.namespace ?? 'default',
      creationTimestamp: p.created ?? '2026-06-26T00:00:00Z',
    },
    spec: {
      trigger: { type: p.trigger ?? 'HTTP' },
      parentRef: { kind: p.parentKind ?? 'TaskAgent', name: p.parentName ?? 'bot' },
    },
    status: { phase: p.phase ?? 'Succeeded' },
  }
}

function rollup(p: Partial<InvocationRollup>): InvocationRollup {
  return {
    durMs: 0,
    tokIn: 0,
    tokOut: 0,
    spanCount: 0,
    statusCode: 200,
    ok: true,
    ...p,
  }
}

const NOW = Date.parse('2026-06-26T01:00:00Z')

// --- invTone ----------------------------------------------------------------

describe('invTone', () => {
  it('maps terminal success to ok', () => {
    expect(invTone('Succeeded')).toBe('ok')
  })

  it('maps terminal failures to fail', () => {
    for (const p of ['Failed', 'Timeout', 'Rejected', 'Error', 'Errored']) {
      expect(invTone(p)).toBe('fail')
    }
  })

  it('maps in-flight and unknown phases to pending', () => {
    for (const p of ['Pending', 'Dispatched', 'Running', '', 'Weird']) {
      expect(invTone(p)).toBe('pending')
    }
  })
})

describe('tone class helpers', () => {
  it('pipClass keeps green plain, ambers pending, reds fail', () => {
    expect(pipClass('ok')).toBe('')
    expect(pipClass('pending')).toBe(' warn')
    expect(pipClass('fail')).toBe(' err')
  })

  it('chipClass maps tone to the design chip modifiers', () => {
    expect(chipClass('ok')).toBe('chip chip--leaf')
    expect(chipClass('pending')).toBe('chip chip--amber')
    expect(chipClass('fail')).toBe('chip chip--coral')
  })
})

// --- distinctAgentRefs ------------------------------------------------------

describe('sortByNewest', () => {
  it('orders by creation time, newest first', () => {
    const out = sortByNewest([
      inv({ name: 'mid', created: '2026-06-26T00:15:00Z' }),
      inv({ name: 'new', created: '2026-06-26T00:30:00Z' }),
      inv({ name: 'old', created: '2026-06-26T00:00:00Z' }),
    ])
    expect(out.map((o) => o.metadata?.name)).toEqual(['new', 'mid', 'old'])
  })
})

describe('distinctAgentRefs', () => {
  it('dedups by kind+namespace+name, newest invocation first', () => {
    const refs = distinctAgentRefs(
      sortByNewest([
        inv({ name: 'a', parentName: 'bot', created: '2026-06-26T00:00:01Z' }),
        inv({ name: 'b', parentName: 'bot', created: '2026-06-26T00:00:03Z' }),
        inv({ name: 'c', parentName: 'other', created: '2026-06-26T00:00:02Z' }),
      ]),
    )
    expect(refs).toEqual([
      { kind: 'TaskAgent', namespace: 'default', name: 'bot' },
      { kind: 'TaskAgent', namespace: 'default', name: 'other' },
    ])
  })

  it('treats the same name in different namespaces or kinds as distinct', () => {
    const refs = distinctAgentRefs([
      inv({ name: 'a', parentName: 'bot', namespace: 'ns1' }),
      inv({ name: 'b', parentName: 'bot', namespace: 'ns2' }),
      inv({ name: 'c', parentName: 'bot', namespace: 'ns1', parentKind: 'DaemonAgent' }),
    ])
    expect(refs).toHaveLength(3)
  })

  it('skips invocations missing a resolvable agent name or namespace', () => {
    const refs = distinctAgentRefs([
      { metadata: { name: 'x', namespace: 'default' }, spec: { parentRef: {} } },
      { metadata: { name: 'y' }, spec: { parentRef: { name: 'bot' } } },
      inv({ name: 'ok', parentName: 'bot' }),
    ])
    expect(refs).toEqual([{ kind: 'TaskAgent', namespace: 'default', name: 'bot' }])
  })

  it('caps the ref set', () => {
    const items = Array.from({ length: 30 }, (_, k) =>
      inv({ name: `i-${k}`, parentName: `bot-${k}` }),
    )
    expect(distinctAgentRefs(items, 5)).toHaveLength(5)
  })
})

// --- mapInvocations ---------------------------------------------------------

describe('mapInvocations', () => {
  it('maps every given item, preserving newest-first input order', () => {
    const rows = mapInvocations(
      sortByNewest([
        inv({ name: 'old', created: '2026-06-26T00:00:00Z' }),
        inv({ name: 'new', created: '2026-06-26T00:30:00Z' }),
        inv({ name: 'mid', created: '2026-06-26T00:15:00Z' }),
      ]),
      undefined,
      NOW,
    )
    expect(rows.map((r) => r.name)).toEqual(['new', 'mid', 'old'])
  })

  it('joins telemetry by invocation id and computes age + tone', () => {
    const tel = new Map<string, InvocationRollup>([
      ['done', rollup({ durMs: 1500, tokIn: 1200, tokOut: 340, spanCount: 7 })],
    ])
    const rows = mapInvocations(
      sortByNewest([
        inv({ name: 'done', phase: 'Succeeded', created: '2026-06-26T00:00:00Z' }),
        inv({ name: 'boom', phase: 'Failed', created: '2026-06-25T00:00:00Z' }),
      ]),
      tel,
      NOW,
    )
    const done = rows[0]!
    expect(done.tone).toBe('ok')
    expect(done.ageMs).toBe(NOW - Date.parse('2026-06-26T00:00:00Z'))
    expect(done.tel).toEqual(rollup({ durMs: 1500, tokIn: 1200, tokOut: 340, spanCount: 7 }))
    // No telemetry for the failed one -- still a row, just no rollup.
    expect(rows[1]!.tone).toBe('fail')
    expect(rows[1]!.tel).toBeUndefined()
  })

  it('defaults a missing phase to Pending and carries agent + trigger', () => {
    const rows = mapInvocations(
      [{ metadata: { name: 'i', namespace: 'default' }, spec: { parentRef: { name: 'bot' }, trigger: { type: 'Cron' } } }],
      undefined,
      NOW,
    )
    expect(rows[0]!.phase).toBe('Pending')
    expect(rows[0]!.tone).toBe('pending')
    expect(rows[0]!.agent).toBe('bot')
    expect(rows[0]!.trigger).toBe('Cron')
  })
})

// --- pagerWindow ------------------------------------------------------------

describe('pagerWindow', () => {
  it('returns the full range when pageCount <= 7', () => {
    expect(pagerWindow(1, 1)).toEqual([1])
    expect(pagerWindow(3, 7)).toEqual([1, 2, 3, 4, 5, 6, 7])
  })

  it('collapses with a trailing gap near the start', () => {
    expect(pagerWindow(1, 10)).toEqual([1, 2, PAGER_GAP, 10])
  })

  it('collapses with gaps on both sides in the middle', () => {
    expect(pagerWindow(5, 10)).toEqual([1, PAGER_GAP, 4, 5, 6, PAGER_GAP, 10])
  })

  it('collapses with a leading gap near the end', () => {
    expect(pagerWindow(10, 10)).toEqual([1, PAGER_GAP, 9, 10])
  })
})
