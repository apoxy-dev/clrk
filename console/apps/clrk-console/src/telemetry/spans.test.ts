import { describe, expect, it } from 'vitest'
import {
  flattenSpans,
  toDaemonCalls,
  toInvocations,
  SPAN_INGRESS_DISPATCH,
} from './spans'
import type { OtlpKeyValue, OtlpSpan, OtlpTracesData } from './otlp'

const MS = 1_000_000 // ns per ms
// A realistic epoch base so `start: 0` maps to a non-zero startTimeUnixNano (real
// spans never carry a 0 timestamp; the transform treats 0 as "missing").
const BASE = 1_700_000_000_000 // ms

function kv(rec: Record<string, string | number | boolean>): OtlpKeyValue[] {
  return Object.entries(rec).map(([key, v]) => ({
    key,
    value:
      typeof v === 'number'
        ? { intValue: String(v) }
        : typeof v === 'boolean'
          ? { boolValue: v }
          : { stringValue: v },
  }))
}

function span(s: {
  name: string
  spanId: string
  start: number // ms
  dur: number // ms
  attrs: Record<string, string | number | boolean>
  error?: boolean
}): OtlpSpan {
  return {
    spanId: s.spanId,
    traceId: 'trace-1',
    name: s.name,
    startTimeUnixNano: String((BASE + s.start) * MS),
    endTimeUnixNano: String((BASE + s.start + s.dur) * MS),
    attributes: kv(s.attrs),
    status: s.error ? { code: 'STATUS_CODE_ERROR' } : { code: 'STATUS_CODE_OK' },
  }
}

function traces(spans: OtlpSpan[]): OtlpTracesData {
  return { resourceSpans: [{ resource: { attributes: kv({ 'agent.name': 'review-bot' }) }, scopeSpans: [{ spans }] }] }
}

const INV = 'inv-9b3c1a40'

describe('flattenSpans / classification', () => {
  const data = traces([
    span({ name: SPAN_INGRESS_DISPATCH, spanId: 'a', start: 0, dur: 1000, attrs: { 'invocation.id': INV, 'http.request.method': 'POST', 'url.path': '/run' } }),
    span({ name: 'chat', spanId: 'b', start: 50, dur: 400, attrs: { 'invocation.id': INV, 'gen_ai.system': 'anthropic', 'gen_ai.request.model': 'claude-sonnet-4', 'gen_ai.usage.input_tokens': 1200, 'gen_ai.usage.output_tokens': 300, 'gen_ai.response.stream': true } }),
    span({ name: 'tools/call', spanId: 'c', start: 500, dur: 120, attrs: { 'invocation.id': INV, 'mcp.method': 'tools/call', 'mcp.tool.name': 'read_file' } }),
    span({ name: 'GET github', spanId: 'd', start: 650, dur: 90, attrs: { 'invocation.id': INV, 'server.address': 'api.github.com', 'http.response.status_code': 200 } }),
    span({ name: 'GET stripe', spanId: 'e', start: 700, dur: 40, attrs: { 'invocation.id': INV, 'server.address': 'api.stripe.com', 'http.response.status_code': 503 }, error: true }),
  ])
  const spans = flattenSpans(data)

  it('assigns one span to each lane by attribute', () => {
    const byId = Object.fromEntries(spans.map((s) => [s.id, s]))
    expect(byId['a']!.lane).toBe('inbound')
    expect(byId['b']!.lane).toBe('llm')
    expect(byId['c']!.lane).toBe('mcp')
    expect(byId['d']!.lane).toBe('net')
  })

  it('reads LLM dimensions and durations', () => {
    const llm = spans.find((s) => s.id === 'b')!
    expect(llm.provider).toBe('anthropic')
    expect(llm.model).toBe('claude-sonnet-4')
    expect(llm.stream).toBe(true)
    expect(llm.tokIn).toBe(1200)
    expect(llm.tokOut).toBe(300)
    expect(llm.durMs).toBe(400)
  })

  it('marks an errored / 5xx span not-ok', () => {
    const err = spans.find((s) => s.id === 'e')!
    expect(err.ok).toBe(false)
    expect(err.statusCode).toBe(503)
    const okNet = spans.find((s) => s.id === 'd')!
    expect(okNet.ok).toBe(true)
    expect(okNet.statusCode).toBe(200)
  })

  it('merges resource attributes onto each span', () => {
    expect(spans[0]!.attrs['agent.name']).toBe('review-bot')
  })
})

describe('toInvocations', () => {
  const spans = flattenSpans(
    traces([
      span({ name: 'tools/call', spanId: 'c', start: 500, dur: 120, attrs: { 'invocation.id': INV, 'mcp.method': 'tools/call' } }),
      span({ name: SPAN_INGRESS_DISPATCH, spanId: 'a', start: 0, dur: 1000, attrs: { 'invocation.id': INV } }),
      span({ name: 'chat', spanId: 'b', start: 50, dur: 400, attrs: { 'invocation.id': INV, 'gen_ai.usage.input_tokens': 1200, 'gen_ai.usage.output_tokens': 300 } }),
      span({ name: 'chat', spanId: 'b2', start: 80, dur: 100, attrs: { 'invocation.id': INV, 'gen_ai.usage.input_tokens': 800, 'gen_ai.usage.output_tokens': 100 } }),
    ]),
  )
  const invs = toInvocations(spans)

  it('groups by invocation.id and orders spans by relative start', () => {
    expect(invs).toHaveLength(1)
    const inv = invs[0]!
    expect(inv.spans.map((s) => s.id)).toEqual(['a', 'b', 'b2', 'c'])
    expect(inv.spans[0]!.t0Ms).toBe(0)
    expect(inv.spans[3]!.t0Ms).toBe(500)
  })

  it('takes duration/status from the inbound root and sums LLM tokens', () => {
    const inv = invs[0]!
    expect(inv.durMs).toBe(1000)
    expect(inv.inbound?.id).toBe('a')
    expect(inv.tokIn).toBe(2000)
    expect(inv.tokOut).toBe(400)
  })

  it('skips spans without an invocation id', () => {
    const orphan = flattenSpans(traces([span({ name: 'chat', spanId: 'x', start: 0, dur: 1, attrs: { 'gen_ai.system': 'openai' } })]))
    expect(toInvocations(orphan)).toHaveLength(0)
  })
})

describe('toDaemonCalls', () => {
  it('drops the inbound lane and ages each call from now, newest first', () => {
    const nowMs = BASE + 10_000
    const spans = flattenSpans(
      traces([
        span({ name: SPAN_INGRESS_DISPATCH, spanId: 'a', start: 0, dur: 10, attrs: {} }),
        span({ name: 'chat', spanId: 'b', start: 1000, dur: 50, attrs: { 'gen_ai.system': 'anthropic' } }),
        span({ name: 'chat', spanId: 'c', start: 8000, dur: 50, attrs: { 'gen_ai.system': 'anthropic' } }),
      ]),
    )
    const calls = toDaemonCalls(spans, nowMs)
    expect(calls.map((c) => c.id)).toEqual(['c', 'b'])
    expect(calls[0]!.agoSec).toBeCloseTo(2, 5)
    expect(calls[1]!.agoSec).toBeCloseTo(9, 5)
  })
})
