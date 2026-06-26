// Trace → lane model. The agents telemetry views render OTLP spans on four
// swimlanes — inbound (the per-invocation root), LLM, MCP, network — exactly as
// the CLRK Dashboard design does. This module flattens an OTLP TracesData
// (telemetry/otlp.ts) into classified spans and groups them two ways: by
// invocation (TaskAgent) and as a flat wall-clock list (DaemonAgent, which has
// no request boundary).
//
// Classification mirrors how the apiserver's agentmetrics aggregation reads the
// same spans (internal/apiserver/agentmetrics, internal/otelemit/attrs.go):
//   - the invocation root is the span named `ingress.dispatch`;
//   - an LLM call carries `gen_ai.*` attributes;
//   - an MCP tool call carries `mcp.*` attributes (`mcp.method` is the key the
//     tool-call rollup counts);
//   - everything else outbound is a plain network call.

import { attrMap, decodeB64Utf8, type OtlpSpan, type OtlpTracesData } from './otlp'

/** The dispatch span that bounds one invocation (otelemit.SpanNameIngressDispatch). */
export const SPAN_INGRESS_DISPATCH = 'ingress.dispatch'

export type Lane = 'inbound' | 'llm' | 'mcp' | 'net'

/**
 * A captured request or response body, carried on the span as an OTLP event
 * (`http.request.body` / `http.response.body`) by the ext_proc OTLP sink. The
 * payload rides base64'd in `clrk.body.b64`, bounded by the gateway's
 * CaptureBody.MaxBytes — `truncated` flags when it hit that cap.
 */
export interface SpanBody {
  /** Decoded UTF-8 text of the captured body. */
  text: string
  /** True when capture hit CaptureBody.MaxBytes and was cut short. */
  truncated: boolean
  /** Captured byte length (clrk.body.bytes). */
  bytes: number
}

/** One flattened span with its lane and the dimensions the inspector shows. */
export interface Span {
  id: string
  traceId: string
  parentId: string
  invocationId: string
  name: string
  lane: Lane
  startNano: number
  endNano: number
  durMs: number
  ok: boolean
  /** HTTP-ish status for display: the captured http status, else 200/500 from the OTLP status. */
  statusCode: number
  attrs: Record<string, string>
  host: string
  label: string
  /** Request path for a network/inbound call (url.path / http.target). */
  path?: string
  /** HTTP method for a network/inbound call (http.request.method / http.method). */
  httpMethod?: string
  // LLM dimensions (gen_ai.*)
  provider?: string
  model?: string
  stream?: boolean
  tokIn?: number
  tokOut?: number
  // MCP dimensions (mcp.*)
  server?: string
  tool?: string
  method?: string
  // routing
  route?: string
  // captured payloads (http.request.body / http.response.body events)
  reqBody?: SpanBody
  respBody?: SpanBody
  /** Assistant message reassembled from a streamed response, when the
   *  backend emitted gen_ai.response.content. Readable counterpart to the
   *  raw SSE frames in respBody. */
  respContent?: string
}

/** An invocation span carries its start offset from the invocation root. */
export type RelSpan = Span & { t0Ms: number }

/** One TaskAgent invocation: its spans (relative-timed) plus rolled-up totals. */
export interface Invocation {
  id: string
  spans: RelSpan[]
  inbound?: RelSpan
  startNano: number
  durMs: number
  ok: boolean
  statusCode: number
  tokIn: number
  tokOut: number
  traceId: string
}

/** A DaemonAgent call carries its age (seconds before `now`) for the wall clock. */
export type DaemonCall = Span & { agoSec: number }

function num(s?: string): number | undefined {
  if (s == null || s === '') return undefined
  const n = Number(s)
  return Number.isFinite(n) ? n : undefined
}

function sum(ns: number[]): number {
  return ns.reduce((a, b) => a + b, 0)
}

function laneOf(name: string, a: Record<string, string>): Lane {
  if (name === SPAN_INGRESS_DISPATCH) return 'inbound'
  if (
    a['gen_ai.system'] != null ||
    a['gen_ai.request.model'] != null ||
    a['gen_ai.usage.input_tokens'] != null ||
    a['gen_ai.usage.output_tokens'] != null
  ) {
    return 'llm'
  }
  if (a['mcp.method'] != null || a['mcp.tool.name'] != null || a['mcp.protocol.version'] != null) {
    return 'mcp'
  }
  return 'net'
}

function hostOf(a: Record<string, string>): string {
  return (
    a['server.address'] ??
    a['clrk.dst.name'] ??
    a['clrk.egress.dst_addr'] ??
    a['clrk.egress.backend.addr'] ??
    a['http.host'] ??
    a['url.full'] ??
    ''
  )
}

function labelOf(
  lane: Lane,
  name: string,
  a: Record<string, string>,
  host: string,
  method: string | undefined,
  path: string | undefined,
): string {
  switch (lane) {
    case 'inbound':
      return `${method ?? 'POST'} ${path ?? '/'}`
    case 'llm':
      return a['gen_ai.request.model'] ?? a['gen_ai.response.model'] ?? a['gen_ai.system'] ?? 'llm'
    case 'mcp':
      return a['mcp.tool.name'] ?? a['mcp.method'] ?? 'mcp'
    default: {
      // A network call is most legible as `METHOD host/path`. `host` may already
      // be a full URL (the url.full fallback) and carry its own path, so only
      // append the path when host is a bare authority (no '/').
      const loc =
        host && path && !host.includes('/')
          ? `${host}${path}`
          : host || a['url.full'] || name || 'request'
      return method ? `${method} ${loc}` : loc
    }
  }
}

/**
 * Pull the captured request/response bodies off a span's OTLP events. The
 * ext_proc sink emits them as `http.request.body` / `http.response.body` events
 * with the payload base64'd in `clrk.body.b64` — the span's plain attribute map
 * never carries them, so the swimlane has to read the events directly.
 */
function bodyEvents(events?: OtlpSpan['events']): { req?: SpanBody; resp?: SpanBody } {
  const out: { req?: SpanBody; resp?: SpanBody } = {}
  for (const ev of events ?? []) {
    if (ev.name !== 'http.request.body' && ev.name !== 'http.response.body') continue
    const ea = attrMap(ev.attributes)
    const b64 = ea['clrk.body.b64']
    if (b64 == null) continue
    const body: SpanBody = {
      text: decodeB64Utf8(b64),
      truncated: ea['clrk.body.truncated'] === 'true',
      bytes: num(ea['clrk.body.bytes']) ?? 0,
    }
    if (ev.name === 'http.request.body') out.req = body
    else out.resp = body
  }
  return out
}

/** Flatten an OTLP TracesData into classified spans (newest-first not guaranteed). */
export function flattenSpans(data?: OtlpTracesData): Span[] {
  const out: Span[] = []
  for (const rs of data?.resourceSpans ?? []) {
    const rAttr = attrMap(rs.resource?.attributes)
    for (const ss of rs.scopeSpans ?? []) {
      for (const sp of ss.spans ?? []) {
        const a = { ...rAttr, ...attrMap(sp.attributes) }
        const startNano = num(sp.startTimeUnixNano) ?? 0
        const endNano = num(sp.endTimeUnixNano) ?? 0
        const durMs = endNano > startNano ? (endNano - startNano) / 1e6 : 0
        const name = sp.name ?? ''
        const lane = laneOf(name, a)
        // protojson renders the OTLP status enum as its name; tolerate the int form too.
        const code = String(sp.status?.code ?? '')
        const errored = code === 'STATUS_CODE_ERROR' || code === '2'
        const httpStatus = num(a['http.response.status_code'] ?? a['http.status_code'])
        const statusCode = httpStatus ?? (errored ? 500 : 200)
        const ok = !errored && statusCode < 400
        const host = hostOf(a)
        const bodies = bodyEvents(sp.events)
        // Derive the HTTP method/path once -- the label and the inspector's
        // dedicated path/method fields read the same OTLP attribute pair.
        const httpMethod = a['http.request.method'] ?? a['http.method']
        const path = a['url.path'] ?? a['http.target']
        out.push({
          // A missing spanId would collide with every other id-less span,
          // producing duplicate React keys and cross-selecting them in the
          // swimlane — fall back to a stable per-trace index instead.
          id: sp.spanId || `sp-${out.length}`,
          traceId: sp.traceId ?? '',
          parentId: sp.parentSpanId ?? '',
          invocationId: a['invocation.id'] ?? '',
          name,
          lane,
          startNano,
          endNano,
          durMs,
          ok,
          statusCode,
          attrs: a,
          host,
          label: labelOf(lane, name, a, host, httpMethod, path),
          path,
          httpMethod,
          provider: a['gen_ai.system'],
          model: a['gen_ai.request.model'] ?? a['gen_ai.response.model'],
          stream: a['gen_ai.response.stream'] === 'true',
          tokIn: num(a['gen_ai.usage.input_tokens']),
          tokOut: num(a['gen_ai.usage.output_tokens']),
          server: a['mcp.server'] ?? a['clrk.mcproute.name'],
          tool: a['mcp.tool.name'],
          method: a['mcp.method'],
          route: a['clrk.aiproviderroute.name'] ?? a['clrk.mcproute.name'] ?? a['clrk.egress_gateway'],
          reqBody: bodies.req,
          respBody: bodies.resp,
          respContent: a['gen_ai.response.content'],
        })
      }
    }
  }
  return out
}

/**
 * Group spans into invocations keyed by `invocation.id`. Each invocation's spans
 * are re-timed relative to the invocation start and sorted; status and token
 * totals come from the inbound root, but duration is the full span extent
 * (min start → max end) — the ingress.dispatch span only measures the dispatch
 * handoff (often ~1ms), not the request lifetime, so it can't bound the chart.
 * Newest invocation first.
 */
export function toInvocations(spans: Span[]): Invocation[] {
  const byId = new Map<string, Span[]>()
  for (const s of spans) {
    if (!s.invocationId) continue
    const arr = byId.get(s.invocationId)
    if (arr) arr.push(s)
    else byId.set(s.invocationId, [s])
  }
  const invs: Invocation[] = []
  for (const [id, group] of byId) {
    const starts = group.map((s) => s.startNano).filter((n) => n > 0)
    const startNano = starts.length ? Math.min(...starts) : 0
    const endNano = Math.max(...group.map((s) => s.endNano), startNano)
    const rel: RelSpan[] = group
      .map((s) => ({ ...s, t0Ms: startNano ? (s.startNano - startNano) / 1e6 : 0 }))
      .sort((x, y) => x.t0Ms - y.t0Ms)
    const inbound = rel.find((s) => s.lane === 'inbound')
    const llm = rel.filter((s) => s.lane === 'llm')
    invs.push({
      id,
      spans: rel,
      inbound,
      startNano,
      durMs: endNano > startNano ? (endNano - startNano) / 1e6 : (inbound?.durMs ?? 0),
      ok: inbound ? inbound.ok : rel.every((s) => s.ok),
      statusCode: inbound?.statusCode ?? 200,
      tokIn: sum(llm.map((s) => s.tokIn ?? 0)),
      tokOut: sum(llm.map((s) => s.tokOut ?? 0)),
      traceId: inbound?.traceId ?? rel[0]?.traceId ?? '',
    })
  }
  return invs.sort((a, b) => b.startNano - a.startNano)
}

/** Per-invocation telemetry totals the Invocations list joins onto each
 *  Invocation CR, keyed by invocation id (== the CR's metadata.name). A thin
 *  projection of {@link Invocation} that drops the span array but keeps its
 *  count, so a list spanning many agents stays cheap to hold. */
export interface InvocationRollup {
  durMs: number
  tokIn: number
  tokOut: number
  /** Total spans captured for the invocation. */
  spanCount: number
  statusCode: number
  ok: boolean
}

/** Roll a span pool up into per-invocation telemetry keyed by invocation id.
 *  Built on {@link toInvocations} so the list and the per-agent swimlane derive
 *  duration/tokens/status the same way. */
export function rollupByInvocation(spans: Span[]): Map<string, InvocationRollup> {
  const out = new Map<string, InvocationRollup>()
  for (const inv of toInvocations(spans)) {
    out.set(inv.id, {
      durMs: inv.durMs,
      tokIn: inv.tokIn,
      tokOut: inv.tokOut,
      spanCount: inv.spans.length,
      statusCode: inv.statusCode,
      ok: inv.ok,
    })
  }
  return out
}

/**
 * Project spans into a DaemonAgent wall-clock list: the inbound lane is dropped
 * (a daemon is never invoked), and each call carries its age in seconds before
 * `nowMs`. Newest (smallest age) first.
 */
export function toDaemonCalls(spans: Span[], nowMs: number): DaemonCall[] {
  return spans
    .filter((s) => s.lane !== 'inbound' && s.startNano > 0)
    .map((s) => ({ ...s, agoSec: Math.max(0, (nowMs - s.startNano / 1e6) / 1000) }))
    .sort((a, b) => a.agoSec - b.agoSec)
}
