// Invocations view-model: the pure transforms behind the bespoke Invocations
// list (the CLRK Dashboard's `view-misc.jsx` InvocationsPage). The Invocation
// CR is the cheap, watched source of truth for a row's identity, owning agent,
// phase, trigger, and age; the duration/tokens/spans/status are an enrichment
// joined in from the OTel spans (keyed by invocation id) -- the Invocation CR's
// status is deliberately phase-only (see api/clrk/v1alpha1/types_invocation.go),
// so the numbers live on the traces. Kept side-effect free and unit-tested
// (invocations-data.test.ts); the route container does the I/O.

import type { AgentKind, AgentTraceRef } from '../telemetry/client'
import type { InvocationRollup } from '../telemetry/spans'
import type { InvocationObj } from './overview-data'

export type { InvocationObj }

/** Coarse health of an invocation, from its phase: a terminal success, a
 *  terminal failure, or still in flight. Drives both the row pip and the
 *  status chip so they never disagree. */
export type InvTone = 'ok' | 'pending' | 'fail'

// Terminal failure phases (Failed/Timeout/Rejected) plus the generic error
// spellings the agent status surfaces use elsewhere in the console.
const FAIL_PHASES = new Set([
  'Failed',
  'Timeout',
  'Rejected',
  'Error',
  'Errored',
])
const OK_PHASES = new Set(['Succeeded'])

/** Classify a phase into a tone. Unknown/empty reads as in-flight (pending),
 *  never as a failure. */
export function invTone(phase: string): InvTone {
  if (FAIL_PHASES.has(phase)) return 'fail'
  if (OK_PHASES.has(phase)) return 'ok'
  return 'pending'
}

// Pip + chip CSS per tone, in one table so the row dot and the status chip
// never disagree (gws-pip: '' green / ' warn' amber / ' err' red; chip: leaf /
// amber / coral).
const TONE_STYLE: Record<InvTone, { pip: string; chip: string }> = {
  ok: { pip: '', chip: 'chip chip--leaf' },
  pending: { pip: ' warn', chip: 'chip chip--amber' },
  fail: { pip: ' err', chip: 'chip chip--coral' },
}

/** The `gws-pip` modifier class for a tone. */
export function pipClass(tone: InvTone): string {
  return TONE_STYLE[tone].pip
}

/** The `chip` modifier class for a tone. */
export function chipClass(tone: InvTone): string {
  return TONE_STYLE[tone].chip
}

/** One row of the Invocations table. Telemetry (`tel`) is absent until the
 *  owning agent's traces load (or for an agent outside the bounded fetch set);
 *  the view renders "—" for the duration/tokens/spans columns then. */
export interface InvocationRow {
  id: string
  name: string
  namespace: string
  /** Owning agent name (spec.parentRef.name). */
  agent: string
  agentKind: AgentKind
  phase: string
  trigger: string
  tone: InvTone
  /** Age in ms at the caller-supplied clock. */
  ageMs: number
  tel?: InvocationRollup
}

function invMs(o: InvocationObj): number {
  const t = Date.parse(o.metadata?.creationTimestamp ?? '')
  return Number.isNaN(t) ? 0 : t
}

function parentKind(o: InvocationObj): AgentKind {
  return o.spec?.parentRef?.kind === 'DaemonAgent' ? 'DaemonAgent' : 'TaskAgent'
}

/** Newest-first by creation time. The list page sorts once and hands the result
 *  to both `distinctAgentRefs` and `mapInvocations`, so neither re-sorts. */
export function sortByNewest(items: InvocationObj[]): InvocationObj[] {
  return [...items].sort((a, b) => invMs(b) - invMs(a))
}

/**
 * The distinct agents that own the newest `cap` invocations, as trace refs the
 * telemetry hook fans out over. Bounded so a cluster with many agents doesn't
 * issue an unbounded number of `traces` GETs per poll: newest invocations win,
 * and an invocation whose agent falls outside the cap simply renders without
 * telemetry. Refs without a resolvable namespace+name are skipped (can't fetch).
 * Expects newest-first input (see {@link sortByNewest}).
 */
export function distinctAgentRefs(
  items: InvocationObj[],
  cap = 16,
): AgentTraceRef[] {
  const seen = new Set<string>()
  const refs: AgentTraceRef[] = []
  for (const o of items) {
    const name = o.spec?.parentRef?.name
    const namespace = o.metadata?.namespace
    if (!name || !namespace) continue
    const kind = parentKind(o)
    const key = `${kind}/${namespace}/${name}`
    if (seen.has(key)) continue
    seen.add(key)
    refs.push({ kind, namespace, name })
    if (refs.length >= cap) break
  }
  return refs
}

/**
 * Map invocation objects to table rows, each joined to its telemetry rollup by
 * id. The caller passes the current page's slice (already newest-first; see
 * {@link sortByNewest}) -- pagination lives in the view, not here. Pure, with
 * the clock passed in so ages are deterministic and unit-testable.
 */
export function mapInvocations(
  items: InvocationObj[],
  telemetry: Map<string, InvocationRollup> | undefined,
  nowMs: number,
): InvocationRow[] {
  return items.map((o): InvocationRow => {
    const name = o.metadata?.name ?? ''
    const phase = o.status?.phase ?? 'Pending'
    return {
      id: name,
      name,
      namespace: o.metadata?.namespace ?? '',
      agent: o.spec?.parentRef?.name ?? '—',
      agentKind: parentKind(o),
      phase,
      trigger: o.spec?.trigger?.type ?? '—',
      tone: invTone(phase),
      ageMs: Math.max(0, nowMs - invMs(o)),
      tel: telemetry?.get(name),
    }
  })
}

/** Sentinel for an elided run of page numbers in {@link pagerWindow}. The view
 *  renders it as an ellipsis glyph. */
export const PAGER_GAP = '...'

/**
 * The page numbers to render in the table-footer pager nav, collapsing long
 * runs to a {@link PAGER_GAP} sentinel (ported from the design's `pagerWindow`):
 * up to 7 pages render in full; beyond that, always first + last + a window of
 * +/-1 around the current page, with gaps between. Pages are 1-based.
 */
export function pagerWindow(
  cur: number,
  pageCount: number,
): Array<number | typeof PAGER_GAP> {
  if (pageCount <= 7) return Array.from({ length: pageCount }, (_, k) => k + 1)
  const out: Array<number | typeof PAGER_GAP> = [1]
  const lo = Math.max(2, cur - 1)
  const hi = Math.min(pageCount - 1, cur + 1)
  if (lo > 2) out.push(PAGER_GAP)
  for (let p = lo; p <= hi; p++) out.push(p)
  if (hi < pageCount - 1) out.push(PAGER_GAP)
  out.push(pageCount)
  return out
}
