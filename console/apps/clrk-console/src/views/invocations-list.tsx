// INVOCATIONS -- the live tail of agent executions, built to the CLRK Dashboard
// design (`view-misc.jsx` InvocationsPage): a header with a pause control, a
// table of recent invocations, and the design's table-footer pager -- "Showing
// A-B of N", a rows-per-page segmented control (25 / 50 / 100), and prev /
// numbered / next nav with ellipsis collapsing. Each row is one Invocation CR
// joined to its OTel telemetry (duration / tokens / spans); rows open the owning
// agent's trace panel with the invocation pre-selected. Presentational only --
// the route container (`_shell.invocations.tsx`) fetches the window and does the
// slice + telemetry join (invocations-data.ts). The telemetry columns render a
// dash until traces load.

import type { ReactNode } from 'react'
import { fmtAgo, fmtK, fmtMs } from '../telemetry/format'
import type { InvocationRollup } from '../telemetry/spans'
import {
  chipClass,
  pagerWindow,
  pipClass,
  PAGER_GAP,
  type InvocationRow,
} from './invocations-data'

export interface InvocationsListViewProps {
  rows: InvocationRow[]
  isLoading?: boolean
  /** Whether the per-agent telemetry rollup has resolved at least once; gates
   *  the duration/tokens/spans columns vs "—". */
  telemetryReady?: boolean
  /** Total rows in the loaded window (the pager's "of N"). */
  total: number
  /** 1-based current page, already clamped to [1, pageCount]. */
  page: number
  pageSize: number
  pageCount: number
  /** Zero-based index of the first row on this page (for "Showing A-B"). */
  start: number
  /** Rows actually on this page (<= pageSize). */
  count: number
  onPage: (p: number) => void
  onPageSize: (s: number) => void
  /** Frozen state of the live tail; flips the action to "Resume stream". */
  paused?: boolean
  onTogglePause?: () => void
  /** Open an invocation's agent trace panel; rows are inert when omitted. */
  onOpen?: (row: InvocationRow) => void
}

const dash = <span style={{ color: 'var(--text-muted)' }}>—</span>

// One telemetry cell: the rendered value once the row's rollup is in, a muted
// dash once telemetry has loaded but this row has none (agent outside the
// fetch cap, or no spans), or a placeholder while the per-agent traces load.
function telCell(
  tel: InvocationRollup | undefined,
  ready: boolean | undefined,
  render: (t: InvocationRollup) => ReactNode,
): ReactNode {
  if (tel) return render(tel)
  return ready ? dash : '…'
}

export function InvocationsListView({
  rows,
  isLoading,
  telemetryReady,
  total,
  page,
  pageSize,
  pageCount,
  start,
  count,
  onPage,
  onPageSize,
  paused,
  onTogglePause,
  onOpen,
}: InvocationsListViewProps) {
  return (
    <div className="invocations-list">
      <div className="page-head">
        <div className="page-head-l">
          <div className="page-head-titles">
            <div className="page-h1-row">
              <h1 className="page-h1">Invocations</h1>
            </div>
            <div className="meta">
              <span>{total.toLocaleString()} in window</span>
              <span className="dot-sep">·</span>
              <span style={paused ? undefined : { color: 'var(--apx-leaf)' }}>
                {paused ? 'paused' : 'live tail'}
              </span>
            </div>
          </div>
        </div>
        {onTogglePause && (
          <div className="page-head-r">
            <button type="button" className="btn btn--secondary" onClick={onTogglePause}>
              {paused ? 'Resume stream' : 'Pause stream'}
            </button>
          </div>
        )}
      </div>

      <div className="viz-frame">
        <table className="ovw-table">
          <thead>
            <tr>
              <th style={{ width: 24 }} />
              <th>Invocation</th>
              <th>Agent</th>
              <th>Status</th>
              <th>Duration</th>
              <th>Tokens in / out</th>
              <th>Spans</th>
              <th>When</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr
                key={`${r.namespace}:${r.id}`}
                className={onOpen ? 'gws-row' : undefined}
                onClick={onOpen ? () => onOpen(r) : undefined}
              >
                <td>
                  <span className={'gws-pip' + pipClass(r.tone)} title={r.phase} />
                </td>
                <td style={{ fontFamily: 'var(--font-mono)', fontSize: 14 }}>{r.name}</td>
                <td style={{ fontWeight: 500 }}>{r.agent}</td>
                <td>
                  <span className={chipClass(r.tone)}>{r.phase}</span>
                </td>
                <td style={{ fontFamily: 'var(--font-mono)' }}>
                  {telCell(r.tel, telemetryReady, (t) => fmtMs(t.durMs))}
                </td>
                <td style={{ fontFamily: 'var(--font-mono)', fontSize: 14 }}>
                  {telCell(r.tel, telemetryReady, (t) => (
                    <>
                      <span>{fmtK(t.tokIn)}</span>
                      <span style={{ color: 'var(--text-muted)' }}> / {fmtK(t.tokOut)}</span>
                    </>
                  ))}
                </td>
                <td style={{ fontFamily: 'var(--font-mono)' }}>
                  {telCell(r.tel, telemetryReady, (t) => t.spanCount)}
                </td>
                <td
                  style={{
                    fontFamily: 'var(--font-mono)',
                    fontSize: 14,
                    color: 'var(--text-muted)',
                  }}
                >
                  {fmtAgo(r.ageMs)}
                </td>
              </tr>
            ))}
            {rows.length === 0 && (
              <tr>
                <td colSpan={8} className="viz-empty-state">
                  {isLoading ? 'Loading invocations…' : 'No invocations recorded yet.'}
                </td>
              </tr>
            )}
          </tbody>
        </table>
        {total > 0 && (
          <Pager
            total={total}
            page={page}
            pageSize={pageSize}
            pageCount={pageCount}
            start={start}
            count={count}
            onPage={onPage}
            onPageSize={onPageSize}
          />
        )}
      </div>
    </div>
  )
}

const PAGE_SIZES = [25, 50, 100]

interface PagerProps {
  total: number
  page: number
  pageSize: number
  pageCount: number
  start: number
  count: number
  onPage: (p: number) => void
  onPageSize: (s: number) => void
}

// Table-footer pager, ported from the design's `Pager` (view-misc.jsx): a
// left-side "Showing A-B of N", a rows-per-page segmented control, and prev /
// numbered / next nav. The numbered window collapses with ellipses via
// pagerWindow().
function Pager({
  total,
  page,
  pageSize,
  pageCount,
  start,
  count,
  onPage,
  onPageSize,
}: PagerProps) {
  const from = total === 0 ? 0 : start + 1
  const to = start + count
  return (
    <div className="pager">
      <div className="pager-info">
        Showing{' '}
        <b>
          {from.toLocaleString()}–{to.toLocaleString()}
        </b>{' '}
        of <b>{total.toLocaleString()}</b>
      </div>
      <div className="pager-ctrls">
        <div className="pager-size">
          <span className="pager-size-lab">Rows</span>
          <div className="pager-seg">
            {PAGE_SIZES.map((s) => (
              <button
                key={s}
                type="button"
                className={s === pageSize ? 'is-on' : ''}
                onClick={() => onPageSize(s)}
              >
                {s}
              </button>
            ))}
          </div>
        </div>
        <div className="pager-nav">
          <button
            type="button"
            className="pager-btn"
            disabled={page <= 1}
            onClick={() => onPage(page - 1)}
            aria-label="Previous page"
          >
            ‹
          </button>
          {pagerWindow(page, pageCount).map((p, idx) =>
            p === PAGER_GAP ? (
              <span key={`gap-${idx}`} className="pager-gap">
                …
              </span>
            ) : (
              <button
                key={p}
                type="button"
                className={'pager-btn' + (p === page ? ' is-active' : '')}
                onClick={() => onPage(p)}
              >
                {p}
              </button>
            ),
          )}
          <button
            type="button"
            className="pager-btn"
            disabled={page >= pageCount}
            onClick={() => onPage(page + 1)}
            aria-label="Next page"
          >
            ›
          </button>
        </div>
      </div>
    </div>
  )
}
