// POLICIES — list view, built to the CLRK Dashboard design
// (`view-policies.jsx` PoliciesPage): a page header, a six-cell stat strip, a
// kind filter, and one table grouped by kind. Each group is one of the five
// egress policy CRDs; rows surface the attachment (targetRefs / reference),
// a kind-specific spec summary, and acceptance. Presentational only — the route
// container (`_shell.policies.tsx`) reads the live policy objects and maps them
// to rows (policies-data.ts). Rows open the policy's YAML.

import { useState } from 'react'
import {
  countByKind,
  isAttachedKind,
  POLICY_KINDS,
  POLICY_SHORT,
  type PolicyKind,
  type PolicyRow,
  type PolTone,
} from './policies-data'

export interface PoliciesListViewProps {
  rows: PolicyRow[]
  isLoading?: boolean
  /** Open a policy's YAML; rows are inert when omitted. */
  onOpen?: (row: PolicyRow) => void
}

const dash = <span style={{ color: 'var(--text-muted)' }}>—</span>

// The row pip: green when Accepted, amber when not in effect, and a muted dot
// for standalone kinds that report no acceptance state.
function StatusPip({ tone, title }: { tone: PolTone; title: string }) {
  if (tone === 'none') {
    return <span className="gws-pip" style={{ background: 'var(--text-muted)' }} title={title} />
  }
  return <span className={'gws-pip' + (tone === 'ok' ? '' : ' warn')} title={title} />
}

// The status cell mirrors the pip: an Accepted/leaf or amber chip for attached
// kinds, a muted dash for the standalone ones.
function StatusCell({ row }: { row: PolicyRow }) {
  if (row.tone === 'none') return dash
  return (
    <span className={row.tone === 'ok' ? 'chip chip--leaf' : 'chip chip--amber'}>{row.status}</span>
  )
}

export function PoliciesListView({ rows, isLoading, onOpen }: PoliciesListViewProps) {
  const [filter, setFilter] = useState<'all' | PolicyKind>('all')

  const counts = countByKind(rows)
  const total = rows.length
  const attachments = rows.reduce((s, r) => s + r.effective, 0)
  // "Not effective": an attached policy that hasn't been accepted yet.
  const pending = rows.filter((r) => r.tone === 'warn').length
  const filtered = filter === 'all' ? rows : rows.filter((r) => r.kind === filter)

  return (
    <div className="policies-list">
      <div className="page-head">
        <div className="page-head-l">
          <div className="page-head-titles">
            <div className="page-h1-row">
              <h1 className="page-h1">Policies</h1>
            </div>
            <div className="meta">
              <span>{total.toLocaleString()} policies</span>
              <span className="dot-sep">·</span>
              <span>{POLICY_KINDS.length} kinds</span>
              {pending > 0 && (
                <>
                  <span className="dot-sep">·</span>
                  <span style={{ color: 'var(--apx-amber)' }}>{pending} not effective</span>
                </>
              )}
            </div>
          </div>
        </div>
      </div>

      <div className="stat-strip">
        <div className="stat">
          <div className="lab">Policies</div>
          <div className="val">{total}</div>
        </div>
        <div className="stat">
          <div className="lab">CredentialInjection</div>
          <div className="val">{counts.CredentialInjectionPolicy}</div>
        </div>
        <div className="stat">
          <div className="lab">Fallback</div>
          <div className="val">{counts.FallbackRoutingPolicy}</div>
        </div>
        <div className="stat">
          <div className="lab">Deny</div>
          <div className="val">{counts.EgressDenyPolicy}</div>
        </div>
        <div className="stat">
          <div className="lab">Attachments</div>
          <div className="val">{attachments}</div>
        </div>
        <div className="stat">
          <div className="lab">Not effective</div>
          <div className="val" style={pending ? { color: 'var(--apx-amber)' } : undefined}>
            {pending}
          </div>
        </div>
      </div>

      <div className="pol-filter">
        <button
          type="button"
          className={'pol-tag' + (filter === 'all' ? ' is-active' : '')}
          onClick={() => setFilter('all')}
        >
          All <span>{total}</span>
        </button>
        {POLICY_KINDS.map((k) => (
          <button
            key={k}
            type="button"
            className={'pol-tag' + (filter === k ? ' is-active' : '')}
            onClick={() => setFilter(k)}
          >
            {k} <span>{counts[k]}</span>
          </button>
        ))}
      </div>

      <PolicyTable
        rows={filtered}
        isLoading={isLoading}
        filtered={filter !== 'all'}
        onOpen={onOpen}
      />
    </div>
  )
}

interface PolicyTableProps {
  rows: PolicyRow[]
  isLoading?: boolean
  filtered: boolean
  onOpen?: (row: PolicyRow) => void
}

// One table, grouped by kind in canonical order. Each group leads with a header
// row (kind-tag + kind + count); empty groups are dropped.
function PolicyTable({ rows, isLoading, filtered, onOpen }: PolicyTableProps) {
  const groups = POLICY_KINDS.map((kind) => ({
    kind,
    items: rows.filter((r) => r.kind === kind),
  })).filter((g) => g.items.length > 0)

  return (
    <div className="viz-frame">
      <table className="ovw-table pol-table">
        <thead>
          <tr>
            <th style={{ width: 24 }} />
            <th>Name</th>
            <th>Attaches via</th>
            <th>Target</th>
            <th>Spec</th>
            <th>Effective</th>
            <th>Status</th>
            <th>Age</th>
          </tr>
        </thead>
        {groups.length === 0 ? (
          <tbody>
            <tr>
              <td colSpan={8} className="viz-empty-state">
                {isLoading
                  ? 'Loading policies…'
                  : filtered
                    ? 'No policies of this kind.'
                    : 'No egress policies yet.'}
              </td>
            </tr>
          </tbody>
        ) : (
          groups.map((grp) => (
            <tbody key={grp.kind}>
              <tr className="pol-grouphd">
                <td colSpan={8}>
                  <span className="kind-tag">{POLICY_SHORT[grp.kind]}</span>
                  {grp.kind}
                  <span className="pol-grouphd-ct">{grp.items.length}</span>
                </td>
              </tr>
              {grp.items.map((r) => (
                <tr
                  key={r.id}
                  className={onOpen ? 'gws-row' : undefined}
                  onClick={onOpen ? () => onOpen(r) : undefined}
                >
                  <td>
                    <StatusPip tone={r.tone} title={r.status} />
                  </td>
                  <td>
                    <div style={{ fontWeight: 500, fontFamily: 'var(--font-mono)' }}>{r.name}</div>
                    <div style={{ fontSize: 12, color: 'var(--text-muted)', marginTop: 2 }}>
                      {r.namespace}
                    </div>
                  </td>
                  <td style={{ fontFamily: 'var(--font-mono)', fontSize: 12, color: 'var(--text-muted)' }}>
                    {r.via}
                  </td>
                  <td style={{ fontFamily: 'var(--font-mono)', fontSize: 14 }}>{r.target}</td>
                  <td
                    style={{
                      fontFamily: 'var(--font-mono)',
                      fontSize: 14,
                      color: 'var(--text-secondary)',
                    }}
                  >
                    {r.spec}
                  </td>
                  <td style={{ fontFamily: 'var(--font-mono)' }}>
                    {isAttachedKind(r.kind) ? (
                      <>
                        {r.effective}
                        <span style={{ color: 'var(--text-muted)', fontSize: 12, marginLeft: 4 }}>
                          rt
                        </span>
                      </>
                    ) : (
                      dash
                    )}
                  </td>
                  <td>
                    <StatusCell row={r} />
                  </td>
                  <td style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-muted)' }}>
                    {r.age}
                  </td>
                </tr>
              ))}
            </tbody>
          ))
        )}
      </table>
    </div>
  )
}
