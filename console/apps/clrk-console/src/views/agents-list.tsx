// AGENTS — combined TaskAgent + DaemonAgent list, built to the CLRK Dashboard
// design (`view-agents.jsx`): a KPI strip, a filter toolbar (kind segmented
// control + namespace/pool dropdowns + search), and the agents table. Rows open
// the per-agent detail. Presentational only — the route container (see
// `_shell.agents.tsx`) joins live CRs with the metrics rollup into rows/totals
// (agents-data.ts). The 24h metric columns render "—" until the rollup loads.

import { useState, type CSSProperties } from 'react'
import { Search } from '@carbon/icons-react'
import { fmtK, fmtMs } from '../telemetry/format'
import { shortImage, type AgentRow, type AgentTotals } from './agents-data'

const SearchIcon = <Search size={16} />

export interface AgentsListViewProps {
  rows: AgentRow[]
  totals: AgentTotals
  isLoading?: boolean
  /** Whether the metrics rollup has resolved; gates the 24h numbers vs "—". */
  metricsReady?: boolean
  /** Open an agent's detail; rows are non-interactive when omitted. */
  onOpen?: (id: string) => void
}

type KindFilter = 'all' | 'TaskAgent' | 'DaemonAgent'

export function AgentsListView({
  rows,
  totals,
  isLoading,
  metricsReady,
  onOpen,
}: AgentsListViewProps) {
  const [q, setQ] = useState('')
  const [kind, setKind] = useState<KindFilter>('all')
  const [ns, setNs] = useState('all')
  const [pool, setPool] = useState('all')

  const namespaces = ['all', ...uniq(rows.map((r) => r.namespace))]
  const pools = ['all', ...uniq(rows.map((r) => r.pool))]

  const filtered = rows.filter((r) => {
    if (kind !== 'all' && r.kind !== kind) return false
    if (ns !== 'all' && r.namespace !== ns) return false
    if (pool !== 'all' && r.pool !== pool) return false
    if (q) {
      const hay = `${r.name} ${r.namespace} ${r.image} ${r.description ?? ''}`.toLowerCase()
      if (!hay.includes(q.toLowerCase())) return false
    }
    return true
  })

  // A metric value: a count formatted when the rollup is in, else "—".
  const metric = (v: number | null): string =>
    metricsReady && v != null ? fmtK(v) : '—'

  return (
    <div className="agents-list">
      <div className="page-head">
        <div className="page-head-l">
          <div className="page-head-titles">
            <div className="page-h1-row">
              <h1 className="page-h1">Agents</h1>
            </div>
            <div className="meta">
              <span>
                {totals.agents} {totals.agents === 1 ? 'agent' : 'agents'}
              </span>
              <span className="dot-sep">·</span>
              <span>{totals.ready} ready</span>
              {totals.degraded > 0 && (
                <>
                  <span className="dot-sep">·</span>
                  <span style={{ color: 'var(--apx-coral)' }}>
                    {totals.degraded} degraded
                  </span>
                </>
              )}
            </div>
          </div>
        </div>
      </div>

      <div className="stat-strip" style={{ '--strip-cols': 5 } as CSSProperties}>
        <div className="stat">
          <div className="lab">Ready</div>
          <div
            className="val"
            style={totals.degraded ? { color: 'var(--apx-coral)' } : undefined}
          >
            {totals.ready}
            <span className="unit">/ {totals.agents}</span>
          </div>
        </div>
        <div className="stat">
          <div className="lab">Invocations · 24h</div>
          <div className="val">{metricsReady ? fmtK(totals.invocations) : '—'}</div>
        </div>
        <div className="stat">
          <div className="lab">Tokens in / out</div>
          <div className="val">
            {metricsReady ? fmtK(totals.tokensIn) : '—'}
            <span className="unit">/ {metricsReady ? fmtK(totals.tokensOut) : '—'}</span>
          </div>
        </div>
        <div className="stat">
          <div className="lab">Tool calls · 24h</div>
          <div className="val">{metricsReady ? fmtK(totals.tools) : '—'}</div>
        </div>
        <div className="stat">
          <div className="lab">Errors · 24h</div>
          <div
            className="val"
            style={metricsReady && totals.errors ? { color: 'var(--apx-coral)' } : undefined}
          >
            {metricsReady ? totals.errors : '—'}
          </div>
        </div>
      </div>

      <div className="viz-toolbar">
        <div className="search">
          {SearchIcon}
          <input
            placeholder="Filter by name, image, description…"
            value={q}
            onChange={(e) => setQ(e.target.value)}
          />
          {q ? (
            <button
              type="button"
              className="search-clear"
              aria-label="Clear filter"
              onClick={() => setQ('')}
            >
              ×
            </button>
          ) : (
            <kbd>/</kbd>
          )}
        </div>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <Seg
            value={kind}
            onChange={(v) => setKind(v as KindFilter)}
            options={[
              { v: 'all', l: 'All' },
              { v: 'TaskAgent', l: 'Task' },
              { v: 'DaemonAgent', l: 'Daemon' },
            ]}
          />
          <Dropdown label="namespace" value={ns} options={namespaces} onChange={setNs} />
          <Dropdown label="pool" value={pool} options={pools} onChange={setPool} />
        </div>
      </div>

      <div className="viz-frame">
        <table className="ovw-table agents-table">
          <thead>
            <tr>
              <th style={{ width: 24 }} />
              <th>Name</th>
              <th>Kind</th>
              <th>Image · Revision</th>
              <th>Pool</th>
              <th>Active</th>
              <th>Warm</th>
              <th>24h calls</th>
              <th>p50 / p99</th>
              <th>Tokens in / out</th>
              <th>Age</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((r) => (
              <tr
                key={`${r.kind}:${r.namespace}:${r.id}`}
                className={onOpen ? 'gws-row' : undefined}
                onClick={onOpen ? () => onOpen(r.id) : undefined}
              >
                <td>
                  <span
                    className={'gws-pip' + (r.ready ? '' : ' err')}
                    title={r.ready ? 'Ready' : (r.readyReason ?? 'Not ready')}
                  />
                </td>
                <td>
                  <div style={{ fontWeight: 500 }}>{r.name}</div>
                  <div
                    style={{
                      fontSize: 12,
                      color: 'var(--text-muted)',
                      fontFamily: 'var(--font-mono)',
                      marginTop: 2,
                    }}
                  >
                    {r.namespace}
                    {r.description ? ` · ${r.description}` : ''}
                  </div>
                </td>
                <td>
                  <span
                    className={
                      'kind-tag' + (r.kind === 'DaemonAgent' ? ' kind-tag--daemon' : '')
                    }
                  >
                    {r.kind === 'TaskAgent' ? 'TA' : 'DA'}
                  </span>
                </td>
                <td>
                  <div style={{ fontFamily: 'var(--font-mono)', fontSize: 14 }}>
                    {shortImage(r.image)}
                  </div>
                  <div
                    style={{
                      fontFamily: 'var(--font-mono)',
                      fontSize: 12,
                      color: 'var(--text-muted)',
                      marginTop: 2,
                    }}
                  >
                    {r.revision}
                  </div>
                </td>
                <td>
                  <span className="chip">{r.pool}</span>
                </td>
                <td style={{ fontFamily: 'var(--font-mono)' }}>
                  {r.active == null ? (
                    <span style={{ color: 'var(--text-disabled)' }}>—</span>
                  ) : (
                    r.active
                  )}
                </td>
                <td style={{ fontFamily: 'var(--font-mono)' }}>
                  {r.warm == null ? (
                    <span style={{ color: 'var(--text-disabled)' }}>—</span>
                  ) : (
                    r.warm
                  )}
                </td>
                <td style={{ fontFamily: 'var(--font-mono)' }}>
                  {metric(r.invocations24h)}
                </td>
                <td style={{ fontFamily: 'var(--font-mono)', fontSize: 14 }}>
                  {r.kind === 'TaskAgent' && metricsReady && r.p50ms != null ? (
                    `${fmtMs(r.p50ms)} / ${fmtMs(r.p99ms ?? 0)}`
                  ) : (
                    <span style={{ color: 'var(--text-disabled)' }}>—</span>
                  )}
                </td>
                <td style={{ fontFamily: 'var(--font-mono)', fontSize: 14 }}>
                  <span>{metric(r.inTokens24h)}</span>
                  <span style={{ color: 'var(--text-muted)' }}> / {metric(r.outTokens24h)}</span>
                </td>
                <td
                  style={{
                    fontFamily: 'var(--font-mono)',
                    fontSize: 14,
                    color: 'var(--text-muted)',
                  }}
                >
                  {r.age}
                </td>
              </tr>
            ))}
            {filtered.length === 0 && (
              <tr>
                <td colSpan={11} className="viz-empty-state">
                  {isLoading
                    ? 'Loading agents…'
                    : rows.length === 0
                      ? 'No agents yet.'
                      : 'No agents match your filters.'}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function uniq(xs: string[]): string[] {
  return Array.from(new Set(xs)).sort()
}

function Seg<T extends string>({
  value,
  onChange,
  options,
}: {
  value: T
  onChange: (v: T) => void
  options: Array<{ v: T; l: string }>
}) {
  return (
    <div className="seg">
      {options.map((o) => (
        <button
          key={o.v}
          type="button"
          className={value === o.v ? 'is-active' : ''}
          onClick={() => onChange(o.v)}
        >
          {o.l}
        </button>
      ))}
    </div>
  )
}

function Dropdown({
  label,
  value,
  options,
  onChange,
}: {
  label: string
  value: string
  options: string[]
  onChange: (v: string) => void
}) {
  return (
    <div className="dropdown">
      <span className="dropdown-lab">{label}</span>
      <select value={value} onChange={(e) => onChange(e.target.value)}>
        {options.map((o) => (
          <option key={o} value={o}>
            {o}
          </option>
        ))}
      </select>
    </div>
  )
}
