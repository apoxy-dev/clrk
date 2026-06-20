// EGRESS GATEWAYS — list view, built to the CLRK Dashboard design
// (`view-egress-list.jsx`): a page header with the primary "New gateway"
// action, a six-cell stat strip, a filter toolbar, and the gateways table.
// Rows open the gateway detail. Presentational only — it renders the rows and
// totals the route container derives from live EgressGateway objects (see
// egress-data.ts). Styling uses the design's component classes (styles.css).

import { useState } from 'react'
import { Add, ChevronDown, Search } from '@carbon/icons-react'
import { fmtK, type EgGatewayRow, type EgressTotals } from './egress-data'

const SearchIcon = <Search size={16} />
const PlusIcon = <Add size={14} />
const CaretIcon = <ChevronDown size={14} />

export interface EgressListViewProps {
  rows: EgGatewayRow[]
  totals: EgressTotals
  isLoading?: boolean
  /** Open a gateway's detail; rows are non-interactive when omitted. */
  onOpen?: (id: string) => void
}

export function EgressListView({
  rows,
  totals,
  isLoading,
  onOpen,
}: EgressListViewProps) {
  const [q, setQ] = useState('')

  const filtered = rows.filter((g) => {
    if (!q) return true
    return `${g.name} ${g.namespace} ${g.description ?? ''}`
      .toLowerCase()
      .includes(q.toLowerCase())
  })

  return (
    <div className="egress-list">
      <div className="page-head">
        <div className="page-head-l">
          <div className="page-head-titles">
            <div className="page-h1-row">
              <h1 className="page-h1">Egress Gateways</h1>
            </div>
            <div className="meta">
              <span>{totals.gateways} gateways</span>
              <span className="dot-sep">·</span>
              <span>{totals.routes} routes</span>
              <span className="dot-sep">·</span>
              <span>production</span>
            </div>
          </div>
        </div>
        <div className="page-head-r">
          <button type="button" className="btn btn--secondary">
            YAML {CaretIcon}
          </button>
          <button type="button" className="btn btn--primary">
            {PlusIcon}
            New gateway
          </button>
        </div>
      </div>

      <div className="stat-strip">
        <div className="stat">
          <div className="lab">Gateways</div>
          <div className="val">{totals.gateways}</div>
        </div>
        <div className="stat">
          <div className="lab">Listeners</div>
          <div className="val">{totals.listeners}</div>
        </div>
        <div className="stat">
          <div className="lab">MCPRoutes</div>
          <div className="val">{totals.mcp}</div>
        </div>
        <div className="stat">
          <div className="lab">AIProviderRoutes</div>
          <div className="val">{totals.ai}</div>
        </div>
        <div className="stat">
          <div className="lab">L4 Routes</div>
          <div className="val">{totals.l4}</div>
        </div>
        <div className="stat">
          <div className="lab">Degraded</div>
          <div
            className="val"
            style={totals.degraded ? { color: 'var(--apx-coral)' } : undefined}
          >
            {totals.degraded}
          </div>
        </div>
      </div>

      <div className="viz-toolbar">
        <div className="search">
          {SearchIcon}
          <input
            placeholder="Filter gateways…"
            value={q}
            onChange={(e) => setQ(e.target.value)}
          />
          <kbd>/</kbd>
        </div>
      </div>

      <div className="viz-frame">
        <table className="ovw-table">
          <thead>
            <tr>
              <th />
              <th>Name</th>
              <th>Namespace</th>
              <th>Default policy</th>
              <th>Listeners</th>
              <th>Routes</th>
              <th>RPS</th>
              <th>Address</th>
              <th>Age</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((g) => (
              <tr
                key={g.id}
                className={onOpen ? 'gws-row' : undefined}
                onClick={onOpen ? () => onOpen(g.id) : undefined}
              >
                <td style={{ width: 24 }}>
                  <span
                    className={'gws-pip' + (g.ready ? '' : ' err')}
                    title={g.statusReason ?? (g.ready ? 'Ready' : 'Degraded')}
                  />
                </td>
                <td>
                  <div style={{ fontWeight: 500 }}>{g.name}</div>
                  {g.description && (
                    <div
                      style={{
                        fontSize: 12,
                        color: 'var(--text-muted)',
                        fontFamily: 'var(--font-mono)',
                        marginTop: 2,
                      }}
                    >
                      {g.description}
                    </div>
                  )}
                </td>
                <td
                  style={{
                    fontFamily: 'var(--font-mono)',
                    color: 'var(--text-muted)',
                  }}
                >
                  {g.namespace}
                </td>
                <td>
                  <span
                    className={
                      'chip' +
                      (g.defaultPolicy === 'deny-all' ? ' chip--ink' : '')
                    }
                  >
                    {g.defaultPolicy}
                  </span>
                </td>
                <td style={{ fontFamily: 'var(--font-mono)' }}>
                  {g.listenersCount}
                </td>
                <td style={{ fontFamily: 'var(--font-mono)' }}>
                  {g.routesCount}
                  {g.routeKinds.length > 0 && (
                    <span
                      style={{
                        color: 'var(--text-muted)',
                        marginLeft: 6,
                        fontSize: 12,
                      }}
                    >
                      {g.routeKinds.join(' · ')}
                    </span>
                  )}
                </td>
                <td style={{ fontFamily: 'var(--font-mono)' }}>
                  {g.rps == null ? '—' : fmtK(g.rps)}
                </td>
                <td
                  style={{
                    fontFamily: 'var(--font-mono)',
                    fontSize: 14,
                    color: 'var(--text-muted)',
                  }}
                >
                  {g.address}
                </td>
                <td
                  style={{
                    fontFamily: 'var(--font-mono)',
                    color: 'var(--text-muted)',
                  }}
                >
                  {g.age}
                </td>
              </tr>
            ))}
            {filtered.length === 0 && (
              <tr>
                <td colSpan={9} className="viz-empty-state">
                  {isLoading
                    ? 'Loading egress gateways…'
                    : q
                      ? 'No gateways match your filter.'
                      : 'No egress gateways yet.'}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
