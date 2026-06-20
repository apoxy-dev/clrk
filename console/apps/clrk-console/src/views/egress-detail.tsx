// EGRESS GATEWAY DETAIL — Miller columns + tabs, built to the CLRK Dashboard
// design (`view-egress-detail.jsx`). Presentational: the route container maps a
// live EgressGateway + its routes + attached policies into the EgDetail shape
// (egress-detail-data.ts) and passes it here, along with the header actions.
//
// Routing tab (default) is the headline: a four-column drill-down
//   Listeners → Routes (for the selected listener) → Rules → Targets / path.
// Telemetry surfaces the OTLP sink config (live metrics pending ClickHouse);
// Policies flattens every filter into a table; Events reads status.conditions.

import {
  useEffect,
  useMemo,
  useState,
  type CSSProperties,
  type ReactNode,
} from 'react'
import {
  Archive,
  ChartLine,
  Router,
  Search,
  Security,
} from '@carbon/icons-react'
import { fmtK } from './egress-data'
import {
  kindShort,
  type ChipVariant,
  type EgDetail,
  type EgFilter,
  type EgListener,
  type EgRoute,
  type EgRule,
} from './egress-detail-data'

type TabId = 'routing' | 'telemetry' | 'policies' | 'events'

const SearchIcon = <Search size={16} />
const RoutingIcon = <Router size={16} />
const TelemetryIcon = <ChartLine size={16} />
const PoliciesIcon = <Security size={16} />
const EventsIcon = <Archive size={16} />

function truncate(s: string, max: number): string {
  if (!s) return ''
  return s.length > max ? `${s.slice(0, max - 1)}…` : s
}
function chipClass(variant: ChipVariant): string {
  return variant ? `chip chip--${variant}` : 'chip'
}
/** Does a route match the Miller filter? Searches its hostnames, name, kind, and
 *  every rule's match (tools/models/endpoints) and filter chips. */
function routeMatchesQuery(r: EgRoute, q: string): boolean {
  if (!q) return true
  const hay = [
    r.name,
    r.kind,
    kindShort(r.kind),
    ...r.hostnames,
    ...r.rules.flatMap((rl) => [
      rl.match.kind,
      rl.match.value,
      ...(rl.match.models ?? []),
      ...(rl.match.endpoints ?? []),
      ...rl.filters.flatMap((f) => [f.type, f.detail]),
    ]),
  ]
    .join(' ')
    .toLowerCase()
  return hay.includes(q.toLowerCase())
}
function countPolicies(d: EgDetail): number {
  return d.routes.reduce(
    (s, r) => s + r.rules.reduce((ss, rl) => ss + rl.filters.length, 0),
    0,
  )
}

export interface EgressDetailViewProps {
  detail: EgDetail
  /** Header actions (YAML / Delete) wired by the container. */
  actions?: ReactNode
}

export function EgressDetailView({
  detail: g,
  actions,
}: EgressDetailViewProps) {
  const [tab, setTab] = useState<TabId>('routing')
  // Telemetry + Policies are disabled for now (greyed, non-interactive): telemetry
  // awaits the ClickHouse query path, and the route filters already surface the
  // policy chips inline. They stay visible as a "coming soon" affordance.
  const tabs: Array<{
    id: TabId
    label: string
    icon: ReactNode
    count?: number
    disabled?: boolean
  }> = [
    {
      id: 'routing',
      label: 'Routing',
      icon: RoutingIcon,
      count: g.routes.length,
    },
    {
      id: 'telemetry',
      label: 'Telemetry',
      icon: TelemetryIcon,
      disabled: true,
    },
    {
      id: 'policies',
      label: 'Policies',
      icon: PoliciesIcon,
      count: countPolicies(g),
      disabled: true,
    },
    { id: 'events', label: 'Events', icon: EventsIcon, count: g.events.length },
  ]

  return (
    <div className="egress-detail">
      <div className="page-head">
        <div className="page-head-l">
          <div className="page-head-titles">
            <div className="page-h1-row">
              <h1 className="page-h1">{g.name}</h1>
              {g.ready ? (
                <span className="gw-status">
                  <span className="pulse" />
                  Ready
                </span>
              ) : (
                <span className="chip chip--coral">
                  {g.statusReason || g.status}
                </span>
              )}
            </div>
            <div className="meta">
              <span>EgressGateway</span>
              <span className="dot-sep">·</span>
              <span>{g.namespace}</span>
              <span className="dot-sep">·</span>
              <span>{g.age}</span>
              <span className="dot-sep">·</span>
              <span>
                policy{' '}
                <b style={{ color: 'var(--text-primary)' }}>
                  {g.defaultPolicy}
                </b>
              </span>
            </div>
          </div>
        </div>
        <div className="page-head-r">{actions}</div>
      </div>

      <div className="tabs">
        {tabs.map((t) => (
          <div
            key={t.id}
            className={
              'tab' +
              (tab === t.id ? ' is-active' : '') +
              (t.disabled ? ' is-disabled' : '')
            }
            aria-disabled={t.disabled || undefined}
            title={t.disabled ? 'Coming soon' : undefined}
            onClick={t.disabled ? undefined : () => setTab(t.id)}
          >
            {t.icon}
            {t.label}
            {t.count != null && <span className="tab-ct">{t.count}</span>}
          </div>
        ))}
      </div>

      {tab === 'routing' && (
        <>
          <StatStrip g={g} />
          <MillerEgress g={g} />
          <FootHint />
        </>
      )}
      {tab === 'telemetry' && <TelemetryBody g={g} />}
      {tab === 'policies' && <PoliciesBody g={g} />}
      {tab === 'events' && <EventsBody g={g} />}
    </div>
  )
}

function StatStrip({ g }: { g: EgDetail }) {
  const rules = g.routes.reduce((s, r) => s + r.rules.length, 0)
  const policies = countPolicies(g)
  return (
    <div className="stat-strip" style={{ '--strip-cols': 5 } as CSSProperties}>
      <div className="stat">
        <div className="lab">Listeners</div>
        <div className="val">{g.listeners.length}</div>
      </div>
      <div className="stat">
        <div className="lab">Routes attached</div>
        <div className="val">
          {g.routes.length}
          <span className="unit">· {rules} rules</span>
        </div>
      </div>
      <div className="stat">
        <div className="lab">Policies</div>
        <div className="val">{policies}</div>
      </div>
      <div className="stat">
        <div className="lab">Default</div>
        <div className="val" style={{ fontFamily: 'var(--font-display)' }}>
          {g.defaultPolicy === 'deny-all' ? 'deny' : 'allow'}
        </div>
      </div>
      <div className="stat">
        <div className="lab">Requests / sec</div>
        <div className="val">{g.rps == null ? '—' : fmtK(g.rps)}</div>
      </div>
    </div>
  )
}

// ── Miller columns ───────────────────────────────────────────────────────────

function MillerEgress({ g }: { g: EgDetail }) {
  const [selListener, setSelListener] = useState<string | null>(
    g.listeners[0]?.id ?? null,
  )
  const [selRoute, setSelRoute] = useState<string | null>(null)
  const [selRule, setSelRule] = useState<string | null>(null)
  const [q, setQ] = useState('')

  const routesForListener = g.routes.filter((r) => r.listener === selListener)
  const visibleRoutes = routesForListener.filter((r) => routeMatchesQuery(r, q))
  const route =
    visibleRoutes.find((r) => r.id === selRoute) ?? visibleRoutes[0] ?? null
  useEffect(() => {
    if (route && !selRoute) setSelRoute(route.id)
  }, [route, selRoute])

  const rulesForRoute = route ? route.rules : []
  const rule =
    rulesForRoute.find((r) => r.id === selRule) ?? rulesForRoute[0] ?? null
  useEffect(() => {
    if (rule && !selRule) setSelRule(rule.id)
  }, [rule, selRule])

  const pickListener = (id: string) => {
    setSelListener(id)
    setSelRoute(null)
    setSelRule(null)
  }
  const pickRoute = (id: string) => {
    setSelRoute(id)
    setSelRule(null)
  }

  const selListenerObj = g.listeners.find((l) => l.id === selListener) ?? null

  return (
    <>
      <div className="viz-toolbar">
        <div className="search">
          {SearchIcon}
          <input
            placeholder="Filter routes, hostnames, tools, models…"
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
      </div>

      <div className="viz-frame">
        <div className="miller">
          {/* Col 1: Listeners */}
          <div className="miller-col">
            <div className="head">
              <span className="lab">Listeners</span>
              <span className="ct">{g.listeners.length}</span>
            </div>
            <div className="body">
              {g.listeners.map((l) => (
                <div
                  key={l.id}
                  className={
                    'mrow' + (l.id === selListener ? ' is-active' : '')
                  }
                  onClick={() => pickListener(l.id)}
                >
                  <span className={'pip' + (l.ready ? '' : ' err')} />
                  <div className="mrow-main">
                    <div className="nm">{l.name}</div>
                    <div className="sub">
                      <ProtoBadge protocol={l.protocol} tls={l.tlsMode} />
                      {l.port > 0 ? (
                        <span>:{l.port}</span>
                      ) : (
                        <span>any port</span>
                      )}
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Col 2: Routes for selected listener (filtered by the search) */}
          <div className="miller-col">
            <div className="head">
              <span className="lab">Routes attached</span>
              <span className="ct">{visibleRoutes.length}</span>
            </div>
            <div className="body">
              {visibleRoutes.map((r) => (
                <div
                  key={r.id}
                  className={'mrow' + (r.id === route?.id ? ' is-active' : '')}
                  onClick={() => pickRoute(r.id)}
                >
                  <span className="pip" />
                  <div className="mrow-main">
                    <div
                      className="nm"
                      style={{ fontFamily: 'var(--font-mono)', fontSize: 14 }}
                    >
                      {r.hostnames.join(', ')}
                    </div>
                    <div className="sub">
                      <RouteKindTag kind={r.kind} />
                      <span>{r.name}</span>
                      <span>
                        · {r.rules.length} rule{r.rules.length === 1 ? '' : 's'}
                      </span>
                    </div>
                  </div>
                </div>
              ))}
              {routesForListener.length === 0 && (
                <EmptyCol msg="No routes attached to this listener." />
              )}
              {routesForListener.length > 0 && visibleRoutes.length === 0 && (
                <EmptyCol msg={`No routes match “${q}”.`} />
              )}
            </div>
          </div>

          {/* Col 3: Rules for selected route */}
          <div className="miller-col">
            <div className="head">
              <span className="lab">Rules</span>
              <span className="ct">{rulesForRoute.length}</span>
            </div>
            <div className="body">
              {rulesForRoute.map((rl) => (
                <div
                  key={rl.id}
                  className={'mrow' + (rl.id === rule?.id ? ' is-active' : '')}
                  onClick={() => setSelRule(rl.id)}
                >
                  <span className="pip" />
                  <div className="mrow-main">
                    <div
                      className="nm"
                      style={{ fontFamily: 'var(--font-mono)', fontSize: 14 }}
                    >
                      <span style={{ color: 'var(--text-muted)' }}>
                        {rl.match.kind}
                      </span>{' '}
                      {truncate(rl.match.value, 32)}
                    </div>
                    <div className="sub">
                      {rl.match.models?.map((m) => (
                        <span key={m} className="chip">
                          model {m}
                        </span>
                      ))}
                      {rl.match.endpoints?.map((e) => (
                        <span key={e} className="chip">
                          path {e}
                        </span>
                      ))}
                      {rl.filters.map((f, i) => (
                        <FilterChip key={i} f={f} />
                      ))}
                      {rl.filters.length === 0 &&
                        !rl.match.models &&
                        !rl.match.endpoints && (
                          <span style={{ color: 'var(--text-disabled)' }}>
                            —
                          </span>
                        )}
                    </div>
                  </div>
                </div>
              ))}
              {rulesForRoute.length === 0 && <EmptyCol msg="Pick a route" />}
            </div>
          </div>

          {/* Col 4: Targets / apparent path */}
          <div className="miller-col">
            <div className="head">
              <span className="lab">Targets · apparent path</span>
            </div>
            <div className="body">
              {rule && route && selListenerObj && (
                <TargetsPane
                  listener={selListenerObj}
                  route={route}
                  rule={rule}
                />
              )}
              {!rule && <EmptyCol msg="Pick a rule" />}
            </div>
          </div>
        </div>
      </div>
    </>
  )
}

function EmptyCol({ msg }: { msg: string }) {
  return (
    <div
      style={{
        padding: '24px 16px',
        textAlign: 'center',
        color: 'var(--text-disabled)',
        fontSize: 14,
        fontFamily: 'var(--font-mono)',
      }}
    >
      {msg}
    </div>
  )
}

function TargetsPane({
  listener,
  route,
  rule,
}: {
  listener: EgListener
  route: EgRoute
  rule: EgRule
}) {
  const passthrough = rule.backends.length === 0
  const host = route.hostnames[0] ?? 'upstream'
  return (
    <>
      {!passthrough &&
        rule.backends.map((b) => (
          <div key={b.name} className="mrow" style={{ cursor: 'default' }}>
            <span className={'pip' + (b.health === 'warn' ? ' warn' : '')} />
            <div className="mrow-main">
              <div
                className="nm"
                style={{ fontFamily: 'var(--font-mono)', fontSize: 14 }}
              >
                {b.name}
              </div>
              <div className="sub">:{b.port}</div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <div className="weight-bar" style={{ flex: 1 }}>
                  <div
                    className={'fill' + (b.weight <= 10 ? ' canary' : '')}
                    style={{ width: `${b.weight}%` }}
                  />
                </div>
                <span
                  style={{
                    fontFamily: 'var(--font-mono)',
                    fontSize: 12,
                    color: 'var(--text-muted)',
                  }}
                >
                  {b.weight}%
                </span>
              </div>
            </div>
          </div>
        ))}

      {passthrough && (
        <div style={{ padding: '20px 16px' }}>
          <div
            style={{
              marginBottom: 12,
              display: 'flex',
              gap: 6,
              flexWrap: 'wrap',
            }}
          >
            {rule.filters.length === 0 ? (
              <span className="chip">passthrough</span>
            ) : (
              rule.filters.map((f, i) => <FilterChip key={i} f={f} large />)
            )}
          </div>
          <div
            style={{
              fontSize: 14,
              color: 'var(--text-muted)',
              lineHeight: 1.5,
            }}
          >
            {rule.filters.length === 0 ? (
              <>
                No backendRef set — request is sent to its original destination
                ({host}).
              </>
            ) : (
              <>{rule.filters[0]?.detail}</>
            )}
          </div>
        </div>
      )}

      <div
        style={{
          padding: '16px 14px',
          borderTop: '1px solid var(--border-subtle)',
          marginTop: 6,
        }}
      >
        <div
          style={{
            fontSize: 12,
            letterSpacing: '0.12em',
            textTransform: 'uppercase',
            color: 'var(--text-muted)',
            fontWeight: 500,
            marginBottom: 8,
          }}
        >
          Apparent path
        </div>
        <div
          style={{
            fontFamily: 'var(--font-mono)',
            fontSize: 12,
            color: 'var(--text-secondary)',
            lineHeight: 1.7,
          }}
        >
          <div>
            {listener.protocol.toLowerCase()}://{host}
            {listener.port > 0 ? `:${listener.port}` : ''}
          </div>
          <div style={{ color: 'var(--apx-slate)' }}>
            {'  └─ '}
            <RouteKindTag kind={route.kind} /> {rule.match.kind}=
            {truncate(rule.match.value, 24)}
          </div>
          {rule.filters.map((f, i) => (
            <div key={i} style={{ color: 'var(--apx-blue-deep)' }}>
              {'     ↳ '}
              {f.type}: {truncate(f.detail, 32)}
            </div>
          ))}
          {passthrough ? (
            <div>
              {'     → upstream '}
              {host}
            </div>
          ) : (
            rule.backends.map((b) => (
              <div key={b.name}>
                {'     → '}
                {b.name}:{b.port}{' '}
                <span style={{ color: 'var(--apx-slate)' }}>({b.weight}%)</span>
              </div>
            ))
          )}
        </div>
      </div>
    </>
  )
}

// ── Visual helpers ───────────────────────────────────────────────────────────

function RouteKindTag({ kind }: { kind: string }) {
  return (
    <span className={'kind-tag kind-tag--' + kind.toLowerCase()}>
      {kindShort(kind)}
    </span>
  )
}

function ProtoBadge({
  protocol,
  tls,
}: {
  protocol: string
  tls: string | null
}) {
  return (
    <span className="proto-badge">
      <span className="proto-badge-proto">{protocol}</span>
      {tls && tls !== 'Passthrough' && (
        <span className="proto-badge-tls">MITM</span>
      )}
      {tls === 'Passthrough' && (
        <span className="proto-badge-tls proto-badge-tls--pass">SNI</span>
      )}
    </span>
  )
}

function FilterChip({ f, large }: { f: EgFilter; large?: boolean }) {
  return (
    <span
      className={chipClass(f.variant)}
      title={f.detail}
      style={large ? { fontSize: 14, padding: '6px 10px' } : undefined}
    >
      {f.type}
    </span>
  )
}

function FootHint() {
  return (
    <div className="foot-hint">
      <span>Click a row to drill into the next column</span>
      <span style={{ color: 'var(--apx-fog)' }}>·</span>
      <span>Listeners → Routes → Rules → Targets</span>
    </div>
  )
}

// ── Secondary tabs ───────────────────────────────────────────────────────────

function TelemetryBody({ g }: { g: EgDetail }) {
  return (
    <div className="viz-frame" style={{ padding: 24 }}>
      <div
        style={{
          fontSize: 12,
          letterSpacing: '0.12em',
          textTransform: 'uppercase',
          color: 'var(--text-muted)',
          fontWeight: 500,
          marginBottom: 12,
        }}
      >
        OTLP sink
      </div>
      {g.otlpEndpoint ? (
        <div
          style={{
            fontFamily: 'var(--font-mono)',
            fontSize: 14,
            color: 'var(--text-primary)',
            marginBottom: 16,
          }}
        >
          Forwarding captured signals to <b>{g.otlpEndpoint}</b>
        </div>
      ) : (
        <div
          style={{ fontSize: 14, color: 'var(--text-muted)', marginBottom: 16 }}
        >
          No OTLP endpoint configured — signals are kept in the embedded
          ClickHouse.
        </div>
      )}
      <div
        style={{ fontSize: 14, color: 'var(--text-muted)', lineHeight: 1.6 }}
      >
        Live request metrics (RPS, token usage, latency) are read from the
        ClickHouse query path, which is not yet wired into the console.
        Per-gateway RPS in the overview is a seeded demo value.
      </div>
    </div>
  )
}

function PoliciesBody({ g }: { g: EgDetail }) {
  const flat: Array<{
    route: string
    kind: string
    rule: string
    f: EgFilter
  }> = []
  g.routes.forEach((r) =>
    r.rules.forEach((rl) =>
      rl.filters.forEach((f) => {
        flat.push({ route: r.name, kind: r.kind, rule: rl.id, f })
      }),
    ),
  )
  if (flat.length === 0) {
    return (
      <div className="viz-frame">
        <div className="viz-empty-state">
          No policies attached to this gateway's routes.
        </div>
      </div>
    )
  }
  return (
    <div className="viz-frame">
      <table className="ovw-table">
        <thead>
          <tr>
            <th>Type</th>
            <th>Route</th>
            <th>Rule</th>
            <th>Detail</th>
          </tr>
        </thead>
        <tbody>
          {flat.map((row, i) => (
            <tr key={i}>
              <td>
                <FilterChip f={row.f} />
              </td>
              <td style={{ fontFamily: 'var(--font-mono)' }}>
                {row.route}{' '}
                <span
                  style={{
                    color: 'var(--text-muted)',
                    marginLeft: 6,
                    fontSize: 12,
                  }}
                >
                  {kindShort(row.kind)}
                </span>
              </td>
              <td
                style={{
                  fontFamily: 'var(--font-mono)',
                  fontSize: 14,
                  color: 'var(--text-muted)',
                }}
              >
                {row.rule}
              </td>
              <td style={{ fontFamily: 'var(--font-mono)', fontSize: 14 }}>
                {row.f.detail}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function EventsBody({ g }: { g: EgDetail }) {
  const events = useMemo(
    () => g.events.filter((e) => e.message || e.type),
    [g.events],
  )
  if (events.length === 0) {
    return (
      <div className="viz-frame">
        <div className="viz-empty-state">No recent events.</div>
      </div>
    )
  }
  return (
    <div className="viz-frame ovw-events">
      {events.map((e, i) => (
        <div key={i} className="ovw-event">
          <div className="ovw-event-time">{fmtEventTime(e.time)}</div>
          <div
            className={
              'ovw-event-rail' +
              (e.tone === 'error'
                ? ' ovw-event-rail--error'
                : e.tone === 'warn'
                  ? ' ovw-event-rail--warn'
                  : '')
            }
          />
          <div className="ovw-event-body">
            <div className="ovw-event-line">
              <span
                className={
                  e.tone === 'error'
                    ? 'chip chip--coral'
                    : e.tone === 'warn'
                      ? 'chip chip--amber'
                      : 'chip'
                }
              >
                {e.type}
              </span>
            </div>
            <div className="ovw-event-detail">{e.message}</div>
          </div>
        </div>
      ))}
    </div>
  )
}

function fmtEventTime(iso: string): string {
  if (!iso) return '—'
  const ms = Date.now() - Date.parse(iso)
  if (Number.isNaN(ms)) return iso
  const mins = Math.floor(ms / 60_000)
  if (mins < 1) return 'just now'
  if (mins < 60) return `${mins}m ago`
  const hours = Math.floor(mins / 60)
  if (hours < 24) return `${hours}h ago`
  return `${Math.floor(hours / 24)}d ago`
}
