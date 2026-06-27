// POLICY DETAIL — a full per-policy page, built to the CLRK Dashboard design
// (`view-policies.jsx` PolicyDetailPage) and modeled on this console's own
// EgressGateway detail. Presentational: the route container resolves a live
// policy + the cluster's routes/gateways into a PolicyDetail
// (policies-detail-data.ts) and passes it here with the header actions.
//
// Layout: a page head (name · status · kind/ns/age) over a status summary band
// (what it does + where it lands), then Specification / Attachment tabs. The
// Specification tab pairs a per-kind spec card (one bespoke renderer per CRD)
// with an "Effective on" table of the routes the policy actually reaches; the
// Attachment tab draws how the policy binds to its targets.

import { useState, type ReactNode } from 'react'
import { Document, Plug } from '@carbon/icons-react'
import { kindShort } from './egress-detail-data'
import { PolicyAttachFlow } from './policies-attach-flow'
import { type PolicyDetail, type PolicyEffectiveRoute } from './policies-detail-data'
import { type PolicySpec } from './policies-data'

type TabId = 'spec' | 'attach'

const SpecIcon = <Document size={16} />
const AttachIcon = <Plug size={16} />

export interface PolicyDetailViewProps {
  detail: PolicyDetail
  /** Header actions (YAML / Edit / Delete) wired by the container. */
  actions?: ReactNode
  /** Navigate to an effective route's owning gateway. */
  onOpenGateway?: (name: string) => void
}

export function PolicyDetailView({ detail: p, actions, onOpenGateway }: PolicyDetailViewProps) {
  const [tab, setTab] = useState<TabId>('spec')
  const ok = p.tone === 'ok'

  const tabs: Array<{ id: TabId; label: string; icon: ReactNode; count?: number }> = [
    { id: 'spec', label: 'Specification', icon: SpecIcon },
    { id: 'attach', label: 'Attachment', icon: AttachIcon, count: p.targets.length || undefined },
  ]

  return (
    <div className="pol-detail-page">
      <div className="page-head">
        <div className="page-head-l">
          <div className="page-head-titles">
            <div className="page-h1-row">
              <h1 className="page-h1">{p.name}</h1>
              {p.tone === 'none' ? null : ok ? (
                <span className="gw-status">
                  <span className="pulse" />
                  Accepted
                </span>
              ) : (
                <span className="chip chip--amber" title={p.statusReason || ''}>
                  {p.status}
                </span>
              )}
            </div>
            <div className="meta">
              <span>{p.kind}</span>
              <span className="dot-sep">·</span>
              <span>{p.namespace}</span>
              <span className="dot-sep">·</span>
              <span>{p.age}</span>
              {p.author && (
                <>
                  <span className="dot-sep">·</span>
                  <span>{p.author}</span>
                </>
              )}
            </div>
          </div>
        </div>
        <div className="page-head-r">{actions}</div>
      </div>

      {/* Status summary band — what it does + where it lands. */}
      <div className="pol-detail-band">
        <div className="pol-detail-band-main">
          <div className="pol-detail-status">
            <span className={'pol-status-dot' + (ok || p.tone === 'none' ? '' : ' is-warn')} />
            {p.headline}
          </div>
          <div className="pol-detail-summary">{p.summary}</div>
          {p.tone === 'warn' && p.statusReason && (
            <div className="pol-d-warn">⚠ {p.statusReason}</div>
          )}
        </div>
        <div className="pol-detail-vitals">
          <Vital lab="scope" val={p.vitals.scope} />
          <Vital lab="routes" val={p.vitals.routeCount} />
          <Vital lab="gateways" val={p.vitals.gwCount} />
        </div>
      </div>

      <div className="tabs">
        {tabs.map((t) => (
          <div
            key={t.id}
            className={'tab' + (tab === t.id ? ' is-active' : '')}
            onClick={() => setTab(t.id)}
          >
            {t.icon}
            {t.label}
            {t.count != null && <span className="tab-ct">{t.count}</span>}
          </div>
        ))}
      </div>

      {tab === 'spec' && (
        <div className="pol-detail-grid">
          <PolicySpecCard p={p} />
          <PolPanel
            title="Effective on"
            sub={`${p.effective.length} route${p.effective.length === 1 ? '' : 's'}`}
            flush
          >
            <EffectiveTable p={p} onOpenGateway={onOpenGateway} />
          </PolPanel>
        </div>
      )}

      {tab === 'attach' && (
        <PolPanel title="Attachment" sub="how this policy reaches its targets" flush>
          <PolicyAttachFlow detail={p} />
        </PolPanel>
      )}
    </div>
  )
}

function Vital({ lab, val }: { lab: string; val: string | number }) {
  return (
    <div className="pol-vital">
      <div className="pol-vital-lab">{lab}</div>
      <div className="pol-vital-val">{val}</div>
    </div>
  )
}

// Card chrome shared by the detail-page sections.
function PolPanel({
  title,
  sub,
  right,
  children,
  flush,
}: {
  title: string
  sub?: string
  right?: ReactNode
  children: ReactNode
  flush?: boolean
}) {
  return (
    <section className="pol-panel">
      <header className="pol-panel-hd">
        <div className="pol-panel-titles">
          <span className="pol-panel-t">{title}</span>
          {sub && <span className="pol-panel-s">{sub}</span>}
        </div>
        {right}
      </header>
      <div className={'pol-panel-bd' + (flush ? ' pol-panel-bd--flush' : '')}>{children}</div>
    </section>
  )
}

// ── Effective-on table ───────────────────────────────────────────────────────

function EffectiveTable({
  p,
  onOpenGateway,
}: {
  p: PolicyDetail
  onOpenGateway?: (name: string) => void
}) {
  if (p.effective.length === 0) {
    const msg =
      p.tone === 'none'
        ? 'Referenced by route rules — this policy applies wherever a route rule names it.'
        : 'No routes match — policy is not in effect.'
    return (
      <div className="pol-eff-empty" style={{ margin: 18 }}>
        {msg}
      </div>
    )
  }
  return (
    <table className="ovw-table pol-eff-table">
      <thead>
        <tr>
          <th>Via</th>
          <th>Host</th>
          <th>Route</th>
          <th>Gateway</th>
          <th />
        </tr>
      </thead>
      <tbody>
        {p.effective.map((e, i) => (
          <EffRow key={`${e.routeKind}/${e.routeName}-${i}`} e={e} onOpenGateway={onOpenGateway} />
        ))}
      </tbody>
    </table>
  )
}

function EffRow({
  e,
  onOpenGateway,
}: {
  e: PolicyEffectiveRoute
  onOpenGateway?: (name: string) => void
}) {
  const clickable = onOpenGateway && e.gatewayName && e.gatewayName !== '—'
  return (
    <tr
      className={'pol-eff-tr pol-eff-tr--' + e.via + (clickable ? ' gws-row' : '')}
      onClick={clickable ? () => onOpenGateway!(e.gatewayName) : undefined}
    >
      <td>
        <span className={'pol-eff-via pol-eff-via--' + e.via}>{e.via}</span>
      </td>
      <td style={{ fontFamily: 'var(--font-mono)', fontSize: 14 }}>{e.host}</td>
      <td>
        <RouteKindTag kind={e.routeKind} />
        <span style={{ marginLeft: 6, fontFamily: 'var(--font-mono)', fontSize: 13 }}>
          {e.routeName}
        </span>
      </td>
      <td style={{ fontFamily: 'var(--font-mono)', fontSize: 13, color: 'var(--text-muted)' }}>
        {e.gatewayName}
      </td>
      <td style={{ textAlign: 'right', color: 'var(--apx-stone)', fontFamily: 'var(--font-mono)' }}>
        {clickable ? '›' : ''}
      </td>
    </tr>
  )
}

// ── per-kind specification cards ─────────────────────────────────────────────

function PolicySpecCard({ p }: { p: PolicyDetail }) {
  const [raw, setRaw] = useState(false)
  const Body = SPEC_BODIES[p.kind]
  return (
    <PolPanel
      title="Specification"
      sub={`${p.short}.spec`}
      right={
        <button type="button" className="spec-toggle" onClick={() => setRaw((r) => !r)}>
          {raw ? 'Visual' : 'Raw paths'}
        </button>
      }
    >
      {raw || !Body ? (
        <div className="kvblock">
          {p.fields.map((row, i) => (
            <div key={i} className="kvrow">
              <span className="kvk">{row.k}</span>
              <span className="kvv">{row.v}</span>
            </div>
          ))}
        </div>
      ) : (
        <Body spec={p.spec} />
      )}
    </PolPanel>
  )
}

function SpecField({ label, children, mono }: { label: string; children: ReactNode; mono?: boolean }) {
  return (
    <div className="spec-field">
      <div className="spec-flab">{label}</div>
      <div className={'spec-fval' + (mono ? ' mono' : '')}>{children}</div>
    </div>
  )
}

function ChipRow({ items, variant }: { items: string[]; variant?: string }) {
  return (
    <div className="spec-chiprow">
      {items.map((it, i) => (
        <span key={i} className={'chip' + (variant ? ' ' + variant : '')}>
          {it}
        </span>
      ))}
    </div>
  )
}

/* CredentialInjectionPolicy — "take this secret → put it here". */
function SpecCredentialInjection({ spec }: { spec: PolicySpec }) {
  const target = spec.target
  const secret = spec.secretRef?.name ?? '—'
  const key = spec.secretKey ?? 'token'
  const secref = (
    <span className="cipr-secref">
      <span className="cipr-lock">⚷</span>
      {secret}
      <span className="cipr-slash"> / </span>
      <span className="cipr-key">{key}</span>
    </span>
  )

  let verb: string
  let line: ReactNode
  let foot: ReactNode
  if (target === 'Header') {
    const h = spec.headerName ?? '?'
    verb = 'set header'
    line = (
      <>
        <span className="cipr-hdr">
          <span className="cipr-k">{h}</span>
          <span className="cipr-sep">: </span>
        </span>
        {secref}
      </>
    )
    foot = (
      <>
        Reads <code>{secret}[{key}]</code> and writes it to the <code>{h}</code> request header on
        every matched request.
      </>
    )
  } else if (target === 'QueryParam') {
    const q = spec.queryParamName ?? '?'
    verb = 'set query'
    line = (
      <>
        <span className="cipr-hdr">
          <span className="cipr-k">?{q}</span>
          <span className="cipr-sep">=</span>
        </span>
        {secref}
      </>
    )
    foot = (
      <>
        Appends <code>?{q}=…</code> to every matched request URL, sourced from{' '}
        <code>{secret}[{key}]</code>.
      </>
    )
  } else {
    const type = spec.providerAuth?.type ?? 'ProviderAuth'
    const scope = [spec.providerAuth?.service, spec.providerAuth?.region].filter(Boolean).join(' / ')
    verb = 'sign ' + type
    line = (
      <>
        {secref}
        {scope && (
          <span className="cipr-hdr">
            <span className="cipr-sep"> · </span>
            {scope}
          </span>
        )}
      </>
    )
    foot = (
      <>
        Signs each request with <code>{type}</code> using <code>{secret}[{key}]</code>
        {scope ? <> ({scope})</> : null}.
      </>
    )
  }

  return (
    <div className="cipr">
      <div className="cipr-line">
        <span className="cipr-verb">{verb}</span>
        {line}
      </div>
      <div className="cipr-foot">{foot}</div>
    </div>
  )
}

// HTTP reason phrases for the deny response status line (design's HTTP_REASON).
const HTTP_REASON: Record<string, string> = {
  '400': 'Bad Request',
  '401': 'Unauthorized',
  '402': 'Payment Required',
  '403': 'Forbidden',
  '404': 'Not Found',
  '405': 'Method Not Allowed',
  '406': 'Not Acceptable',
  '409': 'Conflict',
  '410': 'Gone',
  '422': 'Unprocessable Entity',
  '423': 'Locked',
  '429': 'Too Many Requests',
  '451': 'Unavailable For Legal Reasons',
  '500': 'Internal Server Error',
  '502': 'Bad Gateway',
  '503': 'Service Unavailable',
}

/* EgressDenyPolicy — kill switch. Render the literal response the caller gets. */
function SpecEgressDeny({ spec }: { spec: PolicySpec }) {
  const code = spec.denyResponse?.statusCode ?? 403
  const msg = spec.denyResponse?.message
  const reason = HTTP_REASON[String(code)] ?? ''
  return (
    <div className="deny">
      <div className="deny-banner">
        <span className="deny-mark" aria-hidden="true">
          <svg
            width="14"
            height="14"
            viewBox="0 0 14 14"
            fill="none"
            stroke="currentColor"
            strokeWidth="1.8"
            strokeLinecap="round"
          >
            <path d="M3 3l8 8M11 3l-8 8" />
          </svg>
        </span>
        <div className="deny-banner-txt">
          <div className="deny-banner-head">Matched requests are blocked at the gateway</div>
          <div className="deny-banner-sub">
            No upstream connection is opened — the caller gets this synthetic response instead.
          </div>
        </div>
      </div>
      <div className="deny-resp">
        <div className="deny-resp-lab">Response status</div>
        <div className="deny-statusline">
          <span className="deny-code">{code}</span>
          {reason && <span className="deny-reason">{reason}</span>}
        </div>
        {msg && <div className="deny-body">{msg}</div>}
      </div>
    </div>
  )
}

/* RateLimitPolicy — requests / window, scope. */
function SpecRateLimit({ spec }: { spec: PolicySpec }) {
  const reqs = spec.requests ?? '?'
  const win = spec.window ?? '?'
  const scope = spec.scope ?? 'PerAgent'
  return (
    <div className="spec">
      <div className="spec-hero-row">
        <div className="spec-hero">
          <div className="spec-hero-num">{reqs}</div>
          <div className="spec-hero-unit">requests / {win}</div>
        </div>
        <div className="spec-hero-sep" />
        <SpecField label="Scope">
          <span className="chip chip--ink">{scope}</span>
        </SpecField>
      </div>
      <div className="spec-cap">
        {scope === 'PerRoute'
          ? 'One shared counter for the whole route.'
          : scope === 'PerAgent'
            ? 'An independent counter per calling agent identity.'
            : scope === 'PerExecution'
              ? 'An independent counter per agent execution.'
              : 'Counter keyed by ' + scope + '.'}
      </div>
    </div>
  )
}

/* LoggingPolicy — capture toggles, redaction, sink. */
function SpecLogging({ spec }: { spec: PolicySpec }) {
  const req = Boolean(spec.captureRequest)
  const res = Boolean(spec.captureResponse)
  const redact = spec.redactHeaders ?? []
  const sink = spec.sinkRef
  return (
    <div className="spec">
      <div className="spec-grid-2">
        <SpecField label="Capture request">
          <span className={'chip' + (req ? ' chip--leaf' : '')}>
            {req ? 'body + headers' : 'metadata only'}
          </span>
        </SpecField>
        <SpecField label="Capture response">
          <span className={'chip' + (res ? ' chip--leaf' : '')}>
            {res ? 'body + headers' : 'metadata only'}
          </span>
        </SpecField>
      </div>
      {redact.length > 0 && (
        <SpecField label="Redacted headers">
          <ChipRow items={redact} />
        </SpecField>
      )}
      <SpecField label="OTLP sink" mono>
        {sink ?? 'embedded ClickHouse'}
      </SpecField>
    </div>
  )
}

/* FallbackRoutingPolicy — ordered inter-backend fallback (no design analog; told
   in the same spec vocabulary as the four reference cards). */
function SpecFallback({ spec }: { spec: PolicySpec }) {
  const n = spec.retry?.numRetries
  const codes = spec.retry?.retriableStatusCodes
  const retriable = codes && codes.length ? codes.map(String) : ['429', '503']
  return (
    <div className="spec">
      <div className="spec-hero-row">
        <div className="spec-hero">
          <div className="spec-hero-num">{n != null ? n : 'auto'}</div>
          <div className="spec-hero-unit">{n === 1 ? 'retry' : 'retries'}</div>
        </div>
        <div className="spec-hero-sep" />
        <div className="spec-hero-note">
          <SpecField label="Retriable status codes">
            <ChipRow items={retriable} variant="chip--amber" />
          </SpecField>
          <span className="spec-cap">
            On a failed attempt, the request is retried against the next backend in the rule's
            BackendRefs order (weights are ignored).
          </span>
        </div>
      </div>
      {spec.retry?.perTryTimeout && (
        <SpecField label="Per-try timeout" mono>
          {spec.retry.perTryTimeout}
        </SpecField>
      )}
      {spec.ejection?.maxEjectionTime && (
        <SpecField label="Max ejection time" mono>
          {spec.ejection.maxEjectionTime}
        </SpecField>
      )}
    </div>
  )
}

const SPEC_BODIES: Partial<Record<PolicyDetail['kind'], (props: { spec: PolicySpec }) => ReactNode>> = {
  CredentialInjectionPolicy: SpecCredentialInjection,
  EgressDenyPolicy: SpecEgressDeny,
  RateLimitPolicy: SpecRateLimit,
  LoggingPolicy: SpecLogging,
  FallbackRoutingPolicy: SpecFallback,
}

// ── small helpers ────────────────────────────────────────────────────────────

function RouteKindTag({ kind }: { kind: string }) {
  return <span className={'kind-tag kind-tag--' + kind.toLowerCase()}>{kindShort(kind)}</span>
}
