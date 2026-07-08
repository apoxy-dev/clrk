// The Notification Event Detail Tray -- the design mock's `.evt-tray`. Every
// notification is backed by an events.k8s.io/v1 Event; opening one slides in a
// right-side tray that surfaces the Event across three tabs: Details (the human
// view -- reason + action + message, occurrences with a series timeline, the
// object it regards with a jump to its console page, and related events on the
// same object), Metadata (reporting + Event bookkeeping + the involvedObject
// ref), and YAML (the raw Event). Reuses the SpanAttrsTray scrim + attrs-tray-in
// animation, and console-core's YamlCode/toYaml for the raw view. Presentational:
// data is the mapped NotificationVM; related events are derived from the same
// watched Event set by the caller.

import { useEffect, useMemo, useState } from 'react'
import {
  ArrowRight,
  Checkmark,
  Close,
  Copy,
  Cube,
  Launch,
} from '@carbon/icons-react'
import { YamlCode, toYaml } from '@apoxy/console-core'
import {
  CATEGORY_ICON,
  SEVERITY_GLYPH,
  evtToneVars,
  highlightRefs,
  relTime,
  severityBadgeClass,
} from './notification-visual'
import {
  describeCmd,
  eventToYamlObject,
  type NotificationVM,
  type RelatedEvent,
} from './notifications-data'

export interface NotificationDetailTrayProps {
  item: NotificationVM
  related?: RelatedEvent[]
  onClose: () => void
  onOpenObject?: (regarding: {
    kind: string
    name: string
    namespace: string
  }) => void
}

type Tab = 'details' | 'meta' | 'yaml'
const TABS: { key: Tab; label: string }[] = [
  { key: 'details', label: 'Details' },
  { key: 'meta', label: 'Metadata' },
  { key: 'yaml', label: 'YAML' },
]

// A clipboard copy button that flips to a check + "Copied" for a beat. Shared by
// the YAML bar and the footer's kubectl affordance.
function CopyBtn({
  text,
  label,
  className = 'evt-copy-btn',
}: {
  text: string
  label: string
  className?: string
}) {
  const [done, setDone] = useState(false)
  return (
    <button
      type="button"
      className={className}
      onClick={(e) => {
        e.stopPropagation()
        // Only flip to "Copied" if the write actually succeeds. In an insecure
        // context (plain http on a non-localhost host) navigator.clipboard is
        // undefined, so the write never happens -- claiming "Copied" there would
        // be a lie and the user would paste stale content.
        void navigator.clipboard
          ?.writeText(text)
          .then(() => {
            setDone(true)
            window.setTimeout(() => setDone(false), 1400)
          })
          .catch(() => {})
      }}
    >
      {done ? <Checkmark size={12} /> : <Copy size={12} />}
      {done ? 'Copied' : label}
    </button>
  )
}

// HH:MM:SSZ from an RFC3339 timestamp (empty when absent/short).
function clockOf(iso: string): string {
  if (!iso || iso.length < 19) return ''
  return iso.slice(11, 19) + 'Z'
}

export function NotificationDetailTray({
  item,
  related = [],
  onClose,
  onOpenObject,
}: NotificationDetailTrayProps) {
  const [tab, setTab] = useState<Tab>('details')
  const Glyph = SEVERITY_GLYPH[item.severity]
  const OwnerIcon = CATEGORY_ICON[item.category]
  const involved = item.involved
  const objRef = involved ? `${involved.kind}/${involved.name}` : item.reason
  const yaml = useMemo(() => toYaml(eventToYamlObject(item)), [item])
  const canGoto = Boolean(item.regarding && onOpenObject)

  // Snap back to the Details tab whenever a different notification is opened.
  useEffect(() => {
    setTab('details')
  }, [item.id])

  // Escape closes the tray (the scrim only handles pointer dismissal), matching
  // the attrs-tray precedent.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  }, [onClose])

  const hasReporting =
    item.reportingController ||
    item.reportingInstance ||
    item.source.component ||
    item.source.host
  const describe = describeCmd(item)
  const hasFooter = Boolean(describe) || canGoto

  return (
    <>
      <div className="attrs-tray-scrim" onClick={onClose} />
      <aside
        className="evt-tray"
        role="dialog"
        aria-label={`Event ${item.reason}`}
        style={evtToneVars(item.severity)}
      >
        <header className="evt-hd">
          <span className="evt-glyph">
            <Glyph size={20} />
          </span>
          <div className="evt-hd-titles">
            <div className="evt-eyebrow">
              <span>events.k8s.io/v1 · Event</span>
              <span className="dot" />
              <span>{item.type}</span>
            </div>
            <div className="evt-reason">{item.reason}</div>
            {involved ? (
              <div className="evt-objline">
                <span>on</span>
                <span className="path">{objRef}</span>
                <span
                  className="dot"
                  style={{
                    width: 3,
                    height: 3,
                    background: 'var(--apx-stone)',
                  }}
                />
                <span>{involved.namespace}</span>
              </div>
            ) : null}
          </div>
          <button
            className="evt-hd-close"
            onClick={onClose}
            title="Close (Esc)"
            aria-label="Close"
          >
            <Close size={12} />
          </button>
        </header>

        <nav className="evt-tabs" role="tablist" aria-label="Event detail">
          {TABS.map((t) => (
            <button
              key={t.key}
              type="button"
              role="tab"
              aria-selected={tab === t.key}
              className={'evt-tab' + (tab === t.key ? ' is-active' : '')}
              onClick={() => setTab(t.key)}
            >
              {t.label}
            </button>
          ))}
        </nav>

        <div className="evt-bd">
          {/* ---- DETAILS ---- */}
          {tab === 'details' ? (
            <>
              {/* Message + badges. Order: severity - action - reason - count. */}
              <section className="evt-sec">
                <div className="evt-badges">
                  <span
                    className={'evt-badge ' + severityBadgeClass(item.severity)}
                  >
                    <Glyph size={12} />
                    {item.type}
                  </span>
                  {item.action ? (
                    <span
                      className="evt-badge evt-badge--action"
                      title="action - operation taken on the object"
                    >
                      {item.action}
                    </span>
                  ) : null}
                  <span
                    className="evt-badge evt-badge--reason"
                    title="reason - machine-readable outcome code"
                  >
                    {item.reason}
                  </span>
                  {item.count > 1 ? (
                    <span className="evt-badge evt-badge--count">
                      ×{item.count}
                    </span>
                  ) : null}
                </div>
                <div className="evt-msg">
                  {item.message ? highlightRefs(item.message) : item.title}
                </div>
              </section>

              {/* Occurrences */}
              <section className="evt-sec">
                <div className="evt-sec-h">
                  <span className="evt-sec-lab">Occurrences</span>
                </div>
                <div className="evt-facts">
                  <div className="evt-fact">
                    <div className="k">Count</div>
                    <div className="v">
                      {item.count}
                      <span className="unit">
                        {item.count === 1 ? 'time' : 'times'}
                      </span>
                    </div>
                    <div className="sub">aggregated</div>
                  </div>
                  <div className="evt-fact">
                    <div className="k">First seen</div>
                    <div className="v mono">{relTime(item.firstMs) || '—'}</div>
                    <div className="sub">{clockOf(item.firstTimestamp)}</div>
                  </div>
                  <div className="evt-fact">
                    <div className="k">Last seen</div>
                    <div className="v mono">{relTime(item.lastMs) || '—'}</div>
                    <div className="sub">{clockOf(item.lastTimestamp)}</div>
                  </div>
                </div>
                {item.count > 1 ? (
                  <div className="evt-tl">
                    <div className="evt-tl-track">
                      <span
                        className="evt-tl-tick"
                        style={{ left: '2%', height: '55%' }}
                      />
                      <span
                        className="evt-tl-tick is-last"
                        style={{ left: '97%', height: '95%' }}
                      />
                    </div>
                    <div className="evt-tl-labels">
                      <span>{relTime(item.firstMs) || 'first'}</span>
                      <span>now</span>
                    </div>
                  </div>
                ) : null}
              </section>

              {/* Involved object */}
              {involved ? (
                <section className="evt-sec">
                  <div className="evt-sec-h">
                    <span className="evt-sec-lab">Involved object</span>
                  </div>
                  <div className="evt-obj">
                    <div className="evt-obj-main">
                      <span className="evt-obj-ico">
                        <Cube size={17} />
                      </span>
                      <div className="evt-obj-txt">
                        <div className="evt-obj-name">{involved.name}</div>
                        <div className="evt-obj-tags">
                          <span className="kind-tag">{involved.kind}</span>
                          <span className="evt-obj-ns">
                            ns/{involved.namespace}
                          </span>
                          {involved.fieldPath ? (
                            <span className="evt-obj-field">
                              {involved.fieldPath}
                            </span>
                          ) : null}
                        </div>
                      </div>
                    </div>
                    {canGoto ? (
                      <button
                        type="button"
                        className="evt-obj-goto"
                        onClick={() => onOpenObject!(item.regarding!)}
                      >
                        <span className="lead">
                          <span className="owner-ico">
                            <OwnerIcon size={15} />
                          </span>
                          <span className="txt">
                            <span className="r1">Open in console</span>
                            <span className="r2">
                              {involved.kind}/{involved.name}
                            </span>
                          </span>
                        </span>
                        <span className="arrow">
                          <Launch size={15} />
                        </span>
                      </button>
                    ) : null}
                  </div>
                </section>
              ) : null}

              {/* Related events on this object */}
              {related.length > 0 ? (
                <section className="evt-sec">
                  <div className="evt-sec-h">
                    <span className="evt-sec-lab">
                      Related events on this object
                    </span>
                    <span className="evt-sec-count">{related.length} more</span>
                  </div>
                  <div className="evt-rel">
                    {related.map((r) => (
                      <div key={r.id} className="evt-rel-row">
                        <span
                          className={
                            'evt-rel-pip evt-rel-pip--' +
                            (r.type === 'Warning' ? 'warning' : 'normal')
                          }
                        />
                        <div className="evt-rel-txt">
                          <div className="evt-rel-reason">{r.reason}</div>
                          <div className="evt-rel-msg">{r.message || '—'}</div>
                        </div>
                        <div className="evt-rel-meta">
                          <div className="evt-rel-count">
                            {r.count > 1 ? '×' + r.count : 'once'}
                          </div>
                          <div className="evt-rel-ago">
                            {relTime(r.agoMs) || ''}
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                </section>
              ) : null}
            </>
          ) : null}

          {/* ---- METADATA ---- */}
          {tab === 'meta' ? (
            <>
              {hasReporting ? (
                <section className="evt-sec">
                  <div className="evt-sec-h">
                    <span className="evt-sec-lab">Reporting</span>
                  </div>
                  <div className="evt-kv">
                    {item.reportingController ? (
                      <div className="row">
                        <span className="k">reportingController</span>
                        <span className="v">{item.reportingController}</span>
                      </div>
                    ) : null}
                    {item.reportingInstance ? (
                      <div className="row">
                        <span className="k">reportingInstance</span>
                        <span className="v">{item.reportingInstance}</span>
                      </div>
                    ) : null}
                    {item.source.component ? (
                      <div className="row">
                        <span className="k">source.component</span>
                        <span className="v dim">{item.source.component}</span>
                      </div>
                    ) : null}
                    {item.source.host ? (
                      <div className="row">
                        <span className="k">source.host</span>
                        <span className="v dim">{item.source.host}</span>
                      </div>
                    ) : null}
                  </div>
                </section>
              ) : null}

              <section className="evt-sec">
                <div className="evt-sec-h">
                  <span className="evt-sec-lab">Event metadata</span>
                </div>
                <div className="evt-kv">
                  <div className="row">
                    <span className="k">name</span>
                    <span className="v">{item.meta.name || '—'}</span>
                  </div>
                  <div className="row">
                    <span className="k">namespace</span>
                    <span className="v">{item.meta.namespace || '—'}</span>
                  </div>
                  <div className="row">
                    <span className="k">uid</span>
                    <span className="v dim">{item.meta.uid || '—'}</span>
                  </div>
                  <div className="row">
                    <span className="k">resourceVersion</span>
                    <span className="v dim">
                      {item.meta.resourceVersion || '—'}
                    </span>
                  </div>
                  <div className="row">
                    <span className="k">creationTimestamp</span>
                    <span className="v dim">
                      {item.meta.creationTimestamp || '—'}
                    </span>
                  </div>
                </div>
              </section>

              {involved ? (
                <section className="evt-sec">
                  <div className="evt-sec-h">
                    <span className="evt-sec-lab">Involved object ref</span>
                  </div>
                  <div className="evt-kv">
                    <div className="row">
                      <span className="k">apiVersion</span>
                      <span className="v dim">{involved.apiVersion || '—'}</span>
                    </div>
                    <div className="row">
                      <span className="k">kind</span>
                      <span className="v">{involved.kind}</span>
                    </div>
                    <div className="row">
                      <span className="k">name</span>
                      <span className="v">{involved.name}</span>
                    </div>
                    <div className="row">
                      <span className="k">uid</span>
                      <span className="v dim">{involved.uid || '—'}</span>
                    </div>
                    {involved.fieldPath ? (
                      <div className="row">
                        <span className="k">fieldPath</span>
                        <span className="v dim">{involved.fieldPath}</span>
                      </div>
                    ) : null}
                  </div>
                </section>
              ) : null}
            </>
          ) : null}

          {/* ---- YAML ---- */}
          {tab === 'yaml' ? (
            <section className="evt-sec evt-yaml-sec">
              <div className="evt-yaml-bar">
                <span className="evt-yaml-title">events.k8s.io/v1 · Event</span>
                <span className="spacer" />
                <CopyBtn text={yaml} label="Copy YAML" />
              </div>
              <div className="evt-yaml-body">
                <YamlCode text={yaml} />
              </div>
            </section>
          ) : null}
        </div>

        {hasFooter ? (
          <footer className="evt-foot">
            {describe ? (
              <CopyBtn text={describe} label="kubectl describe" />
            ) : null}
            <span className="spacer" />
            {canGoto ? (
              <button
                className="btn btn--primary"
                onClick={() => onOpenObject!(item.regarding!)}
              >
                Go to {item.regarding!.kind} <ArrowRight size={14} />
              </button>
            ) : null}
          </footer>
        ) : null}
      </aside>
    </>
  )
}
