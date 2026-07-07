// Single-notification detail in a slide-in tray (the SpanAttrsTray precedent:
// .attrs-tray-scrim + .attrs-tray + role="dialog"). The head carries the
// severity-toned category glyph; the body shows the event note and metadata,
// with a primary action to jump to the regarding object.

import { useEffect } from 'react'
import { ArrowUpRight } from '@carbon/icons-react'
import {
  CATEGORY_ICON,
  categoryLabel,
  toneVars,
} from './notification-visual'
import type { NotificationVM } from './notifications-data'

export interface NotificationDetailTrayProps {
  item: NotificationVM
  onClose: () => void
  onOpenObject?: (regarding: {
    kind: string
    name: string
    namespace: string
  }) => void
}

export function NotificationDetailTray({
  item,
  onClose,
  onOpenObject,
}: NotificationDetailTrayProps) {
  const Icon = CATEGORY_ICON[item.category]

  // Escape closes the tray (the scrim only handles pointer dismissal), matching
  // the attrs-tray precedent.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  }, [onClose])

  return (
    <>
      <div className="attrs-tray-scrim" onClick={onClose} />
      <aside
        className="attrs-tray notif-detail"
        role="dialog"
        aria-label="Notification detail"
      >
        <div className="attrs-tray-head">
          <div className="notif-detail-id" style={toneVars(item.severity)}>
            <span className="nc-ico">
              <Icon size={16} />
            </span>
            <div>
              <div className="notif-detail-reason">{item.reason}</div>
              <div className="notif-detail-sub">
                {categoryLabel(item.category)} · {item.type}
              </div>
            </div>
          </div>
          <button className="btn--secondary" onClick={onClose}>
            Close
          </button>
        </div>

        <h3 className="notif-detail-title">{item.title}</h3>
        <p className="notif-detail-msg">{item.message || '—'}</p>

        <dl className="notif-detail-dl">
          <dt>Type</dt>
          <dd>{item.type}</dd>
          <dt>Action</dt>
          <dd>{item.action || '—'}</dd>
          <dt>Occurrences</dt>
          <dd>{item.count}</dd>
          <dt>Namespace</dt>
          <dd>{item.namespace || '—'}</dd>
          {item.regarding ? (
            <>
              <dt>Regarding</dt>
              <dd>
                {item.regarding.kind}/{item.regarding.name}
              </dd>
            </>
          ) : null}
        </dl>

        {item.regarding && onOpenObject ? (
          <button
            className="nc-act nc-act--primary notif-detail-goto"
            onClick={() => onOpenObject(item.regarding!)}
          >
            Go to {item.regarding.kind} <ArrowUpRight size={12} />
          </button>
        ) : null}
      </aside>
    </>
  )
}
