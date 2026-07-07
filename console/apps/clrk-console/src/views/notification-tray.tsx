// The topbar bell's dropdown -- the design mock's `.nc-pop` triage popover:
// header with the unread count + mark-all, All / Unread / Needs-action tabs, a
// day-grouped list of the most recent notifications, and a footer linking to the
// full page. A transparent scrim behind it closes on outside click.

import { Fragment, useEffect, useState } from 'react'
import { ArrowRight, Checkmark, Settings } from '@carbon/icons-react'
import { groupByDay } from './notification-visual'
import { NotificationRow } from './notification-row'
import { NotificationEmpty } from './notification-list'
import type { NotificationVM } from './notifications-data'

export interface NotificationTrayProps {
  rows: NotificationVM[]
  unread: number
  onClose: () => void
  onMarkAllRead: () => void
  onViewAll: () => void
  onOpen: (id: string) => void
}

type Tab = 'all' | 'unread' | 'action'

// The popover shows only the most recent few; the footer links to the full page.
const TRAY_LIMIT = 8

function needsAction(r: NotificationVM): boolean {
  return r.severity === 'critical' || r.severity === 'warning'
}

const TABS: { key: Tab; label: string }[] = [
  { key: 'all', label: 'All' },
  { key: 'unread', label: 'Unread' },
  { key: 'action', label: 'Needs action' },
]

export function NotificationTray({
  rows,
  unread,
  onClose,
  onMarkAllRead,
  onViewAll,
  onOpen,
}: NotificationTrayProps) {
  const [tab, setTab] = useState<Tab>('all')

  // Escape closes the popover (the scrim only handles pointer dismissal), so
  // keyboard users can dismiss it -- matching the attrs-tray precedent.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  }, [onClose])

  // Counts are computed over the FULL row set (not the capped display slice) so
  // the tab badges agree with the header's unread total; only the rendered list
  // is capped to the most recent TRAY_LIMIT.
  const counts = {
    all: rows.length,
    unread: rows.reduce((n, r) => (r.read ? n : n + 1), 0),
    action: rows.filter(needsAction).length,
  }
  let list = rows
  if (tab === 'unread') list = rows.filter((r) => !r.read)
  else if (tab === 'action') list = rows.filter(needsAction)
  const groups = groupByDay(list.slice(0, TRAY_LIMIT))

  return (
    <>
      <div className="nc-scrim" onClick={onClose} />
      <div className="nc-pop" role="dialog" aria-label="Notifications">
        <div className="nc-caret" />

        <div className="nc-head">
          <div className="nc-head-l">
            <span className="nc-h-title">Notifications</span>
            <span className={'nc-count' + (unread === 0 ? ' is-zero' : '')}>
              {unread} new
            </span>
          </div>
          <button
            className="nc-markall"
            onClick={onMarkAllRead}
            disabled={unread === 0}
          >
            <Checkmark size={13} /> Mark all read
          </button>
        </div>

        <div className="nc-tabs">
          {TABS.map((t) => (
            <button
              key={t.key}
              className={'nc-tab' + (tab === t.key ? ' is-active' : '')}
              onClick={() => setTab(t.key)}
            >
              {t.label}
              <span className="n">{counts[t.key]}</span>
            </button>
          ))}
        </div>

        <div className="nc-list">
          {list.length === 0 ? (
            <NotificationEmpty label="No unread notifications right now." />
          ) : (
            groups.map((g) => (
              <Fragment key={g.key}>
                <div className="nc-group">
                  {g.label}
                  <span className="gn">{g.rows.length}</span>
                </div>
                {g.rows.map((r) => (
                  <NotificationRow key={r.id} vm={r} onOpen={onOpen} />
                ))}
              </Fragment>
            ))
          )}
        </div>

        <div className="nc-foot">
          <a
            href="#"
            onClick={(e) => {
              e.preventDefault()
              onViewAll()
            }}
          >
            All notifications <ArrowRight size={13} />
          </a>
          <a
            href="#"
            className="nc-settings"
            title="Notification settings"
            onClick={(e) => {
              e.preventDefault()
              onViewAll()
            }}
          >
            <Settings size={15} />
          </a>
        </div>
      </div>
    </>
  )
}
