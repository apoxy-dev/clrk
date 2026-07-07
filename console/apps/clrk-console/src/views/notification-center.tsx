// The full Notification Center page. Console page-head (consistent with
// Invocations/Policies) over the mock's inbox panel: a toolbar with All / Unread
// / Needs-action tabs and a filter box, then the day-grouped list, then the
// detail tray. Presentational -- data comes from useNotifications via the route
// container; the read model and routes are untouched.

import { useMemo, useState } from 'react'
import { Search } from '@carbon/icons-react'
import type { NotificationVM } from './notifications-data'
import { NotificationList } from './notification-list'
import { NotificationDetailTray } from './notification-detail'

export interface NotificationCenterViewProps {
  rows: NotificationVM[]
  unread: number
  isLoading: boolean
  selectedId?: string
  securityIndicator?: { ok: boolean; label: string }
  onSelect: (id?: string) => void
  onMarkAllRead: () => void
  onOpenObject?: (regarding: {
    kind: string
    name: string
    namespace: string
  }) => void
}

type Tab = 'all' | 'unread' | 'action'

// "Needs action" surfaces the rows an operator should look at: anything at
// warning or critical severity.
function needsAction(r: NotificationVM): boolean {
  return r.severity === 'critical' || r.severity === 'warning'
}

const TABS: { key: Tab; label: string }[] = [
  { key: 'all', label: 'All' },
  { key: 'unread', label: 'Unread' },
  { key: 'action', label: 'Needs action' },
]

export function NotificationCenterView({
  rows,
  unread,
  isLoading,
  selectedId,
  securityIndicator,
  onSelect,
  onMarkAllRead,
  onOpenObject,
}: NotificationCenterViewProps) {
  const [tab, setTab] = useState<Tab>('all')
  const [q, setQ] = useState('')

  const tabCounts = useMemo(
    () => ({
      all: rows.length,
      unread: rows.reduce((n, r) => (r.read ? n : n + 1), 0),
      action: rows.filter(needsAction).length,
    }),
    [rows],
  )

  const filtered = useMemo(() => {
    let list = rows
    if (tab === 'unread') list = list.filter((r) => !r.read)
    else if (tab === 'action') list = list.filter(needsAction)
    const needle = q.trim().toLowerCase()
    if (needle) {
      list = list.filter((r) => {
        const hay = `${r.title} ${r.regarding?.kind ?? ''}/${
          r.regarding?.name ?? ''
        }`.toLowerCase()
        return hay.includes(needle)
      })
    }
    return list
  }, [rows, tab, q])

  const selected = useMemo(
    () => rows.find((r) => r.id === selectedId),
    [rows, selectedId],
  )

  return (
    <>
      <div className="page-head">
        <div className="page-head-l">
          <h1 className="page-h1">Notifications</h1>
          <div className="meta">
            {isLoading ? 'Loading…' : `${rows.length} total · ${unread} unread`}
            {securityIndicator ? (
              <span
                className={
                  'notif-security-ind ' +
                  (securityIndicator.ok ? 'is-ok' : 'is-warn')
                }
                title={securityIndicator.label}
              >
                {securityIndicator.label}
              </span>
            ) : null}
          </div>
        </div>
        <div className="page-head-r">
          <button
            className="btn--secondary"
            onClick={onMarkAllRead}
            disabled={unread === 0}
          >
            Mark all read
          </button>
        </div>
      </div>

      <div className="inbox-panel">
        <div className="inbox-toolbar">
          <div className="nc-tabs">
            {TABS.map((t) => (
              <button
                key={t.key}
                className={'nc-tab' + (tab === t.key ? ' is-active' : '')}
                onClick={() => setTab(t.key)}
              >
                {t.label}
                <span className="n">{tabCounts[t.key]}</span>
              </button>
            ))}
          </div>
          <label className="inbox-search">
            <Search size={14} />
            <input
              value={q}
              onChange={(e) => setQ(e.target.value)}
              placeholder="Filter…"
              aria-label="Filter notifications"
            />
          </label>
        </div>
        <div className="inbox-list">
          <NotificationList
            rows={filtered}
            selectedId={selectedId}
            onSelect={(id) => onSelect(id)}
          />
        </div>
      </div>

      {selected ? (
        <NotificationDetailTray
          item={selected}
          onClose={() => onSelect(undefined)}
          onOpenObject={onOpenObject}
        />
      ) : null}
    </>
  )
}
