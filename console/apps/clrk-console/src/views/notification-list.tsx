// The full-page notification list: rows grouped into Today / Earlier day
// buckets (the mock's grouping), each a NotificationRow. Renders the
// "all caught up" empty state when there is nothing to show.

import { Fragment } from 'react'
import { CheckmarkOutline } from '@carbon/icons-react'
import { groupByDay } from './notification-visual'
import { NotificationRow } from './notification-row'
import type { NotificationVM } from './notifications-data'

export interface NotificationListProps {
  rows: NotificationVM[]
  selectedId?: string
  onSelect: (id: string) => void
}

export function NotificationEmpty({ label }: { label?: string }) {
  return (
    <div className="nc-empty">
      <div className="ring">
        <CheckmarkOutline size={20} />
      </div>
      <div className="t">You're all caught up</div>
      <div className="s">{label ?? 'No notifications match this view.'}</div>
    </div>
  )
}

export function NotificationList({
  rows,
  selectedId,
  onSelect,
}: NotificationListProps) {
  if (rows.length === 0) {
    return <NotificationEmpty />
  }
  const groups = groupByDay(rows)
  return (
    <div className="nc-list">
      {groups.map((g) => (
        <Fragment key={g.key}>
          <div className="nc-group">
            {g.label}
            <span className="gn">{g.rows.length}</span>
          </div>
          {g.rows.map((r) => (
            <NotificationRow
              key={r.id}
              vm={r}
              selected={selectedId === r.id}
              onOpen={onSelect}
            />
          ))}
        </Fragment>
      ))}
    </div>
  )
}
