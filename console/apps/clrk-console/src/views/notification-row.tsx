// A single notification row -- the design mock's `.nc-item`. Category glyph in a
// tinted square, title + relative time, a description line that highlights the
// regarding object, and a category/occurrence tag row. Presentational only:
// clicking calls onOpen(id); read/unread come straight from the view model.

import {
  CATEGORY_ICON,
  categoryLabel,
  relTime,
  toneVars,
} from './notification-visual'
import type { NotificationVM } from './notifications-data'

export interface NotificationRowProps {
  vm: NotificationVM
  onOpen: (id: string) => void
  selected?: boolean
  showCat?: boolean
}

export function NotificationRow({
  vm,
  onOpen,
  selected = false,
  showCat = true,
}: NotificationRowProps) {
  const Icon = CATEGORY_ICON[vm.category]
  const hasTags = showCat || vm.count > 1
  return (
    <div
      className={
        'nc-item ' +
        (vm.read ? 'is-read' : 'is-unread') +
        (selected ? ' is-selected' : '')
      }
      style={toneVars(vm.severity)}
      role="button"
      tabIndex={0}
      onClick={() => onOpen(vm.id)}
      onKeyDown={(e) => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault()
          onOpen(vm.id)
        }
      }}
    >
      <div className="nc-ico">
        <Icon size={16} />
      </div>
      <div className="nc-body">
        <div className="nc-row1">
          {!vm.read && <span className="nc-dot" />}
          <span className="nc-title">{vm.title}</span>
          <span className="nc-time">{relTime(vm.timeMs)}</span>
        </div>
        <div className="nc-desc">
          {vm.message ? <span>{vm.message}</span> : null}
          {vm.regarding ? (
            <>
              {vm.message ? ' ' : null}
              <span className="nc-res">
                {vm.regarding.kind}/{vm.regarding.name}
              </span>
            </>
          ) : null}
          {!vm.message && !vm.regarding ? vm.reason : null}
        </div>
        {hasTags ? (
          <div className="nc-tags">
            {showCat ? (
              <span className="nc-cat">{categoryLabel(vm.category)}</span>
            ) : null}
            {vm.count > 1 ? (
              <span className="nc-chip">×{vm.count}</span>
            ) : null}
          </div>
        ) : null}
      </div>
    </div>
  )
}
