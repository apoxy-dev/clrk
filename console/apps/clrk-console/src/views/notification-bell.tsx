// Topbar bell: unread badge + dropdown popover. Clicking always opens a popover,
// in both states. Signed up -> the recent-notifications triage tray. Not signed
// up -> the gate popover (redacted teaser + "Set up notifications" CTA that routes
// to the signup page); no badge in that state. Self-contained (reads the shared
// hooks + its own navigate), so the shell swap is just <NotificationBell/>.

import { useState } from 'react'
import { useNavigate } from '@tanstack/react-router'
import { IconButton } from '@apoxy/console-core'
import { Notification } from '@carbon/icons-react'
import {
  useNotifications,
  useNotificationSettings,
} from '../notifications-hooks'
import { NotificationTray } from './notification-tray'
import { NotificationGateTray } from './notification-gate-tray'

export function NotificationBell() {
  const navigate = useNavigate()
  const { signedUp } = useNotificationSettings()
  // Only watch Events once signed up -- no point subscribing behind the gate.
  const { rows, unread, markAllRead } = useNotifications({ enabled: signedUp })
  const [open, setOpen] = useState(false)

  const goto = (sel?: string) =>
    void navigate({
      to: '/notifications' as never,
      search: (sel ? { sel } : {}) as never,
    })

  return (
    <div className="notif-bell-wrap">
      <IconButton
        label="Notifications"
        badge={signedUp && unread > 0}
        pressed={open}
        onClick={() => setOpen((o) => !o)}
      >
        <Notification size={16} />
      </IconButton>
      {open ? (
        signedUp ? (
          <NotificationTray
            rows={rows}
            unread={unread}
            onClose={() => setOpen(false)}
            onMarkAllRead={markAllRead}
            onViewAll={() => {
              setOpen(false)
              goto()
            }}
            onOpen={(id) => {
              setOpen(false)
              goto(id)
            }}
          />
        ) : (
          <NotificationGateTray
            onClose={() => setOpen(false)}
            onSetup={() => {
              setOpen(false)
              goto()
            }}
          />
        )
      ) : null}
    </div>
  )
}
