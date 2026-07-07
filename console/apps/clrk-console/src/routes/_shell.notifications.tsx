// Notification Center -- a bespoke view that shadows the generic resource splat
// (`_shell.$.tsx`) for `/notifications`. A static route outranks the splat, so
// this renders the gate + Notification Center while the synthetic Notification
// kind stays registered (for the rail item, breadcrumb, and command palette).
//
// Notifications are real events.k8s.io/v1 Events; the whole feature is gated
// behind an email signup stored in the CLRKConfig singleton. `?sel=<id>` drives
// the detail tray so bell items and deep links open the right notification.

import { createFileRoute, useNavigate } from '@tanstack/react-router'
import {
  useNotifications,
  useNotificationSettings,
} from '../notifications-hooks'
import { NotificationsGate } from '../views/notifications-gate'
import { NotificationCenterView } from '../views/notification-center'

export const Route = createFileRoute('/_shell/notifications')({
  component: NotificationsPage,
  validateSearch: (s: Record<string, unknown>) => ({
    sel: typeof s.sel === 'string' && s.sel ? s.sel : undefined,
  }),
})

// Route the detail tray's "Go to <kind>" to the console page that owns that
// kind. Unknown kinds have no page -- return null and the button no-ops.
function objectRoute(regarding: {
  kind: string
  name: string
}): string | null {
  switch (regarding.kind) {
    case 'TaskAgent':
    case 'DaemonAgent':
      return `/agents/${regarding.name}`
    case 'WorkerPool':
      return `/worker-pools/${regarding.name}`
    case 'EgressGateway':
      return `/egress/${regarding.name}`
    default:
      return null
  }
}

// Derive the header's security-reporting indicator from the CLRKConfig
// notifications Registered condition (written by the phone-home controller). The
// browser sees only the status condition, never the token.
function securityIndicatorOf(
  conditions: { type: string; status: string }[],
): { ok: boolean; label: string } | undefined {
  const registered = conditions.find((c) => c.type === 'Registered')
  if (!registered) return undefined
  return registered.status === 'True'
    ? { ok: true, label: 'Security reporting active' }
    : { ok: false, label: 'Security reporting inactive' }
}

function NotificationsPage() {
  const navigate = useNavigate()
  const { sel } = Route.useSearch()
  const { signedUp, conditions } = useNotificationSettings()
  const { rows, unread, markAllRead, isLoading } = useNotifications({
    enabled: signedUp,
  })

  const select = (id?: string) =>
    void navigate({
      to: '/notifications' as never,
      search: (id ? { sel: id } : {}) as never,
    })

  return (
    <NotificationsGate>
      <NotificationCenterView
        rows={rows}
        unread={unread}
        isLoading={isLoading}
        selectedId={sel}
        securityIndicator={securityIndicatorOf(conditions)}
        onSelect={select}
        onMarkAllRead={markAllRead}
        onOpenObject={(regarding) => {
          const to = objectRoute(regarding)
          if (to) void navigate({ to: to as never })
        }}
      />
    </NotificationsGate>
  )
}
