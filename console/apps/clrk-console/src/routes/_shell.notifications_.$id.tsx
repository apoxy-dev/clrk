// Notification deep-link. A single notification has no standalone page -- it
// opens in the Notification Center's detail tray. This bespoke route shadows the
// generic splat for `/notifications/<id>` and redirects to the center with the
// notification pre-selected (`?sel=<id>`), so bell items and external links
// resolve without leaving a path leaf.

import { createFileRoute, Navigate } from '@tanstack/react-router'

export const Route = createFileRoute('/_shell/notifications_/$id')({
  component: NotificationRedirect,
})

function NotificationRedirect() {
  const { id } = Route.useParams()
  return (
    <Navigate
      to={'/notifications' as never}
      search={{ sel: id } as never}
      replace
    />
  )
}
