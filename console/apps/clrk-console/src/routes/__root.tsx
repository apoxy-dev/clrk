import { createRootRoute, Outlet, useLocation } from '@tanstack/react-router'
import { useAnalyticsPageviews } from '@apoxy/console-core'

function RootComponent() {
  // Anonymous PostHog pageview capture: fires on the initial load and every
  // in-app navigation. Location comes from the router so SPA route changes
  // (which never reload the page) are counted too.
  const location = useLocation()
  useAnalyticsPageviews(location.href)

  return (
    <div className="apx-body h-full">
      <Outlet />
    </div>
  )
}

export const Route = createRootRoute({
  component: RootComponent,
})
