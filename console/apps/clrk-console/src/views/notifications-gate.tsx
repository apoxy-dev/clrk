// Hard signup gate. Until CLRKConfig.spec.notifications.email is set, the whole
// Notification Center is replaced by the email-capture form; once set, children
// render. Signup submits through the console's k8s API (SSA); the browser never
// sees the phone-home token (core/v1 Secrets aren't proxied by the embedded
// apiserver).

import type { ReactNode } from 'react'
import {
  useNotificationSettings,
  useNotificationSignup,
} from '../notifications-hooks'
import { NotificationSignupView } from './notification-signup'

export function NotificationsGate({ children }: { children: ReactNode }) {
  const { signedUp, isLoading } = useNotificationSettings()
  const { submit, isPending, error } = useNotificationSignup()

  if (isLoading) {
    return <div className="viz-empty-state">Loading…</div>
  }
  if (!signedUp) {
    return (
      <NotificationSignupView
        onSubmit={(email) => void submit(email)}
        isPending={isPending}
        error={error?.message ?? null}
      />
    )
  }
  return <>{children}</>
}
