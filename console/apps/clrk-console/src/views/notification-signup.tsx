// The signup gate's email-capture view (presentational). Submitting writes the
// email into CLRKConfig.spec.notifications, which unlocks the Notification Center
// and (server-side) drives phone-home registration with api.apoxy.dev. Styled in
// the Apoxy paper/ink vocabulary; there is no mock for this screen, so it mirrors
// the notification list's category glyphs to preview what gets delivered.

import { useState, type FormEvent } from 'react'
import { Notification } from '@carbon/icons-react'
import {
  CATEGORY_ICON,
  toneVars,
} from './notification-visual'
import type {
  NotificationCategory,
  NotificationSeverity,
} from './notifications-data'

const EMAIL_RE = /^[^@\s]+@[^@\s]+\.[^@\s]+$/

const FEATURES: {
  category: NotificationCategory
  severity: NotificationSeverity
  title: string
  sub: string
}[] = [
  {
    category: 'security',
    severity: 'critical',
    title: 'Security',
    sub: 'Egress denials, orphaned sandboxes, Apoxy advisories',
  },
  {
    category: 'agent',
    severity: 'warning',
    title: 'Agent runs',
    sub: 'Failed, timed-out, or rejected invocations',
  },
  {
    category: 'rollout',
    severity: 'success',
    title: 'Rollouts',
    sub: 'Sandbox revision ready or stalled',
  },
  {
    category: 'fleet',
    severity: 'info',
    title: 'Fleet health',
    sub: 'Worker pool degraded or recovered',
  },
]

export interface NotificationSignupViewProps {
  onSubmit: (email: string) => void
  isPending: boolean
  error?: string | null
}

export function NotificationSignupView({
  onSubmit,
  isPending,
  error,
}: NotificationSignupViewProps) {
  const [email, setEmail] = useState('')
  const valid = EMAIL_RE.test(email)

  const submit = (e: FormEvent) => {
    e.preventDefault()
    if (valid && !isPending) onSubmit(email.trim())
  }

  return (
    <div className="nc-gate">
      <div className="nc-gate-card">
        <div className="nc-gate-ring">
          <Notification size={22} />
        </div>
        <h2 className="nc-gate-title">Turn on notifications</h2>
        <p className="nc-gate-lead">
          Get alerted on security events, failed agent runs, rollouts, and fleet
          health, right in the console. Enabling also receives security
          advisories from Apoxy for this deployment.
        </p>

        <ul className="nc-gate-feats">
          {FEATURES.map((f) => {
            const Icon = CATEGORY_ICON[f.category]
            return (
              <li key={f.category} style={toneVars(f.severity)}>
                <span className="nc-ico">
                  <Icon size={15} />
                </span>
                <div>
                  <div className="ft">{f.title}</div>
                  <div className="fs">{f.sub}</div>
                </div>
              </li>
            )
          })}
        </ul>

        <form className="nc-gate-form" onSubmit={submit}>
          <input
            type="email"
            className="nc-gate-input"
            placeholder="you@company.com"
            value={email}
            autoFocus
            onChange={(e) => setEmail(e.target.value)}
            aria-label="Email address"
          />
          <button
            type="submit"
            className="nc-act nc-act--primary nc-gate-submit"
            disabled={!valid || isPending}
          >
            {isPending ? 'Enabling…' : 'Enable notifications'}
          </button>
        </form>
        {error ? <p className="notif-error">{error}</p> : null}

        <p className="nc-gate-fine">
          Security notifications are reported to api.apoxy.dev for your
          deployment. Phone-home can be disabled per install.
        </p>
      </div>
    </div>
  )
}
