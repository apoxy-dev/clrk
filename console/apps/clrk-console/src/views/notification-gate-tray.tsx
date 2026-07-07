// The signed-out (not-yet-set-up) bell popover -- the design mock's
// SignedOutPanel. A redacted triage teaser (blurred skeleton rows) sits behind a
// veil carrying a gate card that explains what notifications are and a single
// CTA to turn them on. clrk has no auth/SSO: the "gate" is the email signup, so
// the CTA routes to the /notifications signup page rather than a sign-in flow.
//
// Mirrors NotificationTray's chrome (scrim + Escape-to-close + .nc-pop) so the
// two bell states behave identically -- clicking the bell always opens a popover.

import { useEffect } from 'react'
import { Locked } from '@carbon/icons-react'

export interface NotificationGateTrayProps {
  onClose: () => void
  onSetup: () => void
}

// One redacted skeleton row of the teaser (icon + two bars at the given widths).
function SkelRow({ w1, w2 }: { w1: string; w2: string }) {
  return (
    <div className="nc-skel-row">
      <div className="nc-skel-ico" />
      <div className="nc-skel-lines">
        <div className="nc-skel-bar" style={{ width: w1 }} />
        <div className="nc-skel-bar nc-skel-bar--dim" style={{ width: w2 }} />
      </div>
    </div>
  )
}

export function NotificationGateTray({ onClose, onSetup }: NotificationGateTrayProps) {
  // Escape closes the popover (the scrim handles pointer dismissal), matching
  // NotificationTray / the attrs-tray precedent.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  }, [onClose])

  return (
    <>
      <div className="nc-scrim" onClick={onClose} />
      <div className="nc-pop" role="dialog" aria-label="Notifications">
        <div className="nc-caret" />

        <div className="nc-head">
          <div className="nc-head-l">
            <span className="nc-h-title">Notifications</span>
          </div>
        </div>

        <div className="nc-gate">
          {/* The triage structure showing through, blurred + redacted. */}
          <div className="nc-gate-teaser" aria-hidden="true">
            <div className="nc-tabs">
              <span className="nc-tab is-active">All</span>
              <span className="nc-tab">Unread</span>
              <span className="nc-tab">Needs action</span>
            </div>
            <div className="nc-group">Today</div>
            <SkelRow w1="58%" w2="90%" />
            <SkelRow w1="66%" w2="78%" />
            <SkelRow w1="52%" w2="84%" />
            <div className="nc-group">Earlier</div>
            <SkelRow w1="60%" w2="80%" />
          </div>

          <div className="nc-gate-veil">
            <div className="nc-gate-card">
              <div className="nc-gate-ico">
                <Locked size={22} />
              </div>
              <div className="nc-gate-title">Turn on notifications</div>
              <div className="nc-gate-sub">
                Security alerts, agent run failures, rollout status and fleet
                health for this deployment land here.
              </div>
              <div className="nc-gate-actions">
                <button
                  className="nc-act nc-act--primary nc-gate-btn"
                  onClick={onSetup}
                >
                  Set up notifications
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </>
  )
}
