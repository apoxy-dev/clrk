// Data hooks for the Notification Center. Notifications are real
// events.k8s.io/v1 Events (watched cluster-wide); signup state lives in the
// CLRKConfig singleton (spec.notifications). The bell (in the shell) and the
// page share one WatchManager subscription per GVR, so reading in both places
// is free.

import { useCallback, useMemo } from 'react'
import {
  useK8sList,
  useK8sObject,
  useApplyResource,
  type GVR,
  type K8sObject,
} from '@apoxy/console-core'
import {
  mapEvents,
  unreadCount,
  countByCategory,
  type EventObj,
  type NotificationCategory,
  type NotificationVM,
} from './views/notifications-data'
import { useLastSeen } from './notifications-read-state'

export const EVENTS_GVR: GVR = {
  group: 'events.k8s.io',
  version: 'v1',
  resource: 'events',
}

export const CLRK_CONFIG_GVR: GVR = {
  group: 'clrk.apoxy.dev',
  version: 'v1alpha1',
  resource: 'clrkconfigs',
}

/** The install-wide singleton name + namespace (the clrk system namespace). */
export const SETTINGS_NAME = 'default'
export const SETTINGS_NAMESPACE = 'clrk'

interface Condition {
  type: string
  status: string
  reason?: string
  message?: string
  lastTransitionTime?: string
}

export interface CLRKConfigObj extends K8sObject {
  spec?: {
    notifications?: {
      email?: string
      advisoryPollEnabled?: boolean
    }
  }
  status?: {
    notifications?: {
      conditions?: Condition[]
      deploymentID?: string
      registeredAt?: string
    }
  }
}

export interface UseNotificationsResult {
  rows: NotificationVM[]
  unread: number
  counts: Record<NotificationCategory, number>
  markAllRead: () => void
  isLoading: boolean
}

/** List Events, map to view models against the read watermark, and expose the
 *  unread count + per-category counts. */
export function useNotifications(opts?: { enabled?: boolean }): UseNotificationsResult {
  const q = useK8sList<EventObj>(EVENTS_GVR, { enabled: opts?.enabled })
  const [lastSeen, setLastSeen] = useLastSeen()

  const rows = useMemo(
    () => mapEvents((q.data?.items ?? []) as EventObj[], lastSeen),
    [q.data, lastSeen],
  )
  const unread = useMemo(() => unreadCount(rows), [rows])
  const counts = useMemo(() => countByCategory(rows), [rows])

  const markAllRead = useCallback(() => {
    const maxT = rows.reduce((m, r) => Math.max(m, r.timeMs), 0)
    setLastSeen(Math.max(Date.now(), maxT))
  }, [rows, setLastSeen])

  return { rows, unread, counts, markAllRead, isLoading: q.isLoading }
}

export interface UseNotificationSettingsResult {
  settings?: CLRKConfigObj
  isLoading: boolean
  signedUp: boolean
  email?: string
  conditions: Condition[]
}

/** Read the CLRKConfig singleton's notifications section. */
export function useNotificationSettings(): UseNotificationSettingsResult {
  const q = useK8sObject<CLRKConfigObj>(CLRK_CONFIG_GVR, SETTINGS_NAME, {
    selectors: { namespace: SETTINGS_NAMESPACE },
  })
  const settings = q.data
  const email = settings?.spec?.notifications?.email
  return {
    settings,
    isLoading: q.isLoading,
    signedUp: Boolean(email),
    email,
    conditions: settings?.status?.notifications?.conditions ?? [],
  }
}

export interface UseNotificationSignupResult {
  submit: (email: string) => Promise<void>
  isPending: boolean
  error: Error | null
}

/** SSA-apply spec.notifications.email into the CLRKConfig singleton (create or
 *  update), merging into the notifications section only. Seeds the read
 *  watermark so the badge starts clean from signup rather than replaying
 *  history. */
export function useNotificationSignup(): UseNotificationSignupResult {
  const { apply, isPending, error } = useApplyResource<CLRKConfigObj>(CLRK_CONFIG_GVR)
  const [, setLastSeen] = useLastSeen()

  const submit = useCallback(
    async (email: string) => {
      await apply(
        SETTINGS_NAME,
        {
          apiVersion: 'clrk.apoxy.dev/v1alpha1',
          kind: 'CLRKConfig',
          metadata: { name: SETTINGS_NAME, namespace: SETTINGS_NAMESPACE },
          spec: { notifications: { email } },
        } as CLRKConfigObj,
        { namespace: SETTINGS_NAMESPACE },
      )
      setLastSeen(Date.now())
    },
    [apply, setLastSeen],
  )

  return { submit, isPending, error }
}
