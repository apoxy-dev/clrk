// Read/unread state for notifications. The console is single-tenant and
// unauthenticated, so there is no per-user server state -- "unread" is a local
// last-seen watermark in localStorage (a notification is unread when its event
// time is newer than the watermark). Mirrors the theme.ts persistence pattern,
// including cross-tab sync via the 'storage' event.

import { useCallback, useEffect, useState } from 'react'

export const LAST_SEEN_KEY = 'clrk.console.notifications.lastSeen'

export function readLastSeen(): number {
  try {
    const raw = localStorage.getItem(LAST_SEEN_KEY)
    if (!raw) return 0
    const n = Number(raw)
    return Number.isFinite(n) ? n : 0
  } catch {
    return 0
  }
}

export function writeLastSeen(ms: number): void {
  try {
    localStorage.setItem(LAST_SEEN_KEY, String(Math.floor(ms)))
  } catch {
    // Ignore (private mode / disabled storage): unread just won't persist.
  }
}

/** Reactive last-seen watermark. Setting it persists and (via 'storage')
 *  syncs other tabs; other tabs' writes flow back here. */
export function useLastSeen(): [number, (ms: number) => void] {
  const [lastSeen, setLastSeen] = useState<number>(() => readLastSeen())

  useEffect(() => {
    const onStorage = (e: StorageEvent) => {
      if (e.key === LAST_SEEN_KEY) setLastSeen(readLastSeen())
    }
    window.addEventListener('storage', onStorage)
    return () => window.removeEventListener('storage', onStorage)
  }, [])

  const set = useCallback((ms: number) => {
    writeLastSeen(ms)
    setLastSeen(Math.floor(ms))
  }, [])

  return [lastSeen, set]
}
