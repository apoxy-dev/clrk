// Read/unread state for notifications. The console is single-tenant and
// unauthenticated, so there is no per-user server state -- "unread" is a local
// last-seen watermark in localStorage (a notification is unread when its event
// time is newer than the watermark). Mirrors the theme.ts persistence pattern,
// including cross-tab sync via the 'storage' event.

import { useCallback, useSyncExternalStore } from 'react'

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

// Authoritative in-memory watermark, seeded from storage at module load. The
// value lives in memory (not re-read from localStorage) so that a write which
// fails to persist -- Safari private browsing, disabled storage, quota -- still
// advances the value for this session. useSyncExternalStore reads this snapshot,
// so getSnapshot is cheap (no per-render localStorage parse) and returns a
// stable identity that only changes when the watermark actually moves.
let current = readLastSeen()

// Same-tab subscribers. The DOM 'storage' event only fires in OTHER documents,
// so a write here would not reach sibling hook instances in this tab (e.g. the
// page and the topbar bell both read the watermark). Keep an in-process listener
// set and notify it on every write so all consumers re-render together --
// otherwise "Mark all read" clears one and leaves the other stale.
const listeners = new Set<() => void>()

export function writeLastSeen(ms: number): void {
  // Advance the in-memory value first, so the update lands even if persistence
  // throws (private mode / disabled storage / quota).
  current = Math.floor(ms)
  try {
    localStorage.setItem(LAST_SEEN_KEY, String(current))
  } catch {
    // Ignore: unread just won't persist across reloads this session.
  }
  listeners.forEach((l) => l())
}

function getSnapshot(): number {
  return current
}

/** Subscribe to watermark changes from both this tab (writeLastSeen) and other
 *  tabs (the 'storage' event). */
function subscribe(onChange: () => void): () => void {
  listeners.add(onChange)
  const onStorage = (e: StorageEvent) => {
    if (e.key === LAST_SEEN_KEY) {
      current = readLastSeen()
      onChange()
    }
  }
  window.addEventListener('storage', onStorage)
  return () => {
    listeners.delete(onChange)
    window.removeEventListener('storage', onStorage)
  }
}

/** Reactive last-seen watermark shared across every consumer in the tab.
 *  Setting it advances the in-memory value, persists (best-effort), syncs other
 *  tabs (via 'storage'), and immediately updates all same-tab consumers. */
export function useLastSeen(): [number, (ms: number) => void] {
  const lastSeen = useSyncExternalStore(subscribe, getSnapshot, () => 0)
  const set = useCallback((ms: number) => writeLastSeen(ms), [])
  return [lastSeen, set]
}
