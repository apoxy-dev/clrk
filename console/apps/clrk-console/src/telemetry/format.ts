// Compact number + duration formatting shared by the agents telemetry views,
// matching the CLRK Dashboard design's CLRK_fmtK / CLRK_fmtMs so the ported
// markup reads identically.

/** `2_840_000 → "2.8M"`, `4124 → "4.1k"`, `421 → "421"`. */
export function fmtK(n: number): string {
  if (!Number.isFinite(n)) return '0'
  const abs = Math.abs(n)
  if (abs >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M'
  if (abs >= 1_000) return (n / 1_000).toFixed(1) + 'k'
  return String(Math.round(n))
}

/** `0 → "—"`, `840 → "840ms"`, `9220 → "9.2s"`, `92_000 → "1.5m"`. */
export function fmtMs(ms: number): string {
  if (!ms) return '—'
  if (ms < 1000) return Math.round(ms) + 'ms'
  if (ms < 60_000) return (ms / 1000).toFixed(1) + 's'
  return (ms / 60_000).toFixed(1) + 'm'
}

/** A coarse "12s ago" / "4m ago" / "3d ago" from a millisecond delta. */
export function fmtAgo(deltaMs: number): string {
  if (!Number.isFinite(deltaMs) || deltaMs < 0) return '—'
  const s = Math.floor(deltaMs / 1000)
  if (s < 60) return `${s}s ago`
  const m = Math.floor(s / 60)
  if (m < 60) return `${m}m ago`
  const h = Math.floor(m / 60)
  if (h < 24) return `${h}h ago`
  return `${Math.floor(h / 24)}d ago`
}
