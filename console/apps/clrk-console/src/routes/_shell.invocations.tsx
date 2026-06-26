// Invocations list -- a bespoke view that shadows the generic resource splat
// (`_shell.$.tsx`) for `/invocations`. A static route outranks the splat, so
// this renders the paginated live-tail table while the Invocation kind stays
// registered (for the rail item, breadcrumb, and command palette).
//
// Pagination is CLIENT-SIDE over a bounded window, matching the CLRK Dashboard
// design (`view-misc.jsx` InvocationsPage): the design pages a fully-loaded feed
// with a rows-per-page control and jump-to-page nav, which needs a total and
// random access -- things k8s continue-token paging can't give. So we fetch a
// window of the newest invocations (the apiserver's max LIST limit; it returns
// them newest-first) and slice it in the page. The header frames the count as
// "N in window" because that is exactly what it is. Page 1 is the live tail --
// it refetches the window on an interval unless paused; while browsing a deeper
// page the window is frozen so the table doesn't shift underfoot. Each page's
// rows are enriched with per-agent OTel telemetry -- duration, tokens, span
// count -- joined by invocation id (invocations-data.ts). Rows open the owning
// agent's trace panel with the invocation pre-selected (`/agents/<agent>?inv=<id>`),
// the same target the `/invocations/<id>` redirect resolves to.

import { useEffect, useMemo, useState } from 'react'
import { createFileRoute, useNavigate } from '@tanstack/react-router'
import { useConsoleClient, type GVR } from '@apoxy/console-core'
import { InvocationsListView } from '../views/invocations-list'
import {
  distinctAgentRefs,
  mapInvocations,
  sortByNewest,
  type InvocationObj,
} from '../views/invocations-data'
import { useInvocationsTelemetry } from '../telemetry/hooks'

const INVOCATION_GVR: GVR = {
  group: 'clrk.apoxy.dev',
  version: 'v1alpha1',
  resource: 'invocations',
}

/** Newest invocations to load for client-side paging; the apiserver caps an
 *  explicit LIST limit here. */
const WINDOW = 1000
/** Live-tail refresh cadence for the window (only while viewing page 1). */
const LIVE_TAIL_MS = 5_000

export const Route = createFileRoute('/_shell/invocations')({
  component: InvocationsPage,
})

/**
 * Fetch the newest-invocations window off the GVR client (console-core's
 * `useK8sList` can't carry a `limit`). `live` keeps it refreshing on an
 * interval -- the caller passes `false` while paused or browsing a deeper page
 * so the snapshot stays stable.
 */
function useInvocationsWindow(live: boolean): {
  items: InvocationObj[]
  isLoading: boolean
} {
  const client = useConsoleClient()
  const [state, setState] = useState<{ items: InvocationObj[]; isLoading: boolean }>(
    { items: [], isLoading: true },
  )

  useEffect(() => {
    let cancelled = false
    const run = () => {
      client.gvr
        .list(INVOCATION_GVR, { limit: WINDOW })
        .then((list) => {
          if (!cancelled)
            setState({ items: (list.items ?? []) as InvocationObj[], isLoading: false })
        })
        .catch(() => {
          if (!cancelled) setState((s) => ({ ...s, isLoading: false }))
        })
    }
    run()
    if (live) {
      const id = setInterval(run, LIVE_TAIL_MS)
      return () => {
        cancelled = true
        clearInterval(id)
      }
    }
    return () => {
      cancelled = true
    }
  }, [client, live])

  return state
}

function InvocationsPage() {
  const navigate = useNavigate()
  const [paused, setPaused] = useState(false)
  const [pageSize, setPageSize] = useState(50)
  const [page, setPage] = useState(1)

  // Page 1 is the live tail; a deeper page (or a pause) freezes the window so
  // the slice the user is reading can't shift underneath them.
  const live = !paused && page === 1
  const win = useInvocationsWindow(live)

  // The server returns the window newest-first already; sort defensively so the
  // slice never depends on wire order.
  const feed = useMemo(() => sortByNewest(win.items), [win.items])
  const total = feed.length
  const pageCount = Math.max(1, Math.ceil(total / pageSize))
  const cur = Math.min(page, pageCount)
  const start = (cur - 1) * pageSize
  const pageItems = useMemo(
    () => feed.slice(start, start + pageSize),
    [feed, start, pageSize],
  )

  // Enrich only the current page's agents; the poll pauses with the live tail.
  const refs = useMemo(() => distinctAgentRefs(pageItems), [pageItems])
  const tel = useInvocationsTelemetry(refs, !paused)

  // Date.now() is read inside the memo (not a dep) so ages re-stamp on a data
  // change without a render tick of their own -- coarse "Xs ago" is fine.
  const rows = useMemo(
    () => mapInvocations(pageItems, tel.data, Date.now()),
    [pageItems, tel.data],
  )

  return (
    <InvocationsListView
      rows={rows}
      isLoading={win.isLoading}
      telemetryReady={tel.isSuccess}
      total={total}
      page={cur}
      pageSize={pageSize}
      pageCount={pageCount}
      start={start}
      count={pageItems.length}
      onPage={setPage}
      onPageSize={(s) => {
        setPageSize(s)
        setPage(1)
      }}
      paused={paused}
      onTogglePause={() => setPaused((p) => !p)}
      onOpen={(row) =>
        void navigate({
          to: `/agents/${row.agent}` as never,
          search: { inv: row.id } as never,
        })
      }
    />
  )
}
