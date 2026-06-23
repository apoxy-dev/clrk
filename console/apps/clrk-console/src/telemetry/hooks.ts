// React hooks over the telemetry data layer (telemetry/client.ts). These reads
// are polled, not watched, so they don't go through console-core's WatchManager.
// They also deliberately avoid `@tanstack/react-query`: the QueryClientProvider
// is owned by console-core's bundled copy, so the app's own react-query would
// see a null context. Plain useEffect fetching sidesteps that and works the same
// in dev and prod.

import { useEffect, useState } from 'react'
import { useConsoleClient } from '@apoxy/console-core'
import {
  fetchAgentTraces,
  listAgentMetrics,
  type AgentKind,
  type AgentMetrics,
  type TraceQuery,
} from './client'
import type { OtlpTracesData } from './otlp'

/** Minimal async result, mirroring the react-query fields the views read. */
export interface AsyncResult<T> {
  data?: T
  isLoading: boolean
  isSuccess: boolean
  error?: Error
}

const METRICS_POLL_MS = 30_000

/** The 24h per-agent rollup for a kind, refreshed on an interval. */
export function useAgentMetrics(kind: AgentKind, enabled = true): AsyncResult<AgentMetrics[]> {
  const client = useConsoleClient()
  const [state, setState] = useState<AsyncResult<AgentMetrics[]>>({
    isLoading: enabled,
    isSuccess: false,
  })

  useEffect(() => {
    if (!enabled) {
      setState({ isLoading: false, isSuccess: false })
      return
    }
    let cancelled = false
    const run = () => {
      listAgentMetrics(client, kind).then(
        (data) => {
          if (!cancelled) setState({ data, isLoading: false, isSuccess: true })
        },
        (error: Error) => {
          if (!cancelled) setState({ isLoading: false, isSuccess: false, error })
        },
      )
    }
    setState((s) => ({ ...s, isLoading: s.data == null }))
    run()
    const id = setInterval(run, METRICS_POLL_MS)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [client, kind, enabled])

  return state
}

/** One agent's OTLP traces (raw TracesData), filtered by the given query. */
export function useAgentTraces(
  kind: AgentKind,
  namespace: string | undefined,
  name: string | undefined,
  q: TraceQuery = {},
  enabled = true,
): AsyncResult<OtlpTracesData> {
  const client = useConsoleClient()
  const [state, setState] = useState<AsyncResult<OtlpTracesData>>({
    isLoading: false,
    isSuccess: false,
  })
  const qKey = JSON.stringify(q)

  useEffect(() => {
    if (!enabled || !namespace || !name) {
      setState({ isLoading: false, isSuccess: false })
      return
    }
    let cancelled = false
    setState((s) => ({ ...s, isLoading: true }))
    fetchAgentTraces(client, kind, namespace, name, JSON.parse(qKey) as TraceQuery).then(
      (data) => {
        if (!cancelled) setState({ data, isLoading: false, isSuccess: true })
      },
      (error: Error) => {
        if (!cancelled) setState({ isLoading: false, isSuccess: false, error })
      },
    )
    return () => {
      cancelled = true
    }
  }, [client, kind, namespace, name, qKey, enabled])

  return state
}
