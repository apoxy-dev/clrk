// Agent detail — a bespoke panel at `/agents/<name>`. The trailing `_` on
// `agents_` un-nests this from the `/agents` list so it renders directly in the
// shell. It finds the agent across both kinds (TaskAgent / DaemonAgent), joins
// the metrics rollup, the agent's sandbox revisions, its referenced egress
// gateways, and its OTLP traces, and hands them to the detail view.

import { useMemo } from 'react'
import { createFileRoute, useNavigate } from '@tanstack/react-router'
import { useK8sList, type GVR } from '@apoxy/console-core'
import { AgentDetailView } from '../views/agent-detail'
import { mapAgents } from '../views/agents-data'
import {
  mapAgentEgress,
  mapAgentEvents,
  mapRevisions,
} from '../views/agent-detail-data'
import { useAgentMetrics, useAgentTraces } from '../telemetry/hooks'
import type { AgentKind } from '../telemetry/client'
import { taskAgentEntry, daemonAgentEntry } from '../registry'

const gvr = (resource: string): GVR => ({
  group: 'clrk.apoxy.dev',
  version: 'v1alpha1',
  resource,
})
const TASK_GVR = gvr('taskagents')
const DAEMON_GVR = gvr('daemonagents')
const ASR_GVR = gvr('agentsandboxrevisions')
const EG_GVR = gvr('egressgateways')

export const Route = createFileRoute('/_shell/agents_/$name')({
  component: AgentDetailPage,
  // `?inv=<id>` deep-links a specific invocation in the Interaction tab (the
  // landing target for an Invocation row).
  validateSearch: (s: Record<string, unknown>): { inv?: string } => ({
    inv: typeof s.inv === 'string' && s.inv ? s.inv : undefined,
  }),
})

function AgentDetailPage() {
  const { name } = Route.useParams()
  const { inv } = Route.useSearch()
  const navigate = useNavigate()

  const task = useK8sList(TASK_GVR)
  const daemon = useK8sList(DAEMON_GVR)
  const taskObj = task.data?.items.find((o) => o.metadata.name === name)
  const daemonObj = daemon.data?.items.find((o) => o.metadata.name === name)
  const object = taskObj ?? daemonObj
  const kind: AgentKind = daemonObj && !taskObj ? 'DaemonAgent' : 'TaskAgent'
  const namespace = object?.metadata.namespace

  const metrics = useAgentMetrics(kind, Boolean(object))
  const revisionsList = useK8sList(ASR_GVR)
  const gateways = useK8sList(EG_GVR)
  const traces = useAgentTraces(kind, namespace, name, { limit: 200 }, Boolean(object))

  const { rows } = useMemo(
    () =>
      mapAgents(
        taskObj ? [taskObj] : [],
        daemonObj ? [daemonObj] : [],
        kind === 'TaskAgent' ? (metrics.data ?? []) : [],
        kind === 'DaemonAgent' ? (metrics.data ?? []) : [],
      ),
    [taskObj, daemonObj, kind, metrics.data],
  )
  const row = rows[0]

  const revisions = useMemo(
    () => mapRevisions(revisionsList.data?.items ?? [], name),
    [revisionsList.data, name],
  )
  const egress = useMemo(
    () => mapAgentEgress(row?.egressRefs ?? [], namespace ?? '', gateways.data?.items ?? []),
    [row?.egressRefs, namespace, gateways.data],
  )
  const events = useMemo(() => (object ? mapAgentEvents(object) : []), [object])

  if ((task.isLoading || daemon.isLoading) && !object) {
    return <div className="viz-empty-state">Loading agent…</div>
  }
  if (!object || !row) {
    return <div className="viz-empty-state">Agent “{name}” not found.</div>
  }

  return (
    <AgentDetailView
      row={row}
      object={object}
      entry={kind === 'TaskAgent' ? taskAgentEntry : daemonAgentEntry}
      revisions={revisions}
      egress={egress}
      events={events}
      traces={traces.data}
      tracesLoading={traces.isLoading}
      onOpenGateway={(gw) => void navigate({ to: `/egress/${gw}` as never })}
      initialInvocationId={inv}
    />
  )
}
