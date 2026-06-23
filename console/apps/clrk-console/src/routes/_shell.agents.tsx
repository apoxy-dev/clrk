// Agents list — a bespoke view that shadows the generic resource splat
// (`_shell.$.tsx`) for `/agents`. A static route outranks the splat, so this
// renders the combined TaskAgent + DaemonAgent table while the single "Agents"
// kind stays registered (for the rail item, breadcrumb, and ⌘K). It reads both
// agent kinds live and joins each kind's metrics rollup (the 24h activity
// columns), mapping them into the design's table (agents-data.ts).

import { useMemo } from 'react'
import { createFileRoute, useRouter } from '@tanstack/react-router'
import { useK8sList, type GVR } from '@apoxy/console-core'
import { AgentsListView } from '../views/agents-list'
import { mapAgents } from '../views/agents-data'
import { useAgentMetrics } from '../telemetry/hooks'

const gvr = (resource: string): GVR => ({
  group: 'clrk.apoxy.dev',
  version: 'v1alpha1',
  resource,
})
const TASK_GVR = gvr('taskagents')
const DAEMON_GVR = gvr('daemonagents')

export const Route = createFileRoute('/_shell/agents')({ component: AgentsPage })

function AgentsPage() {
  const router = useRouter()
  const task = useK8sList(TASK_GVR)
  const daemon = useK8sList(DAEMON_GVR)
  const taskMetrics = useAgentMetrics('TaskAgent')
  const daemonMetrics = useAgentMetrics('DaemonAgent')

  const { rows, totals } = useMemo(
    () =>
      mapAgents(
        task.data?.items ?? [],
        daemon.data?.items ?? [],
        taskMetrics.data ?? [],
        daemonMetrics.data ?? [],
      ),
    [task.data, daemon.data, taskMetrics.data, daemonMetrics.data],
  )

  return (
    <AgentsListView
      rows={rows}
      totals={totals}
      isLoading={task.isLoading || daemon.isLoading}
      metricsReady={taskMetrics.isSuccess && daemonMetrics.isSuccess}
      onOpen={(id) => void router.navigate({ to: `/agents/${id}` as never })}
    />
  )
}
