// The clrk console's resource registry: the kinds this app surfaces, composed
// from console-core's generic machinery. Adding a kind is an entry here — the
// sidebar, routes, breadcrumbs, list, and detail all derive from it. The Run
// group carries a single combined "Agents" rail item (the bespoke /agents views
// span both TaskAgents and DaemonAgents and join the metrics rollup) plus a
// generic Worker Pools list; Egress and Invocation round out the set.

import {
  Badge,
  defineResource,
  createRegistry,
  type BadgeVariant,
  type GVR,
  type K8sObject,
  type ResourceEntry,
} from '@apoxy/console-core'
import { Activity, Bot, Gateway, Layers } from '@carbon/icons-react'
import type { ReactNode } from 'react'
import { schemaFor } from './schema/schema-for'
import { EgressGatewayWizard } from './views/egress-gateway-wizard'

/** Objects that carry a coarse status phase we can badge (plus the free-form
 *  status/spec fields the columns read). */
interface Phased extends K8sObject {
  status?: Record<string, unknown> & { phase?: string }
  spec?: Record<string, unknown>
}

/**
 * Classify a clrk status phase into a badge variant. clrk phases span the
 * DaemonAgent enum (Running | Stopped | CrashLoopBackOff) and the Invocation
 * enum (Pending | Dispatched | Running | Succeeded | Failed | Timeout |
 * Rejected). Order is danger → warning → success → neutral, so a terminal
 * failure never renders green. `Stopped` is intentional, so it stays neutral.
 */
export function phaseVariant(phase?: string): BadgeVariant {
  const p = (phase ?? '').toLowerCase()
  if (
    /\b(failed|error|errored|crashloopbackoff|timeout|rejected|terminated|denied|evicted)\b/.test(
      p,
    )
  ) {
    return 'danger'
  }
  if (
    /\b(pending|dispatched|progressing|provisioning|updating|unknown|creating)\b/.test(
      p,
    )
  ) {
    return 'warning'
  }
  if (/\b(running|ready|healthy|active|available|succeeded|bound)\b/.test(p)) {
    return 'success'
  }
  return 'neutral'
}

function phaseBadge(phase?: string): ReactNode {
  return <Badge variant={phaseVariant(phase)}>{phase ?? 'Unknown'}</Badge>
}

/** ISO date prefix (no Date parsing) for a `creationTimestamp`. */
function created(obj: K8sObject): string {
  return obj.metadata.creationTimestamp?.slice(0, 10) ?? '—'
}

// One combined "Agents" rail item covers both agent kinds; Worker Pools sit
// beside it under Run.
const agentsIcon = <Bot size={16} />
const poolIcon = <Layers size={16} />
const invocationIcon = <Activity size={16} />
// Egress Gateways use Carbon's Gateway glyph — the same icon the CLRK dashboard
// design picked for the egress rail item.
const egressIcon = <Gateway size={16} />

const nameCol = {
  id: 'name',
  header: 'Name',
  width: '32%',
  cell: (o: K8sObject) => o.metadata.name,
}
const statusCol = {
  id: 'status',
  header: 'Status',
  cell: (o: Phased) => phaseBadge(o.status?.phase),
}
const createdCol = {
  id: 'created',
  header: 'Created',
  mono: true,
  cell: created,
}

// Active in-flight executions — TaskAgent status.activeExecutions, and the same
// field on a WorkerPool's status (its pool-wide total).
const activeCol = {
  id: 'active',
  header: 'Active',
  mono: true,
  cell: (o: Phased) => String(o.status?.activeExecutions ?? 0),
}
// Worker Pool columns: ready/desired replicas, per-worker cap, and warm pool.
const replicasCol = {
  id: 'replicas',
  header: 'Ready',
  mono: true,
  cell: (o: Phased) => {
    const ready = (o.status?.readyReplicas as number | undefined) ?? 0
    const desired = (o.spec?.replicas as number | undefined) ?? 1
    return `${ready}/${desired}`
  },
}
const maxCol = {
  id: 'max',
  header: 'Max / worker',
  mono: true,
  cell: (o: Phased) =>
    String((o.spec?.maxExecutionsPerWorker as number | undefined) ?? '—'),
}
const warmCol = {
  id: 'warm',
  header: 'Warm',
  mono: true,
  cell: (o: Phased) => String((o.spec?.warmPool as number | undefined) ?? 0),
}
const triggerCol = {
  id: 'trigger',
  header: 'Trigger',
  mono: true,
  cell: (o: Phased) =>
    (o.spec?.trigger as { type?: string } | undefined)?.type ?? '—',
}
const parentCol = {
  id: 'parent',
  header: 'Parent',
  cell: (o: Phased) => {
    const p = o.spec?.parentRef as { kind?: string; name?: string } | undefined
    return p?.kind ? `${p.kind}${p.name ? `/${p.name}` : ''}` : '—'
  },
}

export const registry = createRegistry([
  // Agents: a single combined rail item + ⌘K target spanning TaskAgents and
  // DaemonAgents. The list (`/agents`) and per-agent detail are bespoke views
  // (`_shell.agents*`) that read both kinds and join the metrics rollup; the
  // generic columns/gvr here only feed the rail item, breadcrumb, and discovery.
  // Per-kind entries for the detail's YAML tray are exported below.
  defineResource<Phased>({
    kind: 'Agent',
    displayName: 'Agents',
    group: 'clrk.apoxy.dev',
    resource: 'taskagents',
    servedVersion: 'v1alpha1',
    sidebarGroup: 'Run',
    path: 'agents',
    icon: agentsIcon,
    shortcut: 'a',
    columns: [nameCol],
  }),
  // Worker Pools: a basic generic list — the splat route renders these columns
  // straight from the WorkerPool CRs. Raw YAML edit is enabled (the curated
  // pod-template overlay), but there is no bespoke wizard yet.
  defineResource<Phased>({
    kind: 'WorkerPool',
    displayName: 'Worker Pools',
    group: 'clrk.apoxy.dev',
    resource: 'workerpools',
    servedVersion: 'v1alpha1',
    sidebarGroup: 'Run',
    path: 'worker-pools',
    icon: poolIcon,
    shortcut: 'w',
    yamlEditable: true,
    schema: schemaFor('clrk.apoxy.dev', 'v1alpha1', 'WorkerPool'),
    columns: [nameCol, replicasCol, activeCol, maxCol, warmCol, createdCol],
  }),
  // Egress Gateways surface as a bespoke list (`/_shell/egress` shadows the
  // generic splat for `/egress`), but the kind is still registered here so the
  // rail item, breadcrumb, and ⌘K "Go to" all derive from the registry. The
  // generic `columns` below are unused by the shadowing list view. The bespoke
  // wizard powers both create (list "New gateway" + ⌘K) and edit (detail "Edit"
  // + the YAML tray's Edit), and the schema drives its tray validation.
  defineResource<Phased>({
    kind: 'EgressGateway',
    displayName: 'Egress Gateways',
    group: 'clrk.apoxy.dev',
    resource: 'egressgateways',
    servedVersion: 'v1alpha1',
    sidebarGroup: 'Egress',
    path: 'egress',
    icon: egressIcon,
    shortcut: 'e',
    yamlEditable: true,
    schema: schemaFor('clrk.apoxy.dev', 'v1alpha1', 'EgressGateway'),
    createWizard: EgressGatewayWizard,
    columns: [nameCol, statusCol, createdCol],
  }),
  defineResource<Phased>({
    kind: 'Invocation',
    displayName: 'Invocations',
    group: 'clrk.apoxy.dev',
    resource: 'invocations',
    servedVersion: 'v1alpha1',
    sidebarGroup: 'Observe',
    icon: invocationIcon,
    shortcut: 'i',
    // Invocations are system-written and immutable from clients (create/update
    // return 405), so the kind is read-only: no YAML edit, no "New".
    columns: [nameCol, statusCol, triggerCol, parentCol, createdCol],
  }),
])

// Per-kind entries for the bespoke agent detail's YAML view/edit. They are NOT
// registered as rail items (the combined "Agents" entry owns the sidebar) — they
// only carry the real GVR + schema so the detail's YamlMenu/YamlTray write and
// validate against the right kind.
function agentKindEntry(kind: string, resource: string): ResourceEntry {
  const gvr: GVR = { group: 'clrk.apoxy.dev', version: 'v1alpha1', resource }
  return {
    gvr,
    kind,
    displayName: kind,
    path: resource,
    sidebarGroup: 'Run',
    servedVersion: 'v1alpha1',
    columns: [],
    yamlEditable: true,
    requires: [gvr],
    schema: schemaFor('clrk.apoxy.dev', 'v1alpha1', kind),
  }
}

export const taskAgentEntry = agentKindEntry('TaskAgent', 'taskagents')
export const daemonAgentEntry = agentKindEntry('DaemonAgent', 'daemonagents')
