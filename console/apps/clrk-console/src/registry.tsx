// The clrk console's resource registry: the kinds this app surfaces, composed
// from console-core's generic machinery. Adding a kind is an entry here — the
// sidebar, routes, breadcrumbs, list, and detail all derive from it. This is the
// starter set for `@apoxy/console-clrk` (APO-785): TaskAgent, DaemonAgent, and
// Invocation. The richer detail/inspection views (live invocation feeds, sandbox
// traffic) land on top of these entries.

import { Badge, defineResource, createRegistry, type BadgeVariant, type K8sObject } from '@apoxy/console-core'
import { Activity, Application, Bot } from '@carbon/icons-react'
import type { ReactNode } from 'react'
import { schemaFor } from './schema/schema-for'

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
  if (/\b(failed|error|errored|crashloopbackoff|timeout|rejected|terminated|denied|evicted)\b/.test(p)) {
    return 'danger'
  }
  if (/\b(pending|dispatched|progressing|provisioning|updating|unknown|creating)\b/.test(p)) {
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

const agentIcon = <Bot size={16} />
const daemonIcon = <Application size={16} />
const invocationIcon = <Activity size={16} />
// The egress glyph is the CLRK design's own (an arrow exiting into a bar); inline
// rather than a Carbon stand-in so the rail matches the dashboard exactly.
const egressIcon = (
  <svg viewBox="0 0 16 16" width={16} height={16} fill="none" stroke="currentColor" strokeWidth={1.5} aria-hidden="true">
    <path d="M2 8h8" />
    <path d="M7 5l3 3-3 3" />
    <rect x="11" y="3" width="3" height="10" />
  </svg>
)

const nameCol = { id: 'name', header: 'Name', width: '32%', cell: (o: K8sObject) => o.metadata.name }
const statusCol = { id: 'status', header: 'Status', cell: (o: Phased) => phaseBadge(o.status?.phase) }
const createdCol = { id: 'created', header: 'Created', mono: true, cell: created }

// TaskAgent has no coarse status.phase — its readiness lives in conditions and
// scalar status fields — so it shows structural columns instead of a badge.
const activeCol = { id: 'active', header: 'Active', mono: true, cell: (o: Phased) => String(o.status?.activeExecutions ?? 0) }
const readyCol = {
  id: 'ready',
  header: 'Latest ready',
  mono: true,
  cell: (o: Phased) => String(o.status?.latestReadyRevisionName ?? '—'),
}
const restartsCol = { id: 'restarts', header: 'Restarts', mono: true, cell: (o: Phased) => String(o.status?.restartCount ?? 0) }
const triggerCol = {
  id: 'trigger',
  header: 'Trigger',
  mono: true,
  cell: (o: Phased) => (o.spec?.trigger as { type?: string } | undefined)?.type ?? '—',
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
  defineResource<Phased>({
    kind: 'TaskAgent',
    displayName: 'Task agents',
    group: 'clrk.apoxy.dev',
    resource: 'taskagents',
    servedVersion: 'v1alpha1',
    sidebarGroup: 'Run',
    icon: agentIcon,
    shortcut: 't',
    yamlEditable: true,
    schema: schemaFor('clrk.apoxy.dev', 'v1alpha1', 'TaskAgent'),
    columns: [nameCol, activeCol, readyCol, createdCol],
  }),
  defineResource<Phased>({
    kind: 'DaemonAgent',
    displayName: 'Daemon agents',
    group: 'clrk.apoxy.dev',
    resource: 'daemonagents',
    servedVersion: 'v1alpha1',
    sidebarGroup: 'Run',
    icon: daemonIcon,
    shortcut: 'd',
    yamlEditable: true,
    schema: schemaFor('clrk.apoxy.dev', 'v1alpha1', 'DaemonAgent'),
    columns: [nameCol, statusCol, restartsCol, createdCol],
  }),
  // Egress Gateways surface as a bespoke list (`/_shell/egress` shadows the
  // generic splat for `/egress`), but the kind is still registered here so the
  // rail item, breadcrumb, and ⌘K "Go to" all derive from the registry. The
  // generic `columns` below are unused by the shadowing list view.
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
