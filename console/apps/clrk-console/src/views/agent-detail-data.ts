// Mappers for the per-agent detail's secondary tabs: the AgentSandboxRevisions
// the agent has produced, the EgressGateways it references, and a coarse event
// feed reconstructed from the agent's status conditions. The headline
// Interaction/Activity tab is driven by traces (telemetry/*), not these.

import type { K8sObject } from '@apoxy/console-core'
import { ageShort } from './agents-data'

interface Conditioned {
  status?: { conditions?: Array<{ type: string; status: string; reason?: string; message?: string; lastTransitionTime?: string }> }
}

function condTrue(o: Conditioned, type: string): boolean {
  return (o.status?.conditions ?? []).some((c) => c.type === type && c.status === 'True')
}

// ── Revisions ────────────────────────────────────────────────────────────────

interface RevisionObject extends K8sObject, Conditioned {
  spec?: { image?: string }
  status?: Conditioned['status'] & { readyWorkers?: number }
}

export interface RevisionRow {
  name: string
  image: string
  ready: boolean
  active: boolean
  readyWorkers: number
  age: string
}

/** Revisions owned by the named agent, newest generation first. */
export function mapRevisions(
  list: K8sObject[],
  agentName: string,
  nowMs: number = Date.now(),
): RevisionRow[] {
  return list
    .filter(
      (o) =>
        (o.metadata.ownerReferences ?? []).some((r) => r.name === agentName) ||
        (o.metadata.name ?? '').startsWith(`${agentName}-`),
    )
    .map((o) => {
      const r = o as RevisionObject
      return {
        name: r.metadata.name ?? '—',
        image: r.spec?.image ?? '—',
        ready: condTrue(r, 'Ready'),
        active: condTrue(r, 'Active'),
        readyWorkers: r.status?.readyWorkers ?? 0,
        age: ageShort(r.metadata.creationTimestamp, nowMs),
      }
    })
    .sort((a, b) => b.name.localeCompare(a.name))
}

// ── Egress ───────────────────────────────────────────────────────────────────

interface GatewayObject extends K8sObject, Conditioned {
  spec?: { listeners?: unknown[] }
}

export interface AgentEgressRow {
  name: string
  namespace: string
  ready: boolean
  statusReason?: string
  listeners: number
  /** False when the referenced gateway can't be found in the cluster. */
  exists: boolean
}

/** Resolve an agent's egressRefs against the live EgressGateways in its namespace. */
export function mapAgentEgress(
  refs: string[],
  namespace: string,
  gateways: K8sObject[],
): AgentEgressRow[] {
  return refs.map((name) => {
    const g = gateways.find(
      (x) => x.metadata.name === name && (x.metadata.namespace ?? '') === namespace,
    ) as GatewayObject | undefined
    if (!g) {
      return { name, namespace, ready: false, statusReason: 'Not found', listeners: 0, exists: false }
    }
    const ready = condTrue(g, 'Programmed') || condTrue(g, 'Accepted')
    return {
      name,
      namespace,
      ready,
      statusReason: ready ? undefined : 'Degraded',
      listeners: g.spec?.listeners?.length ?? 0,
      exists: true,
    }
  })
}

// ── Events ───────────────────────────────────────────────────────────────────

export interface AgentEventRow {
  type: string
  message: string
  time: string
  tone: 'normal' | 'warn' | 'error'
}

/** A coarse event feed from the agent's status conditions (most recent first). */
export function mapAgentEvents(obj: K8sObject): AgentEventRow[] {
  const o = obj as Conditioned
  return (o.status?.conditions ?? [])
    .map((c) => {
      const label = `${c.reason || c.type}`.toLowerCase()
      const tone: AgentEventRow['tone'] = /fail|error|crashloop|denied|timeout/.test(label)
        ? 'error'
        : c.status === 'False'
          ? 'warn'
          : 'normal'
      return {
        type: c.reason || c.type,
        message: c.message || `${c.type} = ${c.status}`,
        time: c.lastTransitionTime ?? '',
        tone,
      }
    })
    .sort((a, b) => (b.time > a.time ? 1 : b.time < a.time ? -1 : 0))
}
