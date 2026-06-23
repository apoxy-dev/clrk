import { describe, expect, it } from 'vitest'
import {
  registry,
  phaseVariant,
  taskAgentEntry,
  daemonAgentEntry,
} from './registry'

describe('clrk registry', () => {
  it('registers the core clrk kinds under clrk.apoxy.dev/v1alpha1', () => {
    for (const [path, kind] of [
      ['agents', 'Agent'],
      ['worker-pools', 'WorkerPool'],
      ['egress', 'EgressGateway'],
      ['invocations', 'Invocation'],
    ] as const) {
      const entry = registry.byPath(path)
      expect(entry?.kind).toBe(kind)
      expect(entry?.gvr.group).toBe('clrk.apoxy.dev')
      expect(entry?.gvr.version).toBe('v1alpha1')
    }
  })

  it('collapses both agent kinds into one combined Agents rail item', () => {
    expect(registry.byPath('agents')?.kind).toBe('Agent')
    // The individual kinds are not registered as their own rail items.
    expect(registry.byPath('taskagents')).toBeUndefined()
    expect(registry.byPath('daemonagents')).toBeUndefined()
  })

  it('maps Worker Pools to the /worker-pools slug with status columns', () => {
    const wp = registry.byPath('worker-pools')
    expect(wp?.gvr.resource).toBe('workerpools')
    expect(wp?.yamlEditable).toBe(true)
    expect(wp?.schema).toBeDefined()
    expect(wp?.columns.map((c) => c.id)).toContain('replicas')
  })

  it('maps the EgressGateway kind to the /egress slug (bespoke list shadows the splat)', () => {
    const eg = registry.byPath('egress')
    expect(eg?.gvr.resource).toBe('egressgateways')
    expect(eg?.path).toBe('egress')
    expect(eg?.sidebarGroup).toBe('Egress')
  })

  it('groups the rail to the CLRK dashboard IA: Run → Egress → Observe', () => {
    expect(registry.groups().map((g) => g.name)).toEqual(['Run', 'Egress', 'Observe'])
  })

  it('makes invocations read-only (system-written: no YAML edit, no create wizard)', () => {
    const inv = registry.byPath('invocations')
    expect(inv?.yamlEditable).toBe(false)
    expect(inv?.createWizard).toBeUndefined()
  })

  it('exposes per-kind agent entries with tray schemas for the bespoke detail YAML', () => {
    expect(taskAgentEntry.gvr.resource).toBe('taskagents')
    expect(daemonAgentEntry.gvr.resource).toBe('daemonagents')
    expect(taskAgentEntry.schema).toBeDefined()
    expect(daemonAgentEntry.schema).toBeDefined()
  })

  it('gives EgressGateway a create/edit wizard and its tray schema', () => {
    const eg = registry.byPath('egress')
    expect(eg?.createWizard).toBeDefined()
    expect(eg?.schema).toBeDefined()
    expect(eg?.yamlEditable).toBe(true)
  })

  it('classifies clrk phases, never green on a terminal failure', () => {
    expect(phaseVariant('Running')).toBe('success')
    expect(phaseVariant('Succeeded')).toBe('success')
    expect(phaseVariant('Pending')).toBe('warning')
    expect(phaseVariant('Dispatched')).toBe('warning')
    expect(phaseVariant('CrashLoopBackOff')).toBe('danger')
    expect(phaseVariant('Timeout')).toBe('danger')
    expect(phaseVariant('Rejected')).toBe('danger')
    expect(phaseVariant('Stopped')).toBe('neutral')
  })
})
