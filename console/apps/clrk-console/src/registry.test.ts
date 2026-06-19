import { describe, expect, it } from 'vitest'
import { registry, phaseVariant } from './registry'

describe('clrk registry', () => {
  it('registers the core clrk kinds under clrk.apoxy.dev/v1alpha1', () => {
    for (const [path, kind] of [
      ['taskagents', 'TaskAgent'],
      ['daemonagents', 'DaemonAgent'],
      ['egress', 'EgressGateway'],
      ['invocations', 'Invocation'],
    ] as const) {
      const entry = registry.byPath(path)
      expect(entry?.kind).toBe(kind)
      expect(entry?.gvr.group).toBe('clrk.apoxy.dev')
      expect(entry?.gvr.version).toBe('v1alpha1')
    }
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

  it('attaches generated tray schemas to the editable agent kinds', () => {
    expect(registry.byPath('taskagents')?.schema).toBeDefined()
    expect(registry.byPath('daemonagents')?.schema).toBeDefined()
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
