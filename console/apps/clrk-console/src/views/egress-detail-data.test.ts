import { describe, expect, it } from 'vitest'
import type { K8sObject } from '@apoxy/console-core'
import { mapEgressDetail } from './egress-detail-data'

// An EgressGateway and an ingress Gateway can share a name (they're different
// kinds in different API groups). The detail mapper must attach a route only when
// a parentRef points at THIS EgressGateway — matching the name alone leaks the
// agent's inbound HTTPRoute (bound to the ingress Gateway) into the egress view.

const obj = (o: unknown) => o as unknown as K8sObject

const gateway = obj({
  apiVersion: 'clrk.apoxy.dev/v1alpha1',
  kind: 'EgressGateway',
  metadata: { name: 'jq-bot', namespace: 'default', creationTimestamp: '2026-06-20T00:00:00Z' },
  spec: { defaultPolicy: 'allow-all', listeners: [{ name: 'egress', protocol: 'TLS', tls: { mode: 'Terminate' } }] },
  status: { conditions: [{ type: 'Ready', status: 'True' }], listeners: [{ name: 'egress' }] },
})

// Legit egress route: parentRef names this gateway AND is an EgressGateway.
const aiRoute = obj({
  kind: 'AIProviderRoute',
  metadata: { name: 'anthropic', namespace: 'default' },
  spec: {
    parentRefs: [{ group: 'clrk.apoxy.dev', kind: 'EgressGateway', name: 'jq-bot' }],
    rules: [{ matches: [{ provider: 'anthropic' }] }],
  },
})

// The agent's inbound route: same name, but bound to the ingress Gateway. Must NOT appear.
const ingressHttpRoute = obj({
  kind: 'HTTPRoute',
  metadata: { name: 'jq-bot', namespace: 'default' },
  spec: {
    parentRefs: [{ group: 'gateway.networking.k8s.io', kind: 'Gateway', name: 'jq-bot' }],
    rules: [{ matches: [{ path: { value: '/' } }] }],
  },
})

// A bare-name parentRef (the shape the mock backend and older objects use) still attaches.
const bareNameRoute = obj({
  kind: 'MCPRoute',
  metadata: { name: 'tools', namespace: 'default' },
  spec: { parentRefs: [{ name: 'jq-bot' }], rules: [{ matches: [{ tools: ['read_file'] }] }] },
})

describe('mapEgressDetail / route attachment', () => {
  it('excludes an inbound HTTPRoute bound to a same-named ingress Gateway', () => {
    const d = mapEgressDetail(gateway, [aiRoute, ingressHttpRoute, bareNameRoute], [])
    const names = d.routes.map((r) => r.name).sort()
    expect(names).toEqual(['anthropic', 'tools'])
    expect(d.routes.some((r) => r.kind === 'HTTPRoute')).toBe(false)
  })

  it('keeps an HTTPRoute that genuinely parents this EgressGateway', () => {
    const egressHttpRoute = obj({
      kind: 'HTTPRoute',
      metadata: { name: 'passthrough', namespace: 'default' },
      spec: {
        parentRefs: [{ group: 'clrk.apoxy.dev', kind: 'EgressGateway', name: 'jq-bot' }],
        rules: [{ matches: [{ path: { value: '/' } }] }],
      },
    })
    const d = mapEgressDetail(gateway, [egressHttpRoute], [])
    expect(d.routes.map((r) => r.name)).toEqual(['passthrough'])
  })
})

// A clrk EgressGateway publishes its health as a single `Ready` condition, not
// the Gateway API `Programmed`/`Accepted` vocabulary. The detail mapper must
// read `Ready`, else a healthy EgressGateway is mislabeled Degraded.
describe('mapEgressDetail / status', () => {
  const degradedGateway = obj({
    apiVersion: 'clrk.apoxy.dev/v1alpha1',
    kind: 'EgressGateway',
    metadata: { name: 'jq-bot', namespace: 'default', creationTimestamp: '2026-06-20T00:00:00Z' },
    spec: { defaultPolicy: 'allow-all', listeners: [{ name: 'egress', protocol: 'TLS', tls: { mode: 'Terminate' } }] },
    status: {
      conditions: [
        {
          type: 'Ready',
          status: 'False',
          reason: 'GatewayNotProgrammed',
          message: 'Envoy Gateway has not yet reported Programmed=True',
        },
      ],
      listeners: [{ name: 'egress' }],
    },
  })

  it('maps a True Ready condition to Ready', () => {
    const d = mapEgressDetail(gateway, [], [])
    expect(d.ready).toBe(true)
    expect(d.status).toBe('Ready')
  })

  it('maps a False Ready condition to Degraded and carries the message', () => {
    const d = mapEgressDetail(degradedGateway, [], [])
    expect(d.ready).toBe(false)
    expect(d.status).toBe('Degraded')
    expect(d.statusReason).toBe('Envoy Gateway has not yet reported Programmed=True')
  })

  it('surfaces each status condition as one event', () => {
    const d = mapEgressDetail(gateway, [], [])
    expect(d.events).toHaveLength(1)
    expect(d.events[0]).toMatchObject({ type: 'Ready', tone: 'ok' })
  })

  // The controller mirrors the data plane's top-level (Accepted/Programmed) and
  // per-listener (Programmed/Accepted/ResolvedRefs) lifecycle onto the
  // EgressGateway's own status, so the console renders the full timeline from
  // the EgressGateway alone -- it never reads the backing Gateway.
  it('merges top-level and per-listener conditions into the events, newest first', () => {
    const richGateway = obj({
      apiVersion: 'clrk.apoxy.dev/v1alpha1',
      kind: 'EgressGateway',
      metadata: { name: 'jq-bot', namespace: 'default', creationTimestamp: '2026-06-20T00:00:00Z' },
      spec: { defaultPolicy: 'allow-all', listeners: [{ name: 'egress', protocol: 'TLS', tls: { mode: 'Terminate' } }] },
      status: {
        conditions: [
          { type: 'Accepted', status: 'True', reason: 'Accepted', lastTransitionTime: '2026-06-22T20:59:38Z' },
          { type: 'Programmed', status: 'True', reason: 'Programmed', lastTransitionTime: '2026-06-23T00:00:00Z' },
          { type: 'Ready', status: 'True', reason: 'GatewayProgrammed', lastTransitionTime: '2026-06-23T00:00:01Z' },
        ],
        listeners: [
          {
            name: 'egress',
            conditions: [
              { type: 'Programmed', status: 'True', reason: 'Programmed', lastTransitionTime: '2026-06-23T00:00:00Z' },
              { type: 'Accepted', status: 'True', reason: 'Accepted', lastTransitionTime: '2026-06-22T20:59:38Z' },
              { type: 'ResolvedRefs', status: 'True', reason: 'ResolvedRefs', lastTransitionTime: '2026-06-22T20:59:38Z' },
            ],
          },
        ],
      },
    })

    const d = mapEgressDetail(richGateway, [], [])
    expect(d.events).toHaveLength(6) // 3 top-level + 3 per-listener
    // Per-listener events are namespaced by the listener name.
    expect(
      d.events.filter((e) => e.type.startsWith('egress: ')).map((e) => e.type).sort(),
    ).toEqual(['egress: Accepted', 'egress: Programmed', 'egress: ResolvedRefs'])
    // Newest-first: the Ready condition carries the latest timestamp.
    expect(d.events[0]).toMatchObject({ type: 'GatewayProgrammed' })
    expect(d.events.every((e) => e.tone === 'ok')).toBe(true)
  })
})
