import { describe, expect, it } from 'vitest'
import type { K8sObject } from '@apoxy/console-core'
import { attachPayload, mapPolicyDetail } from './policies-detail-data'
import type { PolicyKind, PolicyObj } from './policies-data'

// --- fixtures ---------------------------------------------------------------

interface PolFixture {
  kind: PolicyKind
  name?: string
  namespace?: string
  spec?: Record<string, unknown>
  status?: Record<string, unknown>
  annotations?: Record<string, string>
}

function pol(p: PolFixture): PolicyObj {
  return {
    metadata: {
      name: p.name ?? 'p-1',
      namespace: p.namespace ?? 'platform',
      creationTimestamp: '2026-06-26T00:00:00Z',
      annotations: p.annotations,
    },
    spec: p.spec,
    status: p.status,
  } as PolicyObj
}

function route(kind: string, name: string, gateway: string, hostnames?: string[]): K8sObject {
  return {
    kind,
    metadata: { name, namespace: 'platform' },
    spec: {
      parentRefs: [{ group: 'clrk.apoxy.dev', kind: 'EgressGateway', name: gateway }],
      hostnames,
    },
  } as unknown as K8sObject
}

function gateway(name: string): K8sObject {
  return { kind: 'EgressGateway', metadata: { name, namespace: 'platform' } } as K8sObject
}

function accepted(): Record<string, unknown> {
  return { ancestors: [{ conditions: [{ type: 'Accepted', status: 'True' }] }] }
}

// --- effective resolution ---------------------------------------------------

describe('mapPolicyDetail · effective resolution', () => {
  it('resolves a direct route targetRef and finds its gateway', () => {
    const routes = [route('AIProviderRoute', 'openai-chat', 'llm-egress')]
    const d = mapPolicyDetail(
      'CredentialInjectionPolicy',
      pol({
        kind: 'CredentialInjectionPolicy',
        spec: {
          targetRefs: [{ kind: 'AIProviderRoute', name: 'openai-chat' }],
          secretRef: { name: 'openai-secret' },
          target: 'Header',
          headerName: 'Authorization',
        },
        status: accepted(),
      }),
      routes,
      [gateway('llm-egress')],
    )
    expect(d.effective).toEqual([
      {
        via: 'direct',
        routeKind: 'AIProviderRoute',
        routeName: 'openai-chat',
        gatewayName: 'llm-egress',
        host: 'openai-chat',
      },
    ])
    expect(d.vitals).toEqual({ scope: 'Route', routeCount: 1, gwCount: 1 })
    expect(d.headline).toBe('Enforced on 1 route across 1 gateway.')
  })

  it('fans a gateway catch-all out to every route on that gateway as inherited', () => {
    const routes = [
      route('AIProviderRoute', 'openai-chat', 'llm-egress'),
      route('AIProviderRoute', 'anthropic', 'llm-egress'),
      route('HTTPRoute', 'github-api', 'github-egress'),
    ]
    const d = mapPolicyDetail(
      'CredentialInjectionPolicy',
      pol({
        kind: 'CredentialInjectionPolicy',
        spec: {
          targetRefs: [{ kind: 'EgressGateway', name: 'llm-egress' }],
          secretRef: { name: 'tenant-id' },
          target: 'Header',
          headerName: 'X-Tenant-Id',
        },
        status: accepted(),
      }),
      routes,
      [gateway('llm-egress'), gateway('github-egress')],
    )
    expect(d.effective.map((e) => e.routeName)).toEqual(['openai-chat', 'anthropic'])
    expect(d.effective.every((e) => e.via === 'inherited')).toBe(true)
    expect(d.vitals).toEqual({ scope: 'Gateway', routeCount: 2, gwCount: 1 })
    expect(d.headline).toBe('Enforced gateway-wide — inherited by 2 routes on 1 gateway.')
  })

  it('leaves an unresolved targetRef out of effective but marks the target unresolved', () => {
    const d = mapPolicyDetail(
      'EgressDenyPolicy',
      pol({
        kind: 'EgressDenyPolicy',
        name: 'deny-stripe',
        spec: { targetRefs: [{ kind: 'HTTPRoute', name: 'stripe-api' }] },
        status: { ancestors: [{ conditions: [{ type: 'Accepted', status: 'False', reason: 'TargetNotFound', message: 'route stripe-api not found' }] }] },
      }),
      [],
      [],
    )
    expect(d.effective).toEqual([])
    expect(d.targets).toEqual([
      { kind: 'HTTPRoute', name: 'stripe-api', resolved: false, catchAll: false },
    ])
    expect(d.tone).toBe('warn')
    expect(d.headline).toBe('Not in effect — route stripe-api not found')
    expect(d.statusReason).toBe('route stripe-api not found')
  })

  it('reports an accepted attached policy that matches no routes as not in effect', () => {
    const d = mapPolicyDetail(
      'CredentialInjectionPolicy',
      pol({
        kind: 'CredentialInjectionPolicy',
        spec: { targetRefs: [{ kind: 'EgressGateway', name: 'empty-egress' }] },
        status: accepted(),
      }),
      [],
      [gateway('empty-egress')],
    )
    expect(d.effective).toEqual([])
    expect(d.headline).toBe('Accepted, but it currently matches no routes — not in effect.')
  })
})

// --- standalone kinds -------------------------------------------------------

describe('mapPolicyDetail · standalone kinds', () => {
  it('resolves nothing for a RateLimitPolicy and scopes it to a route rule', () => {
    const d = mapPolicyDetail(
      'RateLimitPolicy',
      pol({
        kind: 'RateLimitPolicy',
        name: 'gh-rl',
        spec: { requests: 120, window: '1m', scope: 'PerRoute' },
      }),
      [route('HTTPRoute', 'github-api', 'github-egress')],
      [gateway('github-egress')],
    )
    expect(d.effective).toEqual([])
    expect(d.targets).toEqual([])
    expect(d.tone).toBe('none')
    expect(d.vitals).toEqual({ scope: 'Route rule', routeCount: 0, gwCount: 0 })
    expect(d.headline).toBe(
      'Referenced by route rules — applied wherever a route rule names this policy.',
    )
  })
})

// --- spec fields + summary --------------------------------------------------

describe('mapPolicyDetail · spec fields', () => {
  it('flattens a credential injection spec to raw paths and a summary', () => {
    const d = mapPolicyDetail(
      'CredentialInjectionPolicy',
      pol({
        kind: 'CredentialInjectionPolicy',
        spec: {
          targetRefs: [{ kind: 'AIProviderRoute', name: 'gemini' }],
          secretRef: { name: 'vertex-sa' },
          secretKey: 'key.json',
          target: 'ProviderAuth',
          providerAuth: { type: 'AWSv4', service: 'bedrock', region: 'us-east-1' },
        },
        status: accepted(),
      }),
      [route('AIProviderRoute', 'gemini', 'llm-egress')],
      [gateway('llm-egress')],
    )
    expect(d.fields).toEqual([
      { k: 'targetRefs[0]', v: 'AIProviderRoute/gemini' },
      { k: 'secretRef.name', v: 'vertex-sa' },
      { k: 'secretKey', v: 'key.json' },
      { k: 'target', v: 'ProviderAuth' },
      { k: 'providerAuth.type', v: 'AWSv4' },
      { k: 'providerAuth.service', v: 'bedrock' },
      { k: 'providerAuth.region', v: 'us-east-1' },
    ])
    expect(d.summary).toBe('Signs requests with AWSv4 using vertex-sa.')
  })

  it('flattens a logging spec including redact headers', () => {
    const d = mapPolicyDetail(
      'LoggingPolicy',
      pol({
        kind: 'LoggingPolicy',
        spec: {
          captureRequest: true,
          captureResponse: false,
          redactHeaders: ['Authorization', 'Cookie'],
          sinkRef: 'otelcol.observability.svc:4318',
        },
      }),
      [],
      [],
    )
    expect(d.fields).toEqual([
      { k: 'captureRequest', v: 'true' },
      { k: 'captureResponse', v: 'false' },
      { k: 'redactHeaders[0]', v: 'Authorization' },
      { k: 'redactHeaders[1]', v: 'Cookie' },
      { k: 'sinkRef', v: 'otelcol.observability.svc:4318' },
    ])
    expect(d.summary).toBe('Captures request bodies for matched traffic.')
  })

  it('prefers a hand-written summary annotation over the derived one', () => {
    const d = mapPolicyDetail(
      'EgressDenyPolicy',
      pol({
        kind: 'EgressDenyPolicy',
        spec: { targetRefs: [], denyResponse: { statusCode: 402 } },
        annotations: { 'clrk.apoxy.dev/summary': 'Block live Stripe in test.' },
      }),
      [],
      [],
    )
    expect(d.summary).toBe('Block live Stripe in test.')
    expect(d.fields).toContainEqual({ k: 'denyResponse.statusCode', v: '402' })
  })
})

// --- attachment graph payload -----------------------------------------------

describe('attachPayload', () => {
  it('routes a direct attachment straight to its route node, carrying the section', () => {
    const d = mapPolicyDetail(
      'CredentialInjectionPolicy',
      pol({
        kind: 'CredentialInjectionPolicy',
        name: 'cross-provider-jq-anthropic',
        spec: {
          targetRefs: [{ kind: 'AIProviderRoute', name: 'cross-provider-jq', sectionName: 'anthropic-haiku' }],
        },
        status: accepted(),
      }),
      [route('AIProviderRoute', 'cross-provider-jq', 'jq')],
      [gateway('jq')],
    )
    const g = attachPayload(d)
    expect(g.short).toBe('CIP · POLICY')
    expect(g.mode).toBe('targetRefs')
    expect(g.catchAll).toBe(false)
    expect(g.gateway).toBeNull()
    expect(g.targets).toEqual([
      {
        kind: 'AIProviderRoute',
        name: 'cross-provider-jq',
        host: 'cross-provider-jq',
        via: 'direct',
        section: 'anthropic-haiku',
      },
    ])
  })

  it('inserts a catch-all gateway hop and inherits every route on it', () => {
    const d = mapPolicyDetail(
      'CredentialInjectionPolicy',
      pol({
        kind: 'CredentialInjectionPolicy',
        spec: { targetRefs: [{ kind: 'EgressGateway', name: 'llm-egress' }] },
        status: accepted(),
      }),
      [route('AIProviderRoute', 'openai', 'llm-egress'), route('AIProviderRoute', 'anthropic', 'llm-egress')],
      [gateway('llm-egress')],
    )
    const g = attachPayload(d)
    expect(g.catchAll).toBe(true)
    expect(g.gateway).toBe('llm-egress')
    expect(g.targets.map((t) => t.via)).toEqual(['inherited', 'inherited'])
    expect(g.targets.map((t) => t.name)).toEqual(['openai', 'anthropic'])
  })

  it('draws an unresolved targetRef as an unresolved node', () => {
    const d = mapPolicyDetail(
      'EgressDenyPolicy',
      pol({ kind: 'EgressDenyPolicy', spec: { targetRefs: [{ kind: 'HTTPRoute', name: 'stripe-api' }] } }),
      [],
      [],
    )
    const g = attachPayload(d)
    expect(g.targets).toEqual([
      { kind: 'HTTPRoute', name: 'stripe-api', host: 'stripe-api', via: 'unresolved' },
    ])
  })

  it('points a standalone policy at a single route-rules node', () => {
    const d = mapPolicyDetail(
      'RateLimitPolicy',
      pol({ kind: 'RateLimitPolicy', spec: { requests: 60, window: '1m', scope: 'PerAgent' } }),
      [],
      [],
    )
    const g = attachPayload(d)
    expect(g.mode).toBe('referencedBy')
    expect(g.catchAll).toBe(false)
    expect(g.targets).toEqual([
      { kind: '', name: 'route rules', host: 'route rules', via: 'reference' },
    ])
  })
})
