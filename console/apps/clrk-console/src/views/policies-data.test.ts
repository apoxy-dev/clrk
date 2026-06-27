import { describe, expect, it } from 'vitest'
import {
  attachSummary,
  countByKind,
  deriveStatus,
  isAttachedKind,
  mapPolicies,
  POLICY_KINDS,
  specLine,
  type PolicyKind,
  type PolicyObj,
} from './policies-data'

// --- fixtures ---------------------------------------------------------------

interface PolFixture {
  kind: PolicyKind
  name?: string
  namespace?: string
  created?: string
  spec?: Record<string, unknown>
  status?: Record<string, unknown>
}

function pol(p: PolFixture): PolicyObj {
  return {
    metadata: {
      name: p.name ?? 'p-1',
      namespace: p.namespace ?? 'platform',
      creationTimestamp: p.created ?? '2026-06-26T00:00:00Z',
    },
    spec: p.spec,
    status: p.status,
  } as PolicyObj
}

function accepted(): Record<string, unknown> {
  return { ancestors: [{ conditions: [{ type: 'Accepted', status: 'True' }] }] }
}

// --- isAttachedKind ---------------------------------------------------------

describe('isAttachedKind', () => {
  it('marks the targetRefs/PolicyStatus kinds as attached', () => {
    for (const k of ['CredentialInjectionPolicy', 'FallbackRoutingPolicy', 'EgressDenyPolicy'] as const) {
      expect(isAttachedKind(k)).toBe(true)
    }
  })

  it('marks the standalone kinds as not attached', () => {
    expect(isAttachedKind('RateLimitPolicy')).toBe(false)
    expect(isAttachedKind('LoggingPolicy')).toBe(false)
  })
})

// --- attachSummary ----------------------------------------------------------

describe('attachSummary', () => {
  it('renders a single targetRef', () => {
    expect(
      attachSummary('CredentialInjectionPolicy', {
        targetRefs: [{ kind: 'AIProviderRoute', name: 'openai-chat' }],
      }),
    ).toEqual({ via: 'targetRef', target: 'AIProviderRoute/openai-chat' })
  })

  it('collapses multiple targetRefs to a count', () => {
    expect(
      attachSummary('CredentialInjectionPolicy', {
        targetRefs: [
          { kind: 'HTTPRoute', name: 'github-api' },
          { kind: 'MCPRoute', name: 'github-mcp' },
        ],
      }),
    ).toEqual({ via: 'targetRefs', target: '2 targets' })
  })

  it('labels a gateway-wide target as a catch-all', () => {
    expect(
      attachSummary('CredentialInjectionPolicy', {
        targetRefs: [{ kind: 'EgressGateway', name: 'llm-egress' }],
      }),
    ).toEqual({ via: 'targetRef · catch-all', target: 'EgressGateway/llm-egress' })
  })

  it('shows an em dash when an attached policy declares no targetRefs', () => {
    expect(attachSummary('EgressDenyPolicy', { targetRefs: [] })).toEqual({
      via: 'targetRefs',
      target: '—',
    })
  })

  it('describes standalone kinds as referenced by rules', () => {
    for (const k of ['RateLimitPolicy', 'LoggingPolicy'] as const) {
      expect(attachSummary(k, { requests: 120 })).toEqual({
        via: 'referenced by rules',
        target: '—',
      })
    }
  })
})

// --- specLine ---------------------------------------------------------------

describe('specLine', () => {
  it('summarizes a header credential injection', () => {
    expect(
      specLine('CredentialInjectionPolicy', {
        secretRef: { name: 'openai-secret' },
        target: 'Header',
        headerName: 'Authorization',
      }),
    ).toBe('openai-secret → Authorization')
  })

  it('summarizes a query-param credential injection', () => {
    expect(
      specLine('CredentialInjectionPolicy', {
        secretRef: { name: 'azure-secret' },
        target: 'QueryParam',
        queryParamName: 'api-key',
      }),
    ).toBe('azure-secret → ?api-key')
  })

  it('summarizes a provider-auth credential injection', () => {
    expect(
      specLine('CredentialInjectionPolicy', {
        secretRef: { name: 'bedrock' },
        target: 'ProviderAuth',
        providerAuth: { type: 'AWSv4' },
      }),
    ).toBe('ProviderAuth · AWSv4')
  })

  it('defaults the deny status code to 403 and honors an override', () => {
    expect(specLine('EgressDenyPolicy', {})).toBe('deny → 403')
    expect(specLine('EgressDenyPolicy', { denyResponse: { statusCode: 402 } })).toBe('deny → 402')
  })

  it('summarizes a rate limit', () => {
    expect(specLine('RateLimitPolicy', { requests: 120, window: '1m', scope: 'PerRoute' })).toBe(
      '120 / 1m · PerRoute',
    )
  })

  it('summarizes logging capture toggles', () => {
    expect(specLine('LoggingPolicy', { captureRequest: true, captureResponse: true })).toBe(
      'capture req+res',
    )
    expect(specLine('LoggingPolicy', { captureRequest: true })).toBe('capture req')
    expect(specLine('LoggingPolicy', { captureResponse: true })).toBe('capture res')
    expect(specLine('LoggingPolicy', {})).toBe('summary only')
  })

  it('summarizes fallback routing with and without explicit retries', () => {
    expect(
      specLine('FallbackRoutingPolicy', {
        retry: { numRetries: 3, retriableStatusCodes: [500, 503] },
      }),
    ).toBe('fallback · 3× · 500,503')
    expect(specLine('FallbackRoutingPolicy', {})).toBe('fallback · 429,503')
  })
})

// --- deriveStatus -----------------------------------------------------------

describe('deriveStatus', () => {
  it('reports Accepted when every ancestor accepts', () => {
    expect(deriveStatus('CredentialInjectionPolicy', accepted())).toEqual({
      label: 'Accepted',
      tone: 'ok',
    })
  })

  it('reports Conflicted when a refusal cites a conflict', () => {
    expect(
      deriveStatus('EgressDenyPolicy', {
        ancestors: [{ conditions: [{ type: 'Accepted', status: 'False', reason: 'Conflicted' }] }],
      }),
    ).toEqual({ label: 'Conflicted', tone: 'warn' })
  })

  it('reports Pending when an attached policy has no ancestors yet', () => {
    expect(deriveStatus('FallbackRoutingPolicy', undefined)).toEqual({
      label: 'Pending',
      tone: 'warn',
    })
  })

  it('reports Pending for a non-conflict refusal', () => {
    expect(
      deriveStatus('CredentialInjectionPolicy', {
        ancestors: [{ conditions: [{ type: 'Accepted', status: 'False', reason: 'TargetNotFound' }] }],
      }),
    ).toEqual({ label: 'Pending', tone: 'warn' })
  })

  it('reports no acceptance state for standalone kinds', () => {
    expect(deriveStatus('RateLimitPolicy', undefined)).toEqual({ label: '—', tone: 'none' })
  })
})

// --- mapPolicies ------------------------------------------------------------

describe('mapPolicies', () => {
  it('flattens the kind groups into canonical kind order', () => {
    const rows = mapPolicies([
      { kind: 'LoggingPolicy', items: [pol({ kind: 'LoggingPolicy', name: 'web-audit' })] },
      {
        kind: 'CredentialInjectionPolicy',
        items: [
          pol({
            kind: 'CredentialInjectionPolicy',
            name: 'openai-key',
            spec: {
              targetRefs: [{ kind: 'AIProviderRoute', name: 'openai-chat' }],
              secretRef: { name: 'openai-secret' },
              target: 'Header',
              headerName: 'Authorization',
            },
            status: accepted(),
          }),
        ],
      },
    ])
    expect(rows.map((r) => r.kind)).toEqual(['CredentialInjectionPolicy', 'LoggingPolicy'])
    const cip = rows[0]!
    expect(cip.id).toBe('CredentialInjectionPolicy:platform:openai-key')
    expect(cip.via).toBe('targetRef')
    expect(cip.target).toBe('AIProviderRoute/openai-chat')
    expect(cip.spec).toBe('openai-secret → Authorization')
    expect(cip.status).toBe('Accepted')
    expect(cip.tone).toBe('ok')
    expect(cip.effective).toBe(1)
  })

  it('leaves standalone kinds without acceptance and with zero effective', () => {
    const rows = mapPolicies([
      {
        kind: 'RateLimitPolicy',
        items: [
          pol({
            kind: 'RateLimitPolicy',
            name: 'gh-write-rl',
            spec: { requests: 60, window: '1m', scope: 'PerAgent' },
          }),
        ],
      },
    ])
    const rl = rows[0]!
    expect(rl.tone).toBe('none')
    expect(rl.status).toBe('—')
    expect(rl.effective).toBe(0)
    expect(rl.spec).toBe('60 / 1m · PerAgent')
  })
})

// --- countByKind ------------------------------------------------------------

describe('countByKind', () => {
  it('counts rows per kind across the canonical set', () => {
    const rows = mapPolicies([
      {
        kind: 'CredentialInjectionPolicy',
        items: [
          pol({ kind: 'CredentialInjectionPolicy', name: 'a' }),
          pol({ kind: 'CredentialInjectionPolicy', name: 'b' }),
        ],
      },
      { kind: 'EgressDenyPolicy', items: [pol({ kind: 'EgressDenyPolicy', name: 'c' })] },
    ])
    const counts = countByKind(rows)
    expect(counts.CredentialInjectionPolicy).toBe(2)
    expect(counts.EgressDenyPolicy).toBe(1)
    expect(counts.LoggingPolicy).toBe(0)
    // Every canonical kind has an entry, even at zero.
    expect(Object.keys(counts).sort()).toEqual([...POLICY_KINDS].sort())
  })
})
