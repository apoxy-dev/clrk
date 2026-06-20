// Dev-only in-memory backend for the mocked console instance (`/mock.html`),
// modeled on the apoxy console's MockBackend. A plain `fetch` shim injected into
// the GVR client — no service worker, no network — implementing just enough of
// the k8s REST surface the console uses: aggregated discovery,
// SelfSubjectAccessReview, LIST, GET, streaming WATCH, Server-Side Apply
// (PATCH), and DELETE. Apply/delete broadcast to open watch streams so YAML-tray
// edits reflect live in the lists.
//
// clrk specifics vs apoxy: every kind is namespaced (objects carry
// metadata.namespace), and the list/watch path is the cluster-wide collection
// (`/apis/<group>/<version>/<resource>`), which the console uses to list across
// all namespaces. Telemetry the CRDs don't carry (per-gateway RPS, a human
// description) rides as demo annotations so the egress list renders like the
// design without inventing CRD fields.
//
// Never imported by the production entry (src/main.tsx); it exists only so the
// feature views are exercisable end-to-end without a real apiserver.

type Json = Record<string, unknown>

interface StoredObject {
  apiVersion: string
  kind: string
  metadata: {
    name: string
    uid: string
    namespace?: string
    resourceVersion?: string
    creationTimestamp?: string
    annotations?: Record<string, string>
  }
  spec?: Json
  status?: Record<string, unknown>
}

type WatchEvent = {
  type: 'ADDED' | 'MODIFIED' | 'DELETED' | 'BOOKMARK'
  object: unknown
}
type Controller = ReadableStreamDefaultController<Uint8Array>

const GROUP = 'clrk.apoxy.dev'
const VERSION = 'v1alpha1'
const GWAPI = 'gateway.networking.k8s.io'
const enc = new TextEncoder()

function colKey(group: string, version: string, resource: string): string {
  return `${group}/${version}/${resource}`
}

export class MockBackend {
  private rv = 0
  private readonly store = new Map<string, Map<string, StoredObject>>()
  private readonly watchers = new Map<string, Set<Controller>>()

  /** The injectable `fetch` (bound) handed to createConsoleClient. */
  fetch = async (
    input: RequestInfo | URL,
    init?: RequestInit,
  ): Promise<Response> => {
    const url = new URL(
      typeof input === 'string'
        ? input
        : input instanceof URL
          ? input.href
          : input.url,
    )
    const method = (init?.method ?? 'GET').toUpperCase()
    const segs = url.pathname.split('/').filter(Boolean)

    // Aggregated discovery v2: GET /apis and GET /api both 200.
    if (method === 'GET' && url.pathname === '/apis')
      return json(this.discoveryDoc())
    if (method === 'GET' && url.pathname === '/api') return json({ items: [] })

    // SelfSubjectAccessReview — allow everything in the mock.
    if (
      method === 'POST' &&
      url.pathname.endsWith('/selfsubjectaccessreviews')
    ) {
      return json({
        apiVersion: 'authorization.k8s.io/v1',
        kind: 'SelfSubjectAccessReview',
        status: { allowed: true },
      })
    }

    // /apis/<group>/<version>/[namespaces/<ns>/]<resource>[/<name>]
    if (segs[0] === 'apis' && segs.length >= 4) {
      const [, group, version] = segs
      let rest = segs.slice(3)
      if (rest[0] === 'namespaces') rest = rest.slice(2) // collapse the namespace scope
      const [resource, name] = rest
      if (!resource) return notFound(method, url.pathname)
      const key = colKey(group!, version!, resource)
      if (!name) {
        if (method === 'GET' && url.searchParams.get('watch') === '1')
          return this.openWatch(key, init?.signal)
        if (method === 'GET') return json(this.listBody(key))
      } else {
        if (method === 'GET') return this.getObject(key, name)
        if (method === 'PATCH') return this.applyObject(key, name, init)
        if (method === 'DELETE') return this.deleteObject(key, name)
      }
    }

    return notFound(method, url.pathname)
  }

  /** Seed a realistic starter set across the kinds the app surfaces. */
  seedDemo(): void {
    this.seedEgress()
    this.seedAgents()
  }

  // --- Egress Gateways + attaching routes + policies (design's data-egress.js) -
  //
  // The demo is authored in a rich shape (EGRESS_DEMO) then *lowered* to faithful
  // k8s objects: EgressGateway (spec.listeners + status.listeners), the four route
  // kinds with real spec.rules (matches / inline filters / backendRefs), and the
  // separate policy CRDs (CredentialInjection / RateLimit / Logging / EgressDeny)
  // that attach to a route by parentRef/targetRef. The detail view's mapper lifts
  // these back into the Miller hierarchy — so the drill-down exercises the real
  // read path, not a fixture.

  private seedEgress(): void {
    for (const g of EGRESS_DEMO) this.lowerGateway(g)
  }

  private lowerGateway(g: DemoGateway): void {
    const ok = !g.degraded
    this.putGVR(GROUP, VERSION, 'egressgateways', {
      apiVersion: `${GROUP}/${VERSION}`,
      kind: 'EgressGateway',
      metadata: {
        name: g.name,
        uid: `uid-${g.name}`,
        namespace: g.namespace,
        creationTimestamp: daysAgo(g.ageDays),
        annotations: {
          'clrk.apoxy.dev/demo-rps': String(g.rps),
          'clrk.apoxy.dev/description': g.description,
        },
      },
      spec: {
        defaultPolicy: g.defaultPolicy,
        listeners: g.listeners.map((l) => ({
          name: l.name,
          protocol: l.protocol,
          ...(l.port ? { port: l.port } : {}),
          ...(l.tls ? { tls: { mode: l.tls } } : {}),
        })),
        ...(g.otlpEndpoint ? { otlp: { endpoint: g.otlpEndpoint } } : {}),
      },
      status: {
        listenerCount: g.listeners.length,
        listeners: g.listeners.map((l, i) => ({
          name: l.name,
          port: 15001 + i,
          backendAddress: `:${15001 + i}`,
          attachedRoutes: g.routes.filter((r) => r.listener === l.name).length,
          conditions: [progCond(ok && l.ready !== false, g.degraded)],
        })),
        conditions: [progCond(ok, g.degraded)],
      },
    })

    for (const r of g.routes) this.lowerRoute(g, r)
  }

  private lowerRoute(g: DemoGateway, r: DemoRoute): void {
    const parentRefs = [{ name: g.name, sectionName: r.listener }]
    const base = {
      metadata: {
        name: r.name,
        uid: `uid-${r.name}`,
        namespace: g.namespace,
        creationTimestamp: daysAgo(g.ageDays - 1),
      },
    }
    const backendRefs = (rule: DemoRule) =>
      (rule.backends ?? []).map((b) => ({
        name: b.name,
        port: b.port,
        weight: b.weight,
      }))

    if (r.kind === 'AIProviderRoute') {
      this.putGVR(GROUP, VERSION, 'aiproviderroutes', {
        ...base,
        apiVersion: `${GROUP}/${VERSION}`,
        kind: 'AIProviderRoute',
        spec: {
          parentRefs,
          rules: r.rules.map((rule) => ({
            matches: [
              {
                provider: r.provider ?? 'custom',
                ...(rule.match.models ? { models: rule.match.models } : {}),
                ...(rule.match.endpoints
                  ? { endpoints: rule.match.endpoints }
                  : {}),
              },
            ],
            filters: rule.tokenBudget
              ? [
                  {
                    type: 'TokenBudget',
                    tokenBudget: {
                      maxTokensPerDay: rule.tokenBudget.maxTokensPerDay,
                    },
                  },
                ]
              : [],
            backendRefs: backendRefs(rule),
          })),
        },
      })
    } else if (r.kind === 'MCPRoute') {
      this.putGVR(GROUP, VERSION, 'mcproutes', {
        ...base,
        apiVersion: `${GROUP}/${VERSION}`,
        kind: 'MCPRoute',
        spec: {
          parentRefs,
          hostnames: r.hostnames,
          rules: r.rules.map((rule) => ({
            matches: [
              { tools: rule.match.value.split(',').map((s) => s.trim()) },
            ],
            filters: rule.toolPolicy
              ? [
                  {
                    type: 'ToolPolicy',
                    toolPolicy: {
                      ...(rule.toolPolicy.allowedTools
                        ? { allowedTools: rule.toolPolicy.allowedTools }
                        : {}),
                      ...(rule.toolPolicy.requireConfirmation
                        ? {
                            requireConfirmation:
                              rule.toolPolicy.requireConfirmation,
                          }
                        : {}),
                      ...(rule.toolPolicy.maxCallsPerExecution
                        ? {
                            maxCallsPerExecution:
                              rule.toolPolicy.maxCallsPerExecution,
                          }
                        : {}),
                      ...(rule.toolPolicy.rateLimit
                        ? {
                            rateLimits: [
                              {
                                requests: rule.toolPolicy.rateLimit.requests,
                                window: rule.toolPolicy.rateLimit.window,
                              },
                            ],
                          }
                        : {}),
                    },
                  },
                ]
              : [],
            backendRefs: backendRefs(rule),
          })),
        },
      })
    } else if (r.kind === 'EgressL4Route') {
      this.putGVR(GROUP, VERSION, 'egressl4routes', {
        ...base,
        apiVersion: `${GROUP}/${VERSION}`,
        kind: 'EgressL4Route',
        spec: {
          parentRefs,
          rules: r.rules.map((rule) => ({
            matches: [
              rule.match.kind === 'cidr'
                ? { destinationCIDRs: [rule.match.value] }
                : { destinationHostnames: [rule.match.value] },
            ],
            filters: [],
            backendRefs: backendRefs(rule),
          })),
        },
      })
    } else {
      this.putGVR(GWAPI, 'v1', 'httproutes', {
        ...base,
        apiVersion: `${GWAPI}/v1`,
        kind: 'HTTPRoute',
        spec: {
          parentRefs,
          hostnames: r.hostnames,
          rules: r.rules.map((rule) => ({
            matches:
              rule.match.kind === 'method'
                ? rule.match.value.split(',').map((m) => ({ method: m.trim() }))
                : [{ path: { type: 'PathPrefix', value: rule.match.value } }],
            backendRefs: backendRefs(rule),
          })),
        },
      })
    }

    // Lower route-level attached policies into their own CRDs.
    if (r.cred) {
      this.putGVR(GROUP, VERSION, 'credentialinjectionpolicies', {
        apiVersion: `${GROUP}/${VERSION}`,
        kind: 'CredentialInjectionPolicy',
        metadata: {
          name: `${r.name}-cred`,
          uid: `uid-${r.name}-cred`,
          namespace: g.namespace,
          creationTimestamp: daysAgo(g.ageDays - 1),
        },
        spec: {
          parentRefs: [{ name: r.name }],
          secretRef: { name: r.cred.secret },
          secretKey: r.cred.key ?? 'token',
          target: r.cred.target,
          ...(r.cred.header ? { headerName: r.cred.header } : {}),
        },
      })
    }
    if (r.rateLimit) {
      this.putGVR(GROUP, VERSION, 'ratelimitpolicies', {
        apiVersion: `${GROUP}/${VERSION}`,
        kind: 'RateLimitPolicy',
        metadata: {
          name: `${r.name}-rl`,
          uid: `uid-${r.name}-rl`,
          namespace: g.namespace,
          creationTimestamp: daysAgo(g.ageDays - 1),
        },
        spec: {
          parentRefs: [{ name: r.name }],
          requests: r.rateLimit.requests,
          window: r.rateLimit.window,
        },
      })
    }
    if (r.logging) {
      this.putGVR(GROUP, VERSION, 'loggingpolicies', {
        apiVersion: `${GROUP}/${VERSION}`,
        kind: 'LoggingPolicy',
        metadata: {
          name: `${r.name}-log`,
          uid: `uid-${r.name}-log`,
          namespace: g.namespace,
          creationTimestamp: daysAgo(g.ageDays - 1),
          annotations: { 'clrk.apoxy.dev/summary': r.logging.summary },
        },
        spec: {
          parentRefs: [{ name: r.name }],
          captureRequest: r.logging.captureRequest ?? false,
          captureResponse: r.logging.captureResponse ?? false,
        },
      })
    }
    if (r.deny) {
      this.putGVR(GROUP, VERSION, 'egressdenypolicies', {
        apiVersion: `${GROUP}/${VERSION}`,
        kind: 'EgressDenyPolicy',
        metadata: {
          name: `${r.name}-deny`,
          uid: `uid-${r.name}-deny`,
          namespace: g.namespace,
          creationTimestamp: daysAgo(g.ageDays - 1),
        },
        spec: {
          targetRef: { group: GROUP, kind: r.kind, name: r.name },
          denyResponse: { statusCode: r.deny.statusCode ?? 403 },
        },
      })
    }
  }

  // --- Agents + Invocations (so the Run / Observe lists populate too) ---------

  private seedAgents(): void {
    const ta = (name: string, ns: string, active: number, revision: string) =>
      this.putGVR(GROUP, VERSION, 'taskagents', {
        apiVersion: `${GROUP}/${VERSION}`,
        kind: 'TaskAgent',
        metadata: {
          name,
          uid: `uid-${name}`,
          namespace: ns,
          creationTimestamp: daysAgo(20),
        },
        spec: { workerPoolRef: 'default' },
        status: {
          activeExecutions: active,
          latestReadyRevisionName: revision,
          conditions: [{ type: 'Ready', status: 'True' }],
        },
      })
    ta('code-reviewer', 'agents', 2, 'code-reviewer-00007')
    ta('nightly-summarizer', 'agents', 0, 'nightly-summarizer-00003')

    const da = (name: string, ns: string, phase: string, restarts: number) =>
      this.putGVR(GROUP, VERSION, 'daemonagents', {
        apiVersion: `${GROUP}/${VERSION}`,
        kind: 'DaemonAgent',
        metadata: {
          name,
          uid: `uid-${name}`,
          namespace: ns,
          creationTimestamp: daysAgo(12),
        },
        spec: { workerPoolRef: 'default' },
        status: { phase, restartCount: restarts },
      })
    da('slack-bot', 'agents', 'Running', 0)
    da('log-watcher', 'agents', 'CrashLoopBackOff', 7)

    const inv = (
      name: string,
      ns: string,
      phase: string,
      trigger: string,
      parent: string,
    ) =>
      this.putGVR(GROUP, VERSION, 'invocations', {
        apiVersion: `${GROUP}/${VERSION}`,
        kind: 'Invocation',
        metadata: {
          name,
          uid: `uid-${name}`,
          namespace: ns,
          creationTimestamp: hoursAgo(1),
        },
        spec: {
          trigger: { type: trigger },
          parentRef: { kind: 'TaskAgent', name: parent },
        },
        status: { phase },
      })
    inv(
      'code-reviewer-pr-4821',
      'agents',
      'Succeeded',
      'Webhook',
      'code-reviewer',
    )
    inv(
      'code-reviewer-pr-4822',
      'agents',
      'Running',
      'Webhook',
      'code-reviewer',
    )
    inv(
      'nightly-summarizer-2026',
      'agents',
      'Failed',
      'Schedule',
      'nightly-summarizer',
    )
  }

  // --- internals -------------------------------------------------------------

  private putGVR(
    group: string,
    version: string,
    resource: string,
    o: StoredObject,
  ): void {
    const key = colKey(group, version, resource)
    const m = this.store.get(key) ?? new Map<string, StoredObject>()
    m.set(o.metadata.name, this.stamp(o))
    this.store.set(key, m)
  }

  private stamp(o: StoredObject): StoredObject {
    return {
      ...o,
      metadata: { ...o.metadata, resourceVersion: String(++this.rv) },
    }
  }

  private listBody(key: string): Json {
    const items = [...(this.store.get(key)?.values() ?? [])]
    return {
      apiVersion: 'v1',
      kind: 'List',
      metadata: { resourceVersion: String(this.rv) },
      items,
    }
  }

  private getObject(key: string, name: string): Response {
    const o = this.store.get(key)?.get(name)
    return o
      ? json(o)
      : json(
          { kind: 'Status', status: 'Failure', code: 404, reason: 'NotFound' },
          404,
        )
  }

  private async applyObject(
    key: string,
    name: string,
    init?: RequestInit,
  ): Promise<Response> {
    const body = JSON.parse((init?.body as string) ?? '{}') as StoredObject
    const existing = this.store.get(key)?.get(name)
    const merged: StoredObject = {
      ...existing,
      ...body,
      metadata: {
        ...existing?.metadata,
        ...body.metadata,
        name,
        uid: existing?.metadata.uid ?? `uid-${name}`,
      },
    } as StoredObject
    const stamped = this.stamp(merged)
    const m = this.store.get(key) ?? new Map<string, StoredObject>()
    m.set(name, stamped)
    this.store.set(key, m)
    this.broadcast(key, {
      type: existing ? 'MODIFIED' : 'ADDED',
      object: stamped,
    })
    return json(stamped)
  }

  private deleteObject(key: string, name: string): Response {
    const o = this.store.get(key)?.get(name)
    if (o) {
      this.store.get(key)?.delete(name)
      this.broadcast(key, { type: 'DELETED', object: o })
    }
    return json({
      kind: 'Status',
      apiVersion: 'v1',
      status: 'Success',
      metadata: {},
    })
  }

  private openWatch(key: string, signal?: AbortSignal | null): Response {
    const set = this.watchers.get(key) ?? new Set<Controller>()
    this.watchers.set(key, set)
    let ctrl: Controller | undefined
    const stream = new ReadableStream<Uint8Array>({
      start: (controller) => {
        ctrl = controller
        set.add(controller)
        const onAbort = () => {
          set.delete(controller)
          try {
            controller.close()
          } catch {
            /* already closed */
          }
        }
        signal?.addEventListener('abort', onAbort, { once: true })
      },
      cancel: () => {
        if (ctrl) set.delete(ctrl)
      },
    })
    return new Response(stream, {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    })
  }

  private broadcast(key: string, ev: WatchEvent): void {
    const set = this.watchers.get(key)
    if (!set) return
    const line = enc.encode(JSON.stringify(ev) + '\n')
    for (const c of set) {
      try {
        c.enqueue(line)
      } catch {
        /* stream closed */
      }
    }
  }

  private discoveryDoc(): Json {
    const res = (names: string[]) => names.map((resource) => ({ resource }))
    return {
      apiVersion: 'apidiscovery.k8s.io/v2',
      kind: 'APIGroupDiscoveryList',
      items: [
        {
          metadata: { name: GROUP },
          versions: [
            {
              version: VERSION,
              resources: res([
                'taskagents',
                'daemonagents',
                'egressgateways',
                'invocations',
                'mcproutes',
                'aiproviderroutes',
                'egressl4routes',
                'workerpools',
                'credentialinjectionpolicies',
                'ratelimitpolicies',
                'loggingpolicies',
                'egressdenypolicies',
              ]),
            },
          ],
        },
        {
          metadata: { name: GWAPI },
          versions: [{ version: 'v1', resources: res(['httproutes']) }],
        },
      ],
    }
  }
}

const DAY = 86_400_000

function daysAgo(n: number): string {
  return new Date(Date.parse('2026-06-19T09:00:00Z') - n * DAY).toISOString()
}
function hoursAgo(n: number): string {
  return new Date(
    Date.parse('2026-06-19T09:00:00Z') - n * (DAY / 24),
  ).toISOString()
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}
function notFound(method: string, path: string): Response {
  return json(
    {
      kind: 'Status',
      status: 'Failure',
      code: 404,
      message: `mock: no route for ${method} ${path}`,
    },
    404,
  )
}

/** A standard `Programmed` condition for an EgressGateway / listener. */
function progCond(ok: boolean, message?: string): Record<string, unknown> {
  return ok
    ? {
        type: 'Programmed',
        status: 'True',
        reason: 'Programmed',
        message: 'gateway programmed',
        lastTransitionTime: daysAgo(0),
      }
    : {
        type: 'Programmed',
        status: 'False',
        reason: 'Invalid',
        message: message ?? 'not programmed',
        lastTransitionTime: daysAgo(0),
      }
}

// ── The demo, authored once in a rich shape and lowered to k8s objects above ──

interface DemoListener {
  name: string
  protocol: 'TCP' | 'TLS' | 'HTTP' | 'HTTPS' | 'UDP'
  port?: number
  tls?: 'Terminate' | 'Passthrough'
  ready?: boolean
}
interface DemoMatch {
  kind: 'provider' | 'tools' | 'path' | 'method' | 'sni' | 'cidr'
  value: string
  models?: string[]
  endpoints?: string[]
}
interface DemoRule {
  match: DemoMatch
  tokenBudget?: { maxTokensPerDay: number }
  toolPolicy?: {
    allowedTools?: string[]
    requireConfirmation?: string[]
    maxCallsPerExecution?: number
    rateLimit?: { requests: number; window: string }
  }
  backends?: Array<{ name: string; port: number; weight: number }>
}
interface DemoRoute {
  kind: 'AIProviderRoute' | 'MCPRoute' | 'EgressL4Route' | 'HTTPRoute'
  name: string
  listener: string
  hostnames?: string[]
  provider?: string
  rules: DemoRule[]
  cred?: {
    secret: string
    key?: string
    target: 'Header' | 'QueryParam' | 'ProviderAuth'
    header?: string
  }
  rateLimit?: { requests: number; window: string }
  logging?: {
    captureRequest?: boolean
    captureResponse?: boolean
    summary: string
  }
  deny?: { statusCode?: number }
}
interface DemoGateway {
  name: string
  namespace: string
  defaultPolicy: 'deny-all' | 'allow-all'
  rps: number
  ageDays: number
  description: string
  degraded?: string
  otlpEndpoint?: string
  listeners: DemoListener[]
  routes: DemoRoute[]
}

const OTLP = 'otelcol.observability.svc:4318'

const EGRESS_DEMO: DemoGateway[] = [
  {
    name: 'llm-egress',
    namespace: 'platform',
    defaultPolicy: 'deny-all',
    rps: 184,
    ageDays: 47,
    description: 'Outbound to AI providers · MITM + token budgets',
    otlpEndpoint: OTLP,
    listeners: [
      { name: 'https-mitm', protocol: 'HTTPS', port: 443, tls: 'Terminate' },
      {
        name: 'tls-passthrough',
        protocol: 'TLS',
        port: 443,
        tls: 'Passthrough',
      },
      { name: 'tcp-fallback', protocol: 'TCP' },
    ],
    routes: [
      {
        kind: 'AIProviderRoute',
        name: 'openai-chat',
        listener: 'https-mitm',
        provider: 'openai',
        hostnames: ['api.openai.com'],
        cred: {
          secret: 'openai-key',
          target: 'Header',
          header: 'Authorization',
        },
        rules: [
          {
            match: {
              kind: 'provider',
              value: 'openai',
              models: ['gpt-4o*', 'gpt-4-turbo'],
            },
            tokenBudget: { maxTokensPerDay: 20_000_000 },
          },
          {
            match: {
              kind: 'provider',
              value: 'openai',
              endpoints: ['/v1/embeddings'],
            },
            tokenBudget: { maxTokensPerDay: 200_000_000 },
          },
        ],
      },
      {
        kind: 'AIProviderRoute',
        name: 'anthropic',
        listener: 'https-mitm',
        provider: 'anthropic',
        hostnames: ['api.anthropic.com'],
        cred: {
          secret: 'anthropic-key',
          target: 'Header',
          header: 'x-api-key',
        },
        rules: [
          {
            match: {
              kind: 'provider',
              value: 'anthropic',
              models: ['claude-3-*', 'claude-sonnet-4*'],
            },
            tokenBudget: { maxTokensPerDay: 40_000_000 },
          },
        ],
      },
      {
        kind: 'AIProviderRoute',
        name: 'gemini',
        listener: 'https-mitm',
        provider: 'google',
        hostnames: ['generativelanguage.googleapis.com'],
        cred: { secret: 'vertex-sa', target: 'ProviderAuth' },
        rules: [
          {
            match: {
              kind: 'provider',
              value: 'google',
              models: ['gemini-2.0-flash*'],
            },
            tokenBudget: { maxTokensPerDay: 10_000_000 },
          },
        ],
      },
      {
        kind: 'HTTPRoute',
        name: 'openai-mirror',
        listener: 'https-mitm',
        hostnames: ['api.openai.com'],
        rules: [
          {
            match: { kind: 'path', value: '/v1/chat/completions' },
            backends: [
              { name: 'api.openai.com', port: 443, weight: 90 },
              { name: 'azure-openai.svc', port: 443, weight: 10 },
            ],
          },
        ],
      },
      {
        kind: 'EgressL4Route',
        name: 'huggingface-pt',
        listener: 'tls-passthrough',
        hostnames: ['*.huggingface.co'],
        logging: { summary: 'summary-only' },
        rules: [{ match: { kind: 'sni', value: '*.huggingface.co' } }],
      },
      {
        kind: 'EgressL4Route',
        name: 'fallback-deny',
        listener: 'tcp-fallback',
        hostnames: ['*'],
        deny: {},
        rules: [{ match: { kind: 'cidr', value: '0.0.0.0/0' } }],
      },
    ],
  },
  {
    name: 'github-egress',
    namespace: 'platform',
    defaultPolicy: 'deny-all',
    rps: 41,
    ageDays: 47,
    description: 'GitHub + MCP server access for review agents',
    otlpEndpoint: OTLP,
    listeners: [
      { name: 'mcp-https', protocol: 'HTTPS', port: 443, tls: 'Terminate' },
      { name: 'api-https', protocol: 'HTTPS', port: 443, tls: 'Terminate' },
    ],
    routes: [
      {
        kind: 'MCPRoute',
        name: 'github-mcp',
        listener: 'mcp-https',
        hostnames: ['mcp.github.com'],
        rules: [
          {
            match: { kind: 'tools', value: 'read_*, list_*, get_*' },
            toolPolicy: { allowedTools: ['read_*', 'list_*', 'get_*'] },
          },
          {
            match: {
              kind: 'tools',
              value: 'create_issue, comment_pr, merge_pr',
            },
            toolPolicy: {
              requireConfirmation: ['merge_pr'],
              rateLimit: { requests: 120, window: '1m' },
            },
          },
        ],
      },
      {
        kind: 'MCPRoute',
        name: 'context7-mcp',
        listener: 'mcp-https',
        hostnames: ['mcp.context7.com'],
        rules: [
          {
            match: { kind: 'tools', value: 'search_*, fetch_*' },
            toolPolicy: { maxCallsPerExecution: 64 },
          },
        ],
      },
      {
        kind: 'HTTPRoute',
        name: 'github-api',
        listener: 'api-https',
        hostnames: ['api.github.com'],
        cred: {
          secret: 'github-token',
          target: 'Header',
          header: 'Authorization',
        },
        rateLimit: { requests: 60, window: '1m' },
        rules: [
          { match: { kind: 'method', value: 'GET' } },
          { match: { kind: 'method', value: 'POST, PATCH, DELETE' } },
        ],
      },
    ],
  },
  {
    name: 'web-egress',
    namespace: 'research',
    defaultPolicy: 'allow-all',
    rps: 28,
    ageDays: 14,
    description: 'Open web crawling for research agents · allow-all',
    otlpEndpoint: OTLP,
    listeners: [{ name: 'any-tcp', protocol: 'TCP' }],
    routes: [
      {
        kind: 'EgressL4Route',
        name: 'any-out',
        listener: 'any-tcp',
        hostnames: ['*'],
        logging: { captureRequest: true, summary: 'headers + status only' },
        rules: [{ match: { kind: 'cidr', value: '0.0.0.0/0' } }],
      },
    ],
  },
  {
    name: 'stripe-egress',
    namespace: 'shop',
    defaultPolicy: 'deny-all',
    rps: 0,
    ageDays: 8,
    description: 'Stripe API for checkout-co-pilot · MITM disabled',
    degraded: 'CA secret stripe-ca not found · listener https-mitm down',
    otlpEndpoint: OTLP,
    listeners: [
      {
        name: 'stripe-https',
        protocol: 'HTTPS',
        port: 443,
        tls: 'Terminate',
        ready: false,
      },
    ],
    routes: [
      {
        kind: 'HTTPRoute',
        name: 'stripe-api',
        listener: 'stripe-https',
        hostnames: ['api.stripe.com'],
        cred: {
          secret: 'stripe-key',
          target: 'Header',
          header: 'Authorization',
        },
        rules: [{ match: { kind: 'path', value: '/v1/*' } }],
      },
    ],
  },
]
