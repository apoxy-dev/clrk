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

type WatchEvent = { type: 'ADDED' | 'MODIFIED' | 'DELETED' | 'BOOKMARK'; object: unknown }
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
  fetch = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const url = new URL(typeof input === 'string' ? input : input instanceof URL ? input.href : input.url)
    const method = (init?.method ?? 'GET').toUpperCase()
    const segs = url.pathname.split('/').filter(Boolean)

    // Aggregated discovery v2: GET /apis and GET /api both 200.
    if (method === 'GET' && url.pathname === '/apis') return json(this.discoveryDoc())
    if (method === 'GET' && url.pathname === '/api') return json({ items: [] })

    // SelfSubjectAccessReview — allow everything in the mock.
    if (method === 'POST' && url.pathname.endsWith('/selfsubjectaccessreviews')) {
      return json({ apiVersion: 'authorization.k8s.io/v1', kind: 'SelfSubjectAccessReview', status: { allowed: true } })
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
        if (method === 'GET' && url.searchParams.get('watch') === '1') return this.openWatch(key, init?.signal)
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

  // --- Egress Gateways + attaching routes (the design's data-egress.js) -------

  private seedEgress(): void {
    // gw(name, ns, policy, [listener: [name, protocol, port, attachedRoutes]],
    //    rps, ageDays, description, degradedReason?)
    const gw = (
      name: string,
      ns: string,
      policy: 'deny-all' | 'allow-all',
      listeners: Array<[string, string, number | null, number]>,
      rps: number,
      ageDays: number,
      description: string,
      degraded?: string,
    ) => {
      this.putGVR(GROUP, VERSION, 'egressgateways', {
        apiVersion: `${GROUP}/${VERSION}`,
        kind: 'EgressGateway',
        metadata: {
          name,
          uid: `uid-${name}`,
          namespace: ns,
          creationTimestamp: daysAgo(ageDays),
          annotations: { 'clrk.apoxy.dev/demo-rps': String(rps), 'clrk.apoxy.dev/description': description },
        },
        spec: {
          defaultPolicy: policy,
          listeners: listeners.map(([ln, protocol, port]) => ({ name: ln, protocol, ...(port ? { port } : {}) })),
        },
        status: {
          listenerCount: listeners.length,
          listeners: listeners.map(([ln, , , attached], i) => ({ name: ln, attachedRoutes: attached, backendAddress: `:1500${i}` })),
          conditions: degraded
            ? [{ type: 'Programmed', status: 'False', reason: 'Invalid', message: degraded }]
            : [{ type: 'Programmed', status: 'True', reason: 'Programmed', message: 'gateway programmed' }],
        },
      })
    }

    // route(kind, resource, group, version, name, ns, parentGateway, listener, hostnames)
    const route = (
      kind: string,
      resource: string,
      group: string,
      version: string,
      name: string,
      ns: string,
      parent: string,
      section: string,
      hostnames: string[],
    ) => {
      this.putGVR(group, version, resource, {
        apiVersion: `${group}/${version}`,
        kind,
        metadata: { name, uid: `uid-${name}`, namespace: ns, creationTimestamp: daysAgo(40) },
        spec: { parentRefs: [{ name: parent, sectionName: section }], hostnames },
      })
    }
    const ai = (n: string, ns: string, p: string, s: string, h: string[]) =>
      route('AIProviderRoute', 'aiproviderroutes', GROUP, VERSION, n, ns, p, s, h)
    const mcp = (n: string, ns: string, p: string, s: string, h: string[]) =>
      route('MCPRoute', 'mcproutes', GROUP, VERSION, n, ns, p, s, h)
    const l4 = (n: string, ns: string, p: string, s: string, h: string[]) =>
      route('EgressL4Route', 'egressl4routes', GROUP, VERSION, n, ns, p, s, h)
    const http = (n: string, ns: string, p: string, s: string, h: string[]) =>
      route('HTTPRoute', 'httproutes', GWAPI, 'v1', n, ns, p, s, h)

    gw('llm-egress', 'platform', 'deny-all', [
      ['https-mitm', 'HTTPS', 443, 4],
      ['tls-passthrough', 'TLS', 443, 1],
      ['tcp-fallback', 'TCP', null, 1],
    ], 184, 47, 'Outbound to AI providers · MITM + token budgets')
    ai('openai-chat', 'platform', 'llm-egress', 'https-mitm', ['api.openai.com'])
    ai('anthropic', 'platform', 'llm-egress', 'https-mitm', ['api.anthropic.com'])
    ai('gemini', 'platform', 'llm-egress', 'https-mitm', ['generativelanguage.googleapis.com'])
    http('openai-mirror', 'platform', 'llm-egress', 'https-mitm', ['api.openai.com'])
    l4('huggingface-pt', 'platform', 'llm-egress', 'tls-passthrough', ['*.huggingface.co'])
    l4('fallback-deny', 'platform', 'llm-egress', 'tcp-fallback', ['*'])

    gw('github-egress', 'platform', 'deny-all', [
      ['mcp-https', 'HTTPS', 443, 2],
      ['api-https', 'HTTPS', 443, 1],
    ], 41, 47, 'GitHub + MCP server access for review agents')
    mcp('github-mcp', 'platform', 'github-egress', 'mcp-https', ['mcp.github.com'])
    mcp('context7-mcp', 'platform', 'github-egress', 'mcp-https', ['mcp.context7.com'])
    http('github-api', 'platform', 'github-egress', 'api-https', ['api.github.com'])

    gw('web-egress', 'research', 'allow-all', [['any-tcp', 'TCP', null, 1]], 28, 14, 'Open web crawling for research agents · allow-all')
    l4('any-out', 'research', 'web-egress', 'any-tcp', ['*'])

    gw(
      'stripe-egress',
      'shop',
      'deny-all',
      [['stripe-https', 'HTTPS', 443, 1]],
      0,
      8,
      'Stripe API for checkout-co-pilot · MITM disabled',
      'CA secret stripe-ca not found · listener https-mitm down',
    )
    http('stripe-api', 'shop', 'stripe-egress', 'stripe-https', ['api.stripe.com'])
  }

  // --- Agents + Invocations (so the Run / Observe lists populate too) ---------

  private seedAgents(): void {
    const ta = (name: string, ns: string, active: number, revision: string) =>
      this.putGVR(GROUP, VERSION, 'taskagents', {
        apiVersion: `${GROUP}/${VERSION}`,
        kind: 'TaskAgent',
        metadata: { name, uid: `uid-${name}`, namespace: ns, creationTimestamp: daysAgo(20) },
        spec: { workerPoolRef: 'default' },
        status: { activeExecutions: active, latestReadyRevisionName: revision, conditions: [{ type: 'Ready', status: 'True' }] },
      })
    ta('code-reviewer', 'agents', 2, 'code-reviewer-00007')
    ta('nightly-summarizer', 'agents', 0, 'nightly-summarizer-00003')

    const da = (name: string, ns: string, phase: string, restarts: number) =>
      this.putGVR(GROUP, VERSION, 'daemonagents', {
        apiVersion: `${GROUP}/${VERSION}`,
        kind: 'DaemonAgent',
        metadata: { name, uid: `uid-${name}`, namespace: ns, creationTimestamp: daysAgo(12) },
        spec: { workerPoolRef: 'default' },
        status: { phase, restartCount: restarts },
      })
    da('slack-bot', 'agents', 'Running', 0)
    da('log-watcher', 'agents', 'CrashLoopBackOff', 7)

    const inv = (name: string, ns: string, phase: string, trigger: string, parent: string) =>
      this.putGVR(GROUP, VERSION, 'invocations', {
        apiVersion: `${GROUP}/${VERSION}`,
        kind: 'Invocation',
        metadata: { name, uid: `uid-${name}`, namespace: ns, creationTimestamp: hoursAgo(1) },
        spec: { trigger: { type: trigger }, parentRef: { kind: 'TaskAgent', name: parent } },
        status: { phase },
      })
    inv('code-reviewer-pr-4821', 'agents', 'Succeeded', 'Webhook', 'code-reviewer')
    inv('code-reviewer-pr-4822', 'agents', 'Running', 'Webhook', 'code-reviewer')
    inv('nightly-summarizer-2026', 'agents', 'Failed', 'Schedule', 'nightly-summarizer')
  }

  // --- internals -------------------------------------------------------------

  private putGVR(group: string, version: string, resource: string, o: StoredObject): void {
    const key = colKey(group, version, resource)
    const m = this.store.get(key) ?? new Map<string, StoredObject>()
    m.set(o.metadata.name, this.stamp(o))
    this.store.set(key, m)
  }

  private stamp(o: StoredObject): StoredObject {
    return { ...o, metadata: { ...o.metadata, resourceVersion: String(++this.rv) } }
  }

  private listBody(key: string): Json {
    const items = [...(this.store.get(key)?.values() ?? [])]
    return { apiVersion: 'v1', kind: 'List', metadata: { resourceVersion: String(this.rv) }, items }
  }

  private getObject(key: string, name: string): Response {
    const o = this.store.get(key)?.get(name)
    return o ? json(o) : json({ kind: 'Status', status: 'Failure', code: 404, reason: 'NotFound' }, 404)
  }

  private async applyObject(key: string, name: string, init?: RequestInit): Promise<Response> {
    const body = JSON.parse((init?.body as string) ?? '{}') as StoredObject
    const existing = this.store.get(key)?.get(name)
    const merged: StoredObject = {
      ...existing,
      ...body,
      metadata: { ...existing?.metadata, ...body.metadata, name, uid: existing?.metadata.uid ?? `uid-${name}` },
    } as StoredObject
    const stamped = this.stamp(merged)
    const m = this.store.get(key) ?? new Map<string, StoredObject>()
    m.set(name, stamped)
    this.store.set(key, m)
    this.broadcast(key, { type: existing ? 'MODIFIED' : 'ADDED', object: stamped })
    return json(stamped)
  }

  private deleteObject(key: string, name: string): Response {
    const o = this.store.get(key)?.get(name)
    if (o) {
      this.store.get(key)?.delete(name)
      this.broadcast(key, { type: 'DELETED', object: o })
    }
    return json({ kind: 'Status', apiVersion: 'v1', status: 'Success', metadata: {} })
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
    return new Response(stream, { status: 200, headers: { 'Content-Type': 'application/json' } })
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
              ]),
            },
          ],
        },
        { metadata: { name: GWAPI }, versions: [{ version: 'v1', resources: res(['httproutes']) }] },
      ],
    }
  }
}

const DAY = 86_400_000

function daysAgo(n: number): string {
  return new Date(Date.parse('2026-06-19T09:00:00Z') - n * DAY).toISOString()
}
function hoursAgo(n: number): string {
  return new Date(Date.parse('2026-06-19T09:00:00Z') - n * (DAY / 24)).toISOString()
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } })
}
function notFound(method: string, path: string): Response {
  return json({ kind: 'Status', status: 'Failure', code: 404, message: `mock: no route for ${method} ${path}` }, 404)
}
