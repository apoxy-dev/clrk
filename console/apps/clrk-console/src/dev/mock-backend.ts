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

type Json = Record<string, unknown>;

interface StoredObject {
  apiVersion: string;
  kind: string;
  metadata: {
    name: string;
    uid: string;
    namespace?: string;
    resourceVersion?: string;
    creationTimestamp?: string;
    annotations?: Record<string, string>;
  };
  spec?: Json;
  status?: Record<string, unknown>;
}

type WatchEvent = {
  type: "ADDED" | "MODIFIED" | "DELETED" | "BOOKMARK";
  object: unknown;
};
type Controller = ReadableStreamDefaultController<Uint8Array>;

const GROUP = "clrk.apoxy.dev";
const VERSION = "v1alpha1";
const GWAPI = "gateway.networking.k8s.io";
const METRICS_GROUP = "metrics.clrk.apoxy.dev";
const enc = new TextEncoder();

function colKey(group: string, version: string, resource: string): string {
  return `${group}/${version}/${resource}`;
}

export class MockBackend {
  private rv = 0;
  private readonly store = new Map<string, Map<string, StoredObject>>();
  private readonly watchers = new Map<string, Set<Controller>>();

  /** The injectable `fetch` (bound) handed to createConsoleClient. */
  fetch = async (
    input: RequestInfo | URL,
    init?: RequestInit,
  ): Promise<Response> => {
    const url = new URL(
      typeof input === "string"
        ? input
        : input instanceof URL
          ? input.href
          : input.url,
    );
    const method = (init?.method ?? "GET").toUpperCase();
    const segs = url.pathname.split("/").filter(Boolean);

    // Aggregated discovery v2: GET /apis and GET /api both 200.
    if (method === "GET" && url.pathname === "/apis")
      return json(this.discoveryDoc());
    if (method === "GET" && url.pathname === "/api") return json({ items: [] });

    // SelfSubjectAccessReview — allow everything in the mock.
    if (
      method === "POST" &&
      url.pathname.endsWith("/selfsubjectaccessreviews")
    ) {
      return json({
        apiVersion: "authorization.k8s.io/v1",
        kind: "SelfSubjectAccessReview",
        status: { allowed: true },
      });
    }

    // /apis/<group>/<version>/[namespaces/<ns>/]<resource>[/<name>]
    if (segs[0] === "apis" && segs.length >= 4) {
      const [, group, version] = segs;
      let rest = segs.slice(3);
      if (rest[0] === "namespaces") rest = rest.slice(2); // collapse the namespace scope
      const [resource, name, sub] = rest;
      if (!resource) return notFound(method, url.pathname);
      const key = colKey(group!, version!, resource);
      // Telemetry subresource: <taskagents|daemonagents>/<name>/traces → seeded
      // OTLP TracesData, the read path behind the swimlane / daemon activity.
      if (method === "GET" && name && sub === "traces") {
        return json(this.tracesFor(resource!, name));
      }
      // Tier-2 metric series subresource: metrics/<id>/series → a synthesized
      // MetricSeriesSet, the read path behind the Overview's KPI shapes + traffic
      // chart (the namespace scope is collapsed above, so it's ignored here).
      if (
        method === "GET" &&
        resource === "metrics" &&
        name &&
        sub === "series"
      ) {
        return json(metricSeriesFor(name, url.searchParams));
      }
      if (!name) {
        if (method === "GET" && url.searchParams.get("watch") === "1")
          return this.openWatch(key, init?.signal);
        if (method === "GET") return json(this.listBody(key));
      } else {
        if (method === "GET") return this.getObject(key, name);
        if (method === "PATCH") return this.applyObject(key, name, init);
        if (method === "DELETE") return this.deleteObject(key, name);
      }
    }

    return notFound(method, url.pathname);
  };

  /** Seed a realistic starter set across the kinds the app surfaces. */
  seedDemo(): void {
    this.seedEgress();
    this.seedAgents();
    this.seedNotifications();
  }

  // --- Notification Center (events.k8s.io/v1 Events + signed-up CLRKConfig) ----
  //
  // The Notification Center reads real Events, gated behind an email signup in
  // the CLRKConfig singleton. Seed a signed-up config (so the gate opens and the
  // security indicator reads "active") plus a spread of Events across the frozen
  // reason vocabulary -- Warning/Normal, aggregated counts, full regarding refs,
  // reporting controllers -- so the day-grouped list, the bell badge, and the
  // detail tray (occurrences, involved object, reporting, related events, raw
  // YAML) all render against the read path rather than a fixture. Two Events
  // share the review-bot TaskAgent so the tray's "Related events" list is real.
  private seedNotifications(): void {
    this.putGVR(GROUP, VERSION, "clrkconfigs", {
      apiVersion: `${GROUP}/${VERSION}`,
      kind: "CLRKConfig",
      metadata: { name: "default", namespace: "clrk", uid: "clrkconfig-default" },
      spec: { notifications: { email: "sre@acme.co", advisoryPollEnabled: true } },
      status: {
        notifications: {
          deploymentID: "dep-7f3a1c9e",
          registeredAt: "2026-07-06T12:00:00Z",
          conditions: [
            {
              type: "Registered",
              status: "True",
              reason: "Registered",
              message: "Phoned home to api.apoxy.dev",
              lastTransitionTime: "2026-07-06T12:00:00Z",
            },
          ],
        },
      },
    } as unknown as StoredObject);

    const now = Date.now();
    const iso = (msAgo: number) => new Date(now - msAgo).toISOString();
    const MIN = 60_000;
    const HOUR = 60 * MIN;
    const CLRK = `${GROUP}/${VERSION}`;
    const AGENTS = "agents";

    interface Seed {
      name: string;
      reason: string;
      type: "Warning" | "Normal";
      note: string;
      count: number;
      firstAgo: number;
      lastAgo: number;
      regarding: {
        apiVersion: string;
        kind: string;
        name: string;
        namespace: string;
        uid: string;
        resourceVersion: string;
        fieldPath?: string;
      };
      reportingController: string;
      reportingInstance: string;
      source: { component: string; host?: string };
    }

    const seeds: Seed[] = [
      {
        name: "review-bot.evt-invfail",
        reason: "InvocationFailed",
        type: "Warning",
        note: "Invocation `inv-9f2c1d` failed: upstream `anthropic-egress` returned 529 overloaded after 3 retries.",
        count: 11,
        firstAgo: 42 * MIN,
        lastAgo: 3 * MIN,
        regarding: {
          apiVersion: CLRK,
          kind: "TaskAgent",
          name: "review-bot",
          namespace: AGENTS,
          uid: "ta-review-bot",
          resourceVersion: "48213412",
          fieldPath: "status.lastInvocation",
        },
        reportingController: "clrk-invocation-controller",
        reportingInstance: "clrk-controller-manager-0",
        source: { component: "clrk-invocation-controller" },
      },
      {
        name: "review-bot.evt-invtimeout",
        reason: "InvocationTimeout",
        type: "Warning",
        note: "Invocation `inv-8b1a0c` exceeded the 120s deadline and was cancelled.",
        count: 4,
        firstAgo: 55 * MIN,
        lastAgo: 12 * MIN,
        regarding: {
          apiVersion: CLRK,
          kind: "TaskAgent",
          name: "review-bot",
          namespace: AGENTS,
          uid: "ta-review-bot",
          resourceVersion: "48211980",
          fieldPath: "status.lastInvocation",
        },
        reportingController: "clrk-invocation-controller",
        reportingInstance: "clrk-controller-manager-0",
        source: { component: "clrk-invocation-controller" },
      },
      {
        name: "llm-egress.evt-denied",
        reason: "EgressDenied",
        type: "Warning",
        note: "Blocked egress to `api.openai.com:443` from `incident-triager` — no matching AIProviderRoute under deny-all default.",
        count: 27,
        firstAgo: 2 * HOUR,
        lastAgo: 6 * MIN,
        regarding: {
          apiVersion: CLRK,
          kind: "EgressGateway",
          name: "llm-egress",
          namespace: AGENTS,
          uid: "eg-llm-egress",
          resourceVersion: "48191002",
          fieldPath: "spec.defaultPolicy",
        },
        reportingController: "clrk-egress-controller",
        reportingInstance: "clrk-controller-manager-0",
        source: { component: "clrk-egress-controller", host: "gpu-a10g-node-3" },
      },
      {
        name: "llm-egress.evt-advisory",
        reason: "SecurityAdvisory",
        type: "Warning",
        note: "Advisory `APOXY-2026-014`: credential-injection upstream `api.anthropic.com` should pin SANs. Review additionalTrustBundle.",
        count: 1,
        firstAgo: 3 * HOUR,
        lastAgo: 3 * HOUR,
        regarding: {
          apiVersion: CLRK,
          kind: "EgressGateway",
          name: "llm-egress",
          namespace: AGENTS,
          uid: "eg-llm-egress",
          resourceVersion: "48170554",
        },
        reportingController: "clrk-sentry",
        reportingInstance: "clrk-controller-manager-0",
        source: { component: "clrk-sentry" },
      },
      {
        name: "gpu-a10g.evt-degraded",
        reason: "WorkerPoolDegraded",
        type: "Warning",
        note: "Worker pool `gpu-a10g` degraded: 2/5 replicas NotReady — `nvidia.com/gpu` allocatable dropped on node `gpu-a10g-node-1`.",
        count: 6,
        firstAgo: 30 * MIN,
        lastAgo: 8 * MIN,
        regarding: {
          apiVersion: CLRK,
          kind: "WorkerPool",
          name: "gpu-a10g",
          namespace: AGENTS,
          uid: "wp-gpu-a10g",
          resourceVersion: "48210077",
          fieldPath: "status.readyReplicas",
        },
        reportingController: "clrk-workerpool-controller",
        reportingInstance: "clrk-controller-manager-0",
        source: { component: "clrk-workerpool-controller" },
      },
      {
        name: "gpu-a10g.evt-healthy",
        reason: "WorkerPoolHealthy",
        type: "Normal",
        note: "Worker pool `gpu-a10g` recovered: 5/5 replicas Ready.",
        count: 1,
        firstAgo: 20 * HOUR,
        lastAgo: 20 * HOUR,
        regarding: {
          apiVersion: CLRK,
          kind: "WorkerPool",
          name: "gpu-a10g",
          namespace: AGENTS,
          uid: "wp-gpu-a10g",
          resourceVersion: "48090441",
        },
        reportingController: "clrk-workerpool-controller",
        reportingInstance: "clrk-controller-manager-0",
        source: { component: "clrk-workerpool-controller" },
      },
      {
        name: "incident-triager.evt-revready",
        reason: "RevisionReady",
        type: "Normal",
        note: "Revision `incident-triager-7c9b4d5` is Ready — image digest verified, sandbox spec frozen.",
        count: 1,
        firstAgo: 26 * HOUR,
        lastAgo: 26 * HOUR,
        regarding: {
          apiVersion: CLRK,
          kind: "AgentSandboxRevision",
          name: "incident-triager-7c9b4d5",
          namespace: AGENTS,
          uid: "asr-incident-triager",
          resourceVersion: "48012004",
        },
        reportingController: "clrk-revision-controller",
        reportingInstance: "clrk-controller-manager-0",
        source: { component: "clrk-revision-controller" },
      },
    ];

    seeds.forEach((s, i) => {
      this.putGVR("events.k8s.io", "v1", "events", {
        apiVersion: "events.k8s.io/v1",
        kind: "Event",
        metadata: {
          name: s.name,
          namespace: s.regarding.namespace,
          uid: `evt-uid-${i}`,
          resourceVersion: String(48213990 + i),
          creationTimestamp: iso(s.firstAgo),
        },
        eventTime: iso(s.firstAgo),
        reason: s.reason,
        note: s.note,
        type: s.type,
        action: "",
        regarding: s.regarding,
        series:
          s.count > 1
            ? { count: s.count, lastObservedTime: iso(s.lastAgo) }
            : undefined,
        reportingController: s.reportingController,
        reportingInstance: s.reportingInstance,
        deprecatedSource: s.source,
      } as unknown as StoredObject);
    });
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
    for (const g of EGRESS_DEMO) this.lowerGateway(g);
  }

  private lowerGateway(g: DemoGateway): void {
    const ok = !g.degraded;
    this.putGVR(GROUP, VERSION, "egressgateways", {
      apiVersion: `${GROUP}/${VERSION}`,
      kind: "EgressGateway",
      metadata: {
        name: g.name,
        uid: `uid-${g.name}`,
        namespace: g.namespace,
        creationTimestamp: daysAgo(g.ageDays),
        annotations: {
          "clrk.apoxy.dev/demo-rps": String(g.rps),
          "clrk.apoxy.dev/description": g.description,
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
    });

    for (const r of g.routes) this.lowerRoute(g, r);
  }

  private lowerRoute(g: DemoGateway, r: DemoRoute): void {
    const parentRefs = [{ name: g.name, sectionName: r.listener }];
    const base = {
      metadata: {
        name: r.name,
        uid: `uid-${r.name}`,
        namespace: g.namespace,
        creationTimestamp: daysAgo(g.ageDays - 1),
      },
    };
    const backendRefs = (rule: DemoRule) =>
      (rule.backends ?? []).map((b) => ({
        name: b.name,
        port: b.port,
        weight: b.weight,
      }));

    if (r.kind === "AIProviderRoute") {
      this.putGVR(GROUP, VERSION, "aiproviderroutes", {
        ...base,
        apiVersion: `${GROUP}/${VERSION}`,
        kind: "AIProviderRoute",
        spec: {
          parentRefs,
          rules: r.rules.map((rule) => ({
            matches: [
              {
                provider: r.provider ?? "custom",
                ...(rule.match.models ? { models: rule.match.models } : {}),
                ...(rule.match.endpoints
                  ? { endpoints: rule.match.endpoints }
                  : {}),
              },
            ],
            filters: rule.tokenBudget
              ? [
                  {
                    type: "TokenBudget",
                    tokenBudget: {
                      maxTokensPerDay: rule.tokenBudget.maxTokensPerDay,
                    },
                  },
                ]
              : [],
            backendRefs: backendRefs(rule),
          })),
        },
      });
    } else if (r.kind === "MCPRoute") {
      this.putGVR(GROUP, VERSION, "mcproutes", {
        ...base,
        apiVersion: `${GROUP}/${VERSION}`,
        kind: "MCPRoute",
        spec: {
          parentRefs,
          hostnames: r.hostnames,
          rules: r.rules.map((rule) => ({
            matches: [
              { tools: rule.match.value.split(",").map((s) => s.trim()) },
            ],
            filters: rule.toolPolicy
              ? [
                  {
                    type: "ToolPolicy",
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
      });
    } else if (r.kind === "EgressL4Route") {
      this.putGVR(GROUP, VERSION, "egressl4routes", {
        ...base,
        apiVersion: `${GROUP}/${VERSION}`,
        kind: "EgressL4Route",
        spec: {
          parentRefs,
          rules: r.rules.map((rule) => ({
            matches: [
              rule.match.kind === "cidr"
                ? { destinationCIDRs: [rule.match.value] }
                : { destinationHostnames: [rule.match.value] },
            ],
            filters: [],
            backendRefs: backendRefs(rule),
          })),
        },
      });
    } else {
      this.putGVR(GWAPI, "v1", "httproutes", {
        ...base,
        apiVersion: `${GWAPI}/v1`,
        kind: "HTTPRoute",
        spec: {
          parentRefs,
          hostnames: r.hostnames,
          rules: r.rules.map((rule) => ({
            matches:
              rule.match.kind === "method"
                ? rule.match.value.split(",").map((m) => ({ method: m.trim() }))
                : [{ path: { type: "PathPrefix", value: rule.match.value } }],
            backendRefs: backendRefs(rule),
          })),
        },
      });
    }

    // Lower route-level attached policies into their own CRDs.
    if (r.cred) {
      this.putGVR(GROUP, VERSION, "credentialinjectionpolicies", {
        apiVersion: `${GROUP}/${VERSION}`,
        kind: "CredentialInjectionPolicy",
        metadata: {
          name: `${r.name}-cred`,
          uid: `uid-${r.name}-cred`,
          namespace: g.namespace,
          creationTimestamp: daysAgo(g.ageDays - 1),
        },
        spec: {
          targetRefs: [{ group: GROUP, kind: r.kind, name: r.name }],
          secretRef: { name: r.cred.secret },
          secretKey: r.cred.key ?? "token",
          target: r.cred.target,
          ...(r.cred.header ? { headerName: r.cred.header } : {}),
        },
      });
    }
    if (r.rateLimit) {
      this.putGVR(GROUP, VERSION, "ratelimitpolicies", {
        apiVersion: `${GROUP}/${VERSION}`,
        kind: "RateLimitPolicy",
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
      });
    }
    if (r.logging) {
      this.putGVR(GROUP, VERSION, "loggingpolicies", {
        apiVersion: `${GROUP}/${VERSION}`,
        kind: "LoggingPolicy",
        metadata: {
          name: `${r.name}-log`,
          uid: `uid-${r.name}-log`,
          namespace: g.namespace,
          creationTimestamp: daysAgo(g.ageDays - 1),
          annotations: { "clrk.apoxy.dev/summary": r.logging.summary },
        },
        spec: {
          parentRefs: [{ name: r.name }],
          captureRequest: r.logging.captureRequest ?? false,
          captureResponse: r.logging.captureResponse ?? false,
        },
      });
    }
    if (r.deny) {
      this.putGVR(GROUP, VERSION, "egressdenypolicies", {
        apiVersion: `${GROUP}/${VERSION}`,
        kind: "EgressDenyPolicy",
        metadata: {
          name: `${r.name}-deny`,
          uid: `uid-${r.name}-deny`,
          namespace: g.namespace,
          creationTimestamp: daysAgo(g.ageDays - 1),
        },
        spec: {
          targetRefs: [{ group: GROUP, kind: r.kind, name: r.name }],
          denyResponse: { statusCode: r.deny.statusCode ?? 403 },
        },
      });
    }
  }

  // --- Agents + Invocations (so the Run / Observe lists populate too) ---------

  private seedAgents(): void {
    const annotations = (description: string) => ({
      "clrk.apoxy.dev/description": description,
    });
    const ta = (
      name: string,
      ns: string,
      active: number,
      revision: string,
      image: string,
      opts: { schedule?: string; egress?: string[]; description: string },
    ) =>
      this.putGVR(GROUP, VERSION, "taskagents", {
        apiVersion: `${GROUP}/${VERSION}`,
        kind: "TaskAgent",
        metadata: {
          name,
          uid: `uid-${name}`,
          namespace: ns,
          creationTimestamp: daysAgo(20),
          annotations: annotations(opts.description),
        },
        spec: {
          workerPoolRef: "default",
          template: { spec: { image } },
          egressRefs: (opts.egress ?? []).map((g) => ({ gatewayRef: g })),
          ...(opts.schedule ? { schedule: opts.schedule } : {}),
        },
        status: {
          activeExecutions: active,
          latestReadyRevisionName: revision,
          conditions: [{ type: "Ready", status: "True" }],
        },
      });
    ta(
      "code-reviewer",
      "platform",
      3,
      "code-reviewer-00007",
      "ghcr.io/acme/review-bot:c41a7e",
      {
        egress: ["llm-egress"],
        description: "GitHub PR review · Claude + repo tools",
      },
    );
    ta(
      "nightly-summarizer",
      "platform",
      0,
      "nightly-summarizer-00003",
      "ghcr.io/acme/summarizer:2e7b0c",
      {
        schedule: "0 2 * * *",
        egress: ["llm-egress"],
        description: "Cron — nightly digest to Slack",
      },
    );

    const da = (
      name: string,
      ns: string,
      phase: string,
      restarts: number,
      image: string,
      upDays: number,
      revision: string,
      opts: { egress?: string[]; description: string },
    ) =>
      this.putGVR(GROUP, VERSION, "daemonagents", {
        apiVersion: `${GROUP}/${VERSION}`,
        kind: "DaemonAgent",
        metadata: {
          name,
          uid: `uid-${name}`,
          namespace: ns,
          creationTimestamp: daysAgo(upDays),
          annotations: annotations(opts.description),
        },
        spec: {
          workerPoolRef: "default",
          template: { spec: { image } },
          egressRefs: (opts.egress ?? []).map((g) => ({ gatewayRef: g })),
        },
        status: {
          phase,
          restartCount: restarts,
          upSince: daysAgo(upDays),
          latestReadyRevisionName: revision,
        },
      });
    da(
      "slack-bot",
      "platform",
      "Running",
      0,
      "ghcr.io/acme/slack-bot:b88a2d",
      14,
      "slack-bot-00008",
      {
        egress: ["llm-egress"],
        description: "Long-lived Slack listener · outbound only",
      },
    );
    da(
      "log-watcher",
      "research",
      "CrashLoopBackOff",
      7,
      "ghcr.io/acme/watcher:0aac11",
      3,
      "log-watcher-00021",
      {
        description: "Tails app logs · stuck on a bad image tag",
      },
    );

    this.seedWorkerPools();
    this.seedAgentMetrics();
    this.seedAgentRevisions();

    const inv = (
      name: string,
      ns: string,
      phase: string,
      trigger: string,
      parent: string,
    ) =>
      this.putGVR(GROUP, VERSION, "invocations", {
        apiVersion: `${GROUP}/${VERSION}`,
        kind: "Invocation",
        metadata: {
          name,
          uid: `uid-${name}`,
          namespace: ns,
          creationTimestamp: hoursAgo(1),
        },
        spec: {
          trigger: { type: trigger },
          parentRef: { kind: "TaskAgent", name: parent },
        },
        status: { phase },
      });
    inv(
      "code-reviewer-pr-4821",
      "agents",
      "Succeeded",
      "Webhook",
      "code-reviewer",
    );
    inv(
      "code-reviewer-pr-4822",
      "agents",
      "Running",
      "Webhook",
      "code-reviewer",
    );
    inv(
      "nightly-summarizer-2026",
      "agents",
      "Failed",
      "Schedule",
      "nightly-summarizer",
    );
  }

  private seedWorkerPools(): void {
    const wp = (
      name: string,
      ns: string,
      replicas: number,
      ready: number,
      active: number,
      maxPer: number,
      warm: number,
    ) =>
      this.putGVR(GROUP, VERSION, "workerpools", {
        apiVersion: `${GROUP}/${VERSION}`,
        kind: "WorkerPool",
        metadata: {
          name,
          uid: `uid-${name}`,
          namespace: ns,
          creationTimestamp: daysAgo(30),
        },
        spec: {
          replicas,
          maxExecutionsPerWorker: maxPer,
          warmPool: warm,
          template: { image: "ghcr.io/apoxy/clrk-worker:v0.3" },
        },
        status: {
          readyReplicas: ready,
          activeExecutions: active,
          capacity: {
            maxExecutions: replicas * maxPer,
            availableExecutions: Math.max(0, replicas * maxPer - active),
          },
        },
      });
    wp("default", "platform", 4, 4, 4, 32, 6);
    wp("data-heavy", "research", 2, 2, 1, 8, 2);
  }

  private seedAgentMetrics(): void {
    const ts = daysAgo(0);
    const put = (
      resource: string,
      kind: string,
      name: string,
      ns: string,
      usage: Record<string, string>,
    ) =>
      this.putGVR(METRICS_GROUP, VERSION, resource, {
        apiVersion: `${METRICS_GROUP}/${VERSION}`,
        kind,
        metadata: { name, uid: `m-${name}`, namespace: ns },
        timestamp: ts,
        window: "24h0m0s",
        usage,
      } as unknown as StoredObject);
    put("taskagentmetrics", "TaskAgentMetrics", "code-reviewer", "platform", {
      invocations: "4124",
      errors: "4",
      warm: "3",
      input_tokens: "2840000",
      output_tokens: "612000",
      tool_calls: "18204",
      latency_p50_ms: "1840",
      latency_p99_ms: "9220",
    });
    put(
      "taskagentmetrics",
      "TaskAgentMetrics",
      "nightly-summarizer",
      "platform",
      {
        invocations: "96",
        errors: "2",
        warm: "0",
        input_tokens: "9812000",
        output_tokens: "184000",
        tool_calls: "5240",
        latency_p50_ms: "38400",
        latency_p99_ms: "91200",
      },
    );
    put("daemonagentmetrics", "DaemonAgentMetrics", "slack-bot", "platform", {
      invocations: "1",
      errors: "0",
      running: "1",
      input_tokens: "240000",
      output_tokens: "92000",
      tool_calls: "84",
    });
    put("daemonagentmetrics", "DaemonAgentMetrics", "log-watcher", "research", {
      invocations: "0",
      errors: "421",
      running: "0",
      input_tokens: "0",
      output_tokens: "0",
      tool_calls: "0",
    });
  }

  private seedAgentRevisions(): void {
    const rev = (
      name: string,
      ns: string,
      owner: string,
      image: string,
      ready: boolean,
      active: boolean,
      workers: number,
      ageDays: number,
    ) =>
      this.putGVR(GROUP, VERSION, "agentsandboxrevisions", {
        apiVersion: `${GROUP}/${VERSION}`,
        kind: "AgentSandboxRevision",
        metadata: {
          name,
          uid: `uid-${name}`,
          namespace: ns,
          creationTimestamp: daysAgo(ageDays),
          ownerReferences: [
            {
              apiVersion: `${GROUP}/${VERSION}`,
              kind: "TaskAgent",
              name: owner,
              uid: `uid-${owner}`,
              controller: true,
            },
          ],
        },
        spec: { image },
        status: {
          readyWorkers: workers,
          conditions: [
            { type: "Ready", status: ready ? "True" : "False" },
            { type: "Active", status: active ? "True" : "False" },
          ],
        },
      } as unknown as StoredObject);
    rev(
      "code-reviewer-00007",
      "platform",
      "code-reviewer",
      "ghcr.io/acme/review-bot:c41a7e",
      true,
      true,
      4,
      1,
    );
    rev(
      "code-reviewer-00006",
      "platform",
      "code-reviewer",
      "ghcr.io/acme/review-bot:b8c124",
      true,
      false,
      0,
      8,
    );
    rev(
      "slack-bot-00008",
      "platform",
      "slack-bot",
      "ghcr.io/acme/slack-bot:b88a2d",
      true,
      true,
      1,
      14,
    );
  }

  /**
   * Synthesize OTLP TracesData for an agent so the swimlane / daemon activity
   * render against the real read path. TaskAgents get invocation-grouped traces
   * (an `ingress.dispatch` root + LLM/MCP/Network children sharing an
   * `invocation.id`); DaemonAgents get ungrouped wall-clock calls. Times are
   * relative to the live clock so the daemon look-back windows include them.
   */
  private tracesFor(resource: string, name: string): Json {
    const now = Date.now();
    const NS = 1_000_000;
    const spans: Json[] = [];
    const kv = (rec: Record<string, string>) =>
      Object.entries(rec).map(([key, v]) => ({
        key,
        value: { stringValue: v },
      }));
    const toB64 = (s: string) => {
      const bytes = enc.encode(s);
      let bin = "";
      for (const b of bytes) bin += String.fromCharCode(b);
      return btoa(bin);
    };
    // A captured-body span event (http.request.body / http.response.body),
    // carrying the payload base64'd in clrk.body.b64 exactly as the ext_proc
    // OTLP sink emits it (internal/extproc/sink_otlp.go).
    const bodyEvent = (evName: string, text: string, atMs: number) => ({
      timeUnixNano: String(Math.round(atMs) * NS),
      name: evName,
      attributes: kv({
        "clrk.body.bytes": String(enc.encode(text).length),
        "clrk.body.truncated": "false",
        "clrk.body.b64": toB64(text),
      }),
    });
    const span = (
      startMs: number,
      durMs: number,
      spanName: string,
      attrs: Record<string, string>,
      ok: boolean,
      ids: { trace: string; span: string; parent?: string },
      bodies?: { req?: string; resp?: string },
    ) => {
      const events = [
        ...(bodies?.req != null
          ? [bodyEvent("http.request.body", bodies.req, startMs)]
          : []),
        ...(bodies?.resp != null
          ? [bodyEvent("http.response.body", bodies.resp, startMs + durMs)]
          : []),
      ];
      return spans.push({
        traceId: ids.trace,
        spanId: ids.span,
        parentSpanId: ids.parent ?? "",
        name: spanName,
        startTimeUnixNano: String(Math.round(startMs) * NS),
        endTimeUnixNano: String(Math.round(startMs + durMs) * NS),
        attributes: kv(attrs),
        status: { code: ok ? "STATUS_CODE_OK" : "STATUS_CODE_ERROR" },
        ...(events.length ? { events } : {}),
      });
    };

    if (resource === "daemonagents") {
      const calls: Array<{
        ago: number;
        lane: "llm" | "mcp" | "net";
        dur: number;
        ok?: boolean;
      }> = [
        { ago: 8, lane: "llm", dur: 600 },
        { ago: 41, lane: "mcp", dur: 120 },
        { ago: 96, lane: "net", dur: 80 },
        { ago: 150, lane: "llm", dur: 540 },
        { ago: 220, lane: "mcp", dur: 90 },
        { ago: 360, lane: "net", dur: 70, ok: false },
        { ago: 540, lane: "llm", dur: 720 },
        { ago: 900, lane: "mcp", dur: 110 },
        { ago: 1500, lane: "net", dur: 65 },
        { ago: 2400, lane: "llm", dur: 480 },
      ];
      calls.forEach((c, i) => {
        const t = `tr-${name}-d${i}`;
        span(
          now - c.ago * 1000,
          c.dur,
          c.lane,
          laneAttrs(c.lane, c.ok !== false),
          c.ok !== false,
          { trace: t, span: `${t}-s` },
          laneBodies(c.lane),
        );
      });
    } else {
      for (let i = 0; i < 3; i++) {
        const invStart = now - (i * 90 + 12) * 1000;
        const invId = `inv-${name}-${4821 - i}`;
        const t = `tr-${invId}`;
        const ok = i !== 2;
        const base = {
          "invocation.id": invId,
          "agent.name": name,
          "agent.kind": "TaskAgent",
        };
        span(
          invStart,
          ok ? 1840 : 320,
          "ingress.dispatch",
          { ...base, "http.request.method": "POST", "url.path": "/run" },
          ok,
          { trace: t, span: `${t}-root` },
          laneBodies("inbound", { invId, ok }),
        );
        span(
          invStart + 50,
          420,
          "chat",
          { ...base, ...laneAttrs("llm") },
          true,
          { trace: t, span: `${t}-llm`, parent: `${t}-root` },
          laneBodies("llm"),
        );
        span(
          invStart + 520,
          120,
          "tools/call",
          { ...base, ...laneAttrs("mcp") },
          true,
          { trace: t, span: `${t}-mcp`, parent: `${t}-root` },
          laneBodies("mcp", { tool: "read_file" }),
        );
        span(
          invStart + 680,
          90,
          "GET github",
          { ...base, ...laneAttrs("net", ok) },
          ok,
          { trace: t, span: `${t}-net`, parent: `${t}-root` },
          laneBodies("net"),
        );
        // The most recent run fires a parallel tool fan-out — six MCP calls at
        // once — so the MCP lane stacks past the fold and renders the "+N more"
        // overflow strip the user can expand (mirrors the review-bot example).
        if (i === 0 && ok) {
          const fan = [
            { tool: "get_pull_request", off: 0, dur: 340 },
            { tool: "list_files", off: 0, dur: 280 },
            { tool: "get_diff", off: 10, dur: 700 },
            { tool: "read_file", off: 20, dur: 520 },
            { tool: "search_code", off: 30, dur: 480 },
            { tool: "list_commits", off: 40, dur: 610 },
          ];
          fan.forEach((f, j) =>
            span(
              invStart + 900 + f.off,
              f.dur,
              "tools/call",
              { ...base, ...laneAttrs("mcp"), "mcp.tool.name": f.tool },
              true,
              { trace: t, span: `${t}-mcp-fan${j}`, parent: `${t}-root` },
              laneBodies("mcp", { tool: f.tool }),
            ),
          );
        }
      }
    }
    return {
      resourceSpans: [
        {
          resource: { attributes: kv({ "service.name": name }) },
          scopeSpans: [{ scope: { name: "clrk" }, spans }],
        },
      ],
    };
  }

  // --- internals -------------------------------------------------------------

  private putGVR(
    group: string,
    version: string,
    resource: string,
    o: StoredObject,
  ): void {
    const key = colKey(group, version, resource);
    const m = this.store.get(key) ?? new Map<string, StoredObject>();
    m.set(o.metadata.name, this.stamp(o));
    this.store.set(key, m);
  }

  private stamp(o: StoredObject): StoredObject {
    return {
      ...o,
      metadata: { ...o.metadata, resourceVersion: String(++this.rv) },
    };
  }

  private listBody(key: string): Json {
    const items = [...(this.store.get(key)?.values() ?? [])];
    return {
      apiVersion: "v1",
      kind: "List",
      metadata: { resourceVersion: String(this.rv) },
      items,
    };
  }

  private getObject(key: string, name: string): Response {
    const o = this.store.get(key)?.get(name);
    return o
      ? json(o)
      : json(
          { kind: "Status", status: "Failure", code: 404, reason: "NotFound" },
          404,
        );
  }

  private async applyObject(
    key: string,
    name: string,
    init?: RequestInit,
  ): Promise<Response> {
    const body = JSON.parse((init?.body as string) ?? "{}") as StoredObject;
    const existing = this.store.get(key)?.get(name);
    const merged: StoredObject = {
      ...existing,
      ...body,
      metadata: {
        ...existing?.metadata,
        ...body.metadata,
        name,
        uid: existing?.metadata.uid ?? `uid-${name}`,
      },
    } as StoredObject;
    const stamped = this.stamp(merged);
    const m = this.store.get(key) ?? new Map<string, StoredObject>();
    m.set(name, stamped);
    this.store.set(key, m);
    this.broadcast(key, {
      type: existing ? "MODIFIED" : "ADDED",
      object: stamped,
    });
    return json(stamped);
  }

  private deleteObject(key: string, name: string): Response {
    const o = this.store.get(key)?.get(name);
    if (o) {
      this.store.get(key)?.delete(name);
      this.broadcast(key, { type: "DELETED", object: o });
    }
    return json({
      kind: "Status",
      apiVersion: "v1",
      status: "Success",
      metadata: {},
    });
  }

  private openWatch(key: string, signal?: AbortSignal | null): Response {
    const set = this.watchers.get(key) ?? new Set<Controller>();
    this.watchers.set(key, set);
    let ctrl: Controller | undefined;
    const stream = new ReadableStream<Uint8Array>({
      start: (controller) => {
        ctrl = controller;
        set.add(controller);
        const onAbort = () => {
          set.delete(controller);
          try {
            controller.close();
          } catch {
            /* already closed */
          }
        };
        signal?.addEventListener("abort", onAbort, { once: true });
      },
      cancel: () => {
        if (ctrl) set.delete(ctrl);
      },
    });
    return new Response(stream, {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  }

  private broadcast(key: string, ev: WatchEvent): void {
    const set = this.watchers.get(key);
    if (!set) return;
    const line = enc.encode(JSON.stringify(ev) + "\n");
    for (const c of set) {
      try {
        c.enqueue(line);
      } catch {
        /* stream closed */
      }
    }
  }

  private discoveryDoc(): Json {
    const res = (names: string[]) => names.map((resource) => ({ resource }));
    return {
      apiVersion: "apidiscovery.k8s.io/v2",
      kind: "APIGroupDiscoveryList",
      items: [
        {
          metadata: { name: GROUP },
          versions: [
            {
              version: VERSION,
              resources: res([
                "taskagents",
                "daemonagents",
                "egressgateways",
                "invocations",
                "mcproutes",
                "aiproviderroutes",
                "egressl4routes",
                "workerpools",
                "credentialinjectionpolicies",
                "ratelimitpolicies",
                "loggingpolicies",
                "egressdenypolicies",
              ]),
            },
          ],
        },
        {
          metadata: { name: GWAPI },
          versions: [{ version: "v1", resources: res(["httproutes"]) }],
        },
      ],
    };
  }
}

const DAY = 86_400_000;

function daysAgo(n: number): string {
  return new Date(Date.parse("2026-06-19T09:00:00Z") - n * DAY).toISOString();
}
function hoursAgo(n: number): string {
  return new Date(
    Date.parse("2026-06-19T09:00:00Z") - n * (DAY / 24),
  ).toISOString();
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}
function notFound(method: string, path: string): Response {
  return json(
    {
      kind: "Status",
      status: "Failure",
      code: 404,
      message: `mock: no route for ${method} ${path}`,
    },
    404,
  );
}

/** Attribute sets that classify a synthetic span into the LLM / MCP / network lane. */
function laneAttrs(
  lane: "llm" | "mcp" | "net",
  ok = true,
): Record<string, string> {
  if (lane === "llm")
    return {
      "gen_ai.system": "anthropic",
      "gen_ai.request.model": "claude-sonnet-4",
      "gen_ai.usage.input_tokens": "1200",
      "gen_ai.usage.output_tokens": "300",
      "gen_ai.response.stream": "true",
    };
  if (lane === "mcp")
    return {
      "mcp.method": "tools/call",
      "mcp.tool.name": "read_file",
      "mcp.server": "github-mcp",
      "server.address": "github-mcp.internal",
    };
  return {
    "server.address": "api.github.com",
    "http.response.status_code": ok ? "200" : "503",
  };
}

/**
 * Sample captured request/response bodies per lane. tracesFor base64's these
 * into `http.request.body` / `http.response.body` span events so the inspector's
 * body panels render in the mock exactly as they will against a real ext_proc
 * capture — JSON the UI then pretty-prints.
 */
function laneBodies(
  lane: "inbound" | "llm" | "mcp" | "net",
  opts: { tool?: string; invId?: string; ok?: boolean } = {},
): { req?: string; resp?: string } {
  const j = (o: unknown) => JSON.stringify(o);
  if (lane === "inbound") {
    return {
      req: j({
        action: "opened",
        number: 4821,
        pull_request: {
          title: "feat: refresh OAuth token flow",
          head: { ref: "oauth-refresh" },
          base: { ref: "main" },
        },
      }),
      resp: j({
        status: opts.ok === false ? "error" : "accepted",
        invocation: opts.invId ?? "",
      }),
    };
  }
  if (lane === "llm") {
    return {
      req: j({
        model: "claude-sonnet-4",
        system: "You are a senior code reviewer. Be concise.",
        messages: [
          {
            role: "user",
            content: "Review PR #4821 — focus on auth/oauth.go.",
          },
        ],
        tools: [
          { name: "get_pull_request" },
          { name: "list_files" },
          { name: "read_file" },
        ],
        stream: true,
      }),
      resp: j({
        id: "msg_01XCanva7",
        model: "claude-sonnet-4",
        stop_reason: "tool_use",
        content: [
          { type: "tool_use", name: "list_files", input: { path: "auth/" } },
        ],
        usage: { input_tokens: 1200, output_tokens: 300 },
      }),
    };
  }
  if (lane === "mcp") {
    const tool = opts.tool ?? "read_file";
    const args: Record<string, unknown> =
      tool === "read_file"
        ? { path: "auth/oauth.go" }
        : tool === "get_pull_request"
          ? { owner: "acme", repo: "svc", number: 4821 }
          : tool === "get_diff"
            ? { number: 4821 }
            : tool === "list_files"
              ? { path: "auth/" }
              : tool === "list_commits"
                ? { sha: "oauth-refresh" }
                : { query: "OAuth2 refresh" };
    const text =
      tool === "read_file"
        ? "package auth\n\nfunc Refresh(ctx context.Context) error { … }"
        : tool === "get_pull_request"
          ? '{"title":"feat: refresh OAuth","state":"open"}'
          : '[{"name":"oauth.go","sha":"3a9f…"}]';
    return {
      req: j({
        jsonrpc: "2.0",
        id: 14,
        method: "tools/call",
        params: { name: tool, arguments: args },
      }),
      resp: j({
        jsonrpc: "2.0",
        id: 14,
        result: { content: [{ type: "text", text }], isError: false },
      }),
    };
  }
  return {
    req: j({
      method: "GET",
      url: "https://api.github.com/repos/acme/svc/pulls/4821",
    }),
    resp: j({ number: 4821, state: "open", mergeable: true }),
  };
}

/** A standard `Programmed` condition for an EgressGateway / listener. */
function progCond(ok: boolean, message?: string): Record<string, unknown> {
  return ok
    ? {
        type: "Programmed",
        status: "True",
        reason: "Programmed",
        message: "gateway programmed",
        lastTransitionTime: daysAgo(0),
      }
    : {
        type: "Programmed",
        status: "False",
        reason: "Invalid",
        message: message ?? "not programmed",
        lastTransitionTime: daysAgo(0),
      };
}

// ── Tier-2 metric series (Overview KPI shapes + traffic chart) ────────────────

/** Parse a Go duration of whole h/m/s units (the Overview only sends 1h/6h/24h). */
function parseDurMs(d: string): number {
  const m = /^(\d+)(h|m|s)$/.exec(d.trim());
  if (!m) return 3_600_000;
  const n = Number(m[1]);
  return m[2] === "h" ? n * 3_600_000 : m[2] === "m" ? n * 60_000 : n * 1000;
}

/** Canonical Go-duration form (e.g. "6h0m0s"), matching what the real apiserver
 *  echoes for MetricSeriesSet.step rather than the abbreviated request form. */
function canonicalGoDur(ms: number): string {
  const sec = Math.max(0, Math.floor(ms / 1000));
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = sec % 60;
  if (h > 0) return `${h}h${m}m${s}s`;
  if (m > 0) return `${m}m${s}s`;
  return `${s}s`;
}

/** One value column of a synthesized metric: its measure label (or "" for a
 *  single-valued metric) and a per-bucket base the deterministic wave scales. */
interface MockMeasure {
  measure: string;
  base: number;
  /** Add an end-of-window spike (the errors crashloop the design seeds). */
  spike?: boolean;
}

const MOCK_METRICS: Record<
  string,
  { type: string; unit: string; measures: MockMeasure[] }
> = {
  "agent.invocations": {
    type: "Counter",
    unit: "invocations",
    measures: [{ measure: "", base: 180 }],
  },
  "gen_ai.tokens": {
    type: "Counter",
    unit: "tokens",
    measures: [
      { measure: "input", base: 120_000 },
      { measure: "output", base: 26_000 },
    ],
  },
  "mcp.calls": {
    type: "Counter",
    unit: "calls",
    measures: [{ measure: "", base: 980 }],
  },
  "agent.errors": {
    type: "Counter",
    unit: "errors",
    measures: [{ measure: "", base: 2, spike: true }],
  },
};

/**
 * Synthesize a MetricSeriesSet for the mock Overview: a deterministic gentle
 * diurnal wave (no randomness, so it's stable across polls) over the bucket grid
 * the query asks for, one series per measure. The shape — not the absolute scale
 * — is what the page reads, mirroring the design's synthetic series.
 */
function metricSeriesFor(metric: string, params: URLSearchParams): Json {
  const def = MOCK_METRICS[metric] ?? {
    type: "Counter",
    unit: "",
    measures: [{ measure: "", base: 10 }],
  };
  const sinceMs =
    Date.parse(params.get("since") ?? "") || Date.now() - 86_400_000;
  const untilMs = Date.parse(params.get("until") ?? "") || Date.now();
  const stepMs = parseDurMs(params.get("step") ?? "1h");
  const buckets = Math.max(1, Math.round((untilMs - sinceMs) / stepMs));

  const series = def.measures.map((ms) => {
    const points = [];
    for (let i = 0; i < buckets; i++) {
      const wave =
        0.7 +
        0.45 *
          Math.sin(
            (i / Math.max(1, buckets - 1)) * Math.PI * 1.7 - Math.PI / 2,
          );
      let v = ms.base * wave;
      if (ms.spike && i >= buckets - 2)
        v = ms.base * (i === buckets - 1 ? 9 : 5);
      points.push({
        timestamp: new Date(sinceMs + i * stepMs).toISOString(),
        value: String(Math.round(v)),
      });
    }
    return ms.measure
      ? { labels: { measure: ms.measure }, points }
      : { points };
  });

  return {
    apiVersion: `${METRICS_GROUP}/${VERSION}`,
    kind: "MetricSeriesSet",
    metadata: { name: metric },
    metric,
    type: def.type,
    unit: def.unit,
    since: new Date(sinceMs).toISOString(),
    until: new Date(untilMs).toISOString(),
    step: canonicalGoDur(stepMs),
    series,
  };
}

// ── The demo, authored once in a rich shape and lowered to k8s objects above ──

interface DemoListener {
  name: string;
  protocol: "TCP" | "TLS" | "HTTP" | "HTTPS" | "UDP";
  port?: number;
  tls?: "Terminate" | "Passthrough";
  ready?: boolean;
}
interface DemoMatch {
  kind: "provider" | "tools" | "path" | "method" | "sni" | "cidr";
  value: string;
  models?: string[];
  endpoints?: string[];
}
interface DemoRule {
  match: DemoMatch;
  tokenBudget?: { maxTokensPerDay: number };
  toolPolicy?: {
    allowedTools?: string[];
    requireConfirmation?: string[];
    maxCallsPerExecution?: number;
    rateLimit?: { requests: number; window: string };
  };
  backends?: Array<{ name: string; port: number; weight: number }>;
}
interface DemoRoute {
  kind: "AIProviderRoute" | "MCPRoute" | "EgressL4Route" | "HTTPRoute";
  name: string;
  listener: string;
  hostnames?: string[];
  provider?: string;
  rules: DemoRule[];
  cred?: {
    secret: string;
    key?: string;
    target: "Header" | "QueryParam" | "ProviderAuth";
    header?: string;
  };
  rateLimit?: { requests: number; window: string };
  logging?: {
    captureRequest?: boolean;
    captureResponse?: boolean;
    summary: string;
  };
  deny?: { statusCode?: number };
}
interface DemoGateway {
  name: string;
  namespace: string;
  defaultPolicy: "deny-all" | "allow-all";
  rps: number;
  ageDays: number;
  description: string;
  degraded?: string;
  otlpEndpoint?: string;
  listeners: DemoListener[];
  routes: DemoRoute[];
}

const OTLP = "otelcol.observability.svc:4318";

const EGRESS_DEMO: DemoGateway[] = [
  {
    name: "llm-egress",
    namespace: "platform",
    defaultPolicy: "deny-all",
    rps: 184,
    ageDays: 47,
    description: "Outbound to AI providers · MITM + token budgets",
    otlpEndpoint: OTLP,
    listeners: [
      { name: "https-mitm", protocol: "HTTPS", port: 443, tls: "Terminate" },
      {
        name: "tls-passthrough",
        protocol: "TLS",
        port: 443,
        tls: "Passthrough",
      },
      { name: "tcp-fallback", protocol: "TCP" },
    ],
    routes: [
      {
        kind: "AIProviderRoute",
        name: "openai-chat",
        listener: "https-mitm",
        provider: "openai",
        hostnames: ["api.openai.com"],
        cred: {
          secret: "openai-key",
          target: "Header",
          header: "Authorization",
        },
        rules: [
          {
            match: {
              kind: "provider",
              value: "openai",
              models: ["gpt-4o*", "gpt-4-turbo"],
            },
            tokenBudget: { maxTokensPerDay: 20_000_000 },
          },
          {
            match: {
              kind: "provider",
              value: "openai",
              endpoints: ["/v1/embeddings"],
            },
            tokenBudget: { maxTokensPerDay: 200_000_000 },
          },
        ],
      },
      {
        kind: "AIProviderRoute",
        name: "anthropic",
        listener: "https-mitm",
        provider: "anthropic",
        hostnames: ["api.anthropic.com"],
        cred: {
          secret: "anthropic-key",
          target: "Header",
          header: "x-api-key",
        },
        rules: [
          {
            match: {
              kind: "provider",
              value: "anthropic",
              models: ["claude-3-*", "claude-sonnet-4*"],
            },
            tokenBudget: { maxTokensPerDay: 40_000_000 },
          },
        ],
      },
      {
        kind: "AIProviderRoute",
        name: "gemini",
        listener: "https-mitm",
        provider: "google",
        hostnames: ["generativelanguage.googleapis.com"],
        cred: { secret: "vertex-sa", target: "ProviderAuth" },
        rules: [
          {
            match: {
              kind: "provider",
              value: "google",
              models: ["gemini-2.0-flash*"],
            },
            tokenBudget: { maxTokensPerDay: 10_000_000 },
          },
        ],
      },
      {
        kind: "HTTPRoute",
        name: "openai-mirror",
        listener: "https-mitm",
        hostnames: ["api.openai.com"],
        rules: [
          {
            match: { kind: "path", value: "/v1/chat/completions" },
            backends: [
              { name: "api.openai.com", port: 443, weight: 90 },
              { name: "azure-openai.svc", port: 443, weight: 10 },
            ],
          },
        ],
      },
      {
        kind: "EgressL4Route",
        name: "huggingface-pt",
        listener: "tls-passthrough",
        hostnames: ["*.huggingface.co"],
        logging: { summary: "summary-only" },
        rules: [{ match: { kind: "sni", value: "*.huggingface.co" } }],
      },
      {
        kind: "EgressL4Route",
        name: "fallback-deny",
        listener: "tcp-fallback",
        hostnames: ["*"],
        deny: {},
        rules: [{ match: { kind: "cidr", value: "0.0.0.0/0" } }],
      },
    ],
  },
  {
    name: "github-egress",
    namespace: "platform",
    defaultPolicy: "deny-all",
    rps: 41,
    ageDays: 47,
    description: "GitHub + MCP server access for review agents",
    otlpEndpoint: OTLP,
    listeners: [
      { name: "mcp-https", protocol: "HTTPS", port: 443, tls: "Terminate" },
      { name: "api-https", protocol: "HTTPS", port: 443, tls: "Terminate" },
    ],
    routes: [
      {
        kind: "MCPRoute",
        name: "github-mcp",
        listener: "mcp-https",
        hostnames: ["mcp.github.com"],
        rules: [
          {
            match: { kind: "tools", value: "read_*, list_*, get_*" },
            toolPolicy: { allowedTools: ["read_*", "list_*", "get_*"] },
          },
          {
            match: {
              kind: "tools",
              value: "create_issue, comment_pr, merge_pr",
            },
            toolPolicy: {
              requireConfirmation: ["merge_pr"],
              rateLimit: { requests: 120, window: "1m" },
            },
          },
        ],
      },
      {
        kind: "MCPRoute",
        name: "context7-mcp",
        listener: "mcp-https",
        hostnames: ["mcp.context7.com"],
        rules: [
          {
            match: { kind: "tools", value: "search_*, fetch_*" },
            toolPolicy: { maxCallsPerExecution: 64 },
          },
        ],
      },
      {
        kind: "HTTPRoute",
        name: "github-api",
        listener: "api-https",
        hostnames: ["api.github.com"],
        cred: {
          secret: "github-token",
          target: "Header",
          header: "Authorization",
        },
        rateLimit: { requests: 60, window: "1m" },
        rules: [
          { match: { kind: "method", value: "GET" } },
          { match: { kind: "method", value: "POST, PATCH, DELETE" } },
        ],
      },
    ],
  },
  {
    name: "web-egress",
    namespace: "research",
    defaultPolicy: "allow-all",
    rps: 28,
    ageDays: 14,
    description: "Open web crawling for research agents · allow-all",
    otlpEndpoint: OTLP,
    listeners: [{ name: "any-tcp", protocol: "TCP" }],
    routes: [
      {
        kind: "EgressL4Route",
        name: "any-out",
        listener: "any-tcp",
        hostnames: ["*"],
        logging: { captureRequest: true, summary: "headers + status only" },
        rules: [{ match: { kind: "cidr", value: "0.0.0.0/0" } }],
      },
    ],
  },
  {
    name: "stripe-egress",
    namespace: "shop",
    defaultPolicy: "deny-all",
    rps: 0,
    ageDays: 8,
    description: "Stripe API for checkout-co-pilot · MITM disabled",
    degraded: "CA secret stripe-ca not found · listener https-mitm down",
    otlpEndpoint: OTLP,
    listeners: [
      {
        name: "stripe-https",
        protocol: "HTTPS",
        port: 443,
        tls: "Terminate",
        ready: false,
      },
    ],
    routes: [
      {
        kind: "HTTPRoute",
        name: "stripe-api",
        listener: "stripe-https",
        hostnames: ["api.stripe.com"],
        cred: {
          secret: "stripe-key",
          target: "Header",
          header: "Authorization",
        },
        rules: [{ match: { kind: "path", value: "/v1/*" } }],
      },
    ],
  },
];
