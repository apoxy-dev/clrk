# model-router

Multi-model routing on an `AIProviderRoute`: model-scoped rules steer
requests to different clrk `Backend`s at the egress proxy, with
per-backend model rewrites, weighted canary splits, and post-selection
credential injection. Builds on `_examples/echo-bot/` (same Anthropic
MITM + capture + credential stack) and adds the backend re-selection
layer.

Two `Backend`s, both speaking the `anthropic` wire schema:

- `anthropic-direct` — api.anthropic.com, untouched.
- `anthropic-budget` — api.anthropic.com plus a `modelRewrites` entry
  remapping `claude-opus-*` to `claude-haiku-4-5` on the wire.

One `AIProviderRoute` with two rules:

1. `models: [claude-opus-*]` → `anthropic-budget`. Any opus-class
   request is silently downgraded to haiku — the agent asks for opus
   and gets haiku back.
2. Catch-all → 90/10 weighted split between `anthropic-direct` and
   `anthropic-budget` (standard Gateway API `BackendRef.weight`).

Selection happens at RequestBody end-of-stream (the model lives in the
JSON body): the proxy pins the request onto a per-rule route whose
cluster holds the rule's backends, Envoy's load balancer picks one, and
a per-attempt upstream filter re-points `:authority`, applies the
backend's model/body rewrites, and injects credentials. The response
parser is keyed to the serving backend's declared schema — so token
usage attribution stays correct after a re-route.

## Run under `clrk dev`

```bash
ANTHROPIC_API_KEY=sk-ant-... clrk dev \
  --apply _examples/model-router/manifests \
  --secret anthropic-credentials=ANTHROPIC_API_KEY:api-key
```

## What you should see

- Agent output (in the `clrk dev` TUI, or wrapped in the worker's
  structured logs: `kubectl logs -l clrk.apoxy.dev/component=worker |
  grep model-router`) alternates between:
  ```
  asked for: claude-opus-4-1
  served:    claude-haiku-4-5      <- rule 1 + backend modelRewrite

  asked for: claude-haiku-4-5
  served:    claude-haiku-4-5      <- rule 2, weighted split
  ```
- `otel-logs` records carry the selection facts when re-selection
  fired: `clrk.backend.name` (`anthropic-budget` for the opus calls),
  `clrk.backend.schema`, `clrk.backend.reselected=true`.
- `kubectl get aiproviderroute model-router -o yaml` — the status
  controller reports `Accepted` and `ResolvedRefs` per parent; a
  dangling or non-Backend ref flips `ResolvedRefs=False` and the rule
  degrades to pass-through.
- `kubectl get backends.clrk.apoxy.dev` — schema printed per column.
  (Qualify the group: bare `backends` can resolve to Envoy Gateway's
  same-named CRD.)

## Rules of the road (what works, what doesn't)

- **Cross-schema backends translate on the wire.** A rule's candidates
  no longer have to match the rule's provider schema: an `anthropic`
  rule can point at an `openai`-schema Backend and the proxy translates
  the request/response (including streams) between schemas per attempt.
  Candidates whose schema pair has no translator are dropped, and if
  none remain the rule passes through to the original host.
  `provider: custom` rules skip the gating entirely (endpoint-only
  matching, no parser) and trust the operator's refs.
- **Model-scoped rules need a model-blind escort.** Whether a request
  defers selection to body time is decided at request-headers time by
  a model-blind match. If the only rule with `backendRefs` is
  model-scoped, it is invisible at header time and re-selection never
  arms. Always include a catch-all rule with `backendRefs` (rule 2
  here) below your model-scoped rules.
- **Weighted picks roll per request.** The 90/10 split is Envoy's
  weighted load balancing over the rule's backends, decided fresh on
  every request. To turn a backendRefs list into an ordered failover
  chain instead of a split, attach a `FallbackRoutingPolicy` — see
  `_examples/fallback-router/`.
- **Credentials attach via CIP, never on the Backend.** Route-wide
  policies apply to every backend; `sectionName: <backend-name>` on
  the CIP's route parentRef scopes a key to one backend, injected only
  after that backend wins.
- **`type: InferencePool` is API-only today.** Selecting such a
  backend returns a clean 501 instead of mis-routing; the in-cluster
  pool data plane is a follow-up.
- **Classifier selection is not wired yet.** The `ExtensionRef` filter
  seam exists on the rule for content-based backend picking (APO-480);
  today the only selector is the weighted static one.
