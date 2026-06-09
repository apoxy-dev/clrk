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
JSON body), after which the proxy re-points `:authority` to the chosen
backend, applies its model/body rewrites, injects credentials, and the
response parser is keyed to the backend's declared schema — so token
usage attribution stays correct after a re-route.

## Run under `clrk dev`

```bash
ANTHROPIC_API_KEY=sk-ant-... clrk dev \
  --apply _examples/model-router/manifests \
  --secret anthropic-credentials=ANTHROPIC_API_KEY:api-key
```

## What you should see

- Agent logs (`kubectl logs -l clrk.apoxy.dev/agent=model-router`)
  alternate between:
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
- `kubectl get backends` — shortname `be`, schema printed per column.

## Rules of the road (what works, what doesn't)

- **Same-schema only.** A rule's candidate set is filtered to backends
  whose `schema.name` matches the rule's provider at route-table build
  time. Pointing an `anthropic` rule at an `openai`-schema Backend
  doesn't error the data plane — the candidate is dropped, and if none
  remain the rule passes through to the original host. Cross-schema
  routing needs request/response translation (planned, APO-572).
  `provider: custom` rules skip the schema check (endpoint-only
  matching, no parser) and trust the operator's refs.
- **Model-scoped rules need a model-blind escort.** Whether a request
  defers selection to body time is decided at request-headers time by
  a model-blind match. If the only rule with `backendRefs` is
  model-scoped, it is invisible at header time and re-selection never
  arms. Always include a catch-all rule with `backendRefs` (rule 2
  here) below your model-scoped rules.
- **Weighted picks are sticky per invocation.** The 90/10 split hashes
  the invocation ID, so one agent run always lands on the same side —
  retries stay stable for budget attribution. Respawns re-roll.
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
