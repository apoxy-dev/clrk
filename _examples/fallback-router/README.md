# fallback-router

Ordered inter-backend failover on an `AIProviderRoute`, opted in with a
`FallbackRoutingPolicy`: a request whose attempt fails (connection
refused, reset, or a retriable status like 429/503) is transparently
retried against the next backend in the rule's `backendRefs` list, with
per-attempt credential injection and — when backends speak different
wire schemas — per-attempt request/response translation. Builds on
`_examples/model-router/` (same Anthropic MITM + capture + credential
stack); where model-router steers and splits traffic, this example
survives a provider outage.

Two `Backend`s, both speaking the `anthropic` wire schema:

- `anthropic-primary` — points at a Service whose selector matches no
  pods: a deterministic stand-in for a provider outage (DNS resolves,
  every connection is refused).
- `anthropic-direct` — api.anthropic.com, the fallback.

One `AIProviderRoute` rule lists them in order, and the attached
`FallbackRoutingPolicy` turns that order into fallback priority:
`anthropic-primary` serves all traffic while healthy (it never is,
here) and `anthropic-direct` catches its failures. Without the policy
the same list would be a plain weighted split — half the requests would
fail. Delete `fallbackroutingpolicy.yaml` and re-apply to see exactly
that.

Fallback fires only before response headers are forwarded to the
agent: a mid-stream provider failure is not retried (it surfaces
through the stream's error-frame convention instead), so a response is
never stitched together across providers.

## Run under `clrk dev`

```bash
ANTHROPIC_API_KEY=sk-ant-... clrk dev \
  --apply _examples/fallback-router/manifests \
  --secret anthropic-credentials=ANTHROPIC_API_KEY:api-key
```

## What you should see

- Agent output (in the `clrk dev` TUI, or wrapped in the worker's
  structured logs: `kubectl logs -l clrk.apoxy.dev/component=worker |
  grep fallback-router`) shows every call succeeding even though the
  first backend refuses every connection:
  ```
  http 200 in 1.18s
  served: claude-haiku-4-5
  ```
- `otel-logs` records carry the walk: `clrk.attempts: 2` and
  `clrk.attempt.backends: ["default/anthropic-primary",
  "default/anthropic-direct"]`, while the `clrk.backend.*` attributes
  describe the serving (final) attempt — `clrk.backend.name:
  anthropic-direct`.
- Outlier detection ejects the dead backend after ~5 consecutive
  failures, so `clrk.attempts` drops to `1` for ~30s (requests go
  straight to the fallback tier), then pops back to `2` when the
  ejection expires and the backend is probed again. The per-attempt
  failure statuses live in Envoy's cluster stats
  (`cluster.clrk-llm-<rule-key>.upstream_rq_retry*`), not in clrk
  telemetry.
- `kubectl get fallbackroutingpolicies fallback-router -o yaml` — the
  policy; `kubectl get aiproviderroute fallback-router -o yaml` —
  `Accepted`/`ResolvedRefs` per parent, as in model-router.

## Rules of the road (what works, what doesn't)

- **Fallback is opt-in.** Without a `FallbackRoutingPolicy`,
  `backendRefs` keep their standard Gateway API meaning: weights split
  traffic and each request gets a single attempt. Attaching the policy
  flips the SAME list to ordered priority (weights are ignored) and
  stamps a retry policy onto the route. Attachment is whole-route for
  now; `sectionName` scoping is a follow-up.
- **Retry defaults are deliberate.** Connection failures, refused
  streams, and resets always retry; `retriableStatusCodes` defaults to
  `[429, 503]`; `numRetries` defaults to one fewer than the rule's
  backend count (capped at 5); `perTryTimeout` defaults to unset
  because a retry can only fire before response headers arrive — a
  bound would only ever cut long, healthy LLM streams.
- **Each attempt is a clean slate.** Credential injection,
  `:authority` re-pointing, model rewrites, and (for cross-schema
  candidates) request translation run per attempt, so a fallback
  backend gets its own credentials and its own wire format — an
  `openai`-schema fallback under this `anthropic` rule works.
- **Non-retriable failures pass through.** Only the configured
  statuses and connection-level failures walk the list; a 401 or 400
  from the primary goes straight back to the agent in the provider's
  schema. Exhausted retries return the LAST attempt's error.
- **Very large request bodies disable fallback.** A request whose body
  exceeds the router's retry buffer cannot be replayed; Envoy silently
  disables its retries and the otel record flags it with
  `clrk.retry.ineligible: body_too_large`.
