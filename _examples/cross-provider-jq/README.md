# cross-provider-jq

The same agent as [`_examples/jq-bot`](../jq-bot/) -- POST a JSON
document and a plain-English description, get back the `jq` filter that
does it -- but its single Claude Code LLM call is load-balanced across
**two providers of two different wire schemas** by the canonization
layer: Anthropic Haiku and an OpenAI model. The agent is byte-for-byte
identical to jq-bot (it speaks only the Anthropic API, `claude --model
haiku`); it has no idea a given request may be served by OpenAI under
translation. That transparency is the whole point.

Where [`_examples/model-router`](../model-router/) and
[`_examples/fallback-router`](../fallback-router/) demonstrate weighting
and failover across *same-schema* Anthropic backends and only describe
cross-provider translation in prose, this example actually wires it: one
canonical model name (`haiku`), two providers behind it, exercising
weighted load-balancing, ordered fallback, cross-schema request/response
translation, and per-backend credential injection together.

## The pieces

Two `Backend`s behind one canonical model:

- `anthropic-haiku` -- `anthropic` schema, `api.anthropic.com`. Same
  schema the agent speaks, so the request passes through untouched.
- `openai-gpt` -- `openai` schema, `api.openai.com`, with a
  `modelRewrite` remapping `claude-*` to `gpt-4.1-mini`. When this
  backend wins, the proxy translates the Anthropic `/v1/messages`
  request into an OpenAI `/v1/chat/completions` request and translates
  the response (streams included) back to the Anthropic schema.

One `AIProviderRoute` rule lists both with `weight: 50` each -- a ~50/50
split rolled per request by Envoy. Two `CredentialInjectionPolicy`s
scope a key to each backend by `sectionName` (the Backend name): the
Anthropic key as `x-api-key`, the OpenAI key as `Authorization`,
injected only after that backend wins.

An optional `FallbackRoutingPolicy` flips the weighted split into an
ordered failover chain (Anthropic primary, OpenAI fallback).

## Run under `clrk dev`

This is a `TaskAgent` with HTTP ingress (like jq-bot), so it uses the
`kubectl apply` + curl flow rather than the curl-loop DaemonAgent flow
of the router examples.

Create both secrets. The OpenAI value **must include the `Bearer `
prefix** -- clrk injects the secret value verbatim into the header and
adds no prefix of its own:

```bash
kubectl create secret generic anthropic-credentials \
  --from-literal=api-key="$ANTHROPIC_API_KEY"
kubectl create secret generic openai-credentials \
  --from-literal=authorization="Bearer $OPENAI_API_KEY"

kubectl apply -f _examples/cross-provider-jq/manifests/
clrk dev wait-ready
kubectl get gateway cross-provider-jq   # PROGRAMMED=True
```

## Invoke

The ingress controller materializes an Envoy data-plane Service
`clrk-cross-provider-jq` in the `clrk` namespace, auto-forwarded by
`clrk dev` to a host port in the 18080-18099 range. Confirm with `clrk
dev status`. The `X-Clrk-TaskAgent` header is required (Envoy's ingress
ext_proc reads it before the HTTPRoute header filter runs).

```bash
curl -sS http://localhost:18080/ \
  -H 'content-type: application/json' \
  -H 'X-Clrk-TaskAgent: default/cross-provider-jq' \
  --data '{
    "input": [
      {"name":"alice","age":30,"role":"eng"},
      {"name":"bob","age":42,"role":"pm"},
      {"name":"carol","age":25,"role":"eng"}
    ],
    "want": "names of engineers, ascending by age"
  }'
# {"filter":"[.[] | select(.role == \"eng\")] | sort_by(.age) | map(.name)","output":["carol","alice"]}
```

Fire it several times to see the split. The agent's response is
identical regardless of which provider served it -- the OpenAI legs are
translated back to the Anthropic schema before they reach the agent.

## What you should see

The routing decision is invisible in the agent's *contract* by design
(every leg returns the same Anthropic-shaped envelope), but it shows up
two ways.

**In telemetry.** The `clrk dev` TUI's `otel-logs` pane carries the
serving backend per call: `clrk.backend.name: anthropic-haiku` /
`clrk.backend.schema: anthropic` on the Anthropic legs,
`clrk.backend.name: openai-gpt` / `clrk.backend.schema: openai` on the
OpenAI legs, with `clrk.backend.reselected: true`.
`kubectl get aiproviderroute cross-provider-jq -o yaml` reports
`Accepted`, `ResolvedRefs`, and -- the one that proves the cross-schema
leg -- `TranslationUnsupported=False (TranslationSupported)`. `kubectl
get backends.clrk.apoxy.dev` prints each backend's schema (qualify the
group -- bare `backends` can resolve to Envoy Gateway's same-named CRD).

**In the answers themselves**, if you don't have the TUI handy: with
the default `gpt-4.1-mini` peer both providers answer correctly, so the
output is uniformly `{"output":["carol","alice"]}` -- the soft tell is
shape and latency. The two models phrase the jq filter differently
(Haiku tends toward `[.[] | select(...)] | sort_by(.age) | map(.name)`;
gpt-4.1-mini toward `map(select(...)) | sort_by(.age) | .[].name` or a
`{name, age}` projection), and the OpenAI legs come back noticeably
faster. Mind the model you pick: a weaker peer like `gpt-4o-mini`
intermittently emits a filter that runs but is wrong (e.g.
`map(select(.role=="eng") | .name) | sort_by(.age)` -> a jq runtime
error), which makes the split loudly visible but is a worse demo -- that
quality gap is the honest consequence of load-balancing one request
across two different models.

Attaching `fallbackroutingpolicy.yaml` pins everything to Anthropic
(ordered primary) -- the OpenAI filter styles disappear, proof the list
flipped from weighted split to ordered priority. Break the primary
(below) and OpenAI takes over as the fallback.

This whole flow was validated live against `clrk dev`: weighted split
(both providers serving, 12/12 correct with `gpt-4.1-mini`), the
cross-schema translation, per-backend credential injection (the OpenAI
leg authenticates -- a content error from the model, never a 401), the
policy flip to ordered routing, and real cross-provider failover
(primary dead -> 100% transparently served by OpenAI in ~2s).

## Switch to cross-provider failover

```bash
kubectl apply -f _examples/cross-provider-jq/manifests/fallbackroutingpolicy.yaml
```

Now `anthropic-haiku` serves everything while healthy and `openai-gpt`
catches its failures (429/503, connection refused/reset), with
per-attempt translation and OpenAI credential injection. The walk shows
up in `otel-logs` as `clrk.attempts: 2` and `clrk.attempt.backends:
["default/anthropic-haiku", "default/openai-gpt"]`. To watch it actually
fail over, temporarily point the primary at a dead host -- every request
keeps returning HTTP 200, now served by OpenAI:

```bash
kubectl patch backend anthropic-haiku --type=merge \
  -p '{"spec":{"upstream":{"host":"dead-anthropic.invalid","port":443}}}'
# ... fire requests: all 200, all served by gpt-4.1-mini (the fallback) ...
kubectl patch backend anthropic-haiku --type=merge \
  -p '{"spec":{"upstream":{"host":"api.anthropic.com","port":443}}}'
```

Delete the policy to return to the weighted split:

```bash
kubectl delete -f _examples/cross-provider-jq/manifests/fallbackroutingpolicy.yaml
```

## Rules of the road

- **The agent never changes.** This example reuses the published jq-bot
  image unmodified. The canonization, load-balancing, translation, and
  credential machinery all live at the egress boundary -- the workload
  speaks one schema and stays oblivious.
- **Credentials are injected verbatim, per backend.** clrk adds no
  `Bearer ` prefix and applies no per-provider default, so the OpenAI
  secret stores `Bearer sk-...` while Anthropic's stores the raw key.
  Each key is `sectionName`-scoped to its backend and injected only
  after that backend wins -- the cross-schema translation first strips
  the agent's source-schema auth headers, so a key never leaks across
  providers.
- **The route must stay model-blind to arm re-selection.** Selection is
  deferred to body time, but whether it defers at all is decided at
  request-headers time by a model-blind match. This rule matches on
  provider + endpoint only, so it arms. A `models`-scoped rule would not
  -- see `_examples/model-router/` for the catch-all-escort pattern.
- **The canonical request must fit the SMALLEST backend's limits.**
  Translation remaps the model id and reshapes the body, but it does
  not (today) clamp `max_tokens` to the target model's ceiling. Claude
  Code defaults `max_tokens` to Haiku's 32000; Anthropic serves it,
  but the translated OpenAI request 400s (`max_tokens is too large:
  32000 ... at most 16384`; gpt-4.x models cap completion at 16384).
  The TaskAgent pins `CLAUDE_CODE_MAX_OUTPUT_TOKENS=4096` so the one
  request is servable on every backend. When you load-balance across
  providers, budget to the tightest ceiling in the set.
- **Pick the right OpenAI peer model.** `gpt-4.1-mini` is the default
  haiku-class stand-in -- correct on this task and non-reasoning, so the
  4096-token cap is safe. `gpt-4o-mini` is cheaper but a notably weaker
  jq author (intermittent wrong-but-valid filters); a reasoning model
  like `gpt-5-mini` can burn the capped budget on reasoning tokens and
  return an empty completion, so raise `CLAUDE_CODE_MAX_OUTPUT_TOKENS`
  if you switch to one. Edit `backends.yaml`'s `modelRewrite` to choose.
- **Cross-schema candidates with no translator are dropped.** A backend
  whose schema pair has no translator is removed from the rule at build
  time; if none remain the rule passes through to the original host.
  `provider: custom` rules skip schema gating entirely.
