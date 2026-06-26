# jq-bot-codex

[`_examples/jq-bot`](../jq-bot/) driven by the **OpenAI Codex CLI**
instead of Claude Code. Same contract -- POST a JSON document and a
plain-English description, get back the `jq` filter that does it,
verified inside the sandbox before it answers. The only things that
change are which LLM CLI runs in the sandbox (`codex` vs `claude`) and
which provider clrk injects credentials for (OpenAI vs Anthropic).

The point of having both is to show the egress machinery is
**agent-CLI-agnostic**: the sandbox runs an off-the-shelf coding agent,
speaks its native provider API, and clrk terminates TLS, injects the
key, and captures the round trip the same way regardless of vendor.

## The one wrinkle: Codex defaults to WebSockets

Codex 0.139 prefers a WebSocket transport
(`wss://api.openai.com/v1/responses`) for its model calls. That is
**awkward for clrk** -- and for any HTTP-oriented egress proxy:

- **Credential injection is per-HTTP-request.** clrk's ext_proc matches
  the AIProviderRoute and rewrites `Authorization` on each outbound
  `POST /v1/responses`. A WebSocket is one long-lived `Upgrade`
  connection; ext_proc sees the handshake once, then model turns flow as
  WS frames inside it. There is no per-inference request to match or
  inject on.
- **Auto-instrumentation parses HTTP bodies.** The codecs, token-usage
  extraction (`gen_ai.usage`), and SSE reassembly all read HTTP request
  bodies and `text/event-stream` responses. Post-upgrade WS frames are
  not HTTP messages, so "all I/O is intercepted and logged" would not
  hold.

The fix is one config line. A **custom provider** with
`wire_api = "responses"` forces Codex onto plain `POST /v1/responses`
over HTTPS -- one request per model turn, exactly the shape clrk's
injection and telemetry are built for. The built-in `openai` provider
id can't be overridden, so the example declares `openai_http`:

```toml
# written by agent.sh into $CODEX_HOME/config.toml
model = "gpt-5-mini"
model_provider = "openai_http"
approval_policy = "never"
sandbox_mode = "danger-full-access"

[model_providers.openai_http]
name = "openai-http"
base_url = "https://api.openai.com/v1"
env_key = "OPENAI_API_KEY"
wire_api = "responses"
```

(`wire_api = "chat"` -- the `/v1/chat/completions` surface -- was removed
in 0.139, so `/v1/responses` is the only HTTP option.)

## Build the image

No prebuilt image is published for this example. Build from the
Dockerfile and push where your worker pool can pull from.

For `clrk dev`, the local registry is one registry reachable under two
names: the worker pulls it in-cluster as `clrk-registry:5000`, but from
your host (where `docker` runs) that name does not resolve -- push to the
host-forwarded port instead. A registry stores images by repository, not
by hostname, so an image pushed to `127.0.0.1:<port>/clrk-jq-bot-codex`
is the same one the worker pulls as `clrk-registry:5000/clrk-jq-bot-codex`:

```bash
# the host-forwarded port for the in-cluster clrk-registry:5000
REG_PORT=$(docker port clrk-registry 5000 | head -1 | cut -d: -f2)

# Build + load into the local daemon, then push. (`buildx --push` from the
# default docker-container builder can't reach the host-published registry
# port mid-build; load locally and push from the host CLI instead. Use
# 127.0.0.1, not localhost -- localhost can resolve to IPv6 ::1, which the
# registry forward does not bind.)
docker buildx build --platform=linux/arm64 --load \
  -t "127.0.0.1:${REG_PORT}/clrk-jq-bot-codex:latest" \
  _examples/jq-bot-codex/
docker push "127.0.0.1:${REG_PORT}/clrk-jq-bot-codex:latest"
```

`manifests/taskagent.yaml` already references the in-cluster name
`clrk-registry:5000/clrk-jq-bot-codex:latest`. For a non-dev cluster,
push to your own registry and edit `spec.template.spec.image`.

**Iterating?** The worker caches an image by its tag and will not re-pull
a changed `:latest`. After rebuilding, push a *fresh* tag (e.g. `:v2`) and
bump `spec.template.spec.image` so a new sandbox revision pulls it -- or
restart the worker pod to drop its image cache.

**The worker must trust the local registry.** The sandbox image puller
(`apoxy/pkg/sandbox`) talks HTTPS by default, but `clrk dev`'s registry
serves plain HTTP -- so the pull fails with `server gave HTTP response
to HTTPS client` and the TaskAgent reports `no ready revision` (HTTP
503). List the registry in the worker's `CLRK_INSECURE_REGISTRIES` (exact
`host:port`, comma-separated) on the WorkerPool:

```bash
kubectl patch workerpool default --type=merge \
  -p '{"spec":{"template":{"env":[{"name":"CLRK_INSECURE_REGISTRIES","value":"clrk-registry:5000"}]}}}'
```

The controller rolls a new worker pod (privileged + `/run/clrk`
re-injected by the controller, so the bootstrap survives the patch).
A registry reachable over HTTPS needs none of this.

## Run under `clrk dev`

Create the OpenAI secret. The value **must include the `Bearer `
prefix** -- clrk injects the secret value verbatim and adds no prefix of
its own (the same secret the [`cross-provider-jq`](../cross-provider-jq/)
example uses):

```bash
kubectl create secret generic openai-credentials \
  --from-literal=authorization="Bearer $OPENAI_API_KEY"

kubectl apply -f _examples/jq-bot-codex/manifests/
clrk dev wait-ready
kubectl get gateway jq-bot-codex   # PROGRAMMED=True
```

## Invoke

The TaskAgent ingress controller materializes an Envoy data-plane
Service `clrk-jq-bot-codex` in the `clrk` namespace; `clrk dev`
auto-forwards it to a host port in the 18080-18099 range. Confirm the
port with `clrk dev status` (it depends on how many exposed Services are
up -- don't assume :18080). The `X-Clrk-TaskAgent` header is required
(Envoy's ingress ext_proc reads it before the HTTPRoute header filter
runs).

```bash
curl -sS http://localhost:<PORT>/ \
  -H 'content-type: application/json' \
  -H 'X-Clrk-TaskAgent: default/jq-bot-codex' \
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

Actual response from a working run (gpt-5-mini, ~9s end-to-end):

```json
{"filter":"map(select(.role==\"eng\")) | sort_by(.age) | .[].name","output":["carol","alice"]}
```

The envelope is identical to jq-bot's `{filter, output}` (or
`{filter, error}` / `{filter:"", codex_error}` on failure). The filter
*style* is the tell that OpenAI served it: gpt-5-mini writes
`map(select(...)) | sort_by(.age) | .[].name` where Claude Haiku writes
`[.[] | select(...)] | sort_by(.age) | map(.name)` -- same result,
different idiom.

## What you should see

In the `clrk dev` TUI's `otel-logs` pane, each invocation carries an
egress span for `POST api.openai.com/v1/responses -> 200` with
`gen_ai.system: openai`, `clrk.aiproviderroute.matched: true`, and the
`Authorization` header redacted in the capture -- proof the placeholder
key was swapped for the real one at the egress boundary, never inside
the sandbox. A `200` (not a `401`) is the injection working; the rule
has no `backendRefs`, so the request passes through the dynamic forward
proxy to `api.openai.com` rather than being reselected.

You will also see incidental spans from Codex itself -- plugin
discovery against `api.github.com`/`codeload.github.com` and telemetry
to `chatgpt.com` -- passing through the EgressGateway's allow-all
default policy without injection. They don't affect the result; tighten
`defaultPolicy` if you want them blocked.

## How it works

`agent.sh` runs once per request inside a fresh sandbox:

1. Pins `HOME` / `CODEX_HOME` at `/tmp` and writes the custom-provider
   `config.toml` (the WebSocket workaround above).
2. Reads the CloudEvents envelope from stdin, splits `.data.input` and
   `.data.want` onto `/tmp/`.
3. Runs `codex exec --skip-git-repo-check --ephemeral
   --dangerously-bypass-approvals-and-sandbox -o /tmp/last.txt -`,
   feeding the prompt on stdin and capturing only the final agent
   message.
4. Runs the returned filter through `jq` to verify it -- verification
   stays *inside* the sandbox; only the model call crosses the netns.
5. Emits one JSON line: `{filter, output}`.

## Gotchas (Codex-specific, on top of jq-bot's)

- **WebSocket transport (above).** The single most important difference
  from jq-bot. Without `wire_api = "responses"` clrk sees no injectable
  HTTP request and the agent egresses with the placeholder key.
- **Codex's own sandbox can't nest inside gVisor.** Its landlock/seccomp
  sandbox fails to initialize under the clrk runtime, so the agent runs
  with `--dangerously-bypass-approvals-and-sandbox`. That is safe here:
  clrk *is* the isolation boundary, and egress is still fully mediated.
- **`CODEX_HOME` must exist and be writable.** Codex aborts at startup if
  it points at a missing path; `agent.sh` `mkdir -p`s it under `/tmp`.
- **`OPENAI_API_KEY` must be set even though it's a placeholder.** Codex
  refuses to start without `env_key` present; the real value arrives only
  at the egress proxy.
- **Token usage may not appear in telemetry.** clrk's OpenAI telemetry
  parser targets the Chat Completions shape; the Responses API body
  differs, so `gen_ai.usage` token counts can be absent on this leg. The
  request still passes through and authenticates -- injection does not
  depend on the codec recognizing the operation.
- **Inherited jq-bot gotchas still apply:** the worker doesn't carry
  image-baked `ENV` (surface vars via `spec.env`), `valueFrom.secretKeyRef`
  is ignored (use literal `.value`), and cold sandboxes can exceed the
  default HTTPRoute timeout (`spec.timeout` is pinned end-to-end; this
  example sets 120s).
