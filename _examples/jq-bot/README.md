# jq-bot

The first **ingress** example in this tree. POST a JSON document and a
plain-English description of what to extract; get back the `jq` filter
that does it (verified by the agent before it answers).

This is a `TaskAgent` — request → sandbox → response — not a
`DaemonAgent`. The controller-manager materializes a per-TA Gateway,
the worker spawns a fresh sandbox per request, and the response body
is whatever the agent prints on stdout.

## What it shows off (vs. the curl-loop demos)

| Layer                      | echo-bot / gemini-bot | jq-bot                              |
|----------------------------|-----------------------|-------------------------------------|
| Trigger                    | self-loop (`while :`) | inbound HTTP POST                   |
| Per-invocation isolation   | one long-lived sandbox| one cold sandbox per request        |
| LLM tool use inside sandbox| none (raw curl)       | Claude Code (Anthropic API)         |
| Egress story               | one POST per second   | Anthropic round-trip per request    |
| Verified output            | no                    | yes (shell runs jq + returns result)|

## Build the image

```bash
docker build --platform=linux/$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/') \
  -t ghcr.io/<your-org>/clrk-jq-bot:latest _examples/jq-bot/
docker push ghcr.io/<your-org>/clrk-jq-bot:latest
```

Edit `manifests/taskagent.yaml` and set:

- `spec.template.spec.image` → your pushed image ref.
- `spec.template.spec.env[ANTHROPIC_API_KEY].value` → your real key,
  OR keep it as a placeholder and wire up the
  `egressgateway.yaml`/`aiproviderroute.yaml`/`credentialinjectionpolicy.yaml`
  manifests (commented-out by default; see "Wiring credential
  injection" below).

## Run

```bash
kubectl apply -f _examples/jq-bot/manifests/taskagent.yaml
```

Wait for the per-TA Gateway to come up:

```bash
clrk dev wait-ready
kubectl get gateway jq-bot   # PROGRAMMED=True
```

## Invoke

The TaskAgent ingress controller materializes an Envoy data-plane
Service named `clrk-jq-bot` in the `clrk` namespace:

```bash
kubectl port-forward -n clrk svc/clrk-jq-bot 18080:80 &
```

POST `{input, want}`. The `X-Clrk-TaskAgent` header is required —
Envoy's ingress ext_proc reads it before the HTTPRoute header-filter
runs. (The in-cluster cron firer at `internal/controller/taskagent_invoker_http.go`
sets it for the same reason; we mimic that here.)

```bash
curl -sS http://localhost:18080/ \
  -H 'content-type: application/json' \
  -H 'X-Clrk-TaskAgent: default/jq-bot' \
  --data '{
    "input": [
      {"name":"alice","age":30,"role":"eng"},
      {"name":"bob","age":42,"role":"pm"},
      {"name":"carol","age":25,"role":"eng"}
    ],
    "want": "names of engineers, ascending by age"
  }'
```

Actual response from a working run (Haiku, ~11s end-to-end):

```json
{"filter":"[.[] | select(.role == \"eng\")] | sort_by(.age) | map(.name)","output":["carol","alice"]}
```

Other prompts that work:

```bash
# Sum + filter
curl -sS http://localhost:18080/ \
  -H 'content-type: application/json' \
  -H 'X-Clrk-TaskAgent: default/jq-bot' \
  --data '{"input":[{"id":1,"total":12.5,"status":"paid"},{"id":2,"total":7,"status":"void"},{"id":3,"total":4.5,"status":"paid"}],"want":"sum of total where status is paid"}'
# {"filter":"map(select(.status == \"paid\") | .total) | add","output":17}
```

## How it works

`agent.sh` runs once per request inside a fresh sandbox:

1. Reads the CloudEvents structured-mode envelope from stdin
   (`internal/worker/dispatcher.go::buildCEEnvelope`).
2. Splits `.data.input` and `.data.want` onto `/tmp/`.
3. Asks Claude Haiku (via `claude --print --bare --no-session-persistence`)
   for ONE jq filter.
4. Runs the filter via `jq` to verify it — the verification step stays
   *inside* the sandbox; only the LLM call crosses the netns.
5. Emits one JSON line: `{filter, output}` (or `{filter, error}` if jq
   rejected the filter).

## Gotchas you'll hit if you fork this

- **The worker doesn't carry image-baked `ENV`.** Only PATH, CA-trust
  hints, `CLRK_METADATA_*`, and `spec.template.spec.env` are visible
  to the agent process. `ENV ANTHROPIC_API_KEY=...` in the Dockerfile
  is dead weight — surface secrets via `spec.env`.
- **`spec.env[].valueFrom.secretKeyRef` is silently ignored** today
  (see `internal/worker/sandbox.go::envVarsToStrings`). Use literal
  `.value` until that's fixed.
- **`HOME` matters for Claude Code.** The bundled CLI writes session
  artifacts under `$HOME` at startup; if it's not writable, `claude
  --print` exits with code 0 and no output (no error to stderr). The
  manifest pins `HOME=/tmp` for this reason. `--bare` and
  `--no-session-persistence` help but don't fully suppress it.
- **`--dangerously-skip-permissions` doesn't work as root**, which
  the sandbox is by default. Run claude tool-less and have the shell
  verify the filter instead.
- **Cold sandboxes need >15s, which exceeds Envoy's default HTTPRoute
  timeout.** The TaskAgent ingress controller pins
  `rules[0].timeouts` to `spec.timeout` (`internal/controller/taskagent_ingress_controller.go::desiredHTTPRoute`),
  so the spec value applies end-to-end. Default is 100s.

## Credential injection

The supplied manifests wire the full MITM path:

```bash
kubectl create secret generic anthropic-credentials \
  --from-literal=api-key="$ANTHROPIC_API_KEY"
kubectl apply -f _examples/jq-bot/manifests/
```

`taskagent.yaml` references `egressgateway.yaml`; the AIProviderRoute
and CredentialInjectionPolicy attach to it. On every outbound
`/v1/messages` the EG envoy terminates TLS, the per-EG ext_proc reads
the policy, and `x-api-key` is rewritten to the secret value before
the request leaves to `api.anthropic.com`. The agent's
`ANTHROPIC_API_KEY=clrk-injected-by-proxy` placeholder never reaches
upstream; ext_proc captures show the header redacted.

The EgressGateway's `Ready=False` condition with reason
`GatewayNotProgrammed` in `clrk dev` is cosmetic — the data plane
reaches the EG envoy via the assigned NodePort and credential
injection runs the same way as in production.
