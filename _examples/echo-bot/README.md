# echo-bot

Minimal end-to-end demo of the clrk MITM + L7 capture + provider
parser + **proxy-side credential injection** stack:

- A `DaemonAgent` issues one `POST /v1/messages` to
  `api.anthropic.com` and exits. The supervisor respawns it
  (`RestartPolicy: OnFailure`).
- The agent **never** holds the Anthropic API key. The proxy injects
  it on the egress side via `CredentialInjectionPolicy`.
- The MITM Envoy terminates TLS (leaf signed by the per-EG CA),
  ext_proc captures the request/response, the Anthropic parser fills
  `gen_ai.*` attributes, and the controller-manager logs a one-line
  summary.

## Run under `clrk dev`

The Anthropic API key is **never** put on the agent. The proxy injects
it on egress via `CredentialInjectionPolicy`. `clrk dev --secret`
reads the key from the host shell env, server-side-applies it as a
Secret in `default`, and only then runs `--apply` on the manifests:

```bash
ANTHROPIC_API_KEY=sk-ant-... clrk dev \
  --apply _examples/echo-bot/manifests \
  --secret anthropic-credentials=ANTHROPIC_API_KEY:api-key
```

The `--secret` argument shape is `NAME=ENVVAR[:KEY]`. Without `:KEY`
the key defaults to `ENVVAR` lowercased with `_` → `-`. Multiple
`--secret` flags sharing a `NAME` merge into one Secret.

## What you should see

- `kubectl get egressgateway echo-bot -o yaml` — Envoy Gateway infra
  (GatewayClass, EnvoyProxy, Gateway) ready.
- `kubectl get secret clrk-egressgateway-ca-echo-bot` — the per-EG
  MITM CA.
- TUI sidebar shows two new panes: `otel-logs` and `otel-traces`.
  After the first echo-bot cycle, `otel-logs` carries one line per
  call:
  ```
  POST api.anthropic.com/v1/messages 200 540ms provider=anthropic
       model=claude-haiku-4-5 input_tokens=12 output_tokens=24
       route=default/anthropic trace=…
  ```
  `otel-traces` carries the matching span. The same line *no longer*
  appears in the controller-manager pane — the dev OTLP receiver
  handles it instead of slogSink.
- `kubectl logs -l clrk.apoxy.dev/agent=echo-bot` — `cat /tmp/resp.json`
  prints Anthropic's response body.

## Verify the agent never sees the key

```bash
# Get the running sandbox PID inside the worker container, dump its env.
# Expect zero matches:
kubectl exec deploy/clrk-worker -- sh -c \
  'pid=$(pgrep -f "POST https://api.anthropic.com" | head -1); \
   cat /proc/$pid/environ | tr "\0" "\n" | grep -i api'
```

The proxy adds `x-api-key` after the request leaves the agent's
network namespace. Captured ext_proc records show the header value as
`<redacted>` — credentials never reach OTLP.

## Smuggle test (optional)

Edit `manifests/daemonagent.yaml` to add `-H 'x-api-key: smuggled'`
to the curl. Re-apply. Expect:

- The request reaching Anthropic still carries the **policy-injected**
  value (overwrite — policy wins, not the agent).
- controller-manager logs a `slog.Warn` flagging the agent-supplied
  header.
