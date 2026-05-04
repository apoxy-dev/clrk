# gemini-bot

Minimal end-to-end demo of the clrk MITM + L7 capture + provider
parser + **proxy-side credential injection** stack for **Google's
Gemini API** (the consumer-facing `generativelanguage.googleapis.com`
endpoint, OTel `gen_ai.system=google_genai`).

Mirrors `_examples/echo-bot/` (Anthropic). The only differences are
the upstream host, the API auth header, and the request/response
shape — the credtable + ext_proc + parser machinery is identical.

- A `DaemonAgent` issues one `POST .../v1beta/models/gemini-2.0-flash:generateContent`
  to `generativelanguage.googleapis.com` and exits. The supervisor
  respawns it (`RestartPolicy: Always`).
- The agent **never** holds the Gemini API key. The proxy injects
  it on the egress side via `CredentialInjectionPolicy`.
- The MITM Envoy terminates TLS (leaf signed by the per-EG CA),
  ext_proc captures the request/response, the Google parser fills
  `gen_ai.*` attributes, and the controller-manager logs a one-line
  summary.

## Run under `clrk dev`

The Gemini API key is **never** put on the agent. The proxy injects
it on egress via `CredentialInjectionPolicy`. `clrk dev --secret`
reads the key from the host shell env, server-side-applies it as a
Secret in `default`, and only then runs `--apply` on the manifests:

```bash
GEMINI_API_KEY=AIza... clrk dev \
  --apply _examples/gemini-bot/manifests \
  --secret google-credentials=GEMINI_API_KEY:api-key
```

The `--secret` argument shape is `NAME=ENVVAR[:KEY]`. Without `:KEY`
the key defaults to `ENVVAR` lowercased with `_` → `-`. Multiple
`--secret` flags sharing a `NAME` merge into one Secret.

> Get a free Gemini API key at <https://aistudio.google.com/apikey>.
> The free tier is enough for the demo loop.

## What you should see

- `kubectl get egressgateway gemini-bot -o yaml` — Envoy Gateway infra
  (GatewayClass, EnvoyProxy, Gateway) ready.
- `kubectl get secret clrk-egressgateway-ca-gemini-bot` — the per-EG
  MITM CA.
- TUI sidebar shows two new panes: `otel-logs` and `otel-traces`.
  After the first gemini-bot cycle, `otel-logs` carries one line per
  call:
  ```
  POST generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent 200 540ms
       provider=google_genai model=gemini-2.0-flash input_tokens=12
       output_tokens=24 route=default/google trace=…
  ```
  `otel-traces` carries the matching span. The same line *no longer*
  appears in the controller-manager pane — the dev OTLP receiver
  handles it instead of slogSink.
- `kubectl logs -l clrk.apoxy.dev/agent=gemini-bot` — `cat /tmp/resp.json`
  prints Gemini's response body.

## Verify the agent never sees the key

```bash
# Get the running sandbox PID inside the worker container, dump its env.
# Expect zero matches:
kubectl exec deploy/clrk-worker -- sh -c \
  'pid=$(pgrep -f "POST https://generativelanguage" | head -1); \
   cat /proc/$pid/environ | tr "\0" "\n" | grep -i api'
```

The proxy adds `x-goog-api-key` after the request leaves the agent's
network namespace. Captured ext_proc records show the header value as
`<redacted>` — credentials never reach OTLP.

## Smuggle test (optional)

Edit `manifests/daemonagent.yaml` to add `-H 'x-goog-api-key: smuggled'`
to the curl. Re-apply. Expect:

- The request reaching Gemini still carries the **policy-injected**
  value (overwrite — policy wins, not the agent).
- controller-manager logs a `slog.Warn` flagging the agent-supplied
  header.
