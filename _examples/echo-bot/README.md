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

## Setup (one-time)

The `anthropic-credentials` Secret is **operator-applied**, never
committed to manifests. This is the architectural rule: API keys live
on the proxy side, never inside agent specs.

```bash
kubectl create secret generic anthropic-credentials \
  --from-literal=api-key="$ANTHROPIC_API_KEY"
```

## Run under `clrk dev`

```bash
clrk dev --apply _examples/echo-bot/manifests
```

## What you should see

- `kubectl get egressgateway echo-bot -o yaml` — Envoy Gateway infra
  (GatewayClass, EnvoyProxy, Gateway) ready.
- `kubectl get secret clrk-egressgateway-ca-echo-bot` — the per-EG
  MITM CA.
- `kubectl logs -l apiserver-control=controller-manager -n default \
   | grep extproc.summary` — one captured record per echo-bot poll:
  ```
  INFO extproc.summary provider=anthropic operation=chat
       model=claude-3-5-haiku-20241022
       input_tokens=12 output_tokens=… status=200
       agent.name=echo-bot route=anthropic
  ```
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
