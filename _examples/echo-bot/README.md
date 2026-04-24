# echo-bot

Minimal DaemonAgent demonstrating the MITM + L7 logging pipeline. Every
5 seconds the bot GETs `https://httpbin.org/anything`; with an
`EgressGateway` attached, the TLS handshake terminates on clrk's Envoy
Gateway (leaf signed by the per-EG CA), `ext_proc` records the
request/response headers + body, and the gateway re-encrypts to the
real upstream.

## Run under `clrk dev`

```
clrk dev --apply _examples/echo-bot/manifests
```

or point `clrk dev` at the manifests dir after the controllers are up.

## What you should see

- `kubectl get egressgateway echo-bot -o yaml` — status shows the
  Envoy Gateway infrastructure (GatewayClass, EnvoyProxy, Gateway)
  ready.
- `kubectl get secret clrk-egressgateway-ca-echo-bot` — the per-EG
  MITM CA.
- `kubectl logs -l apiserver-control=controller-manager -n default |
   grep 'clrk egress HTTP transaction'` — one captured record per
  echo-bot poll, with `agent.name=echo-bot`, `req.authority=httpbin.org`,
  `resp.status=200`.
