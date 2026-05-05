# multi-listener-egress

Demonstrates the **per-listener-port protocol split**: one
`EgressGateway` exposes two listeners on distinct shapes (HTTPS-MITM
TLS-Terminate + plain TCP) and the worker's `IdentityDialer` steers
each outbound dial to the correct listener by destination port.

The same `DaemonAgent` issues two flavors of traffic in a loop and
each one lands on a different listener with a different chain shape:

| Flow                                       | Listener        | Port match     | Chain shape                                                                            | Captured as          |
| ------------------------------------------ | --------------- | -------------- | -------------------------------------------------------------------------------------- | -------------------- |
| `curl https://api.openai.com/v1/models`    | `egress-mitm`   | catch-all      | HCM + leaf cert from per-EG CA + ext_proc (L7)                                         | one L7 record / call |
| `echo ping \| nc -w 2 clrk-k3s 30099`      | `egress-tcp`    | `port: 30099`  | `network_ext_proc` + `tcp_proxy` → `ORIGINAL_DST` cluster, no `TransportSocket` (L4)   | one L4 record / call |

## Run under `clrk dev`

```bash
clrk dev --apply _examples/multi-listener-egress/manifests
```

No secrets needed — the OpenAI call uses a bogus token (the L7
capture is the point, not the auth outcome).

## What you should see

- `kubectl get egressgateway multi-listener-egress -o yaml` — both
  listeners surface under `status.listeners`, each with its own
  `port` (18080, 18081) and `backendAddress` (`clrk-k3s:<NodePort>`).
- `kubectl get gateway clrk-eg-multi-listener-egress -o yaml` — the
  underlying Envoy Gateway resource has two listeners with section
  names `egress-mitm--tls-terminate` and `egress-tcp--tcp` (the
  shape suffix is what the egextension reads to choose the chain
  rewrite).
- `kubectl get svc -n envoy-gateway-system -l
  gateway.envoyproxy.io/owning-gateway-name=clrk-eg-multi-listener-egress`
  — the EG-managed Service has TWO ports (18080 + 18081), each with
  its own NodePort.
- The TUI's `otel-logs` pane shows two record shapes alternating
  every loop iteration:
  ```
  GET api.openai.com/v1/models 401 ...        # L7, from the MITM listener
  egress.tcp agent=default/multi-listener-egress dst=clrk-k3s bytes_up=5B dur=...  # L4, from the TCP listener
  ```

## Verify per-listener routing

The worker's `IdentityDialer.pickBackend` picks the listener by
destination port. Tail the worker log to see each dial labeled with
`backend.name` and `backend.shape`:

```bash
kubectl logs -l app=clrk-worker --tail=50 -f \
  | grep -E '"egress dial".*multi-listener-egress'
```

Expect lines alternating between:

- `dst=...:443  backend.name=egress-mitm  backend.shape=tls-terminate`
- `dst=...:30099  backend.name=egress-tcp  backend.shape=tcp`

If both shapes show up, the per-listener split is working end-to-end:
the reconciler allocated two distinct ports, the egextension rewrote
each chain per shape, and the worker dispatches per dial.

## Caveat — TLS-passthrough is currently broken

This example uses TLS-Terminate (working) + plain TCP (working). A
third shape — TLS-passthrough (`mode: Passthrough`) — is admission-
accepted but doesn't function end-to-end yet because the chain reuses
the HCM-side dynamic_forward_proxy cluster, which TLS-wraps upstream
connections and double-wraps an already-TLS agent stream. Tracked in
`apoxy-cloud//docs/clrk-improvements.md` (entry: "TLS-passthrough
chain TLS-wraps upstream").
