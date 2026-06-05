# CLRK

CLRK is a Kubernetes-native agent sandbox runtime. It runs LLM agents in
isolated containers with fully intercepted networking — auto-instrumented
LLM/MCP/tool-call telemetry, governance and sandbox-escape prevention,
request attribution, audited connectivity to internal services, and
serverless/on-prem scalability.

- **Module**: `github.com/apoxy-dev/clrk`
- **API group**: `clrk.apoxy.dev/v1alpha1`

## License

CLRK is licensed under the **GNU Affero General Public License v3.0**
(AGPL-3.0); see [`LICENSE`](LICENSE).

**Exception:** the [`api/`](api) and [`client/`](client) directories are
licensed under the **Apache License 2.0**; see [`api/LICENSE`](api/LICENSE)
and [`client/LICENSE`](client/LICENSE). These cover the public API types
(`api/clrk/v1alpha1`) and the generated Kubernetes client/SDK, so they can be
imported and used without AGPL copyleft obligations.
