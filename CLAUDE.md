# CLAUDE.md

## Project Overview

CLRK is a Kubernetes-native agent sandbox runtime. It runs LLM agents in isolated containers with fully intercepted networking.

- **Module**: `github.com/apoxy-dev/clrk`
- **API group**: `clrk.apoxy.dev/v1alpha1`

## Problem Statement

Develop framework-agnostic agents runtime that solves these problems:

* Auto-instrumented LLM/MCP/remote toolcall telemetry. All i/o in and out of sandbox is intercepted and logged.
* Governance. Prevent sandbox escape and apply governing policies for your organization.
* Attribution. Enable agent loops attribution to customer requests / triggers.
* Connectivity. Agents have (audited and authorized) access to internal organization services.
* Scalability. Enable simple and reliable scalability for both serverless and "on-prem" Kubernetes environments.
* Reliability. Simple and robust retries, backoffs, routing, load-scheding, etc.


## Architecture

Two binaries:

- `cmd/controller-manager` — Kubernetes controllers (TaskAgent, WorkerPool, etc.). Runs as a Deployment.
- `cmd/worker` — Sandbox runtime on each worker pod. Manages container lifecycle via libcontainer, OCI image pulling via ORAS, per-sandbox network namespaces with TAP devices.

Key packages:

- `api/clrk/v1alpha1/` — CRD types (TaskAgent, DaemonAgent, WorkerPool, AgentSandboxRevision, EgressGateway, etc.)
- `internal/controller/` — controller-runtime reconcilers
- `internal/worker/` — sandbox lifecycle (linux-only, requires CGO for libcontainer/nsenter)

## Build

This repo is built externally and build scripts and tests are not provided as part of this open-source distribution atm.

## Related Repos

- `apoxy-cli` (`github.com/apoxy-dev/apoxy`) — The worker's libcontainer code is ported from `pkg/edgefunc/runc/`. ORAS usage pattern from `pkg/apiserver/ingest/`.

## Code Style

- Log messages: Capitalized first word (e.g., `log.Info("Starting server")`)
- Error returns: Lowercase (e.g., `return fmt.Errorf("failed to connect: %w", err)`)
- Comments: Complete sentences with periods. Comments on "whys" and "how" when necessary, explaining complex logic or decisions.
- Go: Use idiomatic Go style, including naming conventions, error handling, and testing practices.

## Commit Style

- Use bracketed scopes in the subject line (e.g., `[docs] add docs workflow`).
- Keep the subject line concise and imperative.
- Wrap the body at a readable width similar to existing commits in this repo.
- Use the body to explain the concrete change and why it matters, not generic summaries.
- DO NOT include "Co-Authored-By: <clanker>" - nobody gives a fuck.

## CRDs

| CRD | Purpose |
|---|---|
| `TaskAgent` | Short-lived agent execution (request → sandbox → response) |
| `DaemonAgent` | Long-lived agent process with restart policy |
| `WorkerPool` | Fleet of worker pods (Deployment + Service) |
| `AgentSandboxRevision` | Immutable snapshot of sandbox spec, named `{agent}-{hash}` |
| `EgressGateway` | Transparent egress proxy with TLS termination modes |
| `EgressL4Route` | L4 egress routing rules |
| `MCPRoute` | MCP protocol routing |
| `AIProviderRoute` | AI provider-specific egress routing |

## Conventions

- CRD types live in `api/clrk/v1alpha1/types_*.go`. Run `make generate` after modifying (deepcopy).
- Controllers live in `internal/controller/`. One file per controller.
- Worker internals are linux-only (`//go:build linux`). Platform-agnostic types go in untagged files.
- The `Runtime` struct in `internal/worker/` is the only exported type — everything else is private.
