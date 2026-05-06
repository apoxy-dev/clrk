# cron-bot

Minimal demo of the cron-triggered `TaskAgent` flow:

- A `TaskAgent` declares `spec.schedule: "*/1 * * * *"` and a JSON
  `spec.scheduleInput`.
- The controller-manager's `TaskAgentCronReconciler` registers the
  schedule, fires it on every due slot under leader election, and
  POSTs the body to the in-cluster Gateway URL the
  `TaskAgentIngressReconciler` materializes for the agent.
- Each fire updates `status.lastScheduleTime` /
  `status.nextScheduleTime` and the `Scheduled` condition.

Cron and HTTP triggering coexist on the same TaskAgent: the
schedule does not disable HTTP. `MaxConcurrent` (if set) caps
running executions across both paths.

## Run under `clrk dev`

`clrk dev` enables `--ingress-controller` by default — the cron
firer is gated on it because the HTTP invoker targets the
ingress-managed Gateway URL.

```bash
clrk dev --apply _examples/cron-bot/manifests
```

## What you should see

After ~60 s:

```bash
kubectl get ta cron-bot
# NAME       IMAGE           POOL      LATEST READY   ACTIVE  SCHEDULE      LAST RUN  AGE
# cron-bot   busybox:1.36    default   cron-bot-...   0       */1 * * * *   30s       2m
```

Status detail:

```bash
kubectl get ta cron-bot -o yaml | yq '.status'
# conditions:
# - type: Scheduled
#   status: "True"
#   reason: ScheduleRegistered
#   message: 'Last fire at 2026-05-06T...Z succeeded'
# lastScheduleTime: "2026-05-06T..."
# nextScheduleTime: "2026-05-07T..."
```

The TUI's `otel-logs` pane carries one ext_proc record per fire
(POST to the agent's Gateway URL), and `kubectl logs -l
clrk.apoxy.dev/agent=cron-bot` shows each invocation's stdout —
`cron fired at <timestamp>` followed by the JSON body the
controller-manager POSTed.

## Edit the schedule live

```bash
kubectl patch ta cron-bot --type=merge -p '{"spec":{"schedule":"*/5 * * * *"}}'
kubectl get ta cron-bot -o jsonpath='{.status.nextScheduleTime}{"\n"}'
# Jumps forward by ~5 min.
```

Clear the schedule entirely → the cron entry is dropped and the
`Scheduled` condition flips to `False / NotScheduled`. HTTP
triggers continue to work.

```bash
kubectl patch ta cron-bot --type=json -p '[{"op":"remove","path":"/spec/schedule"}]'
```

## Missed-fire policy

At-most-once. If the leader controller-manager is down across N
scheduled slots, only the next due slot fires — no backfill. Layer
a queue on top of HTTP triggers if you need at-least-once
delivery.
