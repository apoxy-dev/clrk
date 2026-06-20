// Egress Gateway detail — a bespoke Miller-columns view at `/egress/<name>`.
// The trailing `_` on `egress_` un-nests this from the `/egress` list route, so it
// renders directly inside the shell (its own page-head + tabs) rather than the
// generic resource detail. It reads the EgressGateway plus the routes and policies
// that attach to it (joined by parentRef/targetRef) and maps them into the Miller
// hierarchy (egress-detail-data.ts). YAML view/edit and Delete are wired through
// console-core's real read/write helpers.

import { useMemo, useState } from 'react'
import { createFileRoute, useNavigate } from '@tanstack/react-router'
import {
  Button,
  ConfirmDialog,
  YamlMenu,
  YamlTray,
  useDeleteResource,
  useK8sList,
  type GVR,
  type K8sObject,
} from '@apoxy/console-core'
import { EgressDetailView } from '../views/egress-detail'
import { mapEgressDetail } from '../views/egress-detail-data'
import { registry } from '../registry'

const v1alpha1 = (resource: string): GVR => ({
  group: 'clrk.apoxy.dev',
  version: 'v1alpha1',
  resource,
})
const EG_GVR = v1alpha1('egressgateways')
const HTTP_GVR: GVR = {
  group: 'gateway.networking.k8s.io',
  version: 'v1',
  resource: 'httproutes',
}

const tag = (items: K8sObject[] | undefined, kind: string) =>
  (items ?? []).map((o) => ({ ...o, kind }))

export const Route = createFileRoute('/_shell/egress_/$gw')({
  component: EgressDetailPage,
})

function EgressDetailPage() {
  const { gw } = Route.useParams()
  const navigate = useNavigate()
  const entry = registry.byPath('egress')

  // One managed list per kind (fixed set — rules-of-hooks safe); the gateway is
  // found in the EG list and the routes/policies are joined by parentRef.
  const gateways = useK8sList(EG_GVR)
  const mcp = useK8sList(v1alpha1('mcproutes'))
  const ai = useK8sList(v1alpha1('aiproviderroutes'))
  const l4 = useK8sList(v1alpha1('egressl4routes'))
  const http = useK8sList(HTTP_GVR)
  const cred = useK8sList(v1alpha1('credentialinjectionpolicies'))
  const ratelimit = useK8sList(v1alpha1('ratelimitpolicies'))
  const logging = useK8sList(v1alpha1('loggingpolicies'))
  const deny = useK8sList(v1alpha1('egressdenypolicies'))

  const gateway = gateways.data?.items?.find((o) => o.metadata.name === gw)

  const detail = useMemo(() => {
    if (!gateway) return null
    const routes = [
      ...tag(mcp.data?.items, 'MCPRoute'),
      ...tag(ai.data?.items, 'AIProviderRoute'),
      ...tag(l4.data?.items, 'EgressL4Route'),
      ...tag(http.data?.items, 'HTTPRoute'),
    ]
    const policies = [
      ...tag(cred.data?.items, 'CredentialInjectionPolicy'),
      ...tag(ratelimit.data?.items, 'RateLimitPolicy'),
      ...tag(logging.data?.items, 'LoggingPolicy'),
      ...tag(deny.data?.items, 'EgressDenyPolicy'),
    ]
    return mapEgressDetail(gateway, routes, policies)
  }, [
    gateway,
    mcp.data,
    ai.data,
    l4.data,
    http.data,
    cred.data,
    ratelimit.data,
    logging.data,
    deny.data,
  ])

  const del = useDeleteResource(EG_GVR)
  const [confirming, setConfirming] = useState(false)
  const [yamlOpen, setYamlOpen] = useState(false)

  if (gateways.isLoading && !gateway) {
    return <div className="viz-empty-state">Loading egress gateway…</div>
  }
  if (!gateway || !detail) {
    return (
      <div className="viz-empty-state">Egress gateway “{gw}” not found.</div>
    )
  }

  const actions = entry ? (
    <>
      <YamlMenu entry={entry} object={gateway} />
      <Button size="sm" variant="secondary" onClick={() => setYamlOpen(true)}>
        Edit
      </Button>
      <Button size="sm" variant="danger" onClick={() => setConfirming(true)}>
        Delete
      </Button>
      <YamlTray
        entry={entry}
        object={gateway}
        open={yamlOpen}
        onClose={() => setYamlOpen(false)}
      />
      <ConfirmDialog
        open={confirming}
        title={`Delete EgressGateway “${gateway.metadata.name}”?`}
        body="This removes the gateway and stops intercepting its egress traffic."
        confirmLabel="Delete"
        tone="danger"
        pending={del.isPending}
        error={del.error?.message ?? null}
        onConfirm={async () => {
          await del.remove(gateway.metadata.name ?? '', {
            namespace: gateway.metadata.namespace,
          })
          setConfirming(false)
          void navigate({ to: '/egress' })
        }}
        onCancel={() => setConfirming(false)}
      />
    </>
  ) : null

  return <EgressDetailView detail={detail} actions={actions} />
}
