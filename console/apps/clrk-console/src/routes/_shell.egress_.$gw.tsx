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
  useCan,
  useDeleteResource,
  useK8sList,
  type GVR,
  type K8sObject,
} from '@apoxy/console-core'
import { EgressDetailView } from '../views/egress-detail'
import { mapEgressDetail } from '../views/egress-detail-data'
import { EgressGatewayWizard } from '../views/egress-gateway-wizard'
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
  const [editing, setEditing] = useState(false) // structured wizard
  const [editingRaw, setEditingRaw] = useState(false) // raw YAML editor

  // Gate the mutating affordances on a SelfSubjectAccessReview, as core's generic
  // ResourceDetailView does: Edit is disabled and Delete hidden for a viewer who
  // lacks update/delete, instead of dead-ending at a 403.
  const canEdit = useCan('update', EG_GVR, { name: gw, namespace: gateway?.metadata.namespace, enabled: !!entry })
  const canDelete = useCan('delete', EG_GVR, { name: gw, namespace: gateway?.metadata.namespace, enabled: !!gateway })

  // The wizard / YAML tray / confirm dialog are kept mounted in every branch (at a
  // stable position after the body) so that if the gateway is deleted on the server
  // while one is open, the not-found branch taking over doesn't unmount it and
  // discard the user's unsaved edits — WizardShell preserves the buffer across a
  // transient object===undefined. Mirrors core ResourceDetailView's `mounts`.
  const mounts = entry ? (
    <>
      <EgressGatewayWizard entry={entry} object={gateway} open={editing} onClose={() => setEditing(false)} />
      <YamlTray entry={entry} object={gateway} open={editingRaw} onClose={() => setEditingRaw(false)} />
      <ConfirmDialog
        open={confirming}
        title={`Delete EgressGateway “${gateway?.metadata.name ?? gw}”?`}
        body="This removes the gateway and stops intercepting its egress traffic."
        confirmLabel="Delete"
        tone="danger"
        pending={del.isPending}
        error={del.error?.message ?? null}
        onConfirm={async () => {
          await del.remove(gateway?.metadata.name ?? gw, {
            namespace: gateway?.metadata.namespace,
          })
          setConfirming(false)
          void navigate({ to: '/egress' })
        }}
        onCancel={() => setConfirming(false)}
      />
    </>
  ) : null

  if (gateways.isLoading && !gateway) {
    return (
      <>
        <div className="viz-empty-state">Loading egress gateway…</div>
        {mounts}
      </>
    )
  }
  if (!gateway || !detail) {
    return (
      <>
        <div className="viz-empty-state">Egress gateway “{gw}” not found.</div>
        {mounts}
      </>
    )
  }

  const actions = entry ? (
    <>
      {/* Two edit paths: the header "Edit" button opens the structured wizard;
          the YAML menu's viewer offers an Edit hand-off (core ManifestTray) that
          drops into the raw YAML editor for text-level changes. */}
      <YamlMenu entry={entry} object={gateway} onEditRaw={() => setEditingRaw(true)} />
      <Button size="sm" variant="secondary" disabled={!canEdit.allowed} onClick={() => setEditing(true)}>
        Edit
      </Button>
      {canDelete.allowed && (
        <Button size="sm" variant="danger" onClick={() => setConfirming(true)}>
          Delete
        </Button>
      )}
    </>
  ) : null

  return (
    <>
      <EgressDetailView detail={detail} actions={actions} />
      {mounts}
    </>
  )
}
