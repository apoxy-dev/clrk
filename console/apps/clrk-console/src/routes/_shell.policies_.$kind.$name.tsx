// Policy detail — a bespoke per-policy page at `/policies/<short>/<name>`. The
// trailing `_` on `policies_` un-nests this from the `/policies` list route so it
// renders directly inside the shell (its own page-head + tabs) rather than the
// generic resource detail. `<short>` is the CRD short name (cip/frp/edp/rlp/lp),
// which selects the kind; an optional `?ns=` pins the namespace when one name is
// reused across namespaces. It reads the policy plus the cluster's egress routes
// and gateways and resolves where the policy lands (policies-detail-data.ts).
// YAML view/edit and Delete are wired through console-core's real read/write
// helpers, gated on a SelfSubjectAccessReview like core's generic detail.

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
import { PolicyDetailView } from '../views/policies-detail'
import { mapPolicyDetail } from '../views/policies-detail-data'
import {
  POLICY_RESOURCE,
  POLICY_SHORT,
  type PolicyKind,
  type PolicyObj,
} from '../views/policies-data'
import { policyKindEntry } from '../registry'

const v1alpha1 = (resource: string): GVR => ({
  group: 'clrk.apoxy.dev',
  version: 'v1alpha1',
  resource,
})
const HTTP_GVR: GVR = {
  group: 'gateway.networking.k8s.io',
  version: 'v1',
  resource: 'httproutes',
}

// Short CRD name (cip/frp/edp/rlp/lp) → kind. The URL carries the short name so
// it stays stable and readable; an unknown short renders the not-found branch.
const KIND_BY_SHORT: Record<string, PolicyKind> = Object.fromEntries(
  (Object.entries(POLICY_SHORT) as Array<[PolicyKind, string]>).map(([kind, short]) => [
    short,
    kind,
  ]),
)

const tag = (items: K8sObject[] | undefined, kind: string) =>
  (items ?? []).map((o) => ({ ...o, kind }))

export const Route = createFileRoute('/_shell/policies_/$kind/$name')({
  component: PolicyDetailPage,
  validateSearch: (search: Record<string, unknown>): { ns?: string } => ({
    ns: typeof search.ns === 'string' ? search.ns : undefined,
  }),
})

function PolicyDetailPage() {
  const { kind: short, name } = Route.useParams()
  const { ns } = Route.useSearch()
  const navigate = useNavigate()

  const kind = KIND_BY_SHORT[short]
  const resource = kind ? POLICY_RESOURCE[kind] : 'credentialinjectionpolicies'
  const POLICY_GVR = v1alpha1(resource)
  const entry = kind ? policyKindEntry(kind) : null

  // The policy of this kind, plus every egress route and gateway it might bind
  // to. Fixed hook set (rules-of-hooks safe); the join happens in the mapper.
  const policies = useK8sList(POLICY_GVR)
  const gateways = useK8sList(v1alpha1('egressgateways'))
  const mcp = useK8sList(v1alpha1('mcproutes'))
  const ai = useK8sList(v1alpha1('aiproviderroutes'))
  const l4 = useK8sList(v1alpha1('egressl4routes'))
  const http = useK8sList(HTTP_GVR)

  const object = policies.data?.items?.find(
    (o) => o.metadata.name === name && (ns === undefined || o.metadata.namespace === ns),
  )

  const detail = useMemo(() => {
    if (!kind || !object) return null
    const routes = [
      ...tag(mcp.data?.items, 'MCPRoute'),
      ...tag(ai.data?.items, 'AIProviderRoute'),
      ...tag(l4.data?.items, 'EgressL4Route'),
      ...tag(http.data?.items, 'HTTPRoute'),
    ]
    return mapPolicyDetail(kind, object as PolicyObj, routes, gateways.data?.items ?? [])
  }, [kind, object, mcp.data, ai.data, l4.data, http.data, gateways.data])

  const del = useDeleteResource(POLICY_GVR)
  const [confirming, setConfirming] = useState(false)
  const [editingRaw, setEditingRaw] = useState(false)

  // Gate the mutating affordances on a SelfSubjectAccessReview, as core's generic
  // ResourceDetailView does: Edit is disabled and Delete hidden for a viewer who
  // lacks update/delete, instead of dead-ending at a 403.
  const canEdit = useCan('update', POLICY_GVR, {
    name,
    namespace: object?.metadata.namespace,
    enabled: !!entry,
  })
  const canDelete = useCan('delete', POLICY_GVR, {
    name,
    namespace: object?.metadata.namespace,
    enabled: !!object,
  })

  // Keep the YAML tray + confirm dialog mounted in every branch (stable position
  // after the body) so a server-side delete taking over the not-found branch
  // doesn't unmount an open editor and discard unsaved edits. Mirrors core
  // ResourceDetailView's `mounts` and the egress detail page.
  const mounts = entry ? (
    <>
      <YamlTray entry={entry} object={object} open={editingRaw} onClose={() => setEditingRaw(false)} />
      <ConfirmDialog
        open={confirming}
        title={`Delete ${kind} “${object?.metadata.name ?? name}”?`}
        body="This removes the policy. Traffic it governed reverts to the route's default behavior."
        confirmLabel="Delete"
        tone="danger"
        pending={del.isPending}
        error={del.error?.message ?? null}
        onConfirm={async () => {
          await del.remove(object?.metadata.name ?? name, {
            namespace: object?.metadata.namespace,
          })
          setConfirming(false)
          void navigate({ to: '/policies' })
        }}
        onCancel={() => setConfirming(false)}
      />
    </>
  ) : null

  if (!kind) {
    return <div className="viz-empty-state">Unknown policy kind “{short}”.</div>
  }
  if (policies.isLoading && !object) {
    return (
      <>
        <div className="viz-empty-state">Loading policy…</div>
        {mounts}
      </>
    )
  }
  if (!object || !detail) {
    return (
      <>
        <div className="viz-empty-state">
          {kind} “{name}” not found.
        </div>
        {mounts}
      </>
    )
  }

  const actions = entry ? (
    <>
      <YamlMenu entry={entry} object={object} onEditRaw={() => setEditingRaw(true)} />
      <Button
        size="sm"
        variant="secondary"
        disabled={!canEdit.allowed}
        onClick={() => setEditingRaw(true)}
      >
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
      <PolicyDetailView
        detail={detail}
        actions={actions}
        onOpenGateway={(gw) => void navigate({ to: '/egress/$gw', params: { gw } })}
      />
      {mounts}
    </>
  )
}
