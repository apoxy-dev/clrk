// Egress Gateways list — a bespoke view that shadows the generic resource splat
// (`_shell.$.tsx`) for `/egress`. A static route outranks the splat, so this
// renders instead of the generic list while the EgressGateway kind stays
// registered (for the rail item, breadcrumb, and ⌘K). It reads live
// EgressGateways plus the routes that attach to them (joined by parentRef) and
// maps them to the design's table. Detail (`/egress/<name>`) still falls through
// to the splat until the Miller detail view lands.

import { useMemo } from 'react'
import { createFileRoute, useRouter } from '@tanstack/react-router'
import { useK8sList, type GVR, type K8sObject } from '@apoxy/console-core'
import { EgressListView } from '../views/egress-list'
import { mapEgress } from '../views/egress-data'

const v1alpha1 = (resource: string): GVR => ({ group: 'clrk.apoxy.dev', version: 'v1alpha1', resource })
const EG_GVR = v1alpha1('egressgateways')
const MCP_GVR = v1alpha1('mcproutes')
const AI_GVR = v1alpha1('aiproviderroutes')
const L4_GVR = v1alpha1('egressl4routes')
// HTTPRoutes are gateway-api, not a clrk kind.
const HTTP_GVR: GVR = { group: 'gateway.networking.k8s.io', version: 'v1', resource: 'httproutes' }

export const Route = createFileRoute('/_shell/egress')({ component: EgressPage })

function EgressPage() {
  const router = useRouter()
  const gateways = useK8sList(EG_GVR)
  const mcp = useK8sList(MCP_GVR)
  const ai = useK8sList(AI_GVR)
  const l4 = useK8sList(L4_GVR)
  const http = useK8sList(HTTP_GVR)

  // Tag each route with its kind from the list it came from rather than relying
  // on per-item `kind` (real list items often omit it).
  const tag = (items: K8sObject[] | undefined, kind: string) => (items ?? []).map((o) => ({ ...o, kind }))
  const { rows, totals } = useMemo(
    () =>
      mapEgress(gateways.data?.items ?? [], [
        ...tag(mcp.data?.items, 'MCPRoute'),
        ...tag(ai.data?.items, 'AIProviderRoute'),
        ...tag(l4.data?.items, 'EgressL4Route'),
        ...tag(http.data?.items, 'HTTPRoute'),
      ]),
    [gateways.data, mcp.data, ai.data, l4.data, http.data],
  )

  return (
    <EgressListView
      rows={rows}
      totals={totals}
      isLoading={gateways.isLoading}
      onOpen={(id) => void router.navigate({ to: `/egress/${id}` as never })}
    />
  )
}
