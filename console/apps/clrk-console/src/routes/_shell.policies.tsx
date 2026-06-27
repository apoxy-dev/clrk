// Policies list — a bespoke view that shadows the generic resource splat
// (`_shell.$.tsx`) for `/policies`. A static route outranks the splat, so this
// renders the grouped-by-kind table while the combined `Policy` rail item stays
// registered (for the sidebar, breadcrumb, and ⌘K). It reads the five egress
// policy kinds live (one LIST+watch each) and folds them into uniform rows
// (policies-data.ts). Clicking a row opens that policy's bespoke detail page
// (`/policies/<short>/<name>`, pinned to its namespace).

import { useMemo } from 'react'
import { createFileRoute, useNavigate } from '@tanstack/react-router'
import { useK8sList, type GVR } from '@apoxy/console-core'
import { PoliciesListView } from '../views/policies-list'
import {
  mapPolicies,
  POLICY_RESOURCE,
  POLICY_SHORT,
  type PolicyKind,
  type PolicyObj,
  type PolicyRow,
} from '../views/policies-data'

const v1alpha1 = (resource: string): GVR => ({
  group: 'clrk.apoxy.dev',
  version: 'v1alpha1',
  resource,
})

export const Route = createFileRoute('/_shell/policies')({ component: PoliciesPage })

function PoliciesPage() {
  const navigate = useNavigate()

  // One live LIST+watch per policy kind. The hooks are called unconditionally
  // in a fixed order, so this stays rules-of-hooks safe.
  const cip = useK8sList(v1alpha1(POLICY_RESOURCE.CredentialInjectionPolicy))
  const frp = useK8sList(v1alpha1(POLICY_RESOURCE.FallbackRoutingPolicy))
  const edp = useK8sList(v1alpha1(POLICY_RESOURCE.EgressDenyPolicy))
  const rlp = useK8sList(v1alpha1(POLICY_RESOURCE.RateLimitPolicy))
  const lp = useK8sList(v1alpha1(POLICY_RESOURCE.LoggingPolicy))

  const byKind = useMemo<Record<PolicyKind, PolicyObj[]>>(
    () => ({
      CredentialInjectionPolicy: (cip.data?.items ?? []) as PolicyObj[],
      FallbackRoutingPolicy: (frp.data?.items ?? []) as PolicyObj[],
      EgressDenyPolicy: (edp.data?.items ?? []) as PolicyObj[],
      RateLimitPolicy: (rlp.data?.items ?? []) as PolicyObj[],
      LoggingPolicy: (lp.data?.items ?? []) as PolicyObj[],
    }),
    [cip.data, frp.data, edp.data, rlp.data, lp.data],
  )

  const rows = useMemo(
    () => mapPolicies(Object.entries(byKind).map(([kind, items]) => ({ kind: kind as PolicyKind, items }))),
    [byKind],
  )

  const isLoading =
    cip.isLoading || frp.isLoading || edp.isLoading || rlp.isLoading || lp.isLoading

  const onOpen = (row: PolicyRow) =>
    void navigate({
      to: '/policies/$kind/$name',
      params: { kind: POLICY_SHORT[row.kind], name: row.name },
      search: { ns: row.namespace },
    })

  return <PoliciesListView rows={rows} isLoading={isLoading} onOpen={onOpen} />
}
