// Invocation detail redirect. An Invocation has no standalone page in the
// console -- its whole story is the owning agent's trace panel. This bespoke
// route shadows the generic splat for `/invocations/<id>`, resolves the agent
// from the Invocation's `spec.parentRef`, and redirects to that agent's
// Interaction tab with the invocation pre-selected (`?inv=<id>`). Both the
// Overview "Recent invocations" rows and the generic Invocations list land here.

import { createFileRoute, Navigate } from '@tanstack/react-router'
import { useK8sObject, type GVR, type K8sObject } from '@apoxy/console-core'

const INVOCATION_GVR: GVR = {
  group: 'clrk.apoxy.dev',
  version: 'v1alpha1',
  resource: 'invocations',
}

export const Route = createFileRoute('/_shell/invocations_/$id')({
  component: InvocationRedirect,
})

interface InvocationObj extends K8sObject {
  spec?: { parentRef?: { kind?: string; name?: string } }
}

function InvocationRedirect() {
  const { id } = Route.useParams()
  const invocation = useK8sObject<InvocationObj>(INVOCATION_GVR, id)
  const parent = invocation.data?.spec?.parentRef?.name

  if (invocation.isLoading && !invocation.data) {
    return <div className="viz-empty-state">Loading invocation…</div>
  }
  // The agent's trace panel selects an invocation by its id (== the Invocation
  // CR name); hand it the id so the right invocation opens.
  if (parent) {
    return <Navigate to={`/agents/${parent}` as never} search={{ inv: id } as never} replace />
  }
  // No owning agent (or the invocation is gone) -- fall back to the list.
  return <Navigate to={'/invocations' as never} replace />
}
