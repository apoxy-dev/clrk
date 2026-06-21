// The EgressGateway create/edit wizard (CLRK Dashboard `GatewayWizard`), built on
// console-core's reusable WizardShell. One component serves create (the list's
// "New gateway" / ⌘K) and edit (the detail's "Edit" + the YAML tray's Edit) — the
// shell flips on whether `object` is present.
//
// Faithful to the real clrk API (clrk.apoxy.dev/v1alpha1 EgressGatewaySpec): an
// EgressGateway owns its `defaultPolicy` and `listeners`, plus gateway-level
// telemetry/DNS — but NOT routes. Routes (MCPRoute / AIProviderRoute /
// EgressL4Route / HTTPRoute) are separate objects that attach by
// parentRef.sectionName, so they are created/edited on their own and the wizard
// stops at listeners — exactly as the apoxy Gateway wizard does for the same
// reason. The design's inline route/rule steps are an in-memory mock; here they'd
// produce a manifest the apiserver rejects, so they're intentionally omitted.
//
// Steps: Identity · Listeners (a master-detail collection) · Telemetry & DNS, plus
// the shell's built-in YAML step for raw authoring/review over the same draft.

import { useMemo } from 'react'
import {
  WizardShell,
  FormSection,
  Field,
  TextField,
  SelectField,
  SegField,
  FieldRow,
  type WizardProps,
  type WizardStep,
  type WizardCollection,
  ListenerGlyph,
  type WizardFormProps,
  type K8sObject,
} from '@apoxy/console-core'

// ── Typed view over the erased K8sObject (mirrors EgressGatewaySpec) ──────────
interface EgListenerTLS {
  mode?: 'Passthrough' | 'Terminate'
  caCertRef?: { name?: string }
}
interface EgListener {
  name?: string
  protocol?: string
  port?: number
  tls?: EgListenerTLS
}
interface EgressGatewaySpec {
  defaultPolicy?: string
  listeners?: EgListener[]
  otlp?: { endpoint?: string }
  dns?: { lookupFamily?: string }
  requestTimeout?: string
}
export interface EgressGatewayObject extends K8sObject {
  spec?: EgressGatewaySpec
}

// API enums (api/clrk/v1alpha1/types_egressgateway.go).
const POLICIES = ['deny-all', 'allow-all']
// UDP is a CRD enum value but the controller's ShapeForListener rejects it ("not
// yet supported"), so a UDP listener reconciles to Ready=False/InvalidSpec — leave
// it out until the controller shapes it.
const PROTOCOLS = ['HTTP', 'HTTPS', 'TLS', 'TCP']
// TLS handling only reaches the wire on HTTPS/TLS. The controller's ShapeForListener
// returns EgressShapeHTTP for protocol=HTTP without ever reading l.TLS, so a tls
// block on HTTP is a silent no-op; TCP/UDP reject a tls block outright.
const TLS_PROTOCOLS = new Set(['HTTPS', 'TLS'])
const DEFAULT_PORT: Record<string, number> = { HTTP: 80, HTTPS: 443, TLS: 443, TCP: 8080 }
const LOOKUP_FAMILIES = ['V4Preferred', 'V4Only', 'V6Only', 'Auto', 'All']

// Description has no spec field — the gateways list reads it from this annotation,
// so the wizard round-trips it there (egress-data.ts).
const DESC_ANNOTATION = 'clrk.apoxy.dev/description'

function emptyEgressGateway(): EgressGatewayObject {
  return {
    apiVersion: 'clrk.apoxy.dev/v1alpha1',
    kind: 'EgressGateway',
    metadata: { name: '', namespace: 'default' },
    // Seed one listener so a fresh draft satisfies the spec's MinItems=1 and the
    // create button isn't blocked on an empty list before the user does anything.
    spec: { defaultPolicy: 'deny-all', listeners: [{ name: 'https', protocol: 'HTTPS', port: 443 }] },
  }
}

export function EgressGatewayWizard({ entry, object, open, onClose, onSaved }: WizardProps) {
  const editing = !!object
  // Memoized so a keystroke re-render doesn't hand WizardShell a fresh steps
  // identity and defeat its allSteps memo.
  const steps = useMemo<WizardStep<EgressGatewayObject>[]>(
    () => [
      { id: 'identity', label: 'Identity', render: (p) => <IdentityStep {...p} editing={editing} /> },
      { id: 'listeners', label: 'Listeners', collection: listenersCollection },
      { id: 'telemetry', label: 'Telemetry & DNS', render: (p) => <TelemetryStep {...p} /> },
    ],
    [editing],
  )
  return (
    <WizardShell<EgressGatewayObject>
      entry={entry}
      object={object as EgressGatewayObject | undefined}
      open={open}
      onClose={onClose}
      onSaved={onSaved}
      emptyDraft={emptyEgressGateway}
      steps={steps}
    />
  )
}

// ── Step 1 · Identity ─────────────────────────────────────────────────────────
function IdentityStep({ draft, setDraft, editing }: WizardFormProps<EgressGatewayObject> & { editing: boolean }) {
  // metadata is required on K8sObject, but a raw YAML edit can drop it; guard the
  // reads so switching back to this step rebuilds metadata instead of crashing.
  const setMeta = (patch: Partial<EgressGatewayObject['metadata']>) =>
    setDraft({ ...draft, metadata: { ...draft.metadata, ...patch } })
  const setSpec = (patch: Partial<EgressGatewaySpec>) => setDraft({ ...draft, spec: { ...draft.spec, ...patch } })
  const setDescription = (v: string) => {
    const annotations = { ...(draft.metadata?.annotations ?? {}) }
    if (v) annotations[DESC_ANNOTATION] = v
    else delete annotations[DESC_ANNOTATION]
    const metadata = { ...draft.metadata }
    // Drop the map entirely when it empties so the manifest stays clean.
    if (Object.keys(annotations).length === 0) delete metadata.annotations
    else metadata.annotations = annotations
    setDraft({ ...draft, metadata })
  }
  return (
    <FormSection step="01" title="Identity" sub="Name, namespace, and the default action for unmatched traffic.">
      <FieldRow>
        <Field
          label="Name"
          required
          htmlFor="eg-name"
          hint={editing ? 'metadata.name — immutable after create.' : 'metadata.name — a DNS-1123 label.'}
        >
          <TextField id="eg-name" mono value={draft.metadata?.name ?? ''} disabled={editing} onChange={(v) => setMeta({ name: v })} placeholder="egress-gateway" />
        </Field>
        <Field label="Namespace" htmlFor="eg-ns" hint="Scopes the gateway and the routes that may attach to it.">
          <TextField id="eg-ns" mono value={draft.metadata?.namespace ?? ''} onChange={(v) => setMeta({ namespace: v })} placeholder="default" />
        </Field>
      </FieldRow>
      <Field
        label="Default policy"
        wide
        hint="spec.defaultPolicy — applies to traffic matching no attached route. deny-all blocks by default; allow-all passes it through."
      >
        <SegField value={draft.spec?.defaultPolicy ?? 'deny-all'} options={POLICIES} onChange={(v) => setSpec({ defaultPolicy: v })} />
      </Field>
      <Field label="Description" htmlFor="eg-desc" wide hint="Optional — shown in the gateways list.">
        <TextField id="eg-desc" value={draft.metadata?.annotations?.[DESC_ANNOTATION] ?? ''} onChange={setDescription} placeholder="What flows through this gateway" />
      </Field>
    </FormSection>
  )
}

// ── Step 2 · Listeners (collection: overview list + per-item editor) ───────────
const listenersCollection: WizardCollection<EgressGatewayObject> = {
  noun: 'listener',
  glyph: <ListenerGlyph />,
  items: (d) =>
    (d.spec?.listeners ?? []).map((l, i) => ({
      id: String(i),
      label: l.name || `listener-${i + 1}`,
      summary: `${l.protocol ?? 'HTTP'}${l.port != null ? `:${l.port}` : ' · any port'}${l.tls?.mode ? ` · ${l.tls.mode}` : ''}`,
    })),
  onAdd: (d) => {
    const ls = d.spec?.listeners ?? []
    // Names must be unique on the gateway; deriving from ls.length alone collides
    // after a remove (e.g. seed `https` + `listener-2`, drop the seed, add again),
    // so skip past any name already in use.
    const used = new Set(ls.map((x) => x.name))
    let n = ls.length + 1
    while (used.has(`listener-${n}`)) n++
    return {
      draft: { ...d, spec: { ...d.spec, listeners: [...ls, { name: `listener-${n}`, protocol: 'HTTPS', port: 443 }] } },
      focusId: String(ls.length),
    }
  },
  onRemove: (d, id) => {
    const i = Number(id)
    const ls = d.spec?.listeners ?? []
    return { ...d, spec: { ...d.spec, listeners: ls.filter((_, j) => j !== i) } }
  },
  renderItem: ({ draft, setDraft, itemId }) => <ListenerFields draft={draft} setDraft={setDraft} index={Number(itemId)} />,
}

function ListenerFields({ draft, setDraft, index }: Pick<WizardFormProps<EgressGatewayObject>, 'draft' | 'setDraft'> & { index: number }) {
  const listeners = draft.spec?.listeners ?? []
  const l = listeners[index]
  if (!l) return null
  const update = (patch: Partial<EgListener>) =>
    setDraft({ ...draft, spec: { ...draft.spec, listeners: listeners.map((x, j) => (j === index ? { ...x, ...patch } : x)) } })
  const tlsCapable = TLS_PROTOCOLS.has(l.protocol ?? 'HTTP')

  return (
    <>
      <FieldRow>
        <Field label="Name" htmlFor={`l-name-${index}`} hint="Unique on the gateway; routes attach via parentRef.sectionName.">
          <TextField id={`l-name-${index}`} mono value={l.name ?? ''} onChange={(v) => update({ name: v })} placeholder="https" />
        </Field>
        <Field label="Port" htmlFor={`l-port-${index}`} hint="Optional — leave blank to intercept all ports at this layer.">
          <TextField
            id={`l-port-${index}`}
            mono
            value={l.port != null ? String(l.port) : ''}
            onChange={(v) => {
              // Blank/whitespace/0/out-of-range => omit the port (intercept all).
              // Require a plain-digit string so Number() coercions ('1e3', '443.0',
              // '0x1bb') can't slip in a port the user never typed.
              const t = v.trim()
              const n = Number(t)
              update({ port: /^\d+$/.test(t) && n > 0 && n <= 65535 ? n : undefined })
            }}
            placeholder="443"
          />
        </Field>
      </FieldRow>
      <Field label="Protocol" wide hint="Selects the interception layer.">
        <SegField
          value={l.protocol ?? 'HTTP'}
          options={PROTOCOLS}
          onChange={(proto) => {
            const patch: Partial<EgListener> = { protocol: proto }
            // Move the port to the new protocol's default only when it still holds
            // the *old* protocol's default. A blank port (undefined = intercept all)
            // never equals a default, so it's preserved; a specific port the user
            // typed is preserved too.
            if (l.port != null && l.port === DEFAULT_PORT[l.protocol ?? 'HTTP']) patch.port = DEFAULT_PORT[proto]
            // TLS only reaches the wire on HTTPS/TLS — drop any tls block otherwise.
            if (!TLS_PROTOCOLS.has(proto)) patch.tls = undefined
            update(patch)
          }}
        />
      </Field>
      {tlsCapable && (
        <FieldRow>
          <Field
            label="TLS mode"
            htmlFor={`l-tls-${index}`}
            hint="Terminate inspects (MITM) and re-encrypts; Passthrough routes by SNI only."
          >
            <SelectField
              id={`l-tls-${index}`}
              value={l.tls?.mode ?? ''}
              options={[
                { value: '', label: 'None' },
                { value: 'Passthrough', label: 'Passthrough' },
                { value: 'Terminate', label: 'Terminate' },
              ]}
              onChange={(mode) => {
                if (mode === '') return update({ tls: undefined })
                const tls: EgListenerTLS = { mode: mode as EgListenerTLS['mode'] }
                // Carry the CA cert only into Terminate — Passthrough has no cert, so
                // switching away must not leave a stale caCertRef behind.
                if (mode === 'Terminate' && l.tls?.caCertRef) tls.caCertRef = l.tls.caCertRef
                update({ tls })
              }}
            />
          </Field>
          {l.tls?.mode === 'Terminate' && (
            <Field label="CA cert secret" htmlFor={`l-ca-${index}`} hint="spec…tls.caCertRef — Secret with the CA cert+key. Required for Terminate.">
              <TextField
                id={`l-ca-${index}`}
                mono
                value={l.tls?.caCertRef?.name ?? ''}
                onChange={(v) =>
                  // Spread the loaded ref so editing the name keeps any
                  // namespace/kind/group a SecretObjectReference carried.
                  update({ tls: { ...l.tls, mode: 'Terminate', caCertRef: v ? { ...l.tls?.caCertRef, name: v } : undefined } })
                }
                placeholder="egress-ca"
              />
            </Field>
          )}
        </FieldRow>
      )}
    </>
  )
}

// ── Step 3 · Telemetry & DNS (gateway-level optional knobs) ────────────────────
function TelemetryStep({ draft, setDraft }: WizardFormProps<EgressGatewayObject>) {
  const spec = draft.spec ?? {}
  // Prune empty nested objects so the manifest never carries `otlp: {}` / `dns: {}`.
  const patchSpec = (next: EgressGatewaySpec) => setDraft({ ...draft, spec: next })
  const setEndpoint = (v: string) => {
    const next = { ...spec }
    if (v) {
      next.otlp = { ...next.otlp, endpoint: v }
    } else if (next.otlp) {
      // OTLPLogsSinkSpec also carries headers (auth) and captureBody; clearing the
      // endpoint disables the external fan-out but must not drop those siblings.
      // The typed view only names `endpoint`, so prune on the runtime object.
      const otlp = { ...next.otlp } as Record<string, unknown>
      delete otlp.endpoint
      if (Object.keys(otlp).length === 0) delete next.otlp
      else next.otlp = otlp as EgressGatewaySpec['otlp']
    }
    patchSpec(next)
  }
  const setLookupFamily = (v: string) => {
    const next = { ...spec }
    if (v) next.dns = { ...next.dns, lookupFamily: v }
    else delete next.dns
    patchSpec(next)
  }
  const setTimeout = (v: string) => {
    const next = { ...spec }
    if (v) next.requestTimeout = v
    else delete next.requestTimeout
    patchSpec(next)
  }
  return (
    <FormSection step="03" title="Telemetry & DNS" sub="Optional gateway-level settings. Captured traffic always lands in the embedded ClickHouse; these shape the rest.">
      <Field label="OTLP endpoint" htmlFor="eg-otlp" wide hint="spec.otlp.endpoint — re-export captured request/response signals to this OTLP/HTTP collector.">
        <TextField id="eg-otlp" mono value={spec.otlp?.endpoint ?? ''} onChange={setEndpoint} placeholder="https://otel.example.com" />
      </Field>
      <FieldRow>
        <Field label="DNS lookup family" htmlFor="eg-dns" hint="spec.dns.lookupFamily — upstream resolver order. Defaults to V4Preferred.">
          <SelectField
            id="eg-dns"
            value={spec.dns?.lookupFamily ?? ''}
            options={[{ value: '', label: 'Default (V4Preferred)' }, ...LOOKUP_FAMILIES.map((f) => ({ value: f }))]}
            onChange={setLookupFamily}
          />
        </Field>
        <Field label="Request timeout" htmlFor="eg-timeout" hint="spec.requestTimeout — caps a single outbound request. Defaults to 5m.">
          <TextField id="eg-timeout" mono value={spec.requestTimeout ?? ''} onChange={setTimeout} placeholder="5m" />
        </Field>
      </FieldRow>
    </FormSection>
  )
}
