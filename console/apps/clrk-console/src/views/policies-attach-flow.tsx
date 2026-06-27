// Attachment graph — the React Flow view behind the policy detail's Attachment
// tab, ported from the CLRK Dashboard design's `attach-flow.html` (which ran
// reactflow@11 in an isolated iframe) onto the bundled `@xyflow/react` (v12,
// React 19). The graph reads a resolved AttachPayload (policies-detail-data.ts):
// a policy node, an optional catch-all gateway hop, and one route node per route
// the policy reaches — edges styled by relationship (direct / inherited /
// reference / unresolved). Read-only: no drag, no connect, no scroll-zoom.

import { useMemo } from 'react'
import {
  Background,
  Controls,
  Handle,
  Position,
  ReactFlow,
  type Edge,
  type Node,
  type NodeProps,
  type NodeTypes,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { kindShort } from './egress-data'
import { attachPayload, type AttachVia, type PolicyDetail } from './policies-detail-data'

// ── custom nodes ─────────────────────────────────────────────────────────────

interface PolicyNodeData extends Record<string, unknown> {
  name: string
  short: string
}
interface GatewayNodeData extends Record<string, unknown> {
  name: string
}
interface RouteNodeData extends Record<string, unknown> {
  name: string
  host: string
  kind: string
  via: AttachVia
  section?: string
}

function PolicyNode({ data }: NodeProps) {
  const d = data as PolicyNodeData
  return (
    <div className="pf-node pf-policy">
      <div className="pf-policy-name">{d.name}</div>
      <div className="pf-policy-kind">{d.short}</div>
      <Handle type="source" position={Position.Right} />
    </div>
  )
}

function GatewayNode({ data }: NodeProps) {
  const d = data as GatewayNodeData
  return (
    <div className="pf-node pf-gw">
      <div className="pf-gw-name">{d.name}</div>
      <div className="pf-gw-sub">EgressGateway · catch-all</div>
      <Handle type="target" position={Position.Left} />
      <Handle type="source" position={Position.Right} />
    </div>
  )
}

function RouteNode({ data }: NodeProps) {
  const d = data as RouteNodeData
  const mod =
    d.via === 'inherited'
      ? ' pf-route--inherited'
      : d.via === 'reference'
        ? ' pf-route--reference'
        : d.via === 'unresolved'
          ? ' pf-route--unresolved'
          : ''
  return (
    <div className={'pf-node pf-route' + mod}>
      <div className="pf-route-body">
        <div className="pf-route-host">{d.host || d.name}</div>
        <div className="pf-route-meta">
          <span>{d.name}</span>
          {d.section && <span className="pf-route-sec">§ {d.section}</span>}
        </div>
      </div>
      {d.kind && <span className="pf-kindtag">{kindShort(d.kind)}</span>}
      <Handle type="target" position={Position.Left} />
    </div>
  )
}

const nodeTypes: NodeTypes = { policy: PolicyNode, gateway: GatewayNode, route: RouteNode }

// Edge stroke per relationship — solid ink for direct, dashed + tinted for the
// looser bindings, matching the design's STROKE table.
const STROKE: Record<AttachVia, React.CSSProperties> = {
  direct: { stroke: 'var(--apx-ink)', strokeWidth: 1.3 },
  inherited: { stroke: 'var(--apx-fog)', strokeWidth: 1, strokeDasharray: '4 4' },
  reference: { stroke: 'var(--apx-blue)', strokeWidth: 1.1, strokeDasharray: '4 4' },
  unresolved: { stroke: 'var(--apx-amber)', strokeWidth: 1, strokeDasharray: '4 4' },
}

// ── deterministic layout ─────────────────────────────────────────────────────
// Columns: policy at x=0, an optional gateway hop, then a stacked column of
// routes. Mirrors the design's buildGraph so the graph reads left-to-right.

function buildGraph(detail: PolicyDetail): { nodes: Node[]; edges: Edge[] } {
  const c = attachPayload(detail)
  const nodes: Node[] = []
  const edges: Edge[] = []
  const n = Math.max(c.targets.length, 1)
  const gap = 62
  const blockH = (n - 1) * gap
  const gwX = 300
  const routeX = c.catchAll ? 560 : 320
  const centerY = blockH / 2

  nodes.push({
    id: 'policy',
    type: 'policy',
    position: { x: 0, y: centerY - 27 },
    data: { name: c.name, short: c.short },
    sourcePosition: Position.Right,
    draggable: false,
  })

  let fromId = 'policy'
  if (c.catchAll && c.gateway) {
    nodes.push({
      id: 'gw',
      type: 'gateway',
      position: { x: gwX, y: centerY - 24 },
      data: { name: c.gateway },
      sourcePosition: Position.Right,
      targetPosition: Position.Left,
      draggable: false,
    })
    edges.push({ id: 'e-policy-gw', source: 'policy', target: 'gw', label: c.mode, style: STROKE.direct })
    fromId = 'gw'
  }

  c.targets.forEach((t, i) => {
    const id = 'r' + i
    nodes.push({
      id,
      type: 'route',
      position: { x: routeX, y: i * gap },
      data: { name: t.name, host: t.host, kind: t.kind, via: t.via, section: t.section },
      targetPosition: Position.Left,
      draggable: false,
    })
    edges.push({
      id: 'e-' + id,
      source: fromId,
      target: id,
      label: i === 0 ? (c.catchAll ? 'inherited' : c.mode) : undefined,
      style: STROKE[t.via] ?? STROKE.direct,
    })
  })

  return { nodes, edges }
}

export function PolicyAttachFlow({ detail }: { detail: PolicyDetail }) {
  // Re-layout only when the policy changes; the `key` on the wrapper remounts
  // React Flow so fitView re-runs for the new graph.
  const { nodes, edges } = useMemo(() => buildGraph(detail), [detail])

  return (
    <div className="pol-attach-flow" key={`${detail.kind}:${detail.namespace}:${detail.name}`}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        fitView
        fitViewOptions={{ padding: 0.22, minZoom: 0.3, maxZoom: 1.4 }}
        minZoom={0.3}
        maxZoom={1.6}
        zoomOnScroll={false}
        panOnScroll={false}
        panOnDrag
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={false}
        zoomOnDoubleClick={false}
        proOptions={{ hideAttribution: true }}
      >
        <Background color="transparent" />
        <Controls showInteractive={false} showFitView={false} />
      </ReactFlow>
    </div>
  )
}
