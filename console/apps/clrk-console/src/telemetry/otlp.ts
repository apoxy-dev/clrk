// OTLP/JSON (protojson) shapes for the trace read subresources the clrk apiserver
// serves at `taskagents/{name}/traces` and `daemonagents/{name}/traces`
// (internal/apiserver/telemetry). The body is protojson of OTLP TracesData:
// proto enums render as their string names (e.g. SPAN_KIND_CLIENT,
// STATUS_CODE_ERROR), 64-bit ints — including every *UnixNano — render as decimal
// strings, and trace/span ids render as base64 (proto3 JSON). These types model
// exactly that wire form; spans.ts flattens them into the lane model the views
// render. Only the fields the console reads are typed.

export interface OtlpAnyValue {
  stringValue?: string
  /** int64 fields arrive as a decimal string in proto3 JSON. */
  intValue?: string
  doubleValue?: number
  boolValue?: boolean
  bytesValue?: string
  arrayValue?: { values?: OtlpAnyValue[] }
  kvlistValue?: { values?: OtlpKeyValue[] }
}

export interface OtlpKeyValue {
  key: string
  value?: OtlpAnyValue
}

export interface OtlpSpan {
  traceId?: string
  spanId?: string
  parentSpanId?: string
  name?: string
  kind?: string | number
  startTimeUnixNano?: string
  endTimeUnixNano?: string
  attributes?: OtlpKeyValue[]
  status?: { code?: string | number; message?: string }
  events?: Array<{ timeUnixNano?: string; name?: string; attributes?: OtlpKeyValue[] }>
}

export interface OtlpScopeSpans {
  scope?: { name?: string; version?: string }
  spans?: OtlpSpan[]
}

export interface OtlpResourceSpans {
  resource?: { attributes?: OtlpKeyValue[] }
  scopeSpans?: OtlpScopeSpans[]
}

export interface OtlpTracesData {
  resourceSpans?: OtlpResourceSpans[]
}

/** Read a primitive AnyValue to a display string (numbers/bools coerced). */
export function scalar(v?: OtlpAnyValue): string | undefined {
  if (!v) return undefined
  if (v.stringValue != null) return v.stringValue
  if (v.intValue != null) return v.intValue
  if (v.doubleValue != null) return String(v.doubleValue)
  if (v.boolValue != null) return String(v.boolValue)
  return undefined
}

/** Flatten an OTLP KeyValue list to a plain string map (scalar values only). */
export function attrMap(attrs?: OtlpKeyValue[]): Record<string, string> {
  const out: Record<string, string> = {}
  for (const kv of attrs ?? []) {
    const s = scalar(kv.value)
    if (s != null) out[kv.key] = s
  }
  return out
}

/**
 * Decode a base64 payload (the captured-body `clrk.body.b64` attribute) to a
 * UTF-8 string. Bodies arrive as raw bytes base64'd by the ext_proc OTLP sink,
 * so a naive `atob` would mangle any multibyte character — round-trip through
 * the byte array. Returns '' on malformed input rather than throwing.
 */
export function decodeB64Utf8(b64: string): string {
  try {
    const bin = atob(b64)
    const bytes = new Uint8Array(bin.length)
    for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i)
    return new TextDecoder().decode(bytes)
  } catch {
    return ''
  }
}
