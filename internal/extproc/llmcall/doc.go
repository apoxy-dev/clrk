// Package llmcall defines clrk's canonical intermediate representation
// (IR) for LLM API traffic and the provider registry that converts wire
// schemas (Anthropic, OpenAI, Google, ...) to and from it.
//
// # Hub and spoke
//
// Translation between provider wire schemas is hub-and-spoke, never
// pairwise: each provider implements a Codec that decodes its wire
// format into the IR and encodes the IR back into its wire format.
// Translating a request captured in schema A to a backend speaking
// schema B is EncodeB(DecodeA(body)). Adding a provider therefore means
// adding one self-contained package under providers/ — no edits to
// other providers or to extproc core.
//
// The IR's structure is based on the LLM-Rosetta paper
// (arXiv:2604.09360): a small set of typed content parts, a typed
// stream-event taxonomy, and explicit preserve/strip handling of
// provider fields the IR doesn't model. Its vocabulary — role names,
// part types, finish reasons, provider names — follows the OTel GenAI
// semantic conventions so the telemetry projection is a copy, not a
// mapping table.
//
// # Preserve vs strip
//
// Every IR node carries a Wire with the bookkeeping needed for two
// encode modes:
//
//   - ModePreserve re-emits unmodeled provider fields and original
//     formatting; a decode/encode round trip of an unmutated body
//     through the same provider's codec is byte-identical. The golden
//     corpus in apoxy-cloud gates this property.
//   - ModeStrip drops unmodeled fields (counting them) and emits the
//     target provider's canonical form. Cross-schema translation always
//     strips — extras are provider-specific by definition.
//
// # Registration
//
// Providers self-register from package init via Register. The blank
// import list at providers/all is the only place that names them all;
// the parsers shim imports it, so any binary linking
// internal/extproc/parsers gets every provider. A program that imports
// llmcall directly without importing providers/all (or a specific
// provider) sees an empty registry — tests included.
//
// # Boundaries
//
// The IR is internal-only. It is never serialized to the wire, never
// persisted, and never surfaces in the versioned clrk API; it may
// change shape freely between releases. Consequently there are no
// kubebuilder markers and no generated deepcopy here.
package llmcall
