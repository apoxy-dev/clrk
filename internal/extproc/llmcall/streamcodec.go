package llmcall

// StreamDecoder consumes one streamed response's raw upstream bytes —
// chunks may split anywhere, including mid-SSE-line — and yields
// canonical StreamEvents in arrival order. One decoder per stream;
// all reassembly and block-index state lives on the decoder. Not safe
// for concurrent use.
//
// Decoders assign dense IR block indexes from 0 in open order and
// always emit content_start before a block's first delta, synthesizing
// one when the provider skipped its open frame. They set
// StreamFinish.FinishReasonRaw for telemetry; encoders ignore it.
type StreamDecoder interface {
	// Decode parses the events completed by chunk. eos marks the final
	// chunk: the decoder flushes its reassembly buffer, closes any
	// still-open blocks, synthesizes a terminal done event when the
	// provider's own terminator never arrived, and reports a dangling
	// partial frame via MalformedError.
	Decode(chunk []byte, eos bool) ([]StreamEvent, error)
}

// StreamEncoder renders canonical StreamEvents in one provider's
// streamed response framing — SSE framing included. One encoder per
// stream. Not safe for concurrent use.
//
// Encoders map only the canonical FinishReason, never FinishReasonRaw:
// streams are never preserve-mode re-encoded, so there is no fidelity
// contract, and the raw value is source-schema vocabulary that must
// not leak into the client's. Usage events are cumulative snapshots —
// last wins — folded into the encoder's terminal frame sequence.
type StreamEncoder interface {
	// ContentType is the content-type the encoded stream rides on.
	ContentType() string
	// Encode appends the wire rendering of events. Empty output is
	// valid — events may buffer until a block completes, and a
	// mid-frame upstream chunk completes no events at all.
	Encode(events []StreamEvent) ([]byte, error)
}

// StreamCodec mints per-stream decoder/encoder state for one
// provider's streamed response framing. Implementations are stateless;
// per-stream state lives on the returned values.
type StreamCodec interface {
	// NewStreamDecoder returns a decoder for a streamed response to
	// req — the request as sent upstream, in this provider's schema.
	NewStreamDecoder(req *Request) StreamDecoder
	// NewStreamEncoder returns an encoder rendering a stream for the
	// client that sent req in this provider's schema. req supplies
	// fallbacks (model) and framing choices.
	NewStreamEncoder(req *Request) StreamEncoder
}
