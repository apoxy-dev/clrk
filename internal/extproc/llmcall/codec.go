package llmcall

import (
	"errors"
	"fmt"
)

// Mode selects how an encoder treats wire bookkeeping. See the package
// documentation for the preserve/strip contract.
type Mode int

const (
	// ModePreserve re-emits extras and original formatting; an
	// unmutated same-provider round trip is byte-identical.
	ModePreserve Mode = iota
	// ModeStrip drops extras (counting them) and emits the provider's
	// canonical form. Cross-schema translation always strips.
	ModeStrip
)

// EncodeOptions parameterizes Encode calls.
type EncodeOptions struct {
	Mode Mode
}

// RequestInput is the captured HTTP request handed to DecodeRequest.
// Header keys are lowercased, as ext_proc delivers them.
type RequestInput struct {
	Method  string
	Path    string
	Headers map[string]string
	Body    []byte
}

// ResponseInput is the captured HTTP response handed to DecodeResponse.
type ResponseInput struct {
	Status  int
	Headers map[string]string
	Body    []byte
}

// EncodedRequest is EncodeRequest's output: the body plus the header
// deltas the caller must apply. Path is the full :path including any
// query — schemas that carry the model in the URL (Gemini) synthesize
// it here.
type EncodedRequest struct {
	Body          []byte
	Path          string
	SetHeaders    map[string]string
	RemoveHeaders []string

	// DroppedExtras counts unmodeled fields and unknown parts dropped
	// in strip mode (0 in preserve mode); surfaced as the
	// clrk.translation.dropped_extras telemetry attribute.
	DroppedExtras int

	// Exact reports that Body is the verbatim decode-time capture,
	// emitted via the equality-gated passthrough (EncodeRaw).
	Exact bool
}

// EncodedResponse is EncodeResponse's output.
type EncodedResponse struct {
	Body          []byte
	SetHeaders    map[string]string
	DroppedExtras int
	Exact         bool
}

// ErrUnsupported reports valid wire bytes outside the codec's modeled
// scope — an endpoint, operation, or response framing (event-stream,
// NDJSON) the codec doesn't translate yet. Callers react by skipping
// translation, not by failing the request.
var ErrUnsupported = errors.New("llmcall: not supported by codec")

// MalformedError reports bytes that are not the provider's wire format
// at all: truncation, non-JSON, HTML error pages. Callers must treat
// the capture as unparseable and never attempt translation.
type MalformedError struct {
	Provider string
	Detail   string
	Err      error
}

func (e *MalformedError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("llmcall: malformed %s body: %s: %v", e.Provider, e.Detail, e.Err)
	}
	return fmt.Sprintf("llmcall: malformed %s body: %s", e.Provider, e.Detail)
}

func (e *MalformedError) Unwrap() error { return e.Err }

// Codec converts one provider's wire format to and from the IR.
// Implementations are stateless and safe for concurrent use.
//
// DecodeRequest must record everything preserve-mode EncodeRequest
// needs to reproduce the original transaction: body bookkeeping in the
// IR's Wire fields, the original :path in Request.Wire.Path, and
// modeled headers in Request.Wire.Headers.
//
// DecodeResponse takes the already-decoded request for context the
// response body alone doesn't carry (operation, model for schemas that
// don't echo it).
type Codec interface {
	DecodeRequest(in RequestInput) (*Request, error)
	EncodeRequest(req *Request, opts EncodeOptions) (*EncodedRequest, error)
	DecodeResponse(in ResponseInput, req *Request) (*Response, error)
	EncodeResponse(resp *Response, opts EncodeOptions) (*EncodedResponse, error)
}
