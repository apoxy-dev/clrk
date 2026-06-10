package azureopenai

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/apoxy-dev/clrk/internal/extproc/llmcall"
	"github.com/apoxy-dev/clrk/internal/extproc/llmcall/providers/openai"
)

// defaultAPIVersion is the api-version synthesized when a translated
// (stripped) request has no decode-time hint to carry one — i.e. for
// every cross-schema encode. Pinned to a GA data-plane version; a
// Backend-level override is a possible future CRD knob.
const defaultAPIVersion = "2024-10-21"

// codec adapts the OpenAI Chat Completions codec to Azure's surface.
// The body IS OpenAI's wire schema; what differs is the envelope: the
// model is a DEPLOYMENT in the path
// (/openai/deployments/{deployment}/chat/completions?api-version=V),
// bodies usually carry no model member, and auth rides the api-key
// header. Model rule: req.Model is the body model when present, else
// the path deployment — preserve fidelity stays trivial (the body
// shadow holds) and translation/modelRewrites see the deployment in
// the dominant no-body-model case.
type codec struct {
	inner llmcall.Codec
}

func newCodec() codec {
	return codec{inner: openai.NewChatCodec("azure_openai")}
}

// azureChatPath parses /openai/deployments/{deployment}/chat/completions
// (query included), returning the unescaped deployment and api-version.
func azureChatPath(path string) (deployment, apiVersion string, ok bool) {
	p, query, _ := strings.Cut(path, "?")
	rest, found := strings.CutPrefix(p, "/openai/deployments/")
	if !found {
		return "", "", false
	}
	dep, tail, found := strings.Cut(rest, "/")
	if !found || tail != "chat/completions" || dep == "" {
		return "", "", false
	}
	d, err := url.PathUnescape(dep)
	if err != nil {
		return "", "", false
	}
	if q, err := url.ParseQuery(query); err == nil {
		apiVersion = q.Get("api-version")
	}
	return d, apiVersion, true
}

func (c codec) DecodeRequest(in llmcall.RequestInput) (*llmcall.Request, error) {
	dep, ver, ok := azureChatPath(in.Path)
	if !ok {
		// Embeddings, the /openai/v1 responses surface, completions:
		// outside the modeled scope.
		return nil, fmt.Errorf("azure path %q: %w", in.Path, llmcall.ErrUnsupported)
	}
	inner := in
	inner.Path = "/v1/chat/completions"
	req, err := c.inner.DecodeRequest(inner)
	if err != nil {
		return nil, err
	}
	// Restore the real envelope: preserve-mode encodes re-emit
	// Wire.Path verbatim, and the api-version hint survives same-schema
	// round trips (translation strips it — defaultAPIVersion applies).
	req.Wire.Path = in.Path
	if ver != "" {
		req.Wire.SetHint("azure_api_version", ver)
	}
	if req.Model == "" {
		req.Model = dep
	}
	return req, nil
}

func (c codec) EncodeRequest(req *llmcall.Request, opts llmcall.EncodeOptions) (*llmcall.EncodedRequest, error) {
	bodyReq := *req
	if !req.Wire.HasKey("model") {
		// The model is path-borne: blanking it on the shallow copy
		// defeats the inner model emitter so a deployment name never
		// leaks into the body of a request that did not carry one.
		bodyReq.Model = ""
	}
	enc, err := c.inner.EncodeRequest(&bodyReq, opts)
	if err != nil {
		return nil, err
	}
	if opts.Mode == llmcall.ModePreserve && req.Wire.Path != "" {
		// The inner encoder already emitted Wire.Path — the original
		// azure path — verbatim.
		return enc, nil
	}
	ver := req.Wire.Hint("azure_api_version")
	if ver == "" {
		ver = defaultAPIVersion
	}
	enc.Path = "/openai/deployments/" + url.PathEscape(req.Model) + "/chat/completions?api-version=" + ver
	return enc, nil
}

func (c codec) DecodeResponse(in llmcall.ResponseInput, req *llmcall.Request) (*llmcall.Response, error) {
	// Azure responses are OpenAI-shaped, model member included.
	return c.inner.DecodeResponse(in, req)
}

func (c codec) EncodeResponse(resp *llmcall.Response, opts llmcall.EncodeOptions) (*llmcall.EncodedResponse, error) {
	return c.inner.EncodeResponse(resp, opts)
}
