package extproc

// upstreamStream handles one upstream (cluster-level) ext_proc stream,
// which Envoy opens fresh for every router attempt against a
// synthesized clrk-llm-* cluster (see internal/egextension/llm.go).
// The filter is request-only: it sees the attempt's request headers
// and replayed body, never the response. Holding response headers in
// an upstream filter races the router's retry decision and violates
// its upstream_requests_ invariant (see buildLLMUpstreamExtProcFilter)
// — response adaptation belongs downstream, keyed off the final
// serving attempt this handler records.
//
// Current scope is observe-and-continue: log the per-attempt facts the
// fallback design stands on (one stream per attempt, the selected
// endpoint's identity via xds.upstream_host_metadata) and forward
// everything unchanged. The per-attempt adapter — schema translation,
// :authority repoint, credential injection — replaces the phase bodies
// here in the cutover commit; until the downstream handler pins
// requests onto the synthesized routes, no production traffic reaches
// this handler.

import (
	"fmt"
	"time"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/go-logr/logr"
)

// upstreamHostMetadataAttr is the request attribute the egextension
// configures on the upstream ext_proc filter. Envoy resolves it to the
// selected endpoint's metadata after load balancing, which is how each
// attempt learns which Backend it targets.
const upstreamHostMetadataAttr = "xds.upstream_host_metadata"

// upstreamStream accumulates one attempt's observed facts.
type upstreamStream struct {
	logger logr.Logger

	requestID string
	authority string
	hostMeta  string
	reqBytes  int
	startedAt time.Time
}

func (s *Server) newUpstreamStream(logger logr.Logger) *upstreamStream {
	return &upstreamStream{
		logger:    logger.WithName("upstream"),
		startedAt: time.Now(),
	}
}

// handle processes one message of the attempt stream and returns the
// response to send, or nil to skip. Only request-direction messages
// arrive (the filter's response modes are SKIP/NONE); the response
// cases are kept as guards against config drift.
func (us *upstreamStream) handle(req *extprocv3.ProcessingRequest) *extprocv3.ProcessingResponse {
	us.captureHostMetadata(req)
	switch m := req.GetRequest().(type) {
	case *extprocv3.ProcessingRequest_RequestHeaders:
		hdrs := headersToMap(m.RequestHeaders)
		us.requestID = hdrs["x-request-id"]
		us.authority = hdrs[":authority"]
		us.logger.V(1).Info("Attempt request headers",
			"requestID", us.requestID,
			"authority", us.authority,
			"hostMetadata", us.hostMeta)
		return headersContinue(true)
	case *extprocv3.ProcessingRequest_RequestBody:
		us.reqBytes += len(m.RequestBody.GetBody())
		return bodyContinue(true)
	case *extprocv3.ProcessingRequest_RequestTrailers:
		return trailersContinue(true)
	case *extprocv3.ProcessingRequest_ResponseHeaders:
		return headersContinue(false)
	case *extprocv3.ProcessingRequest_ResponseBody:
		return bodyContinue(false)
	case *extprocv3.ProcessingRequest_ResponseTrailers:
		return trailersContinue(false)
	default:
		us.logger.V(1).Info("Unhandled upstream message type", "type", fmt.Sprintf("%T", req.GetRequest()))
		return nil
	}
}

// finish logs the attempt summary once the stream closes.
func (us *upstreamStream) finish() {
	us.logger.Info("Attempt finished",
		"requestID", us.requestID,
		"authority", us.authority,
		"hostMetadata", us.hostMeta,
		"reqBytes", us.reqBytes,
		"duration", time.Since(us.startedAt))
}

// captureHostMetadata records the selected endpoint's metadata from the
// message's attributes, when present. Envoy delivers request attributes
// keyed by the requesting filter's name; we don't depend on that key
// and scan all entries for the attribute field instead.
func (us *upstreamStream) captureHostMetadata(req *extprocv3.ProcessingRequest) {
	for _, attrs := range req.GetAttributes() {
		if v, ok := attrs.GetFields()[upstreamHostMetadataAttr]; ok {
			us.hostMeta = v.String()
		}
	}
}
