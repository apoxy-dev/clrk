package extproc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// otlpSink emits one OTel log record per captured HTTP transaction
// through an OTLP/HTTP exporter. Records are batched by the SDK; pending
// batches are flushed via the shutdown function returned alongside the
// sink.
type otlpSink struct {
	logger otellog.Logger
}

// newOTLPSink builds an OTLP/HTTP log exporter pointed at spec.Endpoint
// and returns a Sink plus a shutdown function the caller invokes to
// flush + close on EG removal or process exit.
func newOTLPSink(ctx context.Context, spec clrkv1alpha1.OTLPLogsSinkSpec) (Sink, func(context.Context) error, error) {
	opts := []otlploghttp.Option{
		otlploghttp.WithEndpointURL(spec.Endpoint),
	}
	if len(spec.Headers) > 0 {
		opts = append(opts, otlploghttp.WithHeaders(spec.Headers))
	}
	// Treat plain http:// endpoints as insecure. Production deployments
	// should be https; the env-var override remains available too.
	if strings.HasPrefix(strings.ToLower(spec.Endpoint), "http://") {
		opts = append(opts, otlploghttp.WithInsecure())
	}

	exp, err := otlploghttp.New(ctx, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("construct otlploghttp exporter: %w", err)
	}
	provider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exp)),
		sdklog.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("clrk-egress"),
			attribute.String("clrk.component", "extproc"),
		)),
	)
	return &otlpSink{
			logger: provider.Logger("github.com/apoxy-dev/clrk/internal/extproc"),
		},
		provider.Shutdown,
		nil
}

func (s *otlpSink) Emit(r Record) {
	var rec otellog.Record
	rec.SetTimestamp(r.Timestamp)
	rec.SetSeverity(otellog.SeverityInfo)
	rec.SetBody(otellog.StringValue(renderRecordBody(r)))
	rec.AddAttributes(
		otellog.String("agent.kind", r.AgentKind),
		otellog.String("agent.namespace", r.AgentNamespace),
		otellog.String("agent.name", r.AgentName),
		otellog.String("agent.uid", r.AgentUID),
		otellog.String("agent.revision", r.AgentRevision),
		otellog.String("invocation.id", r.InvocationID),
		otellog.String("http.request.method", r.RequestHeaders[":method"]),
		otellog.String("server.address", r.RequestHeaders[":authority"]),
		otellog.String("url.path", r.RequestHeaders[":path"]),
		otellog.Int("http.response.status_code", parseStatus(r.ResponseHeaders[":status"])),
		otellog.Bool("clrk.req.truncated", r.RequestTruncated),
		otellog.Bool("clrk.resp.truncated", r.ResponseTruncated),
	)
	s.logger.Emit(context.Background(), rec)
}

// recordBody is the JSON payload we stuff into the OTel log record's
// Body. Bodies are base64'd for binary safety even when the content-type
// gate let them through.
type recordBody struct {
	RequestHeaders   map[string]string `json:"req_headers,omitempty"`
	RequestBodyB64   string            `json:"req_body_b64,omitempty"`
	ResponseHeaders  map[string]string `json:"resp_headers,omitempty"`
	ResponseBodyB64  string            `json:"resp_body_b64,omitempty"`
	RequestTruncated bool              `json:"req_truncated,omitempty"`
	RespTruncated    bool              `json:"resp_truncated,omitempty"`
}

func renderRecordBody(r Record) string {
	body := recordBody{
		RequestHeaders:   r.RequestHeaders,
		ResponseHeaders:  r.ResponseHeaders,
		RequestTruncated: r.RequestTruncated,
		RespTruncated:    r.ResponseTruncated,
	}
	if len(r.RequestBody) > 0 {
		body.RequestBodyB64 = base64.StdEncoding.EncodeToString(r.RequestBody)
	}
	if len(r.ResponseBody) > 0 {
		body.ResponseBodyB64 = base64.StdEncoding.EncodeToString(r.ResponseBody)
	}
	out, err := json.Marshal(body)
	if err != nil {
		return fmt.Sprintf(`{"marshal_error":%q}`, err.Error())
	}
	return string(out)
}

func parseStatus(s string) int {
	if s == "" {
		return 0
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return n
}
