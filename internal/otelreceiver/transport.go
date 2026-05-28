// Package otelreceiver is the controller-manager's OTLP/HTTP receiver
// and the home of OTLP/protobuf decode helpers shared with the
// `clrk dev` TUI receiver in internal/cmd/devotel.
//
// Producers (worker, ingress ext_proc, egress ext_proc) ship logs +
// traces to cm at --otlp-addr instead of dialing the customer's
// collector directly. cm always persists the signals to its embedded
// ClickHouse via internal/chwriter and, for EgressGateways whose
// Spec.OTLP.Endpoint is set, best-effort forwards a copy to that
// endpoint via internal/otelforward.
package otelreceiver

import (
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"strings"

	"google.golang.org/protobuf/proto"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

// ReadBody returns the raw OTLP/HTTP payload, decoding gzip when the
// Content-Encoding header advertises it. otlploghttp / otlptracehttp
// default to no compression but the OTel SDK honors WithCompression
// so collectors must accept gzip.
func ReadBody(req *http.Request) ([]byte, error) {
	defer req.Body.Close()
	if strings.EqualFold(req.Header.Get("Content-Encoding"), "gzip") {
		gz, err := gzip.NewReader(req.Body)
		if err != nil {
			return nil, fmt.Errorf("gzip: %w", err)
		}
		defer gz.Close()
		return io.ReadAll(gz)
	}
	return io.ReadAll(req.Body)
}

// EmptyLogsResp and EmptyTracesResp are pre-marshalled empty OTLP
// responses. Handlers write them on success so we don't allocate a
// zero-value proto on every request.
var (
	EmptyLogsResp   []byte
	EmptyTracesResp []byte
)

func init() {
	EmptyLogsResp, _ = proto.Marshal(&collogspb.ExportLogsServiceResponse{})
	EmptyTracesResp, _ = proto.Marshal(&coltracepb.ExportTraceServiceResponse{})
}
