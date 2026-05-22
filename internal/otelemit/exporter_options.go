package otelemit

import (
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

func LogsExporterOptions(spec clrkv1alpha1.OTLPLogsSinkSpec) []otlploghttp.Option {
	opts := []otlploghttp.Option{otlploghttp.WithEndpointURL(EndpointForSignal(spec.Endpoint, "/v1/logs"))}
	if len(spec.Headers) > 0 {
		opts = append(opts, otlploghttp.WithHeaders(spec.Headers))
	}
	return opts
}

func TracesExporterOptions(spec clrkv1alpha1.OTLPLogsSinkSpec) []otlptracehttp.Option {
	opts := []otlptracehttp.Option{otlptracehttp.WithEndpointURL(EndpointForSignal(spec.Endpoint, "/v1/traces"))}
	if len(spec.Headers) > 0 {
		opts = append(opts, otlptracehttp.WithHeaders(spec.Headers))
	}
	return opts
}
