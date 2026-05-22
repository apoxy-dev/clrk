package otelemit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	otellog "go.opentelemetry.io/otel/log"
	lognoop "go.opentelemetry.io/otel/log/noop"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	clrkv1alpha1 "github.com/apoxy-dev/clrk/api/clrk/v1alpha1"
)

// 1s keeps `clrk dev`/TUI feedback snappy; the SDK defaults (logs 1s,
// traces 5s) introduced a visible 5s gap between an HTTP transaction
// completing and its span landing in the otel-traces pane.
const batchTimeout = 1 * time.Second

type Emitter interface {
	Tracer() oteltrace.Tracer
	Logger() otellog.Logger
	Close(ctx context.Context) error
}

// New returns an Emitter wired to spec.Endpoint, or Noop() when the
// endpoint is empty. Callers compose the Resource so this package
// doesn't decide service.name for them.
func New(ctx context.Context, spec clrkv1alpha1.OTLPLogsSinkSpec, scope string, res *resource.Resource) (Emitter, error) {
	if spec.Endpoint == "" {
		return Noop(), nil
	}

	logsExp, err := otlploghttp.New(ctx, LogsExporterOptions(spec)...)
	if err != nil {
		return nil, fmt.Errorf("construct otlploghttp exporter: %w", err)
	}
	logsProvider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logsExp,
			sdklog.WithExportInterval(batchTimeout),
		)),
		sdklog.WithResource(res),
	)

	tracesExp, err := otlptrace.New(ctx, otlptracehttp.NewClient(TracesExporterOptions(spec)...))
	if err != nil {
		_ = logsProvider.Shutdown(ctx)
		return nil, fmt.Errorf("construct otlptracehttp exporter: %w", err)
	}
	tracesProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(tracesExp,
			sdktrace.WithBatchTimeout(batchTimeout),
		),
		sdktrace.WithResource(res),
	)

	return &realEmitter{
		tracesProvider: tracesProvider,
		logsProvider:   logsProvider,
		tracer:         tracesProvider.Tracer(scope),
		logger:         logsProvider.Logger(scope),
	}, nil
}

func Noop() Emitter {
	return noopEmitter{
		tracer: tracenoop.NewTracerProvider().Tracer(""),
		logger: lognoop.NewLoggerProvider().Logger(""),
	}
}

type realEmitter struct {
	tracesProvider *sdktrace.TracerProvider
	logsProvider   *sdklog.LoggerProvider
	tracer         oteltrace.Tracer
	logger         otellog.Logger
}

func (e *realEmitter) Tracer() oteltrace.Tracer { return e.tracer }
func (e *realEmitter) Logger() otellog.Logger   { return e.logger }

func (e *realEmitter) Close(ctx context.Context) error {
	var (
		wg               sync.WaitGroup
		traceErr, logErr error
	)
	wg.Add(2)
	go func() { defer wg.Done(); traceErr = e.tracesProvider.Shutdown(ctx) }()
	go func() { defer wg.Done(); logErr = e.logsProvider.Shutdown(ctx) }()
	wg.Wait()

	var errs []error
	if traceErr != nil {
		errs = append(errs, fmt.Errorf("traces: %w", traceErr))
	}
	if logErr != nil {
		errs = append(errs, fmt.Errorf("logs: %w", logErr))
	}
	return errors.Join(errs...)
}

type noopEmitter struct {
	tracer oteltrace.Tracer
	logger otellog.Logger
}

func (e noopEmitter) Tracer() oteltrace.Tracer    { return e.tracer }
func (e noopEmitter) Logger() otellog.Logger      { return e.logger }
func (e noopEmitter) Close(context.Context) error { return nil }
