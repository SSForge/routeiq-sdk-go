// Package routeiq provides the developer-facing RouteIQ SDK for Go.
// One RouteIQ per agent process. Wire OTel once; emit task/step/tool spans.
package routeiq

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const sdkVersion = "0.2.0"

// Options configures a RouteIQ instance.
type Options struct {
	AgentID      string
	OTLPEndpoint string // default: "localhost:4317" (gRPC)
	TenantID     string // default: "default"
	Model        string
	Environment  string // default: "production"
	AgentVersion string // default: "1.0.0"
	APIKey       string
}

// RouteIQ is the main SDK entry point. One per agent process.
type RouteIQ struct {
	opts      Options
	sessionID string
	tracer    trace.Tracer
	provider  *sdktrace.TracerProvider
}

// New creates and configures a RouteIQ instance.
func New(ctx context.Context, opts Options) (*RouteIQ, error) {
	if opts.OTLPEndpoint == "" {
		opts.OTLPEndpoint = "localhost:4317"
	}
	if opts.TenantID == "" {
		opts.TenantID = "default"
	}
	if opts.Environment == "" {
		opts.Environment = "production"
	}
	if opts.AgentVersion == "" {
		opts.AgentVersion = "1.0.0"
	}

	exp, err := makeExporter(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("routeiq: exporter: %w", err)
	}

	res, _ := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(opts.AgentID),
			semconv.ServiceVersion(opts.AgentVersion),
			attribute.String("routeiq.sdk.version", sdkVersion),
		),
	)

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)

	return &RouteIQ{
		opts:      opts,
		sessionID: newUUID(),
		tracer:    provider.Tracer("routeiq.sdk", trace.WithInstrumentationVersion(sdkVersion)),
		provider:  provider,
	}, nil
}

// newForTest creates a RouteIQ with an injected provider (used in tests).
func newForTest(provider *sdktrace.TracerProvider, opts Options) *RouteIQ {
	return &RouteIQ{
		opts:      opts,
		sessionID: newUUID(),
		tracer:    provider.Tracer("routeiq.sdk"),
		provider:  provider,
	}
}

// Task starts a task span. Call End() when done, or defer it.
func (r *RouteIQ) Task(ctx context.Context, intent string, taskType ...string) *TaskHandle {
	t := newTaskHandle(ctx, r, intent)
	if len(taskType) > 0 {
		t.taskType = taskType[0]
	}
	t.start()
	return t
}

// Flush forces pending spans to be exported. Call before process exit.
func (r *RouteIQ) Flush(ctx context.Context) error {
	return r.provider.ForceFlush(ctx)
}

// Shutdown flushes and shuts down the provider.
func (r *RouteIQ) Shutdown(ctx context.Context) error {
	return r.provider.Shutdown(ctx)
}

func (r *RouteIQ) envelope(task *TaskHandle, step *StepHandle) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("routeiq.agent.id", r.opts.AgentID),
		attribute.String("routeiq.tenant.id", r.opts.TenantID),
		attribute.String("routeiq.environment", r.opts.Environment),
		attribute.String("routeiq.session.id", r.sessionID),
	}
	if task != nil {
		attrs = append(attrs,
			attribute.String("routeiq.task.id", task.taskID),
			attribute.String("routeiq.run.id", task.runID),
		)
	}
	if step != nil {
		attrs = append(attrs, attribute.String("routeiq.step.id", step.stepID))
	}
	if r.opts.Model != "" {
		attrs = append(attrs, attribute.String("routeiq.version.model.name", r.opts.Model))
	}
	if r.opts.AgentVersion != "" {
		attrs = append(attrs, attribute.String("routeiq.version.agent", r.opts.AgentVersion))
	}
	return attrs
}

func makeExporter(ctx context.Context, opts Options) (sdktrace.SpanExporter, error) {
	ep := opts.OTLPEndpoint
	headers := map[string]string{}
	if opts.APIKey != "" {
		headers["authorization"] = "Bearer " + opts.APIKey
	}

	if len(ep) > 8 && ep[:8] == "https://" {
		return otlptracehttp.New(ctx,
			otlptracehttp.WithEndpoint(ep),
			otlptracehttp.WithHeaders(headers),
		)
	}
	grpcOpts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(ep),
	}
	if opts.APIKey == "" {
		grpcOpts = append(grpcOpts, otlptracegrpc.WithInsecure())
	}
	if len(headers) > 0 {
		grpcOpts = append(grpcOpts, otlptracegrpc.WithHeaders(headers))
	}
	return otlptracegrpc.New(ctx, grpcOpts...)
}

// SessionID returns the session UUID generated at init.
func (r *RouteIQ) SessionID() string { return r.sessionID }
