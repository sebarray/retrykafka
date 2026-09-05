package observability_test

import (
	"context"
	"testing"

	"github.com/retrykafka/retrykafka/observability"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestSpanParentChildAndInject(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	tel := observability.New(nil, tp, otel.GetMeterProvider(), propagation.TraceContext{})
	ctx, span := tel.Start(context.Background(), observability.SpanPublish, "payments")
	headers := map[string]string{}
	tel.Inject(ctx, headers)
	if headers["traceparent"] == "" {
		t.Fatal("expected traceparent")
	}
	extracted := tel.Extract(context.Background(), headers)
	_, child := tel.Start(extracted, observability.SpanConsume, "payments")
	span.End()
	child.End()
	spans := exp.GetSpans()
	if len(spans) < 2 {
		t.Fatalf("spans=%d", len(spans))
	}
}
