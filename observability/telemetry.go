package observability

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	SpanPublish    = "messaging.publish"
	SpanConsume    = "messaging.consume"
	SpanProcess    = "messaging.process"
	SpanRetryPub   = "messaging.retry.publish"
	SpanDLQPub     = "messaging.dlq.publish"
	SpanTxnBegin   = "transaction.begin"
	SpanTxnPublish = "transaction.publish"
	SpanTxnOffsets = "transaction.offsets"
	SpanTxnCommit  = "transaction.commit"
	SpanTxnAbort   = "transaction.abort"
)

type Telemetry struct {
	Logger  *slog.Logger
	Tracer  trace.Tracer
	Prop    propagation.TextMapPropagator
	metrics metrics
}

type metrics struct {
	processed    metric.Int64Counter
	failed       metric.Int64Counter
	retries      metric.Int64Counter
	dlq          metric.Int64Counter
	handlerDur   metric.Float64Histogram
	txnStarted   metric.Int64Counter
	txnCommitted metric.Int64Counter
	txnAborted   metric.Int64Counter
	txnFailed    metric.Int64Counter
	txnDur       metric.Float64Histogram
}

func New(logger *slog.Logger, tp trace.TracerProvider, mp metric.MeterProvider, prop propagation.TextMapPropagator) *Telemetry {
	if logger == nil {
		logger = slog.Default()
	}
	if prop == nil {
		prop = propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	}
	meter := mp.Meter("retrykafka")
	m := metrics{}
	m.processed, _ = meter.Int64Counter("messages_processed_total")
	m.failed, _ = meter.Int64Counter("messages_failed_total")
	m.retries, _ = meter.Int64Counter("retry_attempts_total")
	m.dlq, _ = meter.Int64Counter("messages_sent_dlq_total")
	m.handlerDur, _ = meter.Float64Histogram("handler_duration")
	m.txnStarted, _ = meter.Int64Counter("transactions_started_total")
	m.txnCommitted, _ = meter.Int64Counter("transactions_committed_total")
	m.txnAborted, _ = meter.Int64Counter("transactions_aborted_total")
	m.txnFailed, _ = meter.Int64Counter("transaction_failures_total")
	m.txnDur, _ = meter.Float64Histogram("transaction_duration")
	return &Telemetry{
		Logger:  logger,
		Tracer:  tp.Tracer("retrykafka"),
		Prop:    prop,
		metrics: m,
	}
}

func (t *Telemetry) Attrs(messageID, topic string, partition int, offset int64, attempt, maxAttempts int, err error) []any {
	attrs := []any{
		"message_id", messageID,
		"topic", topic,
		"partition", partition,
		"offset", offset,
		"retry_attempt", attempt,
		"max_attempts", maxAttempts,
	}
	if err != nil {
		attrs = append(attrs, "error", err.Error())
	}
	return attrs
}

func (t *Telemetry) Event(ctx context.Context, msg string, args ...any) {
	t.Logger.InfoContext(ctx, msg, args...)
}

func (t *Telemetry) Error(ctx context.Context, msg string, args ...any) {
	t.Logger.ErrorContext(ctx, msg, args...)
}

type headerCarrier map[string]string

func (c headerCarrier) Get(key string) string { return c[key] }
func (c headerCarrier) Set(key, value string) { c[key] = value }
func (c headerCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

func (t *Telemetry) Inject(ctx context.Context, headers map[string]string) {
	t.Prop.Inject(ctx, headerCarrier(headers))
}

func (t *Telemetry) Extract(ctx context.Context, headers map[string]string) context.Context {
	return t.Prop.Extract(ctx, headerCarrier(headers))
}

func (t *Telemetry) Start(ctx context.Context, name string, topic string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	attrs = append([]attribute.KeyValue{attribute.String("messaging.destination.name", topic)}, attrs...)
	return t.Tracer.Start(ctx, name, trace.WithAttributes(attrs...))
}

func (t *Telemetry) EndSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

func metricAttrs(topic string, guarantee string) metric.MeasurementOption {
	return metric.WithAttributes(
		attribute.String("topic", topic),
		attribute.String("processing_guarantee", guarantee),
	)
}

func (t *Telemetry) RecordProcessed(ctx context.Context, topic, guarantee string) {
	t.metrics.processed.Add(ctx, 1, metricAttrs(topic, guarantee))
}

func (t *Telemetry) RecordFailed(ctx context.Context, topic, guarantee string) {
	t.metrics.failed.Add(ctx, 1, metricAttrs(topic, guarantee))
}

func (t *Telemetry) RecordRetry(ctx context.Context, topic, guarantee string) {
	t.metrics.retries.Add(ctx, 1, metricAttrs(topic, guarantee))
}

func (t *Telemetry) RecordDLQ(ctx context.Context, topic, guarantee string) {
	t.metrics.dlq.Add(ctx, 1, metricAttrs(topic, guarantee))
}

func (t *Telemetry) RecordHandlerDuration(ctx context.Context, topic, guarantee string, d time.Duration) {
	t.metrics.handlerDur.Record(ctx, d.Seconds(), metricAttrs(topic, guarantee))
}

func (t *Telemetry) RecordTxnStarted(ctx context.Context, topic, guarantee string) {
	t.metrics.txnStarted.Add(ctx, 1, metricAttrs(topic, guarantee))
}

func (t *Telemetry) RecordTxnCommitted(ctx context.Context, topic, guarantee string) {
	t.metrics.txnCommitted.Add(ctx, 1, metricAttrs(topic, guarantee))
}

func (t *Telemetry) RecordTxnAborted(ctx context.Context, topic, guarantee string) {
	t.metrics.txnAborted.Add(ctx, 1, metricAttrs(topic, guarantee))
}

func (t *Telemetry) RecordTxnFailed(ctx context.Context, topic, op string) {
	t.metrics.txnFailed.Add(ctx, 1, metric.WithAttributes(
		attribute.String("topic", topic),
		attribute.String("operation", op),
	))
}

func (t *Telemetry) RecordTxnDuration(ctx context.Context, topic, guarantee string, d time.Duration) {
	t.metrics.txnDur.Record(ctx, d.Seconds(), metricAttrs(topic, guarantee))
}
