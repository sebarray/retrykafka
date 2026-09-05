# Observabilidad

## Logging (`log/slog`)

Default: `slog.Default()`. Override: `WithLogger(logger)`.

El logger se usa desde varios workers: debe ser concurrent-safe (`slog` lo es).

Eventos: message received/processed, processing failed, retry scheduled/published, dlq published, offset committed, consumer shutdown, publisher error, transaction started/committed/aborted.

Atributos: `message_id`, `topic`, `partition`, `offset`, `retry_attempt`, `max_attempts`, `error`. **No** se loguea el payload completo.

Trade-off: más visibilidad vs volumen. En retries × particiones el ruido crece; usar niveles o sampling en el handler del usuario.

## OpenTelemetry

Default: `otel.GetTracerProvider()` y `otel.GetMeterProvider()` (no-op si no hay SDK). La librería funciona sin Collector, Jaeger, Tempo, Prometheus o Grafana.

Spans:

- `messaging.publish` (publisher; inyecta `traceparent` en headers)
- `messaging.consume` / `messaging.process`
- `messaging.retry.publish` / `messaging.dlq.publish`
- `transaction.*`

Métricas (baja cardinalidad; labels `topic`, `processing_guarantee`, a veces `operation`):

- `messages_processed_total`, `messages_failed_total`, `retry_attempts_total`, `messages_sent_dlq_total`, `handler_duration`
- `transactions_*`

Nunca `message_id`, `offset`, `user_id`, `transaction_id` como labels.

Overhead: un span por publish/consume/process y contadores atómicos. Con exporters no-op el coste es bajo. Con sampling 100% + collector remoto, el export es el coste dominante.
