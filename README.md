# retrykafka

![Go Logo](https://res.cloudinary.com/djkkmecjh/image/upload/logo_eovpts.png)

Toolkit de fiabilidad **nativo de Kafka** para Go. Gestiona el ciclo de vida del mensaje — publicar, procesar, reintentos, DLQ, observabilidad, idempotencia y transacciones Kafka — con Kafka como única infraestructura obligatoria.

```text
Simple by default.
Reliable by design.
Transactional when needed.
```

## Instalar

```bash
go get github.com/retrykafka/retrykafka
```

## Uso rápido

```go
publisher, _ := retrykafka.NewPublisher(
    retrykafka.WithBrokers("localhost:9092"),
)

_ = publisher.Publish(ctx, "payments", PaymentCreated{ID: "payment-123"})

consumer, _ := retrykafka.NewConsumer(
    retrykafka.WithBrokers("localhost:9092"),
    retrykafka.WithGroupID("payments-worker"),
)

consumer.Handle("payments", func(ctx context.Context, msg retrykafka.Message) error {
    return processPayment(ctx, msg)
})

_ = consumer.Run(ctx)
```

## Qué hace la librería

| Área | Comportamiento |
| --- | --- |
| Identidad | Header `x-message-id` estable entre topic principal, retries y DLQ |
| Publisher | Thread-safe, metadata `x-produced-at`, serialización JSON/`[]byte` |
| Consumer | Un worker por partición (orden por partición) |
| Retries | `{topic}.retry.1..N` con backoff 5s / 30s / 5m |
| DLQ | `{topic}.dlq` al agotar intentos |
| Garantía default | At-least-once (puede haber duplicados) |
| Transacciones | Opcional: `WithProcessingGuarantee(retrykafka.Transactional)` |
| Logs | `log/slog` (default `slog.Default()`) |
| OpenTelemetry | Opcional; funciona con providers no-op |
| Idempotencia | Opcional vía `idempotency.Store` |

## Semántica (importante)

**At-least-once (default):** publicar el retry y hacer commit del offset son operaciones independientes. Un crash entre ambas puede duplicar el retry.

**Transactional:** el retry/DLQ y el offset consumido van en la misma transacción Kafka cuando el driver lo soporta. **No** es exactly-once de side effects externos (HTTP, pagos, SQL).

**Ordering default:** *Retry Topics prioritize throughput and availability over strict ordering across retries.*

## Configuración

```go
retrykafka.NewConsumer(
    retrykafka.WithBrokers("localhost:9092"),
    retrykafka.WithGroupID("workers"),
    retrykafka.WithMaxAttempts(3),
    retrykafka.WithBackoff(5*time.Second, 30*time.Second, 5*time.Minute),
    retrykafka.WithLogger(logger),
    retrykafka.WithAutoCreateTopics(false),
    retrykafka.WithProcessingGuarantee(retrykafka.Transactional),
    retrykafka.WithOrdering(retrykafka.OrderingNone),
    retrykafka.WithIdempotencyStore(store),
)
```

Override por topic:

```go
consumer.Handle("payments", handler, retrykafka.WithMaxAttempts(5))
```

Naming custom:

```go
retrykafka.NewConsumer(
    retrykafka.WithBrokers("localhost:9092"),
    retrykafka.WithTopicNamer(retrykafka.TopicNamerFuncs{
        Retry: func(topic string, attempt int) string {
            return fmt.Sprintf("retry_%s_%d", topic, attempt)
        },
        DLQ: func(topic string) string {
            return "dead_" + topic
        },
    }),
)
```

`WithAutoCreateTopics(false)` es el default recomendado para producción: la infraestructura del cluster puede administrar topics principal, retry y DLQ externamente. Con `true`, los drivers que soportan creación explícita intentan preparar los topics requeridos usando `driver.TopicSpecCreator` o el fallback compatible `driver.TopicCreator`.

## Tests

```bash
go test ./...
go test -race ./...   # requiere CGO (gcc) en Windows
```

Los tests unitarios usan un broker in-memory (`driver.Memory`). No hace falta Kafka, DynamoDB, Redis ni Mongo.

## Módulos

```text
retrykafka/            API pública
driver/                abstracción Kafka + Memory + franz-go
observability/         slog + OpenTelemetry
idempotency/           interfaz + store en memoria
adapters/postgres      INSERT ON CONFLICT
adapters/dynamodb      conditional writes
adapters/redis         SET NX EX
adapters/mongo         unique index + findAndModify
examples/payments
```

## Documentación

- [CONCURRENCY.md](CONCURRENCY.md)
- [DECISIONS.md](DECISIONS.md)
- [docs/transactions.md](docs/transactions.md)
- [docs/ordering.md](docs/ordering.md)
- [docs/backoff.md](docs/backoff.md)
- [docs/idempotency.md](docs/idempotency.md)
- [docs/observability.md](docs/observability.md)

## Licencia

Apache-2.0 o MIT a elección del maintainer. Este repositorio arranca como librería open source de referencia.
