# retrykafka — Global Project Context

Estoy desarrollando una librería open source en Go llamada `retrykafka`.

`retrykafka` es una librería **Kafka-native reliability toolkit for Go**.

Su objetivo es simplificar el ciclo de vida completo de un mensaje Kafka, proporcionando una experiencia idiomática en Go inspirada conceptualmente en la experiencia de desarrollo de Spring Kafka.

La librería contempla tanto:

```text
Publisher
Consumer
```

y proporciona primitivas para:

```text
Message Identity
Message Metadata
Retries
Retry Topics
Backoff
Dead Letter Topics
Offset Coordination
Processing Guarantees
Kafka Transactions
Structured Logging
OpenTelemetry
Trace Propagation
Optional Idempotency
Optional Storage Adapters
Concurrency Safety
```

---

# 1. Propuesta de valor

La propuesta central de `retrykafka` es:

> Kafka-native reliability primitives for Go.

La librería no intenta abstraer múltiples brokers ni convertirse en un framework genérico de mensajería.

No busca soportar como objetivo principal:

```text
Kafka
RabbitMQ
NATS
SQS
Redis Streams
```

El proyecto entiende específicamente las primitivas de Kafka:

```text
Topics
Partitions
Offsets
Consumer Groups
Headers
Rebalances
Transactions
```

La propuesta es resolver problemas recurrentes que normalmente cada aplicación termina implementando manualmente:

```text
Manual retries
Manual retry topics
Manual backoff
Manual DLQ
Manual offset coordination
Manual message metadata
Manual trace propagation
Manual idempotency
```

La experiencia objetivo es:

```text
Developer writes business logic
            │
            ▼
retrykafka handles messaging reliability concerns
```

---

# 2. Principio fundamental de infraestructura

Kafka es la única infraestructura obligatoria.

```text
Kafka
  │
  ▼
retrykafka works
```

Cualquier infraestructura adicional debe ser opcional.

Ejemplos:

```text
PostgreSQL
DynamoDB
Redis
MongoDB
OpenTelemetry Collector
Prometheus
Jaeger
Tempo
Grafana
```

La librería debe funcionar sin ninguno de esos componentes.

---

# 3. Arquitectura conceptual

```text
                         retrykafka
                              │
             ┌────────────────┴────────────────┐
             │                                 │
         Publisher                          Consumer
             │                                 │
             │                              Handler
             │                                 │
             └──────────────┬──────────────────┘
                            │
                    Message Lifecycle
                            │
                            ▼
                 Processing Coordinator
                            │
             ┌──────────────┴──────────────┐
             │                             │
        At-Least-Once                 Transactional
             │                             │
             │                       Kafka Transaction
             │                             │
             └──────────────┬──────────────┘
                            │
                         Kafka
                            │
          ┌─────────────────┼─────────────────┐
          │                 │                 │
       Retry             DLQ             Metadata
                            │
                 ┌──────────┴──────────┐
                 │                     │
            Observability          Idempotency
                 │                     │
           ┌─────┴─────┐      ┌────────┼────────┐
           │           │      │        │        │
          slog        OTel    PostgreSQL Redis DynamoDB
                                               │
                                             MongoDB
```

---

# 4. Message Lifecycle

Cada mensaje debe poder recorrer el siguiente ciclo:

```text
Publisher
    │
    ▼
Main Topic
    │
    ▼
Consumer
    │
    ▼
Handler
    │
 ┌──┴──────────────┐
 │                 │
Success           Error
 │                 │
 │                 ▼
 │              Retry
 │                 │
 │                 ▼
 │            Retry Topic
 │                 │
 │                 ▼
 │              Handler
 │                 │
 │            ┌────┴────┐
 │            │         │
 │          Success    Error
 │            │         │
 │            │       Retry
 │            │         │
 │            │    Max Attempts
 │            │         │
 │            │     ┌───┴───┐
 │            │     │       │
 │            │    Retry    DLQ
 │            │
 └────────────┴─────────────► Offset completed
```

La identidad lógica del mensaje debe mantenerse durante todo el lifecycle.

---

# 5. Message Identity

Cada mensaje debe tener un identificador lógico único.

Header default:

```text
x-message-id
```

Este ID debe mantenerse estable durante:

```text
Main Topic
    │
    ▼
Retry Topic
    │
    ▼
Retry Topic
    │
    ▼
DLQ
```

Ejemplo:

```text
Main Topic
message-id = abc-123

Retry 1
message-id = abc-123

Retry 2
message-id = abc-123

DLQ
message-id = abc-123
```

La librería puede generar automáticamente el Message ID si no existe.

Si ya existe, debe preservarlo.

---

# 6. Message Envelope

La librería debe definir un contrato interno estable para representar un mensaje.

Conceptualmente:

```go
type Message struct {
    ID        string
    Payload   []byte
    Headers   Headers

    Topic     string
    Partition int
    Offset    int64
    Timestamp time.Time
}
```

La estructura exacta puede evolucionar.

Debe permitir:

```text
Preserve payload
Preserve headers
Track source metadata
Track retry metadata
Preserve message identity
```

---

# 7. Standard Metadata

El Publisher puede agregar automáticamente:

```text
x-message-id
x-produced-at
x-event-type
x-schema-version
```

Los retries deben agregar:

```text
x-retry-attempt
x-original-topic
x-original-partition
x-original-offset
x-first-failure-at
x-last-error
```

La metadata debe preservarse cuando un mensaje se mueve entre topics.

No sobrescribir metadata existente salvo que sea necesario y documentado.

---

# 8. Publisher

El Publisher debe proporcionar una API simple e idiomática.

Ejemplo conceptual:

```go
publisher := retrykafka.NewPublisher(
    retrykafka.WithBrokers("localhost:9092"),
)

err := publisher.Publish(
    ctx,
    "payments",
    PaymentCreated{
        ID: "payment-123",
    },
)
```

Responsabilidades:

```text
Serialize payload
Generate Message ID when needed
Preserve existing Message ID
Attach standard metadata
Inject trace context when enabled
Publish message
```

---

# 9. Publisher Concurrency

El Publisher debe ser seguro para uso concurrente.

Ejemplo:

```go
go publisher.Publish(ctx, "payments", paymentA)
go publisher.Publish(ctx, "payments", paymentB)
go publisher.Publish(ctx, "payments", paymentC)
```

Evitar estado mutable compartido por operación.

Preferir:

```text
Operation-local state
Immutable configuration
Thread-safe dependencies
```

Toda implementación debe validarse con:

```bash
go test -race ./...
```

---

# 10. Consumer

El Consumer debe permitir registrar handlers de forma simple.

Ejemplo conceptual:

```go
consumer.Handle(
    "payments",
    func(ctx context.Context, msg Message) error {
        return processPayment(ctx, msg)
    },
)
```

El Consumer debe integrar:

```text
Message metadata
Handler execution
Retry coordination
Offset coordination
DLQ routing
Processing guarantees
Observability
Optional idempotency
```

---

# 11. Consumer Concurrency Model

El comportamiento default debe ser explícito:

```text
Sequential processing per partition.
```

Conceptualmente:

```text
Partition 0
Message A
    │
    ▼
Message B
    │
    ▼
Message C
```

Mientras diferentes particiones pueden procesarse independientemente:

```text
Partition 0 → Worker
Partition 1 → Worker
Partition 2 → Worker
```

La librería no debe permitir commits incorrectos por procesamiento concurrente fuera de orden.

La concurrencia avanzada debe ser explícita y configurable.

Nunca introducir paralelismo interno silencioso que pueda romper ordering o offset coordination.

---

# 12. Processing Guarantees

La librería debe soportar diferentes modos de procesamiento.

```go
type ProcessingGuarantee int

const (
    AtLeastOnce ProcessingGuarantee = iota
    Transactional
)
```

Default:

```text
AtLeastOnce
```

Opcional:

```text
Transactional
```

---

# 13. At-Least-Once Mode

Este es el comportamiento default.

Cuando el handler falla:

```text
Handler Error
      │
      ▼
Publish Retry
      │
      ▼
Commit Original Offset
```

Estas operaciones son independientes.

Puede ocurrir:

```text
Publish Retry
      │
      ▼
SUCCESS
      │
      ▼
Process crashes
      │
      ▼
Offset not committed
```

Luego:

```text
Original message is redelivered
      │
      ▼
Retry can be published again
```

Por lo tanto:

```text
Duplicate delivery is possible.
Duplicate retry messages are possible.
```

La librería debe documentarlo claramente.

No prometer exactly-once semantics en este modo.

---

# 14. Transactional Mode

El modo transaccional es opcional.

Objetivo:

Coordinar atómicamente operaciones Kafka-to-Kafka.

Caso principal:

```text
Handler Error
      │
      ▼
BEGIN TRANSACTION
      │
      ├── Publish Retry Message
      │
      └── Send Consumed Offset
      │
      ▼
COMMIT TRANSACTION
```

Conceptualmente:

```text
Produced Kafka Record
+
Consumed Kafka Offset
=
Same Kafka Transaction
```

Si ocurre un fallo antes del commit:

```text
Transaction Abort
```

El retry producido no debe ser tratado como una operación exitosamente confirmada.

---

# 15. Transactional Retry Flow

```text
Main Topic
    │
    ▼
Consumer
    │
    ▼
Handler Error
    │
    ▼
BEGIN TRANSACTION
    │
    ├── Publish Retry Topic Message
    │
    └── Commit Consumed Offset
    │
    ▼
COMMIT TRANSACTION
```

Debe aplicarse a:

```text
Main Topic → Retry Topic

Retry Topic → Retry Topic

Retry Topic → DLQ
```

---

# 16. Transactional Success Flow

Cuando el handler tiene éxito:

```text
Consume
   │
   ▼
Handler Success
   │
   ▼
Commit Offset
```

No abrir una transacción innecesariamente si no existe una operación de producción Kafka asociada.

La implementación debe documentar claramente:

```text
Success path
Failure path
Retry path
DLQ path
```

---

# 17. Kafka Transactions Scope

Kafka Transactions mejoran las garantías del pipeline:

```text
Kafka
   │
Consume
   │
Process
   │
Produce Kafka
```

No garantizan automáticamente:

```text
Exactly-once execution of external side effects.
```

Ejemplo:

```text
Consume Kafka
     │
     ▼
Charge Credit Card
     │
     ▼
SUCCESS
     │
     ▼
Process crashes before Kafka offset coordination
```

El mensaje puede volver a entregarse.

Por lo tanto:

```text
Kafka Transactions
≠
Exactly-once business processing
```

Para side effects externos debe poder utilizarse idempotencia.

---

# 18. Retry Topics

Naming default:

```text
{topic}.retry.1
{topic}.retry.2
{topic}.retry.3
```

DLQ:

```text
{topic}.dlq
```

El naming debe ser configurable.

---

# 19. Backoff

Defaults iniciales:

```text
Attempt 1 → 5 seconds
Attempt 2 → 30 seconds
Attempt 3 → 5 minutes
```

Debe ser configurable.

La implementación debe evitar bloquear incorrectamente consumers y particiones.

El mecanismo de delayed retry debe documentarse claramente.

---

# 20. DLQ

Cuando un mensaje supera el máximo de intentos:

```text
{topic}.dlq
```

Debe preservar:

```text
Original payload
Message ID
Original metadata
Retry metadata
Failure information
```

En modo transaccional:

```text
Publish DLQ
+
Commit consumed offset
```

deben participar en la misma transacción cuando el driver lo soporte.

---

# 21. Offset Coordination

El offset management es parte central de la librería.

Success:

```text
Handler Success
      │
      ▼
Commit Offset
```

Failure en modo AtLeastOnce:

```text
Publish Retry/DLQ
      │
      ▼
Commit Offset
```

Failure en modo Transactional:

```text
BEGIN TRANSACTION
      │
      ├── Publish Retry/DLQ
      └── Send Offset
      │
      ▼
COMMIT
```

Nunca realizar commits fuera de orden.

Nunca confirmar offsets de particiones que el consumer ya no posee.

---

# 22. Driver Architecture

El core no debe depender directamente de una única implementación concreta de Kafka.

Debe existir una abstracción mínima orientada a:

```text
Publishing
Consuming
Offset coordination
Capabilities
Transactions when supported
```

No abstraer todo Kafka ni intentar simular otros brokers.

El objetivo de la abstracción es:

```text
Testability
Driver adapters
Capability detection
Transactional support
```

---

# 23. Driver Capabilities

Cada driver debe declarar sus capacidades.

Conceptualmente:

```go
type DriverCapabilities struct {
    Transactions bool
}
```

Futuras capacidades pueden agregarse solo cuando exista una necesidad real.

Si el usuario solicita:

```text
Transactional processing
```

pero el driver no soporta transacciones:

```text
Return explicit configuration error.
```

Nunca hacer:

```text
Transactional requested
        ↓
Silent fallback
        ↓
At-least-once
```

---

# 24. Transaction Lifecycle

Una transacción debe seguir un lifecycle explícito:

```text
BEGIN
   │
   ├── Produce Retry or DLQ
   │
   ├── Send Offset
   │
   ▼
COMMIT
```

Ante errores:

```text
BEGIN
   │
   ▼
Error
   │
   ▼
ABORT
```

Analizar y manejar explícitamente:

```text
Begin errors
Publish errors
Offset errors
Commit errors
Abort errors
Context cancellation
Transaction timeout
Producer fencing
Consumer rebalance
Shutdown
```

---

# 25. Transaction Concurrency

Una transacción debe tener ownership claro.

No permitir múltiples goroutines modificando una misma transacción sin coordinación explícita.

Analizar:

```text
Concurrent transactions
Producer ownership
Partition processing
Consumer groups
Rebalances
Shutdown races
```

La implementación debe definir claramente:

```text
Who owns a transaction
When it starts
When it commits
When it aborts
```

---

# 26. Rebalances

Escenario crítico:

```text
Consumer processing message
        │
        ▼
Transaction active
        │
        ▼
Consumer rebalance
```

La librería debe:

```text
Detect partition ownership loss
Avoid invalid offset commits
Abort transaction when appropriate
Stop processing revoked partitions
Allow Kafka redelivery
```

Nunca confirmar offsets de una partición cuya ownership fue perdida.

---

# 27. Shutdown

Shutdown debe ser coordinado.

```text
Shutdown requested
      │
      ▼
Stop accepting new work
      │
      ▼
Finish or abort active work
      │
      ▼
Finish or abort active transaction
      │
      ▼
Release consumer
      │
      ▼
Close producer
```

Los timeouts deben ser configurables.

---

# 28. Idempotency

La idempotencia es opcional.

El core debe definir una interfaz.

Conceptualmente:

```go
type IdempotencyStore interface {
    TryStart(
        ctx context.Context,
        key string,
    ) (Result, error)

    Complete(
        ctx context.Context,
        key string,
    ) error

    Fail(
        ctx context.Context,
        key string,
    ) error
}
```

La operación inicial debe ser atómica.

Nunca:

```text
Check
  │
  ▼
Process
  │
  ▼
Save
```

Porque permite:

```text
Consumer A → Check false
Consumer B → Check false

Consumer A → Process
Consumer B → Process
```

Preferir:

```text
Atomic Claim
      │
      ▼
Process
      │
      ▼
Complete
```

---

# 29. Idempotency States

Estados conceptuales:

```text
PROCESSING
COMPLETED
FAILED
```

Resolver:

```text
PROCESSING
     │
     ▼
Process crashes
     │
     ▼
Message redelivery
```

No permitir un estado PROCESSING permanente.

Considerar:

```text
Lease
TTL
Processing timeout
Expiration
```

---

# 30. Idempotency Adapters

Los adapters son opcionales.

Roadmap:

```text
PostgreSQL
DynamoDB
Redis
MongoDB
```

Cada backend debe utilizar sus operaciones atómicas correspondientes.

PostgreSQL:

```text
Unique Constraint
INSERT ON CONFLICT
Transactions
```

DynamoDB:

```text
Conditional Writes
ConditionExpression
TTL
```

Redis:

```text
SET NX
Expiration
Atomic operations
```

MongoDB:

```text
Unique Index
Atomic operations
TTL Index
```

El core nunca debe depender directamente de estos drivers.

---

# 31. Structured Logging

Utilizar:

```text
log/slog
```

Default:

```go
slog.Default()
```

Permitir logger custom.

Eventos:

```text
Message received
Message processed
Message processing failed
Retry scheduled
Retry published
Message sent to DLQ
Offset committed
Consumer shutdown

Transaction started
Transaction committed
Transaction aborted
Transaction failed
```

Atributos:

```text
topic
partition
offset
message_id
retry_attempt
max_attempts
processing_guarantee
error
```

Para transacciones, evitar usar IDs únicos como labels de métricas.

No loguear payload completo por defecto.

---

# 32. OpenTelemetry

OpenTelemetry es opcional.

La librería debe funcionar sin:

```text
OTel Collector
Jaeger
Tempo
Prometheus
Grafana
```

Utilizar providers globales por defecto:

```go
otel.GetTracerProvider()
otel.GetMeterProvider()
```

Permitir providers custom.

Tracing:

```text
messaging.publish
messaging.consume
messaging.process
messaging.retry.publish
messaging.dlq.publish

transaction.begin
transaction.publish
transaction.offsets
transaction.commit
transaction.abort
```

Propagar trace context mediante Kafka Headers.

---

# 33. Metrics

Métricas conceptuales:

```text
messages_processed_total
messages_failed_total
retry_attempts_total
messages_sent_dlq_total
handler_duration

transactions_started_total
transactions_committed_total
transactions_aborted_total
transaction_failures_total
transaction_duration
```

Evitar alta cardinalidad.

Nunca utilizar como labels:

```text
message_id
offset
user_id
transaction_id
```

---

# 34. Ordering

No garantizar strict ordering across retries por defecto.

Documentar:

> Retry Topics prioritize reliability and throughput over strict ordering across retries.

Futuras estrategias:

```text
None
PerPartition
PerKey
```

No implementar soluciones distribuidas incorrectas.

La estrategia default debe priorizar:

```text
Correctness
Simplicity
Throughput
```

antes que intentar garantizar ordering global.

---

# 35. Configuration

Utilizar Functional Options.

Principio:

```text
Simple defaults.
Advanced customization when needed.
```

Ejemplo:

```go
retrykafka.New(
    retrykafka.WithBrokers("localhost:9092"),
)
```

Defaults:

```text
Processing Guarantee:
AtLeastOnce

Retries:
3

Backoff:
5s
30s
5m

DLQ:
Enabled

Retry naming:
{topic}.retry.{attempt}

DLQ naming:
{topic}.dlq

Logging:
slog.Default()

Telemetry:
Global OpenTelemetry providers

Idempotency:
Disabled

Transactions:
Disabled by default

Auto Create Topics:
Disabled
```

---

# 36. Global and Per-Topic Configuration

Permitir configuración global.

También overrides por topic cuando tenga sentido.

Ejemplo conceptual:

```go
consumer.Handle(
    "payments",
    handler,
    retrykafka.WithMaxAttempts(5),
)
```

La configuración específica debe sobrescribir correctamente la configuración global.

---

# 37. Race Conditions

La seguridad de concurrencia es un requisito transversal.

Cada feature debe analizar:

```text
Multiple goroutines
Mutable shared state
Offset commit races
Retry publish races
Transaction races
Consumer rebalance
Shutdown races
Duplicate delivery
Idempotency races
```

Toda la suite debe ejecutarse con:

```bash
go test ./...
go test -race ./...
```

---

# 38. Testing

Cada versión debe incluir:

```text
Unit Tests
Concurrency Tests
Race Detector
Integration Tests when required
```

Escenarios:

```text
Concurrent publishing
Duplicate delivery
Concurrent idempotency claims
Retry failures
DLQ
Shutdown
Consumer crashes
Rebalances
Offset handling
Metadata preservation

Transaction commit
Transaction abort
Retry + Offset atomic coordination
DLQ + Offset atomic coordination
Transaction timeout
Producer fencing
```

---

# 39. Semántica y garantías

La documentación debe ser explícita.

Modo default:

```text
At-least-once delivery.
Duplicates possible.
```

Modo transaccional:

```text
Atomic Kafka-to-Kafka coordination
when supported by the selected driver.
```

No prometer:

```text
Exactly-once business processing
```

sin mecanismos adicionales.

Para side effects externos:

```text
Database writes
HTTP calls
Payments
Emails
```

recomendar:

```text
Idempotency
Transactional Outbox
Application-level guarantees
```

según el caso.

---

# 40. Arquitectura de módulos

El core debe ser pequeño.

Conceptualmente:

```text
retrykafka/
│
├── core/
├── message/
├── publisher/
├── consumer/
├── retry/
├── dlq/
├── processing/
├── transaction/
├── driver/
│
├── observability/
│   ├── logging/
│   └── otel/
│
├── idempotency/
│
├── adapters/
│   ├── postgres/
│   ├── dynamodb/
│   ├── redis/
│   └── mongo/
│
└── examples/
```

Los adapters dependen del core.

Nunca al revés.

El core no debe depender de:

```text
PostgreSQL
Redis
MongoDB
DynamoDB
OTel backend
```

---

# 41. API Philosophy

La API debe ser:

```text
Idiomatic Go
Explicit
Composable
Minimal
Safe by default
```

Evitar:

```text
Magic behavior
Hidden goroutines
Silent fallbacks
Implicit guarantee changes
Heavy framework abstractions
```

Preferir:

```text
Explicit configuration
Documented defaults
Clear ownership
Clear lifecycle
```

---

# 42. Principio de evolución

No implementar features futuras anticipadamente.

Cada versión debe:

```text
Compile
Pass tests
Pass race detector
Document trade-offs
Maintain clear API boundaries
Avoid unnecessary dependencies
```

No romper compatibilidad pública sin una razón clara.

---

# 43. Definición de éxito

`retrykafka` debe permitir que un developer Go implemente un flujo confiable de Kafka sin tener que construir manualmente toda esta lógica:

```text
Kafka Client
    │
    ▼
Manual Message Identity
    │
    ▼
Manual Retry Topics
    │
    ▼
Manual Backoff
    │
    ▼
Manual DLQ
    │
    ▼
Manual Offset Coordination
    │
    ▼
Manual Trace Propagation
    │
    ▼
Manual Idempotency
```

La experiencia objetivo es:

```text
Business Handler
      │
      ▼
retrykafka
      │
      ▼
Reliable Kafka Message Lifecycle
```

---

# Proyecto en una frase

> retrykafka is a Kafka-native reliability toolkit for Go that manages the message lifecycle—from publishing and processing to retries, dead letters, observability, idempotency and transactional Kafka flows—while keeping Kafka as the only required infrastructure.

---

# Principio final

```text
Simple by default.

Reliable by design.

Transactional when needed.

Extensible without mandatory infrastructure.
```
