# Transacciones Kafka

## Qué garantiza

Coordinación atómica **Kafka-to-Kafka**:

```text
Produce retry o DLQ
+
Send consumed offset
=
misma transacción
```

Si el proceso cae antes del `COMMIT`, la transacción aborta. Un consumer con aislamiento `read_committed` no ve el retry.

## Qué no garantiza

Exactly-once de side effects externos:

```text
Kafka Transaction
  ├── Retry topic publish
  └── Offset commit

UPDATE SQL / HTTP / pago / email
  → NO es atómico con la transacción Kafka
```

Para eso usar `idempotency.Store` u outbox.

## API

Default (at-least-once):

```go
retrykafka.NewConsumer(retrykafka.WithBrokers("localhost:9092"))
```

Modo transaccional (preferido para evolucionar garantías):

```go
retrykafka.NewConsumer(
    retrykafka.WithBrokers("localhost:9092"),
    retrykafka.WithProcessingGuarantee(retrykafka.Transactional),
)
```

Equivalente: `WithTransactions()`.

Si el driver no declara `Capabilities.Transactions`, `NewConsumer`/`NewPublisher` devuelven `ErrTransactionsUnsupported`. No hay fallback silencioso.

## Flujos

**Handler OK:** commit de offset normal. No se abre transacción (no hay produce asociado).

**Handler error → retry/DLQ:**

```text
BEGIN
  Produce destino
  SendOffsets(group)
COMMIT
```

Cualquier error → `ABORT`.

## Isolation

En modo transaccional el consumer pide `read_committed` cuando el driver lo soporta (`SubscribeIsolated` en Memory; `FetchIsolationLevel(ReadCommitted)` en franz-go).

Los consumers ajenos a la librería deben usar el mismo aislamiento si no quieren ver records abortados.

## Ownership

Una transacción la posee un único flujo de procesamiento. El consumer serializa el productor transaccional. Un rebalance aborta si se perdió la partición.

## Costes

Ventajas: menos duplicados retry/DLQ, coordinación offset+produce.

Costes: más latencia, `transactional.id`, timeouts, fencing, complejidad operativa.

## Métricas y logs

Spans: `transaction.begin|publish|offsets|commit|abort`.

Métricas: `transactions_started_total`, `transactions_committed_total`, `transactions_aborted_total`, `transaction_failures_total` (attribute `operation`), `transaction_duration`.

`transaction_id` puede aparecer en logs, **nunca** como label de métrica.
