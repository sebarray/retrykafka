# Idempotencia

Opcional. El core solo depende de:

```go
type Store interface {
    TryStart(ctx context.Context, key string) (Result, error)
    Complete(ctx context.Context, key string) error
    Fail(ctx context.Context, key string) error
}
```

`TryStart` es un **claim atómico**. Nunca check-then-insert.

Estados: `PROCESSING` (con lease/TTL), `COMPLETED`, `FAILED`. Un `PROCESSING` no es eterno: el lease se puede robar al expirar (crash recovery).

Clave típica: `x-message-id`.

## Adapters

| Adapter | Primitiva | Garantía |
| --- | --- | --- |
| `idempotency.Memory` | mutex | solo proceso |
| `adapters/postgres` | `INSERT ON CONFLICT` + unique key | durable, transaccional SQL |
| `adapters/dynamodb` | `PutIfAbsent` / condition + TTL | durable, semántica cloud, TTL eventual |
| `adapters/redis` | `SET NX EX` + CAS | **deduplicación con TTL**, no durabilidad transaccional |
| `adapters/mongo` | unique index + findAndModify + TTL index | durable a nivel documento; distinto a SQL |

Redis y PostgreSQL **no** son equivalentes: un `FLUSHALL`, eviction o failover sin persistencia pierde el claim.

## Tests de integración

Los unit tests usan fakes in-process. Integración real (DynamoDB Local, Redis, Testcontainers Mongo, Postgres) va con tag `integration` cuando el entorno esté disponible.
