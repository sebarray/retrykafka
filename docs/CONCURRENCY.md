# Concurrencia

## Publisher

El `Publisher` es **thread-safe**. Cada `Publish` usa estado local (ID, headers, payload). No hay un generador de IDs mutable compartido: el ID se crea por operación con `crypto/rand`.

```go
go publisher.Publish(ctx, "payments", a)
go publisher.Publish(ctx, "payments", b)
```

El logger (`slog`) y el driver deben ser seguros para uso concurrente. `slog.Logger` lo es. No se admite un logger que mutile buffers compartidos sin sincronización.

## Consumer

Default: **procesamiento secuencial por partición**.

- Un goroutine dispatcher hace `Poll`.
- Un worker por `topic+partition` procesa su canal.
- Distintas particiones avanzan en paralelo.
- Dentro de una partición no hay paralelismo silencioso.

Esto evita commits fuera de orden en el modo default.

## Transacciones

Una transacción Kafka **no es thread-safe**. `retrykafka` serializa `Begin/Publish/SendOffsets/Commit/Abort` con un mutex del consumer (`txnMu`). El ownership es: *el worker que está enrutando un fallo posee la transacción hasta commit o abort*.

No se permite que dos goroutines muten la misma transacción.

## Rebalance

Antes de publicar retry/DLQ y antes de commit se consulta `Subscription.Assigned`. Si la partición fue revocada:

- no se confirma el offset
- se aborta la transacción activa
- Kafka puede reentregar el mensaje

## Shutdown

```text
cancel context
  → stop poll
  → workers salen o abortan waits/tx
  → timeout configurable (WithShutdownTimeout / WithTransactionShutdownTimeout)
  → close consumer/producer
```

Un mensaje a mitad de retry at-least-once puede publicarse y no commitearse (duplicado posible). En modo transaccional se aborta si el commit no llegó.

## Race detector

Toda la suite debe correrse con:

```bash
go test -race ./...
```

En Windows hace falta CGO (`gcc` en PATH). Sin CGO, `go test ./...` sigue siendo la verificación funcional.
