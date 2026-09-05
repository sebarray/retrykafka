# Decisiones y trade-offs

## Driver abstraction

El core habla con `driver.Driver`, no con franz-go. Motivo: tests deterministas, capability detection y transacciones sin Kafka real.

El adapter `driver/kafka` usa [franz-go](https://github.com/twmb/franz-go). Los tests de pipeline corren contra `driver.Memory`.

## At-least-once vs transacciones

El default es at-least-once porque:

- no exige `transactional.id`
- menor latencia
- es el modelo mental de la mayoría de consumers Go

El modo transaccional es **opt-in** y falla de forma explícita si `Capabilities.Transactions == false`. Nunca hay fallback silencioso.

## Success path sin transacción

Si el handler tiene éxito **no** se abre transacción: no hay produce de retry/DLQ. Un commit de offset simple es suficiente y más barato. El coste de EOS en el success path no justifica la latencia extra.

## Delayed retry

Se usa header `x-retry-not-before` y el worker de la partición **espera** hasta esa marca. Otras particiones siguen. Limitación: esa partición hace head-of-line blocking durante el delay. Ver [docs/backoff.md](docs/backoff.md).

## Ordering

Default `OrderingNone`: el fallo se va a retry topic y el offset original se confirma; B y C siguen. Prioriza throughput.

`OrderingPerPartition`: retry in-place (la partición espera). Vulnerable a poison pills.

`OrderingPerKey`: experimental. Kafka ya colocaliza keys en una partición; un sequencer distribuido correcto exigiría estado persistente extra. Ver [docs/ordering.md](docs/ordering.md).

## Idempotencia

La interfaz vive en `idempotency` y los adapters **no** entran al core. Redis **no** se documenta como equivalente a PostgreSQL: es deduplicación con TTL, no idempotencia transaccional durable.

## Observabilidad

OpenTelemetry usa providers globales (no-op si no hay SDK). Cero infraestructura extra obligatoria. Las métricas no usan `message_id` / `offset` / `transaction_id` como labels.

## Auto-create topics

Deshabilitado a nivel de producto (`autoCreate` default false en config). Con `WithAutoCreateTopics(true)`, el core intenta crear los topics conocidos mediante drivers que implementen `driver.TopicSpecCreator` o, como fallback compatible, `driver.TopicCreator`: el consumer prepara el topic principal, retries y DLQ; el publisher prepara el topic de publish.

En el adapter Kafka de franz-go esto usa `CreateTopics` con `driver.TopicSpec` y habilita `AllowAutoTopicCreation` sólo cuando se pidió explícitamente. En producción, la recomendación sigue siendo administrar topics desde la infraestructura del cluster cuando se necesita control fino de particiones, replicación, retención y ACLs.

## Versiones cubiertas

Implementación acumulativa de las specs `v0.1.0` … `v1.0.1`, incluyendo `v0.3.0` para configuración/defaults/options/naming y `v0.6.0` para idempotencia core + PostgreSQL.
