# Ordering

Default documentado:

> Retry Topics prioritize throughput and availability over strict ordering across retries.

## None (default)

```text
A falla → retry topic + commit
B se procesa
C se procesa
A reaparece más tarde
```

Correcto, simple, alto throughput. No hay orden estricto entre el retry de A y B/C.

## PerPartition

```text
A falla → la partición espera (retry in-place + backoff)
B espera
C espera
```

Garantiza orden por partición a costa de:

- head-of-line blocking
- poison messages que detienen la partición hasta DLQ
- menor throughput

## PerKey (experimental)

Idea:

```text
key=payment-123  A falla, B y C de esa key esperan
key=payment-999  X e Y siguen
```

Límites reales con consumer groups:

- Kafka ya asigna una key a **una** partición; el orden por key *dentro de la partición* es el único que el log garantiza.
- Un consumer no puede commitear el offset N+1 si N sigue pendiente sin arriesgar pérdida al rebalance.
- Un sequencer distribuido (varias instancias, rebalances, crashes) necesita estado persistente (otro store). Sin eso, la feature es best-effort en un único worker de partición.
- Límites de memoria si se bufferean offsets posteriores a un poison key.

Por eso permanece **experimental**: no se vende como garantía distribuida.

## Coste relativo

| Estrategia | Throughput | HOL | Infra extra |
| --- | --- | --- | --- |
| None | alto | no (en main topic) | no |
| PerPartition | bajo en fallos | sí | no |
| PerKey | medio | por key | sí para garantías reales |
