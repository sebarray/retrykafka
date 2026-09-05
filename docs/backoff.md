# Backoff y delayed retries

Defaults: **5s, 30s, 5m** en los intentos 1, 2 y 3.

## Mecanismo

Al fallar, el mensaje se publica de inmediato al retry topic (o se reintenta in-place en `OrderingPerPartition`) con:

```text
x-retry-not-before
```

El worker de esa partición espera hasta esa marca **antes** del handler. Otras particiones no se bloquean.

## Por qué no un sleep global

Dormir en el dispatcher detendría todo el consumer. El delay vive en el worker de la partición.

## Limitaciones

- La partición del retry topic queda pausada (HOL) mientras el mensaje no es due. Es el precio de no usar un scheduler externo.
- No hay pause/resume de Kafka broker-level más allá del worker.
- Backoffs largos (5m) reducen el paralelismo de esa partición de retry.
- At-least-once: el delay no evita duplicados si hay crash tras publicar el retry y antes del commit.

## Alternativas no implementadas

Topics de delay con `n` particiones temporales, o un scheduler (Redis/ZSET). Requieren infra extra y violan “Kafka only”.
