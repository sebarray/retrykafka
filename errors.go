package retrykafka

import "errors"

var (
	ErrClosed                  = errors.New("retrykafka: client is closed")
	ErrNoHandler               = errors.New("retrykafka: no handler registered for topic")
	ErrTransactionsUnsupported = errors.New("retrykafka: transactional processing requested but the driver does not support transactions")
	ErrInvalidConfig           = errors.New("retrykafka: invalid configuration")
	ErrPartitionRevoked        = errors.New("retrykafka: partition ownership lost")
	ErrIdempotentSkip          = errors.New("retrykafka: message skipped by idempotency store")
)
