package retrykafka

// ProcessingGuarantee selects Kafka pipeline semantics.
type ProcessingGuarantee int

const (
	// AtLeastOnce publishes retry/DLQ and commits the consumed offset independently.
	// Duplicates are possible if the process crashes between those operations.
	AtLeastOnce ProcessingGuarantee = iota

	// Transactional coordinates retry/DLQ publish and the consumed offset in one
	// Kafka transaction when the driver supports it.
	// This is not exactly-once execution of external side effects.
	Transactional
)

func (g ProcessingGuarantee) String() string {
	switch g {
	case Transactional:
		return "transactional"
	default:
		return "at_least_once"
	}
}

// OrderingStrategy controls how failed messages interact with later ones.
type OrderingStrategy int

const (
	// OrderingNone is the default: a failure is routed to a retry topic and the
	// partition continues. Strict order across retries is not guaranteed.
	OrderingNone OrderingStrategy = iota

	// OrderingPerPartition retries in place so later offsets wait. This causes
	// head-of-line blocking and is vulnerable to poison messages.
	OrderingPerPartition

	// OrderingPerKey is experimental. Same-key messages wait while other keys in
	// the same partition may proceed, but offsets are only committed contiguously.
	// Correctness across consumer groups still depends on Kafka key partitioning.
	OrderingPerKey
)

func (s OrderingStrategy) String() string {
	switch s {
	case OrderingPerPartition:
		return "per_partition"
	case OrderingPerKey:
		return "per_key"
	default:
		return "none"
	}
}
