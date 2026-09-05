package driver

import (
	"context"
	"time"
)

// Capabilities describes optional Kafka features offered by a driver.
type Capabilities struct {
	Transactions bool
}

// Header is a single Kafka header.
type Header struct {
	Key   string
	Value []byte
}

// Record is a Kafka record in driver terms.
type Record struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   []Header
	Timestamp time.Time
}

// Offset identifies a consumed record for commit or transactional send-offsets.
type Offset struct {
	Topic     string
	Partition int
	Offset    int64
}

// Driver is the minimum Kafka surface used by retrykafka.
type Driver interface {
	Capabilities() Capabilities
	Publish(ctx context.Context, rec Record) error
	Subscribe(ctx context.Context, groupID string, topics []string) (Subscription, error)
	Close() error
}

// TopicSpec describes a topic a driver should create when auto-create is enabled.
type TopicSpec struct {
	Name              string
	Partitions        int
	ReplicationFactor int
	Configs           map[string]string
}

// TopicCreator is optionally implemented by drivers that can proactively create topics.
type TopicCreator interface {
	CreateTopics(ctx context.Context, topics []string) error
}

// TopicSpecCreator is optionally implemented by drivers that can create topics with settings.
type TopicSpecCreator interface {
	CreateTopicSpecs(ctx context.Context, topics []TopicSpec) error
}

// Subscription is a consumer group assignment.
type Subscription interface {
	Poll(ctx context.Context) (Record, error)
	Commit(ctx context.Context, rec Record) error
	Assigned(topic string, partition int) bool
	Close() error
}

// TxnDriver is implemented by drivers that support Kafka transactions.
type TxnDriver interface {
	Driver
	BeginTxn(ctx context.Context) (Txn, error)
}

// Txn is a single Kafka transaction. It is not safe for concurrent use.
type Txn interface {
	Publish(ctx context.Context, rec Record) error
	SendOffsets(ctx context.Context, offsets []Offset, groupID string) error
	Commit(ctx context.Context) error
	Abort(ctx context.Context) error
}

// Isolation is the consumer read isolation.
type Isolation int

const (
	ReadUncommitted Isolation = iota
	ReadCommitted
)
