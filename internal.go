package retrykafka

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/retrykafka/retrykafka/driver"
)

// TopicNamer builds retry and DLQ topic names for a logical source topic.
type TopicNamer interface {
	RetryTopic(topic string, attempt int) string
	DLQTopic(topic string) string
}

// TopicNamerFuncs is a small adapter for callers that prefer functions over a struct type.
type TopicNamerFuncs struct {
	Retry func(topic string, attempt int) string
	DLQ   func(topic string) string
}

func (f TopicNamerFuncs) RetryTopic(topic string, attempt int) string {
	if f.Retry == nil {
		return defaultTopicNamer.RetryTopic(topic, attempt)
	}
	return f.Retry(topic, attempt)
}

func (f TopicNamerFuncs) DLQTopic(topic string) string {
	if f.DLQ == nil {
		return defaultTopicNamer.DLQTopic(topic)
	}
	return f.DLQ(topic)
}

var defaultTopicNamer = formatTopicNamer{
	retryFmt: "%s.retry.%d",
	dlqFmt:   "%s.dlq",
}

type formatTopicNamer struct {
	retryFmt string
	dlqFmt   string
}

func (n formatTopicNamer) RetryTopic(topic string, attempt int) string {
	return fmt.Sprintf(n.retryFmt, topic, attempt)
}

func (n formatTopicNamer) DLQTopic(topic string) string {
	return fmt.Sprintf(n.dlqFmt, topic)
}

func newMessageID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return hex.EncodeToString(b[:])
}

func serialize(payload any) ([]byte, error) {
	switch v := payload.(type) {
	case nil:
		return nil, nil
	case []byte:
		out := make([]byte, len(v))
		copy(out, v)
		return out, nil
	case string:
		return []byte(v), nil
	default:
		return json.Marshal(v)
	}
}

func backoffFor(backoffs []time.Duration, attempt int) time.Duration {
	if attempt <= 0 || len(backoffs) == 0 {
		return 0
	}
	idx := attempt - 1
	if idx >= len(backoffs) {
		idx = len(backoffs) - 1
	}
	return backoffs[idx]
}

func validateConfig(cfg config) error {
	if cfg.groupID == "" {
		return fmt.Errorf("%w: group id is required", ErrInvalidConfig)
	}
	if cfg.maxRetries < 0 {
		return fmt.Errorf("%w: max attempts must be >= 0", ErrInvalidConfig)
	}
	if cfg.retryFmt == "" && cfg.topicNamer == nil {
		return fmt.Errorf("%w: retry topic format is required", ErrInvalidConfig)
	}
	if cfg.dlqFmt == "" && cfg.topicNamer == nil {
		return fmt.Errorf("%w: DLQ topic format is required", ErrInvalidConfig)
	}
	if cfg.shutdownTimeout < 0 {
		return fmt.Errorf("%w: shutdown timeout must be >= 0", ErrInvalidConfig)
	}
	if cfg.txnShutdown < 0 {
		return fmt.Errorf("%w: transaction shutdown timeout must be >= 0", ErrInvalidConfig)
	}
	if cfg.clock == nil {
		return fmt.Errorf("%w: clock is required", ErrInvalidConfig)
	}
	if err := validateBackoffs(cfg.backoffs); err != nil {
		return err
	}
	return validateTopicNames(cfg, "retrykafka")
}

func validateBackoffs(backoffs []time.Duration) error {
	for _, d := range backoffs {
		if d < 0 {
			return fmt.Errorf("%w: backoff durations must be >= 0", ErrInvalidConfig)
		}
	}
	return nil
}

func validateTopicNames(cfg config, topic string) error {
	if topic == "" {
		return fmt.Errorf("%w: topic is required", ErrInvalidConfig)
	}
	for attempt := 1; attempt <= cfg.maxRetries; attempt++ {
		name := cfg.retryTopic(topic, attempt)
		if name == "" {
			return fmt.Errorf("%w: retry topic name is required", ErrInvalidConfig)
		}
		if name == topic {
			return fmt.Errorf("%w: retry topic %q collides with source topic", ErrInvalidConfig, name)
		}
		if attempt > 1 && name == cfg.retryTopic(topic, attempt-1) {
			return fmt.Errorf("%w: retry topic %q is reused across attempts", ErrInvalidConfig, name)
		}
	}
	if cfg.dlqEnabled {
		name := cfg.dlqTopic(topic)
		if name == "" {
			return fmt.Errorf("%w: DLQ topic name is required", ErrInvalidConfig)
		}
		if name == topic {
			return fmt.Errorf("%w: DLQ topic %q collides with source topic", ErrInvalidConfig, name)
		}
	}
	return nil
}

func defaultTopicSpec(name string) driver.TopicSpec {
	return driver.TopicSpec{
		Name:              name,
		Partitions:        1,
		ReplicationFactor: 1,
	}
}

func createTopics(ctx context.Context, drv driver.Driver, topics []driver.TopicSpec) error {
	if len(topics) == 0 {
		return nil
	}
	if creator, ok := drv.(driver.TopicSpecCreator); ok {
		return creator.CreateTopicSpecs(ctx, topics)
	}
	creator, ok := drv.(driver.TopicCreator)
	if !ok {
		return nil
	}
	names := make([]string, 0, len(topics))
	for _, topic := range topics {
		if topic.Name != "" {
			names = append(names, topic.Name)
		}
	}
	return creator.CreateTopics(ctx, names)
}
