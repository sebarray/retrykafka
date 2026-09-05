package retrykafka

import "time"

// Message is the stable envelope used across publish, consume, retry and DLQ.
type Message struct {
	ID        string
	Key       []byte
	Payload   []byte
	Headers   Headers
	Topic     string
	Partition int
	Offset    int64
	Timestamp time.Time
}

// RetryAttempt returns the attempt stored in metadata. Zero means the original message.
func (m Message) RetryAttempt() int {
	return parseIntHeader(m.Headers, HeaderRetryAttempt)
}

// OriginalTopic returns the first topic that received the message, if known.
func (m Message) OriginalTopic() string {
	if t := m.Headers.Get(HeaderOriginalTopic); t != "" {
		return t
	}
	return m.Topic
}

// NotBefore returns the earliest time a delayed retry should be processed.
func (m Message) NotBefore() time.Time {
	return parseTimeHeader(m.Headers, HeaderRetryNotBefore)
}

func (m Message) clone() Message {
	out := m
	out.Headers = m.Headers.Clone()
	if m.Payload != nil {
		out.Payload = append([]byte(nil), m.Payload...)
	}
	if m.Key != nil {
		out.Key = append([]byte(nil), m.Key...)
	}
	return out
}
