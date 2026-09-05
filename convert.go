package retrykafka

import (
	"strconv"
	"time"

	"github.com/retrykafka/retrykafka/driver"
)

func messageFromRecord(rec driver.Record) Message {
	h := Headers{}
	for _, hdr := range rec.Headers {
		h[hdr.Key] = string(hdr.Value)
	}
	id := h.Get(HeaderMessageID)
	return Message{
		ID:        id,
		Key:       rec.Key,
		Payload:   rec.Value,
		Headers:   h,
		Topic:     rec.Topic,
		Partition: rec.Partition,
		Offset:    rec.Offset,
		Timestamp: rec.Timestamp,
	}
}

func recordFromMessage(msg Message) driver.Record {
	headers := make([]driver.Header, 0, len(msg.Headers))
	for k, v := range msg.Headers {
		headers = append(headers, driver.Header{Key: k, Value: []byte(v)})
	}
	return driver.Record{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Key:       msg.Key,
		Value:     msg.Payload,
		Headers:   headers,
		Timestamp: msg.Timestamp,
	}
}

func attachProduceMetadata(h Headers, id string, now time.Time) Headers {
	if h == nil {
		h = Headers{}
	} else {
		h = h.Clone()
	}
	if h.Get(HeaderMessageID) == "" {
		h.Set(HeaderMessageID, id)
	}
	if h.Get(HeaderProducedAt) == "" {
		h.Set(HeaderProducedAt, now.UTC().Format(time.RFC3339Nano))
	}
	return h
}

func attachRetryMetadata(src Message, now time.Time, nextAttempt int, err error, delay time.Duration) Headers {
	h := src.Headers.Clone()
	if h.Get(HeaderMessageID) == "" {
		h.Set(HeaderMessageID, src.ID)
	}
	origTopic := h.Get(HeaderOriginalTopic)
	if origTopic == "" {
		origTopic = src.Topic
		h.Set(HeaderOriginalTopic, origTopic)
	}
	if h.Get(HeaderOriginalPartition) == "" {
		h.Set(HeaderOriginalPartition, strconv.Itoa(src.Partition))
	}
	if h.Get(HeaderOriginalOffset) == "" {
		h.Set(HeaderOriginalOffset, strconv.FormatInt(src.Offset, 10))
	}
	if h.Get(HeaderFirstFailureAt) == "" {
		h.Set(HeaderFirstFailureAt, now.UTC().Format(time.RFC3339Nano))
	}
	h.Set(HeaderRetryAttempt, strconv.Itoa(nextAttempt))
	if err != nil {
		h.Set(HeaderLastError, err.Error())
	}
	if delay > 0 {
		h.Set(HeaderRetryNotBefore, now.Add(delay).UTC().Format(time.RFC3339Nano))
	}
	return h
}
