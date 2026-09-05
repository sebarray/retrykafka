package retrykafka

import (
	"strconv"
	"time"
)

// Standard Kafka header keys used by retrykafka.
const (
	HeaderMessageID         = "x-message-id"
	HeaderProducedAt        = "x-produced-at"
	HeaderEventType         = "x-event-type"
	HeaderSchemaVersion     = "x-schema-version"
	HeaderRetryAttempt      = "x-retry-attempt"
	HeaderOriginalTopic     = "x-original-topic"
	HeaderOriginalPartition = "x-original-partition"
	HeaderOriginalOffset    = "x-original-offset"
	HeaderFirstFailureAt    = "x-first-failure-at"
	HeaderLastError         = "x-last-error"
	HeaderRetryNotBefore    = "x-retry-not-before"
)

// Headers is a case-sensitive Kafka header map. Duplicate keys keep the last value.
type Headers map[string]string

func (h Headers) Get(key string) string {
	if h == nil {
		return ""
	}
	return h[key]
}

func (h Headers) Set(key, value string) {
	h[key] = value
}

func (h Headers) Clone() Headers {
	if h == nil {
		return Headers{}
	}
	out := make(Headers, len(h))
	for k, v := range h {
		out[k] = v
	}
	return out
}

func (h Headers) MergePreserve(extra Headers) Headers {
	out := h.Clone()
	for k, v := range extra {
		if _, exists := out[k]; !exists && v != "" {
			out[k] = v
		}
	}
	return out
}

func parseIntHeader(h Headers, key string) int {
	v := h.Get(key)
	if v == "" {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0
	}
	return n
}

func parseInt64Header(h Headers, key string) int64 {
	v := h.Get(key)
	if v == "" {
		return 0
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func parseTimeHeader(h Headers, key string) time.Time {
	v := h.Get(key)
	if v == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		t, err = time.Parse(time.RFC3339, v)
		if err != nil {
			return time.Time{}
		}
	}
	return t
}
