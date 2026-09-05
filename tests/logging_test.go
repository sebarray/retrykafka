package retrykafka_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	retrykafka "github.com/retrykafka/retrykafka"
)

func TestLogEvents(t *testing.T) {
	h := &memHandler{}
	logger := slog.New(h)
	_, p, c := testPair(t, retrykafka.WithLogger(logger))
	c.Handle("payments", func(ctx context.Context, msg retrykafka.Message) error {
		return errors.New("e")
	})
	startConsumer(t, c)
	_ = p.Publish(context.Background(), "payments", []byte("z"))
	waitUntil(t, 5*time.Second, func() bool {
		return h.has("dlq published")
	})
	for _, ev := range []string{"message received", "processing failed", "retry scheduled", "retry published", "dlq published", "offset committed"} {
		if !h.has(ev) {
			t.Fatalf("missing log %q in %v", ev, h.msgs())
		}
	}
}

type memHandler struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (m *memHandler) Enabled(context.Context, slog.Level) bool { return true }
func (m *memHandler) Handle(_ context.Context, r slog.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recs = append(m.recs, r.Clone())
	return nil
}
func (m *memHandler) WithAttrs([]slog.Attr) slog.Handler { return m }
func (m *memHandler) WithGroup(string) slog.Handler      { return m }
func (m *memHandler) has(msg string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.recs {
		if r.Message == msg {
			return true
		}
	}
	return false
}
func (m *memHandler) msgs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.recs))
	for i, r := range m.recs {
		out[i] = r.Message
	}
	return out
}
