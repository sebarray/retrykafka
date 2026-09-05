package retrykafka_test

import (
	"context"
	"sync"
	"testing"
	"time"

	retrykafka "github.com/retrykafka/retrykafka"
	"github.com/retrykafka/retrykafka/driver"
	"github.com/retrykafka/retrykafka/idempotency"
)

func TestIdempotencySkipCompleted(t *testing.T) {
	store := idempotency.NewMemory(time.Minute)
	mem, p, c := testPair(t, retrykafka.WithIdempotencyStore(store))
	var n int
	var mu sync.Mutex
	c.Handle("payments", func(ctx context.Context, msg retrykafka.Message) error {
		mu.Lock()
		n++
		mu.Unlock()
		return nil
	})
	startConsumer(t, c)
	_ = p.Publish(context.Background(), "payments", []byte("1"), retrykafka.WithHeaders(retrykafka.Headers{retrykafka.HeaderMessageID: "dup"}))
	waitUntil(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return n == 1
	})
	_ = mem.Publish(context.Background(), driver.Record{
		Topic:   "payments",
		Value:   []byte("1"),
		Headers: []driver.Header{{Key: retrykafka.HeaderMessageID, Value: []byte("dup")}},
	})
	time.Sleep(150 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if n != 1 {
		t.Fatalf("processed %d times", n)
	}
}
