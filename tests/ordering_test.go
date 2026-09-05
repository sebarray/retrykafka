package retrykafka_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	retrykafka "github.com/retrykafka/retrykafka"
)

func TestOrderingPerPartitionBlocks(t *testing.T) {
	_, p, c := testPair(t, retrykafka.WithOrdering(retrykafka.OrderingPerPartition), retrykafka.WithMaxRetries(1), retrykafka.WithBackoff(30*time.Millisecond))
	var mu sync.Mutex
	var seq []string
	fails := 0
	c.Handle("payments", func(ctx context.Context, msg retrykafka.Message) error {
		body := string(msg.Payload)
		if body == "a" {
			mu.Lock()
			fails++
			n := fails
			mu.Unlock()
			if n == 1 {
				return errors.New("once")
			}
		}
		mu.Lock()
		seq = append(seq, body)
		mu.Unlock()
		return nil
	})
	startConsumer(t, c)
	ctx := context.Background()
	_ = p.Publish(ctx, "payments", []byte("a"), retrykafka.WithKey([]byte("k")))
	_ = p.Publish(ctx, "payments", []byte("b"), retrykafka.WithKey([]byte("k")))
	waitUntil(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seq) >= 2
	})
	mu.Lock()
	defer mu.Unlock()
	if seq[0] != "a" || seq[1] != "b" {
		t.Fatalf("seq=%v (b must wait for a)", seq)
	}
}
