package retrykafka_test

import (
	"context"
	"testing"
	"time"

	retrykafka "github.com/retrykafka/retrykafka"
	"github.com/retrykafka/retrykafka/driver"
)

func waitUntil(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting")
}

func startConsumer(t *testing.T, c *retrykafka.Consumer) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = c.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("consumer did not stop")
		}
	})
}

func testPair(t *testing.T, opts ...retrykafka.Option) (*driver.Memory, *retrykafka.Publisher, *retrykafka.Consumer) {
	t.Helper()
	mem := driver.NewMemory()
	base := []retrykafka.Option{
		retrykafka.WithDriver(mem),
		retrykafka.WithGroupID(t.Name()),
		retrykafka.WithBackoff(time.Millisecond, 2*time.Millisecond, 3*time.Millisecond),
		retrykafka.WithMaxRetries(3),
		retrykafka.WithShutdownTimeout(2 * time.Second),
	}
	base = append(base, opts...)
	p, err := retrykafka.NewPublisher(base...)
	if err != nil {
		t.Fatal(err)
	}
	c, err := retrykafka.NewConsumer(base...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return mem, p, c
}

func header(r driver.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
