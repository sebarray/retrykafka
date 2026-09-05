package retrykafka_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	retrykafka "github.com/retrykafka/retrykafka"
)

func TestConsumerSuccessCommits(t *testing.T) {
	mem, p, c := testPair(t)
	got := make(chan retrykafka.Message, 1)
	c.Handle("payments", func(ctx context.Context, msg retrykafka.Message) error {
		got <- msg
		return nil
	})
	startConsumer(t, c)
	if err := p.Publish(context.Background(), "payments", []byte("ok")); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-got:
		if msg.ID == "" || !bytes.Equal(msg.Payload, []byte("ok")) {
			t.Fatalf("%+v", msg)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout")
	}
	waitUntil(t, 2*time.Second, func() bool {
		return mem.GroupOffset(t.Name(), "payments", 0) > 0
	})
}

func TestConsumerRetryThenDLQPreservesID(t *testing.T) {
	mem, p, c := testPair(t)
	var mu sync.Mutex
	var attempts []int
	c.Handle("payments", func(ctx context.Context, msg retrykafka.Message) error {
		mu.Lock()
		attempts = append(attempts, msg.RetryAttempt())
		mu.Unlock()
		return errors.New("boom")
	})
	startConsumer(t, c)
	if err := p.Publish(context.Background(), "payments", []byte("p"), retrykafka.WithHeaders(retrykafka.Headers{retrykafka.HeaderMessageID: "stay"})); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 5*time.Second, func() bool {
		return len(mem.TopicRecords("payments.dlq")) == 1
	})
	dlq := mem.TopicRecords("payments.dlq")[0]
	if header(dlq, retrykafka.HeaderMessageID) != "stay" {
		t.Fatalf("id=%s", header(dlq, retrykafka.HeaderMessageID))
	}
	if header(dlq, retrykafka.HeaderOriginalTopic) != "payments" {
		t.Fatalf("orig=%s", header(dlq, retrykafka.HeaderOriginalTopic))
	}
	if header(dlq, retrykafka.HeaderLastError) == "" {
		t.Fatal("missing last error")
	}
	if len(mem.TopicRecords("payments.retry.1")) == 0 || len(mem.TopicRecords("payments.retry.3")) == 0 {
		t.Fatal("expected retry topics")
	}
	mu.Lock()
	n := len(attempts)
	mu.Unlock()
	if n < 4 {
		t.Fatalf("attempts=%d", n)
	}
}

func TestSequentialPerPartition(t *testing.T) {
	_, p, c := testPair(t)
	var mu sync.Mutex
	var live int
	maxLive := 0
	order := make([]string, 0, 8)
	c.Handle("payments", func(ctx context.Context, msg retrykafka.Message) error {
		mu.Lock()
		live++
		if live > maxLive {
			maxLive = live
		}
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		mu.Lock()
		order = append(order, string(msg.Payload))
		live--
		mu.Unlock()
		return nil
	})
	startConsumer(t, c)
	ctx := context.Background()
	for _, body := range []string{"a", "b", "c"} {
		if err := p.Publish(ctx, "payments", []byte(body), retrykafka.WithKey([]byte("same"))); err != nil {
			t.Fatal(err)
		}
	}
	waitUntil(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) == 3
	})
	mu.Lock()
	defer mu.Unlock()
	if maxLive != 1 {
		t.Fatalf("max concurrent on partition = %d", maxLive)
	}
	if order[0] != "a" || order[1] != "b" || order[2] != "c" {
		t.Fatalf("order=%v", order)
	}
}

func TestShutdownStopsConsumer(t *testing.T) {
	_, _, c := testPair(t)
	c.Handle("payments", func(ctx context.Context, msg retrykafka.Message) error { return nil })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("run did not return")
	}
}
