package retrykafka_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	retrykafka "github.com/retrykafka/retrykafka"
	"github.com/retrykafka/retrykafka/driver"
)

func TestV030PerTopicMaxAttemptsOverride(t *testing.T) {
	mem, p, c := testPair(t)
	c.Handle("payments", func(ctx context.Context, msg retrykafka.Message) error {
		return errors.New("boom")
	}, retrykafka.WithMaxAttempts(1))
	startConsumer(t, c)
	if err := p.Publish(context.Background(), "payments", []byte("x")); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 5*time.Second, func() bool {
		return len(mem.TopicRecords("payments.dlq")) == 1
	})
	if n := len(mem.TopicRecords("payments.retry.1")); n != 1 {
		t.Fatalf("retry.1 records=%d", n)
	}
	if n := len(mem.TopicRecords("payments.retry.2")); n != 0 {
		t.Fatalf("retry.2 should not be used with topic max attempts=1, got %d", n)
	}
}

type customNamer struct{}

func (customNamer) RetryTopic(topic string, attempt int) string {
	return fmt.Sprintf("retry_%s_%d", topic, attempt)
}

func (customNamer) DLQTopic(topic string) string {
	return "dead_" + topic
}

func TestV030CustomTopicNamer(t *testing.T) {
	mem, p, c := testPair(t, retrykafka.WithTopicNamer(customNamer{}), retrykafka.WithMaxAttempts(2))
	c.Handle("payments", func(ctx context.Context, msg retrykafka.Message) error {
		return errors.New("boom")
	})
	startConsumer(t, c)
	if err := p.Publish(context.Background(), "payments", []byte("x")); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 5*time.Second, func() bool {
		return len(mem.TopicRecords("dead_payments")) == 1
	})
	if len(mem.TopicRecords("retry_payments_1")) != 1 {
		t.Fatal("missing first custom retry topic")
	}
	if len(mem.TopicRecords("retry_payments_2")) != 1 {
		t.Fatal("missing second custom retry topic")
	}
	if len(mem.TopicRecords("payments.retry.1")) != 0 {
		t.Fatal("default retry topic should not be used with a custom namer")
	}
}

type createTrackingDriver struct {
	driver.Driver
	mu      sync.Mutex
	created []string
}

func (d *createTrackingDriver) CreateTopics(ctx context.Context, topics []string) error {
	d.mu.Lock()
	d.created = append(d.created, topics...)
	d.mu.Unlock()
	if creator, ok := d.Driver.(driver.TopicCreator); ok {
		return creator.CreateTopics(ctx, topics)
	}
	return nil
}

func (d *createTrackingDriver) createdTopics() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.created...)
}

func hasTopic(topics []string, want string) bool {
	for _, topic := range topics {
		if topic == want {
			return true
		}
	}
	return false
}

func TestV030AutoCreateEnabled(t *testing.T) {
	mem := driver.NewMemory()
	tracker := &createTrackingDriver{Driver: mem}
	c, err := retrykafka.NewConsumer(
		retrykafka.WithDriver(tracker),
		retrykafka.WithGroupID(t.Name()),
		retrykafka.WithMaxAttempts(2),
		retrykafka.WithBackoff(time.Millisecond),
		retrykafka.WithAutoCreateTopics(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	c.Handle("payments", func(ctx context.Context, msg retrykafka.Message) error { return nil })
	startConsumer(t, c)
	waitUntil(t, 2*time.Second, func() bool {
		return len(tracker.createdTopics()) > 0
	})
	created := tracker.createdTopics()
	for _, topic := range []string{"payments", "payments.retry.1", "payments.retry.2", "payments.dlq"} {
		if !hasTopic(created, topic) {
			t.Fatalf("topic %q not created; got %v", topic, created)
		}
	}
}

func TestV030AutoCreateDisabled(t *testing.T) {
	mem := driver.NewMemory()
	tracker := &createTrackingDriver{Driver: mem}
	c, err := retrykafka.NewConsumer(
		retrykafka.WithDriver(tracker),
		retrykafka.WithGroupID(t.Name()),
		retrykafka.WithBackoff(time.Millisecond),
		retrykafka.WithAutoCreateTopics(false),
	)
	if err != nil {
		t.Fatal(err)
	}
	c.Handle("payments", func(ctx context.Context, msg retrykafka.Message) error { return nil })
	startConsumer(t, c)
	time.Sleep(50 * time.Millisecond)
	if created := tracker.createdTopics(); len(created) != 0 {
		t.Fatalf("auto-create disabled but created %v", created)
	}
}

func TestV030InvalidConfigurations(t *testing.T) {
	cases := []struct {
		name string
		opts []retrykafka.Option
	}{
		{name: "negative max attempts", opts: []retrykafka.Option{retrykafka.WithDriver(driver.NewMemory()), retrykafka.WithMaxAttempts(-1)}},
		{name: "negative max retries", opts: []retrykafka.Option{retrykafka.WithDriver(driver.NewMemory()), retrykafka.WithMaxRetries(-1)}},
		{name: "negative backoff", opts: []retrykafka.Option{retrykafka.WithDriver(driver.NewMemory()), retrykafka.WithBackoff(-time.Second)}},
		{name: "empty retry format", opts: []retrykafka.Option{retrykafka.WithDriver(driver.NewMemory()), retrykafka.WithRetryTopicFormat("")}},
		{name: "empty group", opts: []retrykafka.Option{retrykafka.WithDriver(driver.NewMemory()), retrykafka.WithGroupID("")}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			_, err := retrykafka.NewConsumer(tt.opts...)
			if !errors.Is(err, retrykafka.ErrInvalidConfig) {
				t.Fatalf("err=%v", err)
			}
		})
	}

	c, err := retrykafka.NewConsumer(retrykafka.WithDriver(driver.NewMemory()), retrykafka.WithGroupID(t.Name()))
	if err != nil {
		t.Fatal(err)
	}
	c.Handle("payments", func(ctx context.Context, msg retrykafka.Message) error { return nil }, retrykafka.WithMaxAttempts(-1))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = c.Run(ctx)
	if !errors.Is(err, retrykafka.ErrInvalidConfig) {
		t.Fatalf("topic override err=%v", err)
	}
}
