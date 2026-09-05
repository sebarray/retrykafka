package retrykafka

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/retrykafka/retrykafka/driver"
)

func TestV030Defaults(t *testing.T) {
	cfg := defaultConfig()
	if cfg.maxRetries != 3 {
		t.Fatalf("maxRetries=%d", cfg.maxRetries)
	}
	wantBackoff := []time.Duration{5 * time.Second, 30 * time.Second, 5 * time.Minute}
	if len(cfg.backoffs) != len(wantBackoff) {
		t.Fatalf("backoffs=%v", cfg.backoffs)
	}
	for i := range wantBackoff {
		if cfg.backoffs[i] != wantBackoff[i] {
			t.Fatalf("backoffs=%v", cfg.backoffs)
		}
	}
	if !cfg.dlqEnabled {
		t.Fatal("DLQ should be enabled by default")
	}
	if cfg.autoCreate {
		t.Fatal("auto-create should be disabled by default")
	}
	if cfg.logger == nil {
		t.Fatal("logger should default to slog.Default")
	}
	if cfg.idempotency != nil {
		t.Fatal("idempotency should be disabled by default")
	}
	if got := cfg.retryTopic("payments", 2); got != "payments.retry.2" {
		t.Fatalf("retry topic=%q", got)
	}
	if got := cfg.dlqTopic("payments"); got != "payments.dlq" {
		t.Fatalf("DLQ topic=%q", got)
	}
}

func TestV030GlobalConfigurationWithMaxAttempts(t *testing.T) {
	mem := driver.NewMemory()
	client, err := New(
		WithDriver(mem),
		WithGroupID(t.Name()),
		WithMaxAttempts(5),
		WithBackoff(time.Second, 2*time.Second),
		WithAutoCreateTopics(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if client.Publisher.cfg.maxRetries != 5 || client.Consumer.cfg.maxRetries != 5 {
		t.Fatalf("max retries not shared: publisher=%d consumer=%d", client.Publisher.cfg.maxRetries, client.Consumer.cfg.maxRetries)
	}
	if !client.Publisher.cfg.autoCreate || !client.Consumer.cfg.autoCreate {
		t.Fatal("auto-create should be enabled globally")
	}
	if len(client.Consumer.cfg.backoffs) != 2 {
		t.Fatalf("backoffs=%v", client.Consumer.cfg.backoffs)
	}
}

type specTrackingDriver struct {
	driver.Driver
	specs []driver.TopicSpec
}

func (d *specTrackingDriver) CreateTopicSpecs(ctx context.Context, specs []driver.TopicSpec) error {
	d.specs = append(d.specs, specs...)
	return nil
}

func TestCreateTopicsPrefersTopicSpecCreator(t *testing.T) {
	tracker := &specTrackingDriver{Driver: driver.NewMemory()}
	specs := []driver.TopicSpec{{
		Name:              "payments.retry.1",
		Partitions:        6,
		ReplicationFactor: 3,
		Configs:           map[string]string{"retention.ms": "60000"},
	}}
	if err := createTopics(context.Background(), tracker, specs); err != nil {
		t.Fatal(err)
	}
	if len(tracker.specs) != 1 {
		t.Fatalf("specs=%v", tracker.specs)
	}
	got := tracker.specs[0]
	if got.Name != specs[0].Name || got.Partitions != specs[0].Partitions || got.ReplicationFactor != specs[0].ReplicationFactor {
		t.Fatalf("spec=%+v", got)
	}
	if got.Configs["retention.ms"] != "60000" {
		t.Fatalf("configs=%v", got.Configs)
	}
}

func TestV030ConcurrentConfigurationUsage(t *testing.T) {
	c, err := NewConsumer(WithDriver(driver.NewMemory()), WithGroupID(t.Name()))
	if err != nil {
		t.Fatal(err)
	}
	const n = 50
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			c.Handle(fmt.Sprintf("topic-%02d", i), func(ctx context.Context, msg Message) error {
				return nil
			}, WithMaxAttempts(1), WithTopicBackoff(time.Millisecond))
		}()
	}
	wg.Wait()
	c.mu.Lock()
	handlers := len(c.handlers)
	topics := c.subscribeTopicsLocked()
	c.mu.Unlock()
	if handlers != n {
		t.Fatalf("handlers=%d", handlers)
	}
	if len(topics) != n*2 {
		t.Fatalf("subscribe topics=%d want %d (%v)", len(topics), n*2, topics)
	}
}
