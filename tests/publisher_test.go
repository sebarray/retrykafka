package retrykafka_test

import (
	"context"
	"sync"
	"testing"

	retrykafka "github.com/retrykafka/retrykafka"
	"github.com/retrykafka/retrykafka/driver"
)

func TestPublisherConcurrentMessageIDs(t *testing.T) {
	mem, p, _ := testPair(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	n := 50
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if err := p.Publish(ctx, "payments", []byte("x")); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	recs := mem.TopicRecords("payments")
	if len(recs) != n {
		t.Fatalf("got %d records", len(recs))
	}
	seen := map[string]struct{}{}
	for _, r := range recs {
		id := header(r, retrykafka.HeaderMessageID)
		if id == "" {
			t.Fatal("missing message id")
		}
		if _, ok := seen[id]; ok {
			t.Fatalf("duplicate id %s", id)
		}
		seen[id] = struct{}{}
		if header(r, retrykafka.HeaderProducedAt) == "" {
			t.Fatal("missing produced-at")
		}
	}
}

func TestPublisherPreservesMessageID(t *testing.T) {
	mem, p, _ := testPair(t)
	err := p.Publish(context.Background(), "payments", "body", retrykafka.WithHeaders(retrykafka.Headers{retrykafka.HeaderMessageID: "abc-123"}))
	if err != nil {
		t.Fatal(err)
	}
	recs := mem.TopicRecords("payments")
	if header(recs[0], retrykafka.HeaderMessageID) != "abc-123" {
		t.Fatalf("id=%s", header(recs[0], retrykafka.HeaderMessageID))
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

func TestPublisherAutoCreateUsesTopicSpec(t *testing.T) {
	tracker := &specTrackingDriver{Driver: driver.NewMemory()}
	p, err := retrykafka.NewPublisher(
		retrykafka.WithDriver(tracker),
		retrykafka.WithGroupID(t.Name()),
		retrykafka.WithAutoCreateTopics(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Publish(context.Background(), "payments", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if len(tracker.specs) != 1 {
		t.Fatalf("specs=%v", tracker.specs)
	}
	if tracker.specs[0].Name != "payments" {
		t.Fatalf("spec=%+v", tracker.specs[0])
	}
}
