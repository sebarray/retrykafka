package retrykafka_test

import (
	"context"
	"errors"
	"testing"
	"time"

	retrykafka "github.com/retrykafka/retrykafka"
	"github.com/retrykafka/retrykafka/driver"
)

func TestTransactionsUnsupported(t *testing.T) {
	mem := driver.NewMemory()
	wrapped := driver.CapsOverride{Driver: mem, Caps: driver.Capabilities{Transactions: false}}
	_, err := retrykafka.NewConsumer(retrykafka.WithDriver(wrapped), retrykafka.WithTransactions())
	if !errors.Is(err, retrykafka.ErrTransactionsUnsupported) {
		t.Fatalf("err=%v", err)
	}
}

func TestTransactionalRetryAtomic(t *testing.T) {
	mem, p, c := testPair(t, retrykafka.WithTransactions())
	c.Handle("payments", func(ctx context.Context, msg retrykafka.Message) error {
		return errors.New("fail")
	})
	startConsumer(t, c)
	if err := p.Publish(context.Background(), "payments", []byte("x"), retrykafka.WithHeaders(retrykafka.Headers{retrykafka.HeaderMessageID: "t1"})); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 5*time.Second, func() bool {
		return len(mem.TopicRecords("payments.retry.1")) >= 1 && mem.GroupOffset(t.Name(), "payments", 0) >= 1
	})
	if mem.UncommittedCount("payments.retry.1") != 0 {
		t.Fatal("retry should be committed in txn")
	}
}

func TestTransactionalAbortOnPublishFence(t *testing.T) {
	mem, p, c := testPair(t, retrykafka.WithTransactions())
	c.Handle("payments", func(ctx context.Context, msg retrykafka.Message) error {
		mem.SetFenced(true)
		return errors.New("fail")
	})
	startConsumer(t, c)
	_ = p.Publish(context.Background(), "payments", []byte("x"))
	time.Sleep(200 * time.Millisecond)
	if len(mem.TopicRecords("payments.retry.1")) != 0 {
		t.Fatal("fenced publish should abort txn")
	}
}

func TestRebalanceSkipsCommit(t *testing.T) {
	mem, p, c := testPair(t)
	started := make(chan struct{})
	c.Handle("payments", func(ctx context.Context, msg retrykafka.Message) error {
		close(started)
		time.Sleep(50 * time.Millisecond)
		mem.Revoke(msg.Topic, msg.Partition)
		return errors.New("fail")
	})
	startConsumer(t, c)
	_ = p.Publish(context.Background(), "payments", []byte("x"))
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler not called")
	}
	time.Sleep(100 * time.Millisecond)
	if len(mem.TopicRecords("payments.retry.1")) != 0 {
		t.Fatal("should not publish retry after revoke")
	}
}
