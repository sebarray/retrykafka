package dynamodb

import (
	"context"
	"testing"
	"time"

	"github.com/retrykafka/retrykafka/idempotency"
)

func TestTryStartComplete(t *testing.T) {
	s := New(NewMemoryAPI(), time.Minute)
	res, err := s.TryStart(context.Background(), "a")
	if err != nil || res != idempotency.ResultProceed {
		t.Fatal(res, err)
	}
	_ = s.Complete(context.Background(), "a")
	res, _ = s.TryStart(context.Background(), "a")
	if res != idempotency.ResultSkipCompleted {
		t.Fatal(res)
	}
}
