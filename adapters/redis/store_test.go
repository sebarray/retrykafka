package redis

import (
	"context"
	"testing"
	"time"

	"github.com/retrykafka/retrykafka/idempotency"
)

func TestTryStartFailRetry(t *testing.T) {
	s := New(NewMemoryClient(), time.Minute)
	res, err := s.TryStart(context.Background(), "a")
	if err != nil || res != idempotency.ResultProceed {
		t.Fatal(res, err)
	}
	_ = s.Fail(context.Background(), "a")
	res, _ = s.TryStart(context.Background(), "a")
	if res != idempotency.ResultProceed {
		t.Fatal(res)
	}
}
