package mongo

import (
	"context"
	"testing"
	"time"

	"github.com/retrykafka/retrykafka/idempotency"
)

func TestDuplicateInsert(t *testing.T) {
	s := New(NewMemoryCollection(), time.Minute)
	res, err := s.TryStart(context.Background(), "a")
	if err != nil || res != idempotency.ResultProceed {
		t.Fatal(res, err)
	}
	res, err = s.TryStart(context.Background(), "a")
	if err != nil {
		t.Fatal(err)
	}
	if res != idempotency.ResultSkipInProgress {
		t.Fatal(res)
	}
}
