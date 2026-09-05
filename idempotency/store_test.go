package idempotency_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/retrykafka/retrykafka/adapters/dynamodb"
	"github.com/retrykafka/retrykafka/adapters/mongo"
	"github.com/retrykafka/retrykafka/adapters/redis"
	"github.com/retrykafka/retrykafka/idempotency"
)

func TestMemoryConcurrentClaim(t *testing.T) {
	s := idempotency.NewMemory(time.Minute)
	testConcurrent(t, s)
}

func TestDynamoConcurrentClaim(t *testing.T) {
	s := dynamodb.New(dynamodb.NewMemoryAPI(), time.Minute)
	testConcurrent(t, s)
}

func TestRedisConcurrentClaim(t *testing.T) {
	s := redis.New(redis.NewMemoryClient(), time.Minute)
	testConcurrent(t, s)
}

func TestMongoConcurrentClaim(t *testing.T) {
	s := mongo.New(mongo.NewMemoryCollection(), time.Minute)
	testConcurrent(t, s)
}

func testConcurrent(t *testing.T, s idempotency.Store) {
	t.Helper()
	var proceed atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := s.TryStart(context.Background(), "k")
			if err != nil {
				t.Error(err)
				return
			}
			if res == idempotency.ResultProceed {
				proceed.Add(1)
			}
		}()
	}
	wg.Wait()
	if proceed.Load() != 1 {
		t.Fatalf("proceed=%d", proceed.Load())
	}
	if err := s.Complete(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	res, err := s.TryStart(context.Background(), "k")
	if err != nil {
		t.Fatal(err)
	}
	if res != idempotency.ResultSkipCompleted {
		t.Fatalf("res=%v", res)
	}
}

func TestMemoryLeaseExpires(t *testing.T) {
	s := idempotency.NewMemory(10 * time.Millisecond)
	res, _ := s.TryStart(context.Background(), "k")
	if res != idempotency.ResultProceed {
		t.Fatal(res)
	}
	time.Sleep(20 * time.Millisecond)
	res, _ = s.TryStart(context.Background(), "k")
	if res != idempotency.ResultProceed {
		t.Fatalf("expected steal after ttl, got %v", res)
	}
}
