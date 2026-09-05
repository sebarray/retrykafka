package dynamodb

import (
	"context"
	"sync"
	"time"

	"github.com/retrykafka/retrykafka/idempotency"
)

const (
	attrKey        = "pk"
	attrState      = "state"
	attrLease      = "lease_until"
	attrExpiration = "expiration"
)

// Item is the persisted idempotency record.
type Item struct {
	Key        string
	State      string
	LeaseUntil int64
	Expiration int64
}

// API is the subset of DynamoDB used by the adapter.
type API interface {
	PutIfAbsent(ctx context.Context, item Item) (bool, error)
	Get(ctx context.Context, key string) (Item, bool, error)
	ReplaceIf(ctx context.Context, item Item, condState string, leaseExpiredBefore int64) (bool, error)
	Put(ctx context.Context, item Item) error
}

// Store uses conditional writes. TTL must be enabled on the expiration attribute.
type Store struct {
	API API
	TTL time.Duration
	Now func() time.Time
}

func New(api API, ttl time.Duration) *Store {
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}
	return &Store{API: api, TTL: ttl, Now: time.Now}
}

func (s *Store) TryStart(ctx context.Context, key string) (idempotency.Result, error) {
	now := s.Now()
	item := Item{
		Key:        key,
		State:      idempotency.StateProcessing,
		LeaseUntil: now.Add(s.TTL).Unix(),
		Expiration: now.Add(s.TTL * 4).Unix(),
	}
	ok, err := s.API.PutIfAbsent(ctx, item)
	if err != nil {
		return 0, err
	}
	if ok {
		return idempotency.ResultProceed, nil
	}
	cur, found, err := s.API.Get(ctx, key)
	if err != nil {
		return 0, err
	}
	if !found {
		ok, err = s.API.PutIfAbsent(ctx, item)
		if err != nil {
			return 0, err
		}
		if ok {
			return idempotency.ResultProceed, nil
		}
		return idempotency.ResultSkipInProgress, nil
	}
	if cur.State == idempotency.StateCompleted {
		return idempotency.ResultSkipCompleted, nil
	}
	if cur.State == idempotency.StateFailed || (cur.State == idempotency.StateProcessing && cur.LeaseUntil < now.Unix()) {
		stolen, err := s.API.ReplaceIf(ctx, item, cur.State, now.Unix())
		if err != nil {
			return 0, err
		}
		if stolen {
			return idempotency.ResultProceed, nil
		}
		return idempotency.ResultSkipInProgress, nil
	}
	return idempotency.ResultSkipInProgress, nil
}

func (s *Store) Complete(ctx context.Context, key string) error {
	now := s.Now()
	return s.API.Put(ctx, Item{
		Key:        key,
		State:      idempotency.StateCompleted,
		Expiration: now.Add(s.TTL * 4).Unix(),
	})
}

func (s *Store) Fail(ctx context.Context, key string) error {
	now := s.Now()
	return s.API.Put(ctx, Item{
		Key:        key,
		State:      idempotency.StateFailed,
		Expiration: now.Add(s.TTL * 4).Unix(),
	})
}

// MemoryAPI is a process-local stand-in for unit tests (not DynamoDB).
type MemoryAPI struct {
	mu    sync.Mutex
	items map[string]Item
}

func NewMemoryAPI() *MemoryAPI {
	return &MemoryAPI{items: make(map[string]Item)}
}

func (m *MemoryAPI) PutIfAbsent(_ context.Context, item Item) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.items[item.Key]; ok {
		return false, nil
	}
	m.items[item.Key] = item
	return true, nil
}

func (m *MemoryAPI) Get(_ context.Context, key string) (Item, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	it, ok := m.items[key]
	return it, ok, nil
}

func (m *MemoryAPI) ReplaceIf(_ context.Context, item Item, condState string, leaseExpiredBefore int64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.items[item.Key]
	if !ok {
		return false, nil
	}
	if cur.State != condState {
		return false, nil
	}
	if condState == idempotency.StateProcessing && cur.LeaseUntil >= leaseExpiredBefore {
		return false, nil
	}
	m.items[item.Key] = item
	return true, nil
}

func (m *MemoryAPI) Put(_ context.Context, item Item) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items[item.Key] = item
	return nil
}
