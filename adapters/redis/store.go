package redis

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/retrykafka/retrykafka/idempotency"
)

// Client is the atomic Redis surface used by the adapter.
type Client interface {
	SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key, value string, ttl time.Duration) error
	Del(ctx context.Context, key string) error
	Expire(ctx context.Context, key string, ttl time.Duration) error
	CompareAndSwap(ctx context.Context, key, old, neu string, ttl time.Duration) (bool, error)
}

var ErrNil = errors.New("redis: nil")

// Store uses SET NX EX for the initial claim. Redis is not a durable
// transactional store: eviction, flush and replication lag can lose records.
type Store struct {
	Client Client
	TTL    time.Duration
}

func New(c Client, ttl time.Duration) *Store {
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}
	return &Store{Client: c, TTL: ttl}
}

func (s *Store) TryStart(ctx context.Context, key string) (idempotency.Result, error) {
	ok, err := s.Client.SetNX(ctx, key, idempotency.StateProcessing, s.TTL)
	if err != nil {
		return 0, err
	}
	if ok {
		return idempotency.ResultProceed, nil
	}
	cur, err := s.Client.Get(ctx, key)
	if err != nil {
		if errors.Is(err, ErrNil) {
			ok, err = s.Client.SetNX(ctx, key, idempotency.StateProcessing, s.TTL)
			if err != nil {
				return 0, err
			}
			if ok {
				return idempotency.ResultProceed, nil
			}
			return idempotency.ResultSkipInProgress, nil
		}
		return 0, err
	}
	switch cur {
	case idempotency.StateCompleted:
		return idempotency.ResultSkipCompleted, nil
	case idempotency.StateFailed:
		swapped, err := s.Client.CompareAndSwap(ctx, key, idempotency.StateFailed, idempotency.StateProcessing, s.TTL)
		if err != nil {
			return 0, err
		}
		if swapped {
			return idempotency.ResultProceed, nil
		}
		return idempotency.ResultSkipInProgress, nil
	default:
		return idempotency.ResultSkipInProgress, nil
	}
}

func (s *Store) Complete(ctx context.Context, key string) error {
	return s.Client.Set(ctx, key, idempotency.StateCompleted, s.TTL*4)
}

func (s *Store) Fail(ctx context.Context, key string) error {
	return s.Client.Set(ctx, key, idempotency.StateFailed, s.TTL)
}

// MemoryClient is an in-process Redis-like map for unit tests.
type MemoryClient struct {
	mu    sync.Mutex
	items map[string]string
}

func NewMemoryClient() *MemoryClient {
	return &MemoryClient{items: map[string]string{}}
}

func (m *MemoryClient) SetNX(_ context.Context, key, value string, _ time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.items[key]; ok {
		return false, nil
	}
	m.items[key] = value
	return true, nil
}

func (m *MemoryClient) Get(_ context.Context, key string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.items[key]
	if !ok {
		return "", ErrNil
	}
	return v, nil
}

func (m *MemoryClient) Set(_ context.Context, key, value string, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items[key] = value
	return nil
}

func (m *MemoryClient) Del(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.items, key)
	return nil
}

func (m *MemoryClient) Expire(context.Context, string, time.Duration) error { return nil }

func (m *MemoryClient) CompareAndSwap(_ context.Context, key, old, neu string, _ time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.items[key] != old {
		return false, nil
	}
	m.items[key] = neu
	return true, nil
}
