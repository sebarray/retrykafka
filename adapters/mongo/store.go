package mongo

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/retrykafka/retrykafka/idempotency"
)

type Document struct {
	Key        string
	State      string
	LeaseUntil time.Time
	ExpireAt   time.Time
}

// Collection is the atomic Mongo surface used by the adapter.
type Collection interface {
	InsertOne(ctx context.Context, doc Document) error
	FindOne(ctx context.Context, key string) (Document, bool, error)
	FindOneAndUpdate(ctx context.Context, key string, update Document, onlyIfStates []string, leaseExpired bool, now time.Time) (bool, error)
	UpdateByKey(ctx context.Context, key string, update Document) error
}

var ErrDuplicate = errors.New("mongo: duplicate key")

// Store uses a unique index on key plus atomic findAndModify.
type Store struct {
	Col Collection
	TTL time.Duration
	Now func() time.Time
}

func New(col Collection, ttl time.Duration) *Store {
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}
	return &Store{Col: col, TTL: ttl, Now: time.Now}
}

func (s *Store) TryStart(ctx context.Context, key string) (idempotency.Result, error) {
	now := s.Now()
	doc := Document{
		Key:        key,
		State:      idempotency.StateProcessing,
		LeaseUntil: now.Add(s.TTL),
		ExpireAt:   now.Add(s.TTL * 4),
	}
	err := s.Col.InsertOne(ctx, doc)
	if err == nil {
		return idempotency.ResultProceed, nil
	}
	if !isDup(err) {
		return 0, err
	}
	cur, found, err := s.Col.FindOne(ctx, key)
	if err != nil {
		return 0, err
	}
	if !found {
		return idempotency.ResultSkipInProgress, nil
	}
	if cur.State == idempotency.StateCompleted {
		return idempotency.ResultSkipCompleted, nil
	}
	if cur.State == idempotency.StateFailed || (cur.State == idempotency.StateProcessing && now.After(cur.LeaseUntil)) {
		ok, err := s.Col.FindOneAndUpdate(ctx, key, doc, []string{cur.State}, cur.State == idempotency.StateProcessing, now)
		if err != nil {
			return 0, err
		}
		if ok {
			return idempotency.ResultProceed, nil
		}
	}
	return idempotency.ResultSkipInProgress, nil
}

func (s *Store) Complete(ctx context.Context, key string) error {
	now := s.Now()
	return s.Col.UpdateByKey(ctx, key, Document{
		Key:      key,
		State:    idempotency.StateCompleted,
		ExpireAt: now.Add(s.TTL * 4),
	})
}

func (s *Store) Fail(ctx context.Context, key string) error {
	now := s.Now()
	return s.Col.UpdateByKey(ctx, key, Document{
		Key:      key,
		State:    idempotency.StateFailed,
		ExpireAt: now.Add(s.TTL * 4),
	})
}

func isDup(err error) bool {
	return errors.Is(err, ErrDuplicate)
}

// MemoryCollection is a unique-index analogue for unit tests.
type MemoryCollection struct {
	mu    sync.Mutex
	items map[string]Document
}

func NewMemoryCollection() *MemoryCollection {
	return &MemoryCollection{items: map[string]Document{}}
}

func (m *MemoryCollection) InsertOne(_ context.Context, doc Document) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.items[doc.Key]; ok {
		return ErrDuplicate
	}
	m.items[doc.Key] = doc
	return nil
}

func (m *MemoryCollection) FindOne(_ context.Context, key string) (Document, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.items[key]
	return d, ok, nil
}

func (m *MemoryCollection) FindOneAndUpdate(_ context.Context, key string, update Document, onlyIfStates []string, leaseExpired bool, now time.Time) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.items[key]
	if !ok {
		return false, nil
	}
	match := false
	for _, st := range onlyIfStates {
		if cur.State == st {
			match = true
			break
		}
	}
	if !match {
		return false, nil
	}
	if leaseExpired && !now.After(cur.LeaseUntil) && cur.State == idempotency.StateProcessing {
		return false, nil
	}
	m.items[key] = update
	return true, nil
}

func (m *MemoryCollection) UpdateByKey(_ context.Context, key string, update Document) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items[key] = update
	return nil
}
