package idempotency

import (
	"context"
	"sync"
	"time"
)

// Result is the outcome of an atomic claim.
type Result int

const (
	// ResultProceed means this caller owns the processing lease.
	ResultProceed Result = iota
	// ResultSkipCompleted means the message already completed successfully.
	ResultSkipCompleted
	// ResultSkipInProgress means another worker holds a valid lease.
	ResultSkipInProgress
)

func (r Result) String() string {
	switch r {
	case ResultSkipCompleted:
		return "skip_completed"
	case ResultSkipInProgress:
		return "skip_in_progress"
	default:
		return "proceed"
	}
}

const (
	StateProcessing = "PROCESSING"
	StateCompleted  = "COMPLETED"
	StateFailed     = "FAILED"
)

// Store is the optional atomic idempotency interface.
// TryStart must be atomic: never check-then-insert in two steps.
type Store interface {
	TryStart(ctx context.Context, key string) (Result, error)
	Complete(ctx context.Context, key string) error
	Fail(ctx context.Context, key string) error
}

type memRec struct {
	state      string
	leaseUntil time.Time
}

// Memory is a process-local store. It is not durable across restarts.
type Memory struct {
	mu    sync.Mutex
	items map[string]memRec
	ttl   time.Duration
	now   func() time.Time
}

func NewMemory(ttl time.Duration) *Memory {
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}
	return &Memory{
		items: make(map[string]memRec),
		ttl:   ttl,
		now:   time.Now,
	}
}

func (m *Memory) TryStart(_ context.Context, key string) (Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	cur, ok := m.items[key]
	if !ok || cur.state == StateFailed || (cur.state == StateProcessing && now.After(cur.leaseUntil)) {
		m.items[key] = memRec{state: StateProcessing, leaseUntil: now.Add(m.ttl)}
		return ResultProceed, nil
	}
	if cur.state == StateCompleted {
		return ResultSkipCompleted, nil
	}
	return ResultSkipInProgress, nil
}

func (m *Memory) Complete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items[key] = memRec{state: StateCompleted, leaseUntil: m.now().Add(m.ttl)}
	return nil
}

func (m *Memory) Fail(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items[key] = memRec{state: StateFailed, leaseUntil: time.Time{}}
	return nil
}
