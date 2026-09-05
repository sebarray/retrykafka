package driver

import (
	"context"
	"errors"
	"hash/fnv"
	"sync"
	"time"
)

var (
	ErrClosed      = errors.New("driver: closed")
	ErrTxnActive   = errors.New("driver: transaction already active")
	ErrNoTxn       = errors.New("driver: no active transaction")
	ErrFenced      = errors.New("driver: producer fenced")
	ErrRevoked     = errors.New("driver: partition revoked")
	ErrNotAssigned = errors.New("driver: partition not assigned")
)

// Memory is an in-process Kafka-like broker used by tests and local development.
// It supports transactions with read-committed isolation.
type Memory struct {
	mu          sync.Mutex
	topics      map[string]*memTopic
	groups      map[string]*memGroup
	closed      bool
	partitions  int
	txnSeq      int64
	fenced      bool
	onRebalance func()
}

type memTopic struct {
	parts [][]memRec
}

type memRec struct {
	rec       Record
	txnID     int64
	committed bool
}

type memGroup struct {
	offsets map[tp]int64 // next offset to consume
	subs    []*memSub
}

type tp struct {
	topic     string
	partition int
}

type memSub struct {
	broker    *Memory
	groupID   string
	topics    []string
	isolation Isolation
	mu        sync.Mutex
	revoked   map[tp]struct{}
	fetch     map[tp]int64
	closed    bool
	notify    chan struct{}
}

type memTxn struct {
	broker    *Memory
	id        int64
	mu        sync.Mutex
	publishes []Record
	offsets   []Offset
	groupID   string
	done      bool
}

// NewMemory returns a shared in-memory broker with 3 partitions per topic.
func NewMemory() *Memory {
	return &Memory{
		topics:     make(map[string]*memTopic),
		groups:     make(map[string]*memGroup),
		partitions: 3,
	}
}

func (m *Memory) Capabilities() Capabilities {
	return Capabilities{Transactions: true}
}

func (m *Memory) SetFenced(v bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fenced = v
}

func (m *Memory) Revoke(topic string, partition int) {
	m.mu.Lock()
	subs := make([]*memSub, 0)
	for _, g := range m.groups {
		subs = append(subs, g.subs...)
	}
	m.mu.Unlock()
	for _, s := range subs {
		s.revoke(topic, partition)
	}
}

func (m *Memory) Publish(ctx context.Context, rec Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	if m.fenced {
		return ErrFenced
	}
	m.appendLocked(rec, 0, true)
	m.wakeLocked()
	return nil
}

func (m *Memory) CreateTopics(ctx context.Context, topics []string) error {
	specs := make([]TopicSpec, 0, len(topics))
	for _, topic := range topics {
		specs = append(specs, TopicSpec{Name: topic})
	}
	return m.CreateTopicSpecs(ctx, specs)
}

func (m *Memory) CreateTopicSpecs(ctx context.Context, topics []TopicSpec) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	for _, spec := range topics {
		if spec.Name == "" {
			continue
		}
		partitions := spec.Partitions
		if partitions <= 0 {
			partitions = m.partitions
		}
		m.ensureTopicPartitionsLocked(spec.Name, partitions)
	}
	m.wakeLocked()
	return nil
}

func (m *Memory) Subscribe(_ context.Context, groupID string, topics []string) (Subscription, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	g, ok := m.groups[groupID]
	if !ok {
		g = &memGroup{offsets: make(map[tp]int64)}
		m.groups[groupID] = g
	}
	sub := &memSub{
		broker:    m,
		groupID:   groupID,
		topics:    append([]string(nil), topics...),
		isolation: ReadUncommitted,
		revoked:   make(map[tp]struct{}),
		fetch:     make(map[tp]int64),
		notify:    make(chan struct{}, 1),
	}
	g.subs = append(g.subs, sub)
	return sub, nil
}

func (m *Memory) SubscribeIsolated(ctx context.Context, groupID string, topics []string, iso Isolation) (Subscription, error) {
	sub, err := m.Subscribe(ctx, groupID, topics)
	if err != nil {
		return nil, err
	}
	sub.(*memSub).isolation = iso
	return sub, nil
}

func (m *Memory) BeginTxn(ctx context.Context) (Txn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	if m.fenced {
		return nil, ErrFenced
	}
	m.txnSeq++
	return &memTxn{broker: m, id: m.txnSeq}, nil
}

func (m *Memory) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	m.wakeLocked()
	return nil
}

func (m *Memory) TopicRecords(topic string) []Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.topics[topic]
	if t == nil {
		return nil
	}
	var out []Record
	for _, p := range t.parts {
		for _, r := range p {
			if r.committed {
				out = append(out, r.rec)
			}
		}
	}
	return out
}

func (m *Memory) UncommittedCount(topic string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.topics[topic]
	if t == nil {
		return 0
	}
	n := 0
	for _, p := range t.parts {
		for _, r := range p {
			if !r.committed {
				n++
			}
		}
	}
	return n
}

func (m *Memory) GroupOffset(group, topic string, partition int) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.groups[group]
	if g == nil {
		return 0
	}
	return g.offsets[tp{topic, partition}]
}

func (m *Memory) ensureTopicLocked(name string) *memTopic {
	return m.ensureTopicPartitionsLocked(name, m.partitions)
}

func (m *Memory) ensureTopicPartitionsLocked(name string, partitions int) *memTopic {
	t, ok := m.topics[name]
	if !ok {
		if partitions <= 0 {
			partitions = m.partitions
		}
		t = &memTopic{parts: make([][]memRec, partitions)}
		m.topics[name] = t
	}
	return t
}

func (m *Memory) appendLocked(rec Record, txnID int64, committed bool) {
	t := m.ensureTopicLocked(rec.Topic)
	if rec.Partition < 0 || rec.Partition >= len(t.parts) {
		rec.Partition = partitionFor(rec.Key, len(t.parts))
	}
	rec.Offset = int64(len(t.parts[rec.Partition]))
	if rec.Timestamp.IsZero() {
		rec.Timestamp = time.Now()
	}
	t.parts[rec.Partition] = append(t.parts[rec.Partition], memRec{
		rec:       rec,
		txnID:     txnID,
		committed: committed,
	})
}

func (m *Memory) wakeLocked() {
	for _, g := range m.groups {
		for _, s := range g.subs {
			select {
			case s.notify <- struct{}{}:
			default:
			}
		}
	}
}

func partitionFor(key []byte, n int) int {
	if n <= 0 {
		return 0
	}
	if len(key) == 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write(key)
	return int(h.Sum32() % uint32(n))
}

func (s *memSub) Poll(ctx context.Context) (Record, error) {
	for {
		rec, ok, err := s.tryPoll()
		if err != nil {
			return Record{}, err
		}
		if ok {
			return rec, nil
		}
		select {
		case <-ctx.Done():
			return Record{}, ctx.Err()
		case <-s.notify:
		}
	}
}

func (s *memSub) tryPoll() (Record, bool, error) {
	s.broker.mu.Lock()
	defer s.broker.mu.Unlock()
	if s.broker.closed {
		return Record{}, false, ErrClosed
	}
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return Record{}, false, ErrClosed
	}
	g := s.broker.groups[s.groupID]
	for _, topic := range s.topics {
		t := s.broker.topics[topic]
		if t == nil {
			continue
		}
		for p := 0; p < len(t.parts); p++ {
			key := tp{topic, p}
			s.mu.Lock()
			_, revoked := s.revoked[key]
			s.mu.Unlock()
			if revoked {
				continue
			}
			next, ok := s.fetch[key]
			if !ok {
				next = g.offsets[key]
			}
			for _, r := range t.parts[p] {
				if r.rec.Offset < next {
					continue
				}
				if s.isolation == ReadCommitted && !r.committed {
					continue
				}
				s.fetch[key] = r.rec.Offset + 1
				return r.rec, true, nil
			}
		}
	}
	return Record{}, false, nil
}

func (s *memSub) Commit(ctx context.Context, rec Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !s.Assigned(rec.Topic, rec.Partition) {
		return ErrRevoked
	}
	s.broker.mu.Lock()
	defer s.broker.mu.Unlock()
	if s.broker.closed {
		return ErrClosed
	}
	g := s.broker.groups[s.groupID]
	key := tp{rec.Topic, rec.Partition}
	cur := g.offsets[key]
	next := rec.Offset + 1
	if next > cur {
		g.offsets[key] = next
	}
	return nil
}

func (s *memSub) Assigned(topic string, partition int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	_, revoked := s.revoked[tp{topic, partition}]
	return !revoked
}

func (s *memSub) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	select {
	case s.notify <- struct{}{}:
	default:
	}
	return nil
}

func (s *memSub) revoke(topic string, partition int) {
	s.mu.Lock()
	s.revoked[tp{topic, partition}] = struct{}{}
	s.mu.Unlock()
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (t *memTxn) Publish(ctx context.Context, rec Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return ErrNoTxn
	}
	t.publishes = append(t.publishes, rec)
	return nil
}

func (t *memTxn) SendOffsets(ctx context.Context, offsets []Offset, groupID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return ErrNoTxn
	}
	t.offsets = append(t.offsets, offsets...)
	t.groupID = groupID
	return nil
}

func (t *memTxn) Commit(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		_ = t.Abort(context.Background())
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return ErrNoTxn
	}
	t.broker.mu.Lock()
	defer t.broker.mu.Unlock()
	if t.broker.closed {
		return ErrClosed
	}
	if t.broker.fenced {
		return ErrFenced
	}
	for _, rec := range t.publishes {
		t.broker.appendLocked(rec, t.id, true)
	}
	if t.groupID != "" {
		g, ok := t.broker.groups[t.groupID]
		if !ok {
			g = &memGroup{offsets: make(map[tp]int64)}
			t.broker.groups[t.groupID] = g
		}
		for _, o := range t.offsets {
			key := tp{o.Topic, o.Partition}
			next := o.Offset + 1
			if next > g.offsets[key] {
				g.offsets[key] = next
			}
		}
	}
	t.done = true
	t.broker.wakeLocked()
	return nil
}

func (t *memTxn) Abort(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.done = true
	t.publishes = nil
	t.offsets = nil
	return ctx.Err()
}
