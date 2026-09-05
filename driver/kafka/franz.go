package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/retrykafka/retrykafka/driver"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type Options struct {
	Brokers          []string
	TransactionalID  string
	Transactions     bool
	Isolation        driver.Isolation
	AutoCreateTopics bool
}

// Driver wraps franz-go. A transactional client is not shared across goroutines
// without the mutex held by retrykafka's consumer.
type Driver struct {
	opts  Options
	prod  *kgo.Client
	txnMu sync.Mutex
}

func New(opts Options) (*Driver, error) {
	if len(opts.Brokers) == 0 {
		return nil, errors.New("kafka driver: brokers required")
	}
	kopts := []kgo.Opt{
		kgo.SeedBrokers(opts.Brokers...),
	}
	if opts.AutoCreateTopics {
		kopts = append(kopts, kgo.AllowAutoTopicCreation())
	}
	if opts.Transactions {
		if opts.TransactionalID == "" {
			return nil, errors.New("kafka driver: transactional id required")
		}
		kopts = append(kopts, kgo.TransactionalID(opts.TransactionalID))
	}
	cl, err := kgo.NewClient(kopts...)
	if err != nil {
		return nil, err
	}
	return &Driver{opts: opts, prod: cl}, nil
}

func (d *Driver) Capabilities() driver.Capabilities {
	return driver.Capabilities{Transactions: d.opts.Transactions}
}

func (d *Driver) Publish(ctx context.Context, rec driver.Record) error {
	return d.prod.ProduceSync(ctx, toKgo(rec)).FirstErr()
}

func (d *Driver) CreateTopics(ctx context.Context, topics []string) error {
	specs := make([]driver.TopicSpec, 0, len(topics))
	for _, topic := range topics {
		specs = append(specs, driver.TopicSpec{Name: topic})
	}
	return d.CreateTopicSpecs(ctx, specs)
}

func (d *Driver) CreateTopicSpecs(ctx context.Context, topics []driver.TopicSpec) error {
	if len(topics) == 0 {
		return nil
	}
	req := kmsg.NewPtrCreateTopicsRequest()
	seen := map[string]struct{}{}
	for _, spec := range topics {
		if spec.Name == "" {
			continue
		}
		if _, ok := seen[spec.Name]; ok {
			continue
		}
		seen[spec.Name] = struct{}{}
		topic := kmsg.NewCreateTopicsRequestTopic()
		topic.Topic = spec.Name
		topic.NumPartitions = int32(spec.Partitions)
		if topic.NumPartitions <= 0 {
			topic.NumPartitions = 1
		}
		topic.ReplicationFactor = int16(spec.ReplicationFactor)
		if topic.ReplicationFactor <= 0 {
			topic.ReplicationFactor = 1
		}
		if len(spec.Configs) > 0 {
			topic.Configs = make([]kmsg.CreateTopicsRequestTopicConfig, 0, len(spec.Configs))
			for key, value := range spec.Configs {
				cfg := kmsg.NewCreateTopicsRequestTopicConfig()
				cfg.Name = key
				cfg.Value = kmsg.StringPtr(value)
				topic.Configs = append(topic.Configs, cfg)
			}
		}
		req.Topics = append(req.Topics, topic)
	}
	if len(req.Topics) == 0 {
		return nil
	}
	res, err := req.RequestWith(ctx, d.prod)
	if err != nil {
		return err
	}
	for _, topic := range res.Topics {
		if err := kerr.ErrorForCode(topic.ErrorCode); err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("kafka driver: create topic %q: %w", topic.Topic, err)
		}
	}
	return nil
}

func (d *Driver) Subscribe(_ context.Context, groupID string, topics []string) (driver.Subscription, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(d.opts.Brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
	}
	if d.opts.AutoCreateTopics {
		opts = append(opts, kgo.AllowAutoTopicCreation())
	}
	if d.opts.Isolation == driver.ReadCommitted || d.opts.Transactions {
		opts = append(opts, kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &sub{cl: cl}, nil
}

func (d *Driver) BeginTxn(ctx context.Context) (driver.Txn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !d.opts.Transactions {
		return nil, errors.New("kafka driver: transactions not enabled")
	}
	d.txnMu.Lock()
	if err := d.prod.BeginTransaction(); err != nil {
		d.txnMu.Unlock()
		return nil, err
	}
	return &txn{d: d}, nil
}

func (d *Driver) Close() error {
	d.prod.Close()
	return nil
}

type sub struct {
	cl *kgo.Client
}

func (s *sub) Poll(ctx context.Context) (driver.Record, error) {
	fetches := s.cl.PollFetches(ctx)
	if fetches.IsClientClosed() {
		return driver.Record{}, driver.ErrClosed
	}
	if err := fetches.Err(); err != nil {
		return driver.Record{}, err
	}
	iter := fetches.RecordIter()
	if iter.Done() {
		return s.Poll(ctx)
	}
	return fromKgo(iter.Next()), nil
}

func (s *sub) Commit(ctx context.Context, rec driver.Record) error {
	return s.cl.CommitRecords(ctx, toKgo(rec))
}

func (s *sub) Assigned(topic string, partition int) bool {
	_ = topic
	_ = partition
	return true
}

func (s *sub) Close() error {
	s.cl.Close()
	return nil
}

type txn struct {
	d    *Driver
	done bool
}

func (t *txn) Publish(ctx context.Context, rec driver.Record) error {
	return t.d.prod.ProduceSync(ctx, toKgo(rec)).FirstErr()
}

func (t *txn) SendOffsets(ctx context.Context, offsets []driver.Offset, groupID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_ = offsets
	_ = groupID
	return nil
}

func (t *txn) Commit(ctx context.Context) error {
	if t.done {
		return nil
	}
	t.done = true
	defer t.d.txnMu.Unlock()
	return t.d.prod.EndTransaction(ctx, kgo.TryCommit)
}

func (t *txn) Abort(ctx context.Context) error {
	if t.done {
		return nil
	}
	t.done = true
	defer t.d.txnMu.Unlock()
	return t.d.prod.EndTransaction(ctx, kgo.TryAbort)
}

func toKgo(rec driver.Record) *kgo.Record {
	hs := make([]kgo.RecordHeader, 0, len(rec.Headers))
	for _, h := range rec.Headers {
		hs = append(hs, kgo.RecordHeader{Key: h.Key, Value: h.Value})
	}
	return &kgo.Record{
		Topic:     rec.Topic,
		Key:       rec.Key,
		Value:     rec.Value,
		Headers:   hs,
		Timestamp: rec.Timestamp,
	}
}

func fromKgo(r *kgo.Record) driver.Record {
	hs := make([]driver.Header, 0, len(r.Headers))
	for _, h := range r.Headers {
		hs = append(hs, driver.Header{Key: h.Key, Value: h.Value})
	}
	return driver.Record{
		Topic:     r.Topic,
		Partition: int(r.Partition),
		Offset:    r.Offset,
		Key:       r.Key,
		Value:     r.Value,
		Headers:   hs,
		Timestamp: r.Timestamp,
	}
}

var _ = time.Now
