package retrykafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/retrykafka/retrykafka/driver"
	kafkadriver "github.com/retrykafka/retrykafka/driver/kafka"
	"github.com/retrykafka/retrykafka/observability"
)

// PublishOption customizes a single publish call.
type PublishOption func(*publishCfg)

type publishCfg struct {
	key     []byte
	headers Headers
	event   string
	schema  string
}

func WithKey(key []byte) PublishOption {
	return func(p *publishCfg) { p.key = append([]byte(nil), key...) }
}

func WithHeaders(h Headers) PublishOption {
	return func(p *publishCfg) { p.headers = h.Clone() }
}

func WithEventType(t string) PublishOption {
	return func(p *publishCfg) { p.event = t }
}

func WithSchemaVersion(v string) PublishOption {
	return func(p *publishCfg) { p.schema = v }
}

// Publisher is safe for concurrent use. Each Publish keeps operation-local state.
type Publisher struct {
	cfg    config
	drv    driver.Driver
	tel    *observability.Telemetry
	mu     sync.RWMutex
	closed bool
}

func NewPublisher(opts ...Option) (*Publisher, error) {
	cfg, err := buildConfig(opts...)
	if err != nil {
		return nil, err
	}
	drv, err := resolveDriver(cfg)
	if err != nil {
		return nil, err
	}
	return &Publisher{
		cfg: cfg,
		drv: drv,
		tel: observability.New(cfg.logger, cfg.tracerProvider, cfg.meterProvider, nil),
	}, nil
}

func (p *Publisher) Publish(ctx context.Context, topic string, payload any, opts ...PublishOption) error {
	p.mu.RLock()
	closed := p.closed
	p.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	if topic == "" {
		return fmt.Errorf("%w: topic is required", ErrInvalidConfig)
	}
	if err := p.createTopics(ctx, []driver.TopicSpec{defaultTopicSpec(topic)}); err != nil {
		p.tel.Error(ctx, "publisher error", "error", err, "topic", topic)
		return err
	}

	pc := publishCfg{headers: Headers{}}
	for _, opt := range opts {
		opt(&pc)
	}

	body, err := serialize(payload)
	if err != nil {
		p.tel.Error(ctx, "publisher error", "error", err, "topic", topic)
		return err
	}

	id := pc.headers.Get(HeaderMessageID)
	if id == "" {
		id = newMessageID()
	}
	now := p.cfg.clock()
	h := attachProduceMetadata(pc.headers, id, now)
	if pc.event != "" && h.Get(HeaderEventType) == "" {
		h.Set(HeaderEventType, pc.event)
	}
	if pc.schema != "" && h.Get(HeaderSchemaVersion) == "" {
		h.Set(HeaderSchemaVersion, pc.schema)
	}

	msg := Message{
		ID:        h.Get(HeaderMessageID),
		Key:       pc.key,
		Payload:   body,
		Headers:   h,
		Topic:     topic,
		Timestamp: now,
	}

	ctx, span := p.tel.Start(ctx, observability.SpanPublish, topic)
	p.tel.Inject(ctx, msg.Headers)
	err = p.drv.Publish(ctx, recordFromMessage(msg))
	p.tel.EndSpan(span, err)
	if err != nil {
		p.tel.Error(ctx, "publisher error", p.tel.Attrs(msg.ID, topic, 0, 0, 0, p.cfg.maxRetries, err)...)
		return err
	}
	return nil
}

func (p *Publisher) createTopics(ctx context.Context, topics []driver.TopicSpec) error {
	if !p.cfg.autoCreate || len(topics) == 0 {
		return nil
	}
	return createTopics(ctx, p.drv, topics)
}

func (p *Publisher) Close() error {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	return p.drv.Close()
}

func resolveDriver(cfg config) (driver.Driver, error) {
	if cfg.drv != nil {
		if cfg.guarantee == Transactional && !cfg.drv.Capabilities().Transactions {
			return nil, fmt.Errorf("%w", ErrTransactionsUnsupported)
		}
		return cfg.drv, nil
	}
	if len(cfg.brokers) == 0 {
		return nil, fmt.Errorf("%w: brokers or driver required", ErrInvalidConfig)
	}
	kopts := kafkadriver.Options{
		Brokers:          cfg.brokers,
		TransactionalID:  cfg.transactionalID,
		Isolation:        cfg.isolation,
		AutoCreateTopics: cfg.autoCreate,
	}
	if cfg.guarantee == Transactional {
		kopts.Transactions = true
		if kopts.TransactionalID == "" {
			kopts.TransactionalID = cfg.groupID + "-txn"
		}
		cfg.isolation = driver.ReadCommitted
		kopts.Isolation = driver.ReadCommitted
	}
	drv, err := kafkadriver.New(kopts)
	if err != nil {
		return nil, err
	}
	if cfg.guarantee == Transactional && !drv.Capabilities().Transactions {
		_ = drv.Close()
		return nil, ErrTransactionsUnsupported
	}
	return drv, nil
}
