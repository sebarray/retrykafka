package retrykafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/retrykafka/retrykafka/driver"
	"github.com/retrykafka/retrykafka/idempotency"
	"github.com/retrykafka/retrykafka/observability"
	"go.opentelemetry.io/otel/attribute"
)

// Handler processes a single message. A non-nil error triggers retry/DLQ routing.
type Handler func(ctx context.Context, msg Message) error

type registration struct {
	handler Handler
	cfg     topicCfg
}

type partID struct {
	topic     string
	partition int
}

type partJob struct {
	msg Message
}

// Consumer registers handlers and processes messages sequentially per partition.
type Consumer struct {
	cfg      config
	drv      driver.Driver
	tel      *observability.Telemetry
	mu       sync.Mutex
	handlers map[string]registration
	closed   bool
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	txnMu    sync.Mutex // serializes transactional producer ownership
}

func NewConsumer(opts ...Option) (*Consumer, error) {
	cfg, err := buildConfig(opts...)
	if err != nil {
		return nil, err
	}
	drv, err := resolveDriver(cfg)
	if err != nil {
		return nil, err
	}
	return &Consumer{
		cfg:      cfg,
		drv:      drv,
		tel:      observability.New(cfg.logger, cfg.tracerProvider, cfg.meterProvider, nil),
		handlers: make(map[string]registration),
	}, nil
}

func (c *Consumer) Handle(topic string, h Handler, opts ...topicOption) {
	var tc topicCfg
	for _, opt := range opts {
		opt.applyTopic(&tc)
	}
	c.mu.Lock()
	c.handlers[topic] = registration{handler: h, cfg: tc}
	c.mu.Unlock()
}

func (c *Consumer) Run(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	topics, createTopics, err := c.topicPlanLocked()
	c.mu.Unlock()
	if err != nil {
		return err
	}
	if len(topics) == 0 {
		return fmt.Errorf("%w: no handlers", ErrInvalidConfig)
	}

	runCtx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.cancel = cancel
	c.mu.Unlock()

	if err := c.createTopics(runCtx, createTopics); err != nil {
		cancel()
		return err
	}

	sub, err := c.subscribe(runCtx, topics)
	if err != nil {
		cancel()
		return err
	}
	defer sub.Close()

	workers := make(map[partID]chan partJob)
	var wmu sync.Mutex

	ensureWorker := func(id partID) chan partJob {
		wmu.Lock()
		defer wmu.Unlock()
		ch, ok := workers[id]
		if ok {
			return ch
		}
		ch = make(chan partJob, 32)
		workers[id] = ch
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.partitionLoop(runCtx, sub, id, ch)
		}()
		return ch
	}

	c.tel.Event(runCtx, "consumer started", "group", c.cfg.groupID)

	for {
		rec, err := sub.Poll(runCtx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, driver.ErrClosed) {
				break
			}
			c.tel.Error(runCtx, "consumer poll error", "error", err)
			if runCtx.Err() != nil {
				break
			}
			continue
		}
		id := partID{topic: rec.Topic, partition: rec.Partition}
		ch := ensureWorker(id)
		select {
		case ch <- partJob{msg: messageFromRecord(rec)}:
		case <-runCtx.Done():
			goto shutdown
		}
	}

shutdown:
	cancel()
	wmu.Lock()
	for _, ch := range workers {
		close(ch)
	}
	wmu.Unlock()
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()
	selCtx, selCancel := context.WithTimeout(context.Background(), c.cfg.shutdownTimeout)
	defer selCancel()
	select {
	case <-done:
	case <-selCtx.Done():
		c.tel.Error(context.Background(), "consumer shutdown timeout")
	}
	c.tel.Event(context.Background(), "consumer shutdown", "group", c.cfg.groupID)
	return nil
}

func (c *Consumer) Close() error {
	c.mu.Lock()
	c.closed = true
	if c.cancel != nil {
		c.cancel()
	}
	c.mu.Unlock()
	return c.drv.Close()
}

func (c *Consumer) subscribeTopicsLocked() []string {
	topics, _, _ := c.topicPlanLocked()
	return topics
}

func (c *Consumer) topicPlanLocked() ([]string, []driver.TopicSpec, error) {
	seen := map[string]struct{}{}
	seenCreate := map[string]struct{}{}
	var subscribe []string
	var create []driver.TopicSpec
	addSubscribe := func(t string) {
		if t == "" {
			return
		}
		if _, ok := seen[t]; ok {
			return
		}
		seen[t] = struct{}{}
		subscribe = append(subscribe, t)
	}
	addCreate := func(spec driver.TopicSpec) {
		if spec.Name == "" {
			return
		}
		if _, ok := seenCreate[spec.Name]; ok {
			return
		}
		seenCreate[spec.Name] = struct{}{}
		create = append(create, spec)
	}
	for topic, reg := range c.handlers {
		if reg.handler == nil {
			return nil, nil, fmt.Errorf("%w: handler for topic %q is nil", ErrInvalidConfig, topic)
		}
		max := c.cfg.maxRetriesFor(reg.cfg)
		if max < 0 {
			return nil, nil, fmt.Errorf("%w: max attempts for topic %q must be >= 0", ErrInvalidConfig, topic)
		}
		if err := validateBackoffs(c.cfg.backoffsFor(reg.cfg)); err != nil {
			return nil, nil, err
		}
		tcfg := c.cfg
		tcfg.maxRetries = max
		if err := validateTopicNames(tcfg, topic); err != nil {
			return nil, nil, err
		}
		addSubscribe(topic)
		addCreate(defaultTopicSpec(topic))
		for i := 1; i <= max; i++ {
			name := c.cfg.retryTopic(topic, i)
			addSubscribe(name)
			addCreate(defaultTopicSpec(name))
		}
		if c.cfg.dlqEnabled {
			addCreate(defaultTopicSpec(c.cfg.dlqTopic(topic)))
		}
	}
	return subscribe, create, nil
}

func (c *Consumer) createTopics(ctx context.Context, topics []driver.TopicSpec) error {
	if !c.cfg.autoCreate || len(topics) == 0 {
		return nil
	}
	return createTopics(ctx, c.drv, topics)
}

func (c *Consumer) subscribe(ctx context.Context, topics []string) (driver.Subscription, error) {
	if c.cfg.guarantee == Transactional {
		if iso, ok := c.drv.(interface {
			SubscribeIsolated(context.Context, string, []string, driver.Isolation) (driver.Subscription, error)
		}); ok {
			return iso.SubscribeIsolated(ctx, c.cfg.groupID, topics, driver.ReadCommitted)
		}
	}
	return c.drv.Subscribe(ctx, c.cfg.groupID, topics)
}

func (c *Consumer) lookup(msg Message) (registration, string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if reg, ok := c.handlers[msg.Topic]; ok {
		return reg, msg.Topic, true
	}
	if base := msg.Headers.Get(HeaderOriginalTopic); base != "" {
		reg, ok := c.handlers[base]
		return reg, base, ok
	}
	for topic, reg := range c.handlers {
		if c.topicMatchesLogical(msg.Topic, topic, c.cfg.maxRetriesFor(reg.cfg)) {
			return reg, topic, true
		}
	}
	return registration{}, "", false
}

func (c *Consumer) partitionLoop(ctx context.Context, sub driver.Subscription, id partID, ch <-chan partJob) {
	switch c.orderingForPartition(id.topic) {
	case OrderingPerKey:
		c.partitionLoopPerKey(ctx, sub, ch)
	default:
		for job := range ch {
			if ctx.Err() != nil {
				return
			}
			c.process(ctx, sub, job.msg)
		}
	}
}

func (c *Consumer) orderingForPartition(topic string) OrderingStrategy {
	c.mu.Lock()
	defer c.mu.Unlock()
	if reg, ok := c.handlers[topic]; ok {
		return c.cfg.orderingFor(reg.cfg)
	}
	for name, reg := range c.handlers {
		if c.topicMatchesLogical(topic, name, c.cfg.maxRetriesFor(reg.cfg)) {
			return c.cfg.orderingFor(reg.cfg)
		}
	}
	return c.cfg.ordering
}

func (c *Consumer) topicMatchesLogical(topic, logical string, maxRetries int) bool {
	if topic == logical {
		return true
	}
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if c.cfg.retryTopic(logical, attempt) == topic {
			return true
		}
	}
	return c.cfg.dlqEnabled && c.cfg.dlqTopic(logical) == topic
}

func (c *Consumer) process(ctx context.Context, sub driver.Subscription, msg Message) {
	reg, logical, ok := c.lookup(msg)
	if !ok {
		c.tel.Error(ctx, "message received", append(c.tel.Attrs(msg.ID, msg.Topic, msg.Partition, msg.Offset, msg.RetryAttempt(), 0, ErrNoHandler), "error", ErrNoHandler)...)
		_ = c.commit(ctx, sub, msg)
		return
	}
	maxRetries := c.cfg.maxRetriesFor(reg.cfg)
	backoffs := c.cfg.backoffsFor(reg.cfg)
	ordering := c.cfg.orderingFor(reg.cfg)

	if err := c.waitNotBefore(ctx, msg); err != nil {
		return
	}

	ctx = c.tel.Extract(ctx, msg.Headers)
	ctx, consumeSpan := c.tel.Start(ctx, observability.SpanConsume, msg.Topic,
		attribute.Int("messaging.kafka.partition", msg.Partition),
		attribute.Int64("messaging.kafka.offset", msg.Offset),
	)
	defer consumeSpan.End()

	c.tel.Event(ctx, "message received", c.tel.Attrs(msg.ID, msg.Topic, msg.Partition, msg.Offset, msg.RetryAttempt(), maxRetries, nil)...)

	if c.cfg.idempotency != nil && msg.ID != "" {
		res, err := c.cfg.idempotency.TryStart(ctx, msg.ID)
		if err != nil {
			c.failAndRoute(ctx, sub, msg, logical, reg.handler, maxRetries, backoffs, ordering, err)
			return
		}
		if res == idempotency.ResultSkipCompleted {
			_ = c.commit(ctx, sub, msg)
			return
		}
		if res == idempotency.ResultSkipInProgress {
			return
		}
	}

	if ordering == OrderingPerPartition {
		c.processInPlace(ctx, sub, msg, logical, reg, maxRetries, backoffs)
		return
	}

	err := c.runHandler(ctx, msg, logical, reg.handler)
	if err == nil {
		if c.cfg.idempotency != nil && msg.ID != "" {
			_ = c.cfg.idempotency.Complete(ctx, msg.ID)
		}
		c.tel.Event(ctx, "message processed", c.tel.Attrs(msg.ID, msg.Topic, msg.Partition, msg.Offset, msg.RetryAttempt(), maxRetries, nil)...)
		c.tel.RecordProcessed(ctx, logical, c.cfg.guarantee.String())
		_ = c.commit(ctx, sub, msg)
		return
	}
	if c.cfg.idempotency != nil && msg.ID != "" {
		_ = c.cfg.idempotency.Fail(ctx, msg.ID)
	}
	c.failAndRoute(ctx, sub, msg, logical, reg.handler, maxRetries, backoffs, ordering, err)
}

func (c *Consumer) runHandler(ctx context.Context, msg Message, logical string, h Handler) error {
	ctx, span := c.tel.Start(ctx, observability.SpanProcess, msg.Topic)
	start := c.cfg.clock()
	err := h(ctx, msg)
	c.tel.RecordHandlerDuration(ctx, logical, c.cfg.guarantee.String(), c.cfg.clock().Sub(start))
	c.tel.EndSpan(span, err)
	return err
}

func (c *Consumer) processInPlace(ctx context.Context, sub driver.Subscription, msg Message, logical string, reg registration, maxRetries int, backoffs []time.Duration) {
	attempt := msg.RetryAttempt()
	cur := msg
	for {
		err := c.runHandler(ctx, cur, logical, reg.handler)
		if err == nil {
			if c.cfg.idempotency != nil && cur.ID != "" {
				_ = c.cfg.idempotency.Complete(ctx, cur.ID)
			}
			c.tel.Event(ctx, "message processed", c.tel.Attrs(cur.ID, cur.Topic, cur.Partition, cur.Offset, attempt, maxRetries, nil)...)
			c.tel.RecordProcessed(ctx, logical, c.cfg.guarantee.String())
			_ = c.commit(ctx, sub, msg)
			return
		}
		c.tel.Error(ctx, "processing failed", c.tel.Attrs(cur.ID, cur.Topic, cur.Partition, cur.Offset, attempt, maxRetries, err)...)
		c.tel.RecordFailed(ctx, logical, c.cfg.guarantee.String())
		if attempt >= maxRetries {
			if c.cfg.idempotency != nil && cur.ID != "" {
				_ = c.cfg.idempotency.Fail(ctx, cur.ID)
			}
			_ = c.routeTerminal(ctx, sub, msg, logical, attempt, maxRetries, err)
			return
		}
		attempt++
		delay := backoffFor(backoffs, attempt)
		c.tel.Event(ctx, "retry scheduled", append(c.tel.Attrs(cur.ID, cur.Topic, cur.Partition, cur.Offset, attempt, maxRetries, err), "delay", delay.String())...)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		cur.Headers = attachRetryMetadata(cur, c.cfg.clock(), attempt, err, 0)
	}
}

func (c *Consumer) failAndRoute(ctx context.Context, sub driver.Subscription, msg Message, logical string, _ Handler, maxRetries int, backoffs []time.Duration, _ OrderingStrategy, err error) {
	c.tel.Error(ctx, "processing failed", c.tel.Attrs(msg.ID, msg.Topic, msg.Partition, msg.Offset, msg.RetryAttempt(), maxRetries, err)...)
	c.tel.RecordFailed(ctx, logical, c.cfg.guarantee.String())
	attempt := msg.RetryAttempt()
	if attempt >= maxRetries {
		_ = c.routeTerminal(ctx, sub, msg, logical, attempt, maxRetries, err)
		return
	}
	next := attempt + 1
	delay := backoffFor(backoffs, next)
	dest := c.cfg.retryTopic(logical, next)
	c.tel.Event(ctx, "retry scheduled", append(c.tel.Attrs(msg.ID, dest, msg.Partition, msg.Offset, next, maxRetries, err), "delay", delay.String())...)
	out := c.retryMessage(msg, dest, next, err, delay)
	if pubErr := c.publishOutcome(ctx, sub, msg, out, observability.SpanRetryPub, "retry published"); pubErr != nil {
		c.tel.Error(ctx, "retry published", c.tel.Attrs(msg.ID, dest, msg.Partition, msg.Offset, next, maxRetries, pubErr)...)
		return
	}
	c.tel.RecordRetry(ctx, logical, c.cfg.guarantee.String())
}

func (c *Consumer) routeTerminal(ctx context.Context, sub driver.Subscription, msg Message, logical string, attempt, maxRetries int, err error) error {
	if !c.cfg.dlqEnabled {
		return c.commit(ctx, sub, msg)
	}
	dest := c.cfg.dlqTopic(logical)
	out := c.retryMessage(msg, dest, attempt, err, 0)
	if pubErr := c.publishOutcome(ctx, sub, msg, out, observability.SpanDLQPub, "dlq published"); pubErr != nil {
		c.tel.Error(ctx, "dlq published", c.tel.Attrs(msg.ID, dest, msg.Partition, msg.Offset, attempt, maxRetries, pubErr)...)
		return pubErr
	}
	c.tel.RecordDLQ(ctx, logical, c.cfg.guarantee.String())
	return nil
}

func (c *Consumer) retryMessage(src Message, dest string, attempt int, err error, delay time.Duration) Message {
	now := c.cfg.clock()
	h := attachRetryMetadata(src, now, attempt, err, delay)
	out := src.clone()
	out.Topic = dest
	out.Headers = h
	out.ID = h.Get(HeaderMessageID)
	out.Partition = 0
	out.Offset = 0
	out.Timestamp = now
	return out
}

func (c *Consumer) publishOutcome(ctx context.Context, sub driver.Subscription, consumed Message, out Message, spanName, logEvent string) error {
	if !sub.Assigned(consumed.Topic, consumed.Partition) {
		return ErrPartitionRevoked
	}
	if c.cfg.guarantee == Transactional {
		return c.publishTransactional(ctx, sub, consumed, out, spanName, logEvent)
	}
	ctx, span := c.tel.Start(ctx, spanName, out.Topic)
	err := c.drv.Publish(ctx, recordFromMessage(out))
	c.tel.EndSpan(span, err)
	if err != nil {
		return err
	}
	c.tel.Event(ctx, logEvent, c.tel.Attrs(out.ID, out.Topic, consumed.Partition, consumed.Offset, out.RetryAttempt(), c.cfg.maxRetries, nil)...)
	return c.commit(ctx, sub, consumed)
}

func (c *Consumer) publishTransactional(ctx context.Context, sub driver.Subscription, consumed Message, out Message, spanName, logEvent string) error {
	td, ok := c.drv.(driver.TxnDriver)
	if !ok {
		return ErrTransactionsUnsupported
	}
	if !sub.Assigned(consumed.Topic, consumed.Partition) {
		return ErrPartitionRevoked
	}

	c.txnMu.Lock()
	defer c.txnMu.Unlock()

	start := c.cfg.clock()
	ctx, beginSpan := c.tel.Start(ctx, observability.SpanTxnBegin, out.Topic)
	c.tel.RecordTxnStarted(ctx, consumed.Topic, c.cfg.guarantee.String())
	c.tel.Event(ctx, "transaction started", c.tel.Attrs(consumed.ID, consumed.Topic, consumed.Partition, consumed.Offset, consumed.RetryAttempt(), c.cfg.maxRetries, nil)...)
	txn, err := td.BeginTxn(ctx)
	c.tel.EndSpan(beginSpan, err)
	if err != nil {
		c.tel.RecordTxnFailed(ctx, consumed.Topic, "begin")
		c.tel.RecordTxnAborted(ctx, consumed.Topic, c.cfg.guarantee.String())
		return err
	}

	abort := func(op string, e error) error {
		_, as := c.tel.Start(ctx, observability.SpanTxnAbort, out.Topic)
		ae := txn.Abort(context.Background())
		c.tel.EndSpan(as, ae)
		c.tel.RecordTxnAborted(ctx, consumed.Topic, c.cfg.guarantee.String())
		c.tel.RecordTxnFailed(ctx, consumed.Topic, op)
		c.tel.Error(ctx, "transaction aborted", c.tel.Attrs(consumed.ID, consumed.Topic, consumed.Partition, consumed.Offset, consumed.RetryAttempt(), c.cfg.maxRetries, e)...)
		return e
	}

	if !sub.Assigned(consumed.Topic, consumed.Partition) {
		return abort("rebalance", ErrPartitionRevoked)
	}

	ctx, pubSpan := c.tel.Start(ctx, observability.SpanTxnPublish, out.Topic)
	_ = spanName
	err = txn.Publish(ctx, recordFromMessage(out))
	c.tel.EndSpan(pubSpan, err)
	if err != nil {
		return abort("publish", err)
	}

	ctx, offSpan := c.tel.Start(ctx, observability.SpanTxnOffsets, consumed.Topic)
	err = txn.SendOffsets(ctx, []driver.Offset{{
		Topic:     consumed.Topic,
		Partition: consumed.Partition,
		Offset:    consumed.Offset,
	}}, c.cfg.groupID)
	c.tel.EndSpan(offSpan, err)
	if err != nil {
		return abort("offsets", err)
	}

	if !sub.Assigned(consumed.Topic, consumed.Partition) {
		return abort("rebalance", ErrPartitionRevoked)
	}

	ctx, commitSpan := c.tel.Start(ctx, observability.SpanTxnCommit, out.Topic)
	err = txn.Commit(ctx)
	c.tel.EndSpan(commitSpan, err)
	c.tel.RecordTxnDuration(ctx, consumed.Topic, c.cfg.guarantee.String(), c.cfg.clock().Sub(start))
	if err != nil {
		return abort("commit", err)
	}
	c.tel.RecordTxnCommitted(ctx, consumed.Topic, c.cfg.guarantee.String())
	c.tel.Event(ctx, "transaction committed", c.tel.Attrs(consumed.ID, consumed.Topic, consumed.Partition, consumed.Offset, consumed.RetryAttempt(), c.cfg.maxRetries, nil)...)
	c.tel.Event(ctx, logEvent, c.tel.Attrs(out.ID, out.Topic, consumed.Partition, consumed.Offset, out.RetryAttempt(), c.cfg.maxRetries, nil)...)
	c.tel.Event(ctx, "offset committed", c.tel.Attrs(consumed.ID, consumed.Topic, consumed.Partition, consumed.Offset, consumed.RetryAttempt(), c.cfg.maxRetries, nil)...)
	return nil
}

func (c *Consumer) commit(ctx context.Context, sub driver.Subscription, msg Message) error {
	if !sub.Assigned(msg.Topic, msg.Partition) {
		return ErrPartitionRevoked
	}
	err := sub.Commit(ctx, recordFromMessage(msg))
	if err != nil {
		c.tel.Error(ctx, "offset committed", c.tel.Attrs(msg.ID, msg.Topic, msg.Partition, msg.Offset, msg.RetryAttempt(), c.cfg.maxRetries, err)...)
		return err
	}
	c.tel.Event(ctx, "offset committed", c.tel.Attrs(msg.ID, msg.Topic, msg.Partition, msg.Offset, msg.RetryAttempt(), c.cfg.maxRetries, nil)...)
	return nil
}

func (c *Consumer) waitNotBefore(ctx context.Context, msg Message) error {
	nb := msg.NotBefore()
	if nb.IsZero() {
		return nil
	}
	d := time.Until(nb)
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// partitionLoopPerKey holds later offsets until earlier ones complete, while
// allowing different keys to run their handlers before a contiguous commit.
func (c *Consumer) partitionLoopPerKey(ctx context.Context, sub driver.Subscription, ch <-chan partJob) {
	type item struct {
		msg     Message
		done    bool
		success bool
	}
	var q []*item
	active := map[string]*item{}

	drain := func() {
		for len(q) > 0 && q[0].done {
			head := q[0]
			if head.success {
				_ = c.commit(ctx, sub, head.msg)
			}
			q = q[1:]
		}
	}

	process := func(it *item) {
		key := string(it.msg.Key)
		c.process(ctx, sub, it.msg)
		it.done = true
		it.success = true
		delete(active, key)
		drain()
	}

	for job := range ch {
		if ctx.Err() != nil {
			return
		}
		it := &item{msg: job.msg}
		q = append(q, it)
		key := string(job.msg.Key)
		if key != "" {
			if _, busy := active[key]; busy {
				continue
			}
			active[key] = it
		}
		process(it)
		for {
			progress := false
			for _, pending := range q {
				if pending.done {
					continue
				}
				k := string(pending.msg.Key)
				if k != "" {
					if cur, ok := active[k]; ok && cur != pending {
						continue
					}
					active[k] = pending
				}
				process(pending)
				progress = true
			}
			if !progress {
				break
			}
		}
	}
}
