package retrykafka

import (
	"log/slog"
	"time"

	"github.com/retrykafka/retrykafka/driver"
	"github.com/retrykafka/retrykafka/idempotency"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type config struct {
	brokers          []string
	groupID          string
	drv              driver.Driver
	logger           *slog.Logger
	guarantee        ProcessingGuarantee
	maxRetries       int
	backoffs         []time.Duration
	retryFmt         string
	dlqFmt           string
	topicNamer       TopicNamer
	dlqEnabled       bool
	autoCreate       bool
	ordering         OrderingStrategy
	shutdownTimeout  time.Duration
	txnShutdown      time.Duration
	idempotency      idempotency.Store
	idempotencyTTL   time.Duration
	tracerProvider   trace.TracerProvider
	meterProvider    metric.MeterProvider
	clock            func() time.Time
	partitionWorkers bool
	isolation        driver.Isolation
	transactionalID  string
}

func defaultConfig() config {
	return config{
		logger:           slog.Default(),
		guarantee:        AtLeastOnce,
		maxRetries:       3,
		backoffs:         []time.Duration{5 * time.Second, 30 * time.Second, 5 * time.Minute},
		retryFmt:         "%s.retry.%d",
		dlqFmt:           "%s.dlq",
		dlqEnabled:       true,
		ordering:         OrderingNone,
		shutdownTimeout:  30 * time.Second,
		txnShutdown:      30 * time.Second,
		idempotencyTTL:   15 * time.Minute,
		tracerProvider:   otel.GetTracerProvider(),
		meterProvider:    otel.GetMeterProvider(),
		clock:            time.Now,
		partitionWorkers: true,
		isolation:        driver.ReadUncommitted,
		groupID:          "retrykafka",
	}
}

func buildConfig(opts ...Option) (config, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt.applyConfig(&cfg)
	}
	if cfg.guarantee == Transactional {
		cfg.isolation = driver.ReadCommitted
	}
	if err := validateConfig(cfg); err != nil {
		return config{}, err
	}
	return cfg, nil
}

// Option configures a Publisher, Consumer or Client.
type Option interface {
	applyConfig(*config)
}

type optionFunc func(*config)

func (f optionFunc) applyConfig(c *config) { f(c) }

func WithBrokers(brokers ...string) Option {
	return optionFunc(func(c *config) { c.brokers = append([]string{}, brokers...) })
}

func WithGroupID(id string) Option {
	return optionFunc(func(c *config) { c.groupID = id })
}

func WithDriver(d driver.Driver) Option {
	return optionFunc(func(c *config) { c.drv = d })
}

func WithLogger(l *slog.Logger) Option {
	return optionFunc(func(c *config) {
		if l != nil {
			c.logger = l
		}
	})
}

func WithProcessingGuarantee(g ProcessingGuarantee) Option {
	return optionFunc(func(c *config) { c.guarantee = g })
}

func WithTransactions() Option {
	return optionFunc(func(c *config) { c.guarantee = Transactional })
}

func WithMaxRetries(n int) Option {
	return optionFunc(func(c *config) { c.maxRetries = n })
}

func WithBackoff(durations ...time.Duration) Option {
	return optionFunc(func(c *config) { c.backoffs = append([]time.Duration{}, durations...) })
}

func WithRetryTopicFormat(format string) Option {
	return optionFunc(func(c *config) { c.retryFmt = format })
}

func WithDLQTopicFormat(format string) Option {
	return optionFunc(func(c *config) { c.dlqFmt = format })
}

// WithTopicNamer overrides retry and DLQ topic naming.
func WithTopicNamer(namer TopicNamer) Option {
	return optionFunc(func(c *config) { c.topicNamer = namer })
}

func WithAutoCreateTopics(enabled bool) Option {
	return optionFunc(func(c *config) { c.autoCreate = enabled })
}

func WithDLQ(enabled bool) Option {
	return optionFunc(func(c *config) { c.dlqEnabled = enabled })
}

func WithOrdering(s OrderingStrategy) Option {
	return optionFunc(func(c *config) { c.ordering = s })
}

func WithShutdownTimeout(d time.Duration) Option {
	return optionFunc(func(c *config) { c.shutdownTimeout = d })
}

func WithTransactionShutdownTimeout(d time.Duration) Option {
	return optionFunc(func(c *config) { c.txnShutdown = d })
}

func WithIdempotencyStore(store idempotency.Store) Option {
	return optionFunc(func(c *config) { c.idempotency = store })
}

func WithTracerProvider(tp trace.TracerProvider) Option {
	return optionFunc(func(c *config) { c.tracerProvider = tp })
}

func WithMeterProvider(mp metric.MeterProvider) Option {
	return optionFunc(func(c *config) { c.meterProvider = mp })
}

func WithTransactionalID(id string) Option {
	return optionFunc(func(c *config) { c.transactionalID = id })
}

func WithClock(now func() time.Time) Option {
	return optionFunc(func(c *config) {
		if now != nil {
			c.clock = now
		}
	})
}

type topicOption interface {
	applyTopic(*topicCfg)
}

type topicOptionFunc func(*topicCfg)

func (f topicOptionFunc) applyTopic(t *topicCfg) { f(t) }

type topicCfg struct {
	maxRetries *int
	backoffs   []time.Duration
	ordering   *OrderingStrategy
}

type maxAttemptsOption int

func (m maxAttemptsOption) applyConfig(c *config) {
	c.maxRetries = int(m)
}

func (m maxAttemptsOption) applyTopic(t *topicCfg) {
	n := int(m)
	t.maxRetries = &n
}

// WithMaxAttempts sets the number of retry attempts globally or for a single handler.
func WithMaxAttempts(n int) maxAttemptsOption {
	return maxAttemptsOption(n)
}

func WithTopicBackoff(durations ...time.Duration) topicOption {
	return topicOptionFunc(func(t *topicCfg) { t.backoffs = append([]time.Duration{}, durations...) })
}

func WithTopicOrdering(s OrderingStrategy) topicOption {
	return topicOptionFunc(func(t *topicCfg) { t.ordering = &s })
}

func (c config) maxRetriesFor(t topicCfg) int {
	if t.maxRetries != nil {
		return *t.maxRetries
	}
	return c.maxRetries
}

func (c config) backoffsFor(t topicCfg) []time.Duration {
	if len(t.backoffs) > 0 {
		return t.backoffs
	}
	return c.backoffs
}

func (c config) orderingFor(t topicCfg) OrderingStrategy {
	if t.ordering != nil {
		return *t.ordering
	}
	return c.ordering
}

func (c config) namer() TopicNamer {
	if c.topicNamer != nil {
		return c.topicNamer
	}
	return formatTopicNamer{retryFmt: c.retryFmt, dlqFmt: c.dlqFmt}
}

func (c config) retryTopic(topic string, attempt int) string {
	return c.namer().RetryTopic(topic, attempt)
}

func (c config) dlqTopic(topic string) string {
	return c.namer().DLQTopic(topic)
}
