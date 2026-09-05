package retrykafka

// Client shares configuration between a Publisher and a Consumer.
type Client struct {
	Publisher *Publisher
	Consumer  *Consumer
}

func New(opts ...Option) (*Client, error) {
	p, err := NewPublisher(opts...)
	if err != nil {
		return nil, err
	}
	c, err := NewConsumer(opts...)
	if err != nil {
		_ = p.Close()
		return nil, err
	}
	return &Client{Publisher: p, Consumer: c}, nil
}

func (cl *Client) Close() error {
	var first error
	if cl.Consumer != nil {
		first = cl.Consumer.Close()
	}
	if cl.Publisher != nil {
		if err := cl.Publisher.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
