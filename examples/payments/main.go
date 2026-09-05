package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/retrykafka/retrykafka"
)

func main() {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}

	pub, err := retrykafka.NewPublisher(retrykafka.WithBrokers(brokers))
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	cons, err := retrykafka.NewConsumer(
		retrykafka.WithBrokers(brokers),
		retrykafka.WithGroupID("payments-worker"),
		retrykafka.WithBackoff(5*time.Second, 30*time.Second, 5*time.Minute),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer cons.Close()

	cons.Handle("payments", func(ctx context.Context, msg retrykafka.Message) error {
		log.Printf("id=%s topic=%s attempt=%d", msg.ID, msg.Topic, msg.RetryAttempt())
		return nil
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	go func() {
		_ = pub.Publish(ctx, "payments", map[string]string{"id": "payment-123"})
	}()

	if err := cons.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
