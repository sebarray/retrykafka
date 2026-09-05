package postgres_test

import (
	"testing"

	"github.com/retrykafka/retrykafka/adapters/postgres"
)

func TestConstructorDefaults(t *testing.T) {
	s := postgres.New(nil, "", 0)
	if s.Table != "retrykafka_idempotency" {
		t.Fatal(s.Table)
	}
	if s.TTL <= 0 {
		t.Fatal("ttl")
	}
}
