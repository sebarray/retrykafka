package postgres

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/retrykafka/retrykafka/idempotency"
)

// Store implements idempotency.Store with INSERT ... ON CONFLICT and a lease TTL.
type Store struct {
	DB    *sql.DB
	Table string
	TTL   time.Duration
	Now   func() time.Time
}

func New(db *sql.DB, table string, ttl time.Duration) *Store {
	if table == "" {
		table = "retrykafka_idempotency"
	}
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}
	return &Store{DB: db, Table: table, TTL: ttl, Now: time.Now}
}

func (s *Store) EnsureSchema(ctx context.Context) error {
	q := `CREATE TABLE IF NOT EXISTS ` + s.Table + ` (
		key TEXT PRIMARY KEY,
		state TEXT NOT NULL,
		lease_until TIMESTAMPTZ,
		updated_at TIMESTAMPTZ NOT NULL
	)`
	_, err := s.DB.ExecContext(ctx, q)
	return err
}

func (s *Store) TryStart(ctx context.Context, key string) (idempotency.Result, error) {
	now := s.Now()
	lease := now.Add(s.TTL)
	q := `INSERT INTO ` + s.Table + ` (key, state, lease_until, updated_at)
VALUES ($1, $2, $3, $3)
ON CONFLICT (key) DO UPDATE
SET state = EXCLUDED.state,
    lease_until = EXCLUDED.lease_until,
    updated_at = EXCLUDED.updated_at
WHERE ` + s.Table + `.state = $4
   OR (` + s.Table + `.state = $2 AND ` + s.Table + `.lease_until < $5)
RETURNING state`
	row := s.DB.QueryRowContext(ctx, q, key, idempotency.StateProcessing, lease, idempotency.StateFailed, now)
	var state string
	err := row.Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		var existing string
		err = s.DB.QueryRowContext(ctx, `SELECT state FROM `+s.Table+` WHERE key=$1`, key).Scan(&existing)
		if err != nil {
			return 0, err
		}
		if existing == idempotency.StateCompleted {
			return idempotency.ResultSkipCompleted, nil
		}
		return idempotency.ResultSkipInProgress, nil
	}
	if err != nil {
		return 0, err
	}
	return idempotency.ResultProceed, nil
}

func (s *Store) Complete(ctx context.Context, key string) error {
	_, err := s.DB.ExecContext(ctx,
		`UPDATE `+s.Table+` SET state=$2, lease_until=NULL, updated_at=$3 WHERE key=$1`,
		key, idempotency.StateCompleted, s.Now())
	return err
}

func (s *Store) Fail(ctx context.Context, key string) error {
	_, err := s.DB.ExecContext(ctx,
		`UPDATE `+s.Table+` SET state=$2, lease_until=NULL, updated_at=$3 WHERE key=$1`,
		key, idempotency.StateFailed, s.Now())
	return err
}
