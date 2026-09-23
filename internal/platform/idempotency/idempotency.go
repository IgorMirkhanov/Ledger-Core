// Package idempotency stores idempotency keys in the SAME transaction as the business operation.
//
// Protocol (inside one DB transaction):
//
//	rec, err := store.Begin(ctx, tx, scope, key, hash)
//	switch {
//	case errors.Is(err, idempotency.ErrKeyReused): // same key, different request → client error
//	case err != nil:                              // infra error
//	case rec != nil:                              // replay: return rec.Response, do nothing else
//	default:                                      // first execution: do the work, then
//	    store.Complete(ctx, tx, scope, key, respBytes, "OK")
//	}
//
// Why this is safe under concurrency: a duplicate request's INSERT blocks on the unique index
// until the first transaction commits or rolls back (Postgres semantics). After commit the duplicate
// sees the completed record; after rollback its INSERT succeeds and it executes the operation itself.
// Therefore no "in_progress" state is needed. See docs/adr/0004-idempotency.md.
package idempotency

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/postgres"
)

var (
	ErrKeyReused  = errors.New("IDEMPOTENCY_KEY_REUSED")
	ErrKeyMissing = errors.New("IDEMPOTENCY_KEY_MISSING")
	ErrKeyInvalid = errors.New("IDEMPOTENCY_KEY_INVALID")
)

const MaxKeyLen = 128

// Record is a previously completed response.
type Record struct {
	Response     []byte
	ResponseCode string
}

type Store struct{}

func NewStore() *Store { return &Store{} }

// ValidateKey checks the client-supplied key format.
func ValidateKey(key string) error {
	if key == "" {
		return ErrKeyMissing
	}
	if len(key) > MaxKeyLen {
		return ErrKeyInvalid
	}
	return nil
}

// Begin registers the key. It returns (nil, nil) when the caller must execute the operation,
// or a non-nil Record when the operation was already completed and its response must be replayed.
func (s *Store) Begin(ctx context.Context, q postgres.Querier, scope, key string, requestHash []byte) (*Record, error) {
	var inserted int
	err := q.QueryRow(ctx, `
		INSERT INTO idempotency_keys (scope, key, request_hash)
		VALUES ($1, $2, $3)
		ON CONFLICT (scope, key) DO NOTHING
		RETURNING 1`, scope, key, requestHash).Scan(&inserted)
	if err == nil {
		return nil, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("idempotency: insert: %w", err)
	}

	var (
		storedHash []byte
		rec        Record
	)
	err = q.QueryRow(ctx, `
		SELECT request_hash, response, COALESCE(response_code, '')
		FROM idempotency_keys
		WHERE scope = $1 AND key = $2`, scope, key).Scan(&storedHash, &rec.Response, &rec.ResponseCode)
	if err != nil {
		return nil, fmt.Errorf("idempotency: select: %w", err)
	}
	if string(storedHash) != string(requestHash) {
		return nil, ErrKeyReused
	}
	return &rec, nil
}

// Complete stores the response for replays. Must be called in the same transaction as Begin.
func (s *Store) Complete(ctx context.Context, q postgres.Querier, scope, key string, response []byte, code string) error {
	tag, err := q.Exec(ctx, `
		UPDATE idempotency_keys SET response = $3, response_code = $4
		WHERE scope = $1 AND key = $2`, scope, key, response, code)
	if err != nil {
		return fmt.Errorf("idempotency: complete: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("idempotency: complete: key %s/%s not found", scope, key)
	}
	return nil
}

// DeleteExpired removes old keys; run periodically. Returns the number of deleted rows.
func (s *Store) DeleteExpired(ctx context.Context, q postgres.Querier, limit int) (int64, error) {
	tag, err := q.Exec(ctx, `
		DELETE FROM idempotency_keys
		WHERE ctid IN (SELECT ctid FROM idempotency_keys WHERE expires_at < now() LIMIT $1)`, limit)
	if err != nil {
		return 0, fmt.Errorf("idempotency: cleanup: %w", err)
	}
	return tag.RowsAffected(), nil
}

// HashRequest returns a stable hash of a protobuf request (deterministic marshaling).
func HashRequest(m proto.Message) ([]byte, error) {
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("idempotency: hash: %w", err)
	}
	sum := sha256.Sum256(b)
	return sum[:], nil
}
