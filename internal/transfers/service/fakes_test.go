package service

import (
	"bytes"
	"context"
	"math/big"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/IgorMirkhanov/ledger-core/internal/money"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/idempotency"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/outbox"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/postgres"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/domain"
)

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

type memTx struct {
	mu   sync.Mutex
	repo *memRepo
	idem *memIdem
	ob   *memOutbox
}

func (m *memTx) WithTx(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	repoSnap := m.repo.snapshot()
	idemSnap := m.idem.snapshot()
	obSnap := m.ob.snapshot()
	err := fn(ctx, nil)
	if err != nil {
		m.repo.restore(repoSnap)
		m.idem.restore(idemSnap)
		m.ob.restore(obSnap)
	}
	return err
}

type memRepo struct {
	transfers map[uuid.UUID]*domain.Transfer
	steps     []StepLog
	rate      *big.Rat
}

func newMemRepo() *memRepo {
	return &memRepo{transfers: map[uuid.UUID]*domain.Transfer{}}
}

func (r *memRepo) snapshot() map[uuid.UUID]*domain.Transfer {
	out := make(map[uuid.UUID]*domain.Transfer, len(r.transfers))
	for id, tr := range r.transfers {
		out[id] = cloneTransfer(tr)
	}
	return out
}

func (r *memRepo) restore(snap map[uuid.UUID]*domain.Transfer) {
	r.transfers = snap
}

func (r *memRepo) Create(_ context.Context, _ postgres.Querier, t *domain.Transfer) error {
	if _, ok := r.transfers[t.ID]; ok {
		return domain.ErrValidation
	}
	r.transfers[t.ID] = cloneTransfer(t)
	return nil
}

func (r *memRepo) Get(_ context.Context, _ postgres.Querier, id uuid.UUID) (*domain.Transfer, error) {
	tr, ok := r.transfers[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return cloneTransfer(tr), nil
}

func (r *memRepo) Lock(ctx context.Context, q postgres.Querier, id uuid.UUID) (*domain.Transfer, error) {
	return r.Get(ctx, q, id)
}

func (r *memRepo) Update(_ context.Context, _ postgres.Querier, t *domain.Transfer) error {
	cur, ok := r.transfers[t.ID]
	if !ok || cur.Version != t.Version-1 {
		return ErrConcurrentUpdate
	}
	r.transfers[t.ID] = cloneTransfer(t)
	return nil
}

func (r *memRepo) ListByOwner(context.Context, postgres.Querier, uuid.UUID, *ListCursor, int) ([]*domain.Transfer, error) {
	return nil, nil
}

func (r *memRepo) ClaimPending(context.Context, postgres.Querier, time.Time, int) ([]uuid.UUID, error) {
	return nil, nil
}

func (r *memRepo) AppendStep(_ context.Context, _ postgres.Querier, s StepLog) error {
	r.steps = append(r.steps, s)
	return nil
}

func (r *memRepo) GetRate(context.Context, postgres.Querier, money.Currency, money.Currency) (*big.Rat, error) {
	if r.rate == nil {
		return nil, domain.ErrFXRateNotFound
	}
	return new(big.Rat).Set(r.rate), nil
}

type memRec struct {
	hash     []byte
	response []byte
	code     string
}

type memIdem struct {
	keys        map[string]memRec
	pendingID   string
	pendingHash []byte
}

func newMemIdem() *memIdem {
	return &memIdem{keys: map[string]memRec{}}
}

func (m *memIdem) snapshot() *memIdem {
	keys := make(map[string]memRec, len(m.keys))
	for k, v := range m.keys {
		keys[k] = v
	}
	return &memIdem{keys: keys, pendingID: m.pendingID, pendingHash: append([]byte(nil), m.pendingHash...)}
}

func (m *memIdem) restore(snap *memIdem) { *m = *snap }

func (m *memIdem) Begin(_ context.Context, _ postgres.Querier, scope, key string, requestHash []byte) (*idempotency.Record, error) {
	id := scope + "\x00" + key
	if rec, ok := m.keys[id]; ok {
		if !bytes.Equal(rec.hash, requestHash) {
			return nil, idempotency.ErrKeyReused
		}
		return &idempotency.Record{Response: rec.response, ResponseCode: rec.code}, nil
	}
	m.pendingID = id
	m.pendingHash = append([]byte(nil), requestHash...)
	return nil, nil
}

func (m *memIdem) Complete(_ context.Context, _ postgres.Querier, _, _ string, response []byte, code string) error {
	m.keys[m.pendingID] = memRec{hash: m.pendingHash, response: append([]byte(nil), response...), code: code}
	return nil
}

type memOutbox struct {
	events []outbox.Event
}

func (m *memOutbox) snapshot() []outbox.Event {
	return append([]outbox.Event(nil), m.events...)
}

func (m *memOutbox) restore(events []outbox.Event) { m.events = events }

func (m *memOutbox) Add(_ context.Context, _ postgres.Querier, e outbox.Event) error {
	m.events = append(m.events, e)
	return nil
}

func cloneTransfer(t *domain.Transfer) *domain.Transfer {
	c := *t
	if t.FXRate != nil {
		c.FXRate = new(big.Rat).Set(t.FXRate)
	}
	if t.NextAttemptAt != nil {
		tm := *t.NextAttemptAt
		c.NextAttemptAt = &tm
	}
	if t.CompletedAt != nil {
		tm := *t.CompletedAt
		c.CompletedAt = &tm
	}
	return &c
}
