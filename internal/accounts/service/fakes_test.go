package service

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/IgorMirkhanov/ledger-core/internal/accounts/domain"
	"github.com/IgorMirkhanov/ledger-core/internal/money"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/idempotency"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/outbox"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/postgres"
)

type fixedClock time.Time

func (c fixedClock) Now() time.Time { return time.Time(c) }

type fakeTx struct{}

func (fakeTx) WithTx(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) error {
	return fn(ctx, nil)
}

type memIdem struct {
	mu        sync.Mutex
	hash      map[string][]byte
	response  map[string][]byte
	completes int
}

func newMemIdem() *memIdem {
	return &memIdem{hash: map[string][]byte{}, response: map[string][]byte{}}
}

func idemKey(scope, key string) string { return scope + "\x00" + key }

func (m *memIdem) Begin(_ context.Context, _ postgres.Querier, scope, key string, requestHash []byte) (*idempotency.Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := idemKey(scope, key)
	if raw, ok := m.response[k]; ok {
		if string(m.hash[k]) != string(requestHash) {
			return nil, idempotency.ErrKeyReused
		}
		return &idempotency.Record{Response: raw, ResponseCode: "OK"}, nil
	}
	m.hash[k] = append([]byte(nil), requestHash...)
	return nil, nil
}

func (m *memIdem) Complete(_ context.Context, _ postgres.Querier, scope, key string, response []byte, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.response[idemKey(scope, key)] = append([]byte(nil), response...)
	m.completes++
	return nil
}

type memOutbox struct {
	mu     sync.Mutex
	events []outbox.Event
}

func (m *memOutbox) Add(_ context.Context, _ postgres.Querier, e outbox.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, e)
	return nil
}

type fakeRepo struct {
	mu       sync.Mutex
	accounts map[uuid.UUID]*domain.Account
	byCode   map[string]uuid.UUID
	holds    map[uuid.UUID]*domain.Hold
	byRef    map[string]*domain.Hold
	entries  []*domain.JournalEntry
	inserts  int
	locks    int
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		accounts: map[uuid.UUID]*domain.Account{},
		byCode:   map[string]uuid.UUID{},
		holds:    map[uuid.UUID]*domain.Hold{},
		byRef:    map[string]*domain.Hold{},
	}
}

func (f *fakeRepo) putAccount(a *domain.Account) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.accounts[a.ID] = a
	if a.Code != "" {
		f.byCode[a.Code] = a.ID
	}
}

func (f *fakeRepo) putHold(h *domain.Hold) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.holds[h.ID] = h
	f.byRef[h.AccountID.String()+"/"+h.ReferenceID] = h
}

func (f *fakeRepo) CreateAccount(_ context.Context, _ postgres.Querier, a *domain.Account) error {
	f.putAccount(a)
	return nil
}

func (f *fakeRepo) GetAccount(_ context.Context, _ postgres.Querier, id uuid.UUID) (*domain.Account, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	a, ok := f.accounts[id]
	if !ok {
		return nil, domain.ErrAccountNotFound
	}
	return a, nil
}

func (f *fakeRepo) ListAccountsByOwner(_ context.Context, _ postgres.Querier, owner uuid.UUID) ([]*domain.Account, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*domain.Account
	for _, a := range f.accounts {
		if a.OwnerID == owner {
			out = append(out, a)
		}
	}
	return out, nil
}

func (f *fakeRepo) SystemAccountID(_ context.Context, _ postgres.Querier, prefix string, cur money.Currency) (uuid.UUID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id, ok := f.byCode[domain.SystemCode(prefix, cur)]
	if !ok {
		return uuid.Nil, domain.ErrAccountNotFound
	}
	return id, nil
}

func (f *fakeRepo) LockAccounts(_ context.Context, _ postgres.Querier, ids []uuid.UUID) (map[uuid.UUID]*domain.Account, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.locks++
	out := make(map[uuid.UUID]*domain.Account, len(ids))
	seen := map[uuid.UUID]struct{}{}
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		a, ok := f.accounts[id]
		if !ok {
			return nil, domain.ErrAccountNotFound
		}
		out[id] = a
	}
	return out, nil
}

func (f *fakeRepo) UpdateBalances(context.Context, postgres.Querier, ...*domain.Account) error {
	return nil
}

func (f *fakeRepo) InsertEntry(_ context.Context, _ postgres.Querier, e *domain.JournalEntry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e == nil || !e.IsApplied() {
		return domain.ErrEntryNotApplied
	}
	f.entries = append(f.entries, e)
	f.inserts++
	return nil
}

func (f *fakeRepo) CreateHold(_ context.Context, _ postgres.Querier, h *domain.Hold) error {
	f.putHold(h)
	return nil
}

func (f *fakeRepo) FindHold(_ context.Context, _ postgres.Querier, accountID uuid.UUID, referenceID string) (*domain.Hold, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	h, ok := f.byRef[accountID.String()+"/"+referenceID]
	if !ok {
		return nil, domain.ErrHoldNotFound
	}
	return h, nil
}

func (f *fakeRepo) LockHold(_ context.Context, _ postgres.Querier, id uuid.UUID) (*domain.Hold, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	h, ok := f.holds[id]
	if !ok {
		return nil, domain.ErrHoldNotFound
	}
	return h, nil
}

func (f *fakeRepo) UpdateHold(context.Context, postgres.Querier, *domain.Hold) error { return nil }

func (f *fakeRepo) LockExpiredHolds(context.Context, postgres.Querier, time.Time, int) ([]*domain.Hold, error) {
	return nil, nil
}

func (f *fakeRepo) Statement(context.Context, postgres.Querier, StatementFilter) ([]StatementLine, error) {
	return nil, nil
}
