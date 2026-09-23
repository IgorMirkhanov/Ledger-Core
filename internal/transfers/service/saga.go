package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/IgorMirkhanov/ledger-core/internal/money"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/idempotency"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/outbox"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/domain"
)

const (
	scopeCreateTransfer = "transfers.CreateTransfer"
	advanceReserve      = 500 * time.Millisecond
	claimBatch          = 50
)

type storedTransfer struct {
	ID uuid.UUID `json:"id"`
}

// CreateTransfer persists the transfer idempotently, then drives Advance until it is terminal,
// the context is within 500ms of its deadline, or a step hits a transient error.
// cmd.IdemKey is the raw client key; the stored key is namespaced by owner.
// Description is not sent to accounts: it is not persisted, and a retry must hash the same capture request.
func (s *Service) CreateTransfer(ctx context.Context, cmd CreateTransferCmd) (*domain.Transfer, error) {
	if err := idempotency.ValidateKey(cmd.IdemKey); err != nil {
		return nil, err
	}
	src, err := s.accounts.GetAccountInfo(ctx, cmd.OwnerID, cmd.SourceID)
	if err != nil {
		return nil, err
	}
	dst, err := s.accounts.GetAccountInfo(ctx, uuid.Nil, cmd.DestID)
	if err != nil {
		return nil, err
	}
	if src.Currency.Code != cmd.Amount.Currency().Code {
		return nil, fmt.Errorf("%w: account currency does not match the request", domain.ErrValidation)
	}
	destCur := dst.Currency
	if cmd.DestCurrency.Code != "" {
		if cmd.DestCurrency.Code != dst.Currency.Code {
			return nil, fmt.Errorf("%w: dest currency does not match the account", domain.ErrValidation)
		}
		destCur = cmd.DestCurrency
	}

	var created *domain.Transfer
	err = s.tx.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		key := idempotency.Namespace(cmd.OwnerID.String(), cmd.IdemKey)
		rec, err := s.idem.Begin(ctx, tx, scopeCreateTransfer, key, cmd.RequestHash)
		if err != nil {
			return err
		}
		if rec != nil {
			idempotency.MarkReplayed(ctx)
			var stored storedTransfer
			if err := json.Unmarshal(rec.Response, &stored); err != nil {
				return fmt.Errorf("transfers: replay: %w", err)
			}
			created, err = s.repo.Get(ctx, tx, stored.ID)
			return err
		}

		now := s.clock.Now()
		rate, err := s.rateFor(ctx, tx, src.Currency, dst.Currency)
		if err != nil {
			return err
		}
		tr, err := domain.NewTransfer(uuid.Must(uuid.NewV7()), cmd.OwnerID, cmd.SourceID, cmd.DestID, cmd.Amount, destCur, rate, now)
		if err != nil {
			return err
		}
		if err := s.repo.Create(ctx, tx, tr); err != nil {
			return err
		}
		if err := s.addEvent(ctx, tx, "transfer.created", tr.ID.String(), transferCreated{
			TransferID:      tr.ID.String(),
			OwnerID:         tr.OwnerID.String(),
			SourceAccountID: tr.SourceAccountID.String(),
			DestAccountID:   tr.DestAccountID.String(),
			Amount:          tr.Amount.Amount(),
			Currency:        tr.Amount.Currency().Code,
			DestAmount:      tr.DestAmount.Amount(),
			DestCurrency:    tr.DestAmount.Currency().Code,
		}); err != nil {
			return err
		}
		raw, err := json.Marshal(storedTransfer{ID: tr.ID})
		if err != nil {
			return fmt.Errorf("transfers: marshal id: %w", err)
		}
		if err := s.idem.Complete(ctx, tx, scopeCreateTransfer, key, raw, "OK"); err != nil {
			return err
		}
		created = tr
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s.drive(ctx, created)
}

// Advance executes saga steps for one transfer until it is terminal, a step is transient,
// or another worker already moved the row.
func (s *Service) Advance(ctx context.Context, id uuid.UUID) (*domain.Transfer, error) {
	var current *domain.Transfer
	err := s.tx.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		current, err = s.repo.Get(ctx, tx, id)
		return err
	})
	if err != nil {
		return nil, err
	}
	return s.drive(ctx, current)
}

// ClaimPending leases a batch of due transfers and returns their ids. The transaction ends
// before Advance, so the row lock is not held during the accounts call. The lease keeps
// other workers from claiming the same ids until it expires.
func (s *Service) ClaimPending(ctx context.Context, limit int) ([]uuid.UUID, error) {
	if limit <= 0 {
		limit = claimBatch
	}
	now := s.clock.Now()
	var ids []uuid.UUID
	err := s.tx.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		ids, err = s.repo.ClaimPending(ctx, tx, now, now.Add(StepLease), limit)
		return err
	})
	return ids, err
}

func (s *Service) GetTransfer(ctx context.Context, owner, id uuid.UUID) (*domain.Transfer, error) {
	var tr *domain.Transfer
	err := s.tx.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		tr, err = s.repo.Get(ctx, tx, id)
		return err
	})
	if err != nil {
		return nil, err
	}
	if tr.OwnerID != owner {
		return nil, domain.ErrNotFound
	}
	return tr, nil
}

func (s *Service) ListTransfers(ctx context.Context, owner uuid.UUID, cursor *ListCursor, limit int) ([]*domain.Transfer, *ListCursor, error) {
	var list []*domain.Transfer
	err := s.tx.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		list, err = s.repo.ListByOwner(ctx, tx, owner, cursor, limit)
		return err
	})
	if err != nil {
		return nil, nil, err
	}
	if limit > 0 && len(list) == limit {
		last := list[len(list)-1]
		return list, &ListCursor{CreatedAt: last.CreatedAt, ID: last.ID}, nil
	}
	return list, nil, nil
}

func (s *Service) drive(ctx context.Context, tr *domain.Transfer) (*domain.Transfer, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if deadlineNear(ctx) {
			return tr, nil
		}
		next, stop, err := s.advanceOnce(ctx, tr.ID)
		if err != nil {
			return nil, err
		}
		tr = next
		if stop || tr.Status.IsTerminal() {
			return tr, nil
		}
	}
}

func deadlineNear(ctx context.Context) bool {
	dl, ok := ctx.Deadline()
	return ok && time.Until(dl) < advanceReserve
}

// advanceOnce runs one saga step. stop is set when the caller must not immediately continue:
// terminal status, transient error, release bug waiting for backoff, or a concurrent Advance.
func (s *Service) advanceOnce(ctx context.Context, id uuid.UUID) (*domain.Transfer, bool, error) {
	var snapshot *domain.Transfer
	err := s.tx.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		snapshot, err = s.repo.Lock(ctx, tx, id)
		if err != nil {
			return err
		}
		if snapshot.Status.IsTerminal() || snapshot.NextStep() == domain.StepNone {
			return nil
		}
		return s.repo.Lease(ctx, tx, id, s.clock.Now().Add(StepLease))
	})
	if err != nil {
		return nil, false, err
	}
	if snapshot.Status.IsTerminal() || snapshot.NextStep() == domain.StepNone {
		return snapshot, true, nil
	}

	started := time.Now()
	step := snapshot.NextStep()
	holdID, entryID, callErr := s.callAccounts(ctx, snapshot, step)
	duration := time.Since(started)

	var updated *domain.Transfer
	var stop bool
	var appliedTerminal bool
	err = s.tx.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		current, err := s.repo.Lock(ctx, tx, id)
		if err != nil {
			return err
		}
		if current.Version != snapshot.Version {
			updated = current
			stop = true
			return nil
		}
		outcome, code, detail, terminal, retry, err := s.apply(current, step, holdID, entryID, callErr)
		if err != nil {
			return err
		}
		if err := s.repo.Update(ctx, tx, current); err != nil {
			if errors.Is(err, ErrConcurrentUpdate) {
				updated, err = s.repo.Get(ctx, tx, id)
				stop = true
				return err
			}
			return err
		}
		if err := s.repo.AppendStep(ctx, tx, StepLog{
			TransferID: id,
			Step:       step,
			Outcome:    outcome,
			ErrorCode:  code,
			Error:      detail,
			Duration:   duration,
		}); err != nil {
			return err
		}
		if err := s.emitStatus(ctx, tx, snapshot.Status, current); err != nil {
			return err
		}
		updated = current
		stop = terminal || retry
		appliedTerminal = terminal && current.Status.IsTerminal()
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if appliedTerminal {
		recordTerminal(string(updated.Status), s.clock.Now().Sub(updated.CreatedAt).Seconds())
	}
	return updated, stop, nil
}

func (s *Service) callAccounts(ctx context.Context, t *domain.Transfer, step domain.Step) (uuid.UUID, uuid.UUID, error) {
	key := t.IdempotencyKey(step)
	switch step {
	case domain.StepHold:
		id, err := s.accounts.CreateHold(ctx, key, t.SourceAccountID, t.Amount, t.ID.String(), HoldTTL)
		return id, uuid.Nil, err
	case domain.StepCapture:
		id, err := s.accounts.CaptureHold(ctx, key, t.HoldID, t.DestAccountID, t.DestAmount, t.ID.String(), "")
		return uuid.Nil, id, err
	case domain.StepRelease:
		err := s.accounts.ReleaseHold(ctx, key, t.HoldID, t.FailureCode)
		return uuid.Nil, uuid.Nil, err
	default:
		return uuid.Nil, uuid.Nil, fmt.Errorf("%w: step %q", domain.ErrInvalidTransition, step)
	}
}

// apply mutates t. retry means the step must wait for backoff instead of looping immediately.
func (s *Service) apply(t *domain.Transfer, step domain.Step, holdID, entryID uuid.UUID, callErr error) (outcome, code, detail string, terminal, retry bool, err error) {
	now := s.clock.Now()
	if callErr == nil {
		switch step {
		case domain.StepHold:
			err = t.OnHoldCreated(holdID, now)
		case domain.StepCapture:
			err = t.OnCaptured(entryID, now)
		case domain.StepRelease:
			err = t.OnReleased(now)
		default:
			err = fmt.Errorf("%w: step %q", domain.ErrInvalidTransition, step)
		}
		return "ok", "", "", t.Status.IsTerminal(), false, err
	}

	var biz *BusinessError
	if errors.As(callErr, &biz) {
		switch step {
		case domain.StepHold:
			err = t.OnHoldRejected(biz.Code, biz.Message, now)
		case domain.StepCapture:
			err = t.OnCaptureRejected(biz.Code, biz.Message, now)
		case domain.StepRelease:
			recordAnomaly(t.ID.String(), biz.Code, biz.Message)
			err = t.OnReleaseBug(biz.Message, now, Backoff)
			retry = err == nil && !t.Status.IsTerminal()
		default:
			err = fmt.Errorf("%w: step %q", domain.ErrInvalidTransition, step)
		}
		return "business_error", biz.Code, biz.Message, t.Status.IsTerminal(), retry, err
	}

	t.OnTransientError(now, Backoff)
	return "transient_error", "", callErr.Error(), false, true, nil
}

func (s *Service) emitStatus(ctx context.Context, tx pgx.Tx, before domain.Status, t *domain.Transfer) error {
	if t.Status == before {
		return nil
	}
	switch t.Status {
	case domain.StatusCompleted:
		return s.addEvent(ctx, tx, "transfer.completed", t.ID.String(), transferCompleted{
			TransferID:     t.ID.String(),
			OwnerID:        t.OwnerID.String(),
			JournalEntryID: t.JournalEntryID.String(),
			Amount:         t.Amount.Amount(),
			Currency:       t.Amount.Currency().Code,
			DestAmount:     t.DestAmount.Amount(),
			DestCurrency:   t.DestAmount.Currency().Code,
		})
	case domain.StatusFailed:
		return s.addEvent(ctx, tx, "transfer.failed", t.ID.String(), transferFailed{
			TransferID:    t.ID.String(),
			OwnerID:       t.OwnerID.String(),
			FailureCode:   t.FailureCode,
			FailureReason: t.FailureReason,
		})
	default:
		return nil
	}
}

func (s *Service) rateFor(ctx context.Context, tx pgx.Tx, base, quote money.Currency) (*big.Rat, error) {
	if base.Code == quote.Code {
		return nil, nil
	}
	return s.repo.GetRate(ctx, tx, base, quote)
}

func (s *Service) addEvent(ctx context.Context, tx pgx.Tx, eventType, aggregateID string, payload any) error {
	return s.outbox.Add(ctx, tx, outbox.Event{
		Topic:         TopicTransferEvents,
		AggregateType: "transfer",
		AggregateID:   aggregateID,
		EventType:     eventType,
		Payload:       payload,
	})
}
