package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/IgorMirkhanov/ledger-core/internal/accounts/domain"
	"github.com/IgorMirkhanov/ledger-core/internal/money"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/idempotency"
)

const (
	scopeCreateAccount = "accounts.CreateAccount"
	scopeDeposit       = "accounts.Deposit"
	scopeWithdraw      = "accounts.Withdraw"
	scopeCreateHold    = "accounts.CreateHold"
	scopeCaptureHold   = "accounts.CaptureHold"
	scopeReleaseHold   = "accounts.ReleaseHold"
)

// runIdempotent runs fn in one transaction with the idempotency key.
// A stored response is unmarshaled into dest and the business function is not called.
// A business error rolls the transaction back, so the key is not kept.
func (s *Service) runIdempotent(ctx context.Context, scope string, idem Idem, dest any, fn func(ctx context.Context, tx pgx.Tx) (any, error)) error {
	return s.tx.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		rec, err := s.idem.Begin(ctx, tx, scope, idem.Key, idem.RequestHash)
		if err != nil {
			return err
		}
		if rec != nil {
			// TODO(prompt-03): increment idempotency_replays_total.
			idempotency.MarkReplayed(ctx)
			if err := json.Unmarshal(rec.Response, dest); err != nil {
				return fmt.Errorf("accounts: replay %s: %w", scope, err)
			}
			return nil
		}
		result, err := fn(ctx, tx)
		if err != nil {
			return err
		}
		raw, err := json.Marshal(result)
		if err != nil {
			return fmt.Errorf("accounts: marshal %s: %w", scope, err)
		}
		if err := json.Unmarshal(raw, dest); err != nil {
			return fmt.Errorf("accounts: copy %s: %w", scope, err)
		}
		return s.idem.Complete(ctx, tx, scope, idem.Key, raw, "OK")
	})
}

func (s *Service) CreateAccount(ctx context.Context, cmd CreateAccountCmd) (*domain.Account, error) {
	if cmd.OwnerID == uuid.Nil || cmd.Currency.Code == "" {
		return nil, fmt.Errorf("%w: owner and currency are required", domain.ErrValidation)
	}
	var out domain.Account
	err := s.runIdempotent(ctx, scopeCreateAccount, cmd.Idem, &out, func(ctx context.Context, tx pgx.Tx) (any, error) {
		now := s.clock.Now()
		acc := domain.NewCustomerAccount(s.ids.New(), cmd.OwnerID, cmd.Currency, now)
		if err := s.repo.CreateAccount(ctx, tx, acc); err != nil {
			return nil, err
		}
		if err := s.addEvent(ctx, tx, "account.opened", "account", acc.ID.String(), accountOpened{
			AccountID: acc.ID.String(),
			OwnerID:   acc.OwnerID.String(),
			Currency:  acc.Currency.Code,
		}); err != nil {
			return nil, err
		}
		return acc, nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *Service) GetAccount(ctx context.Context, owner, id uuid.UUID) (*domain.Account, error) {
	var acc *domain.Account
	err := s.tx.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		acc, err = s.repo.GetAccount(ctx, tx, id)
		if err != nil {
			return err
		}
		return checkOwner(owner, acc)
	})
	if err != nil {
		return nil, err
	}
	return acc, nil
}

func (s *Service) ListAccounts(ctx context.Context, owner uuid.UUID) ([]*domain.Account, error) {
	var list []*domain.Account
	err := s.tx.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		list, err = s.repo.ListAccountsByOwner(ctx, tx, owner)
		return err
	})
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (s *Service) Deposit(ctx context.Context, cmd DepositCmd) (*EntryResult, error) {
	return s.postCash(ctx, scopeDeposit, domain.EntryDeposit, "deposit", "account.credited", 1, cmd)
}

func (s *Service) Withdraw(ctx context.Context, cmd WithdrawCmd) (*EntryResult, error) {
	return s.postCash(ctx, scopeWithdraw, domain.EntryWithdrawal, "withdrawal", "account.debited", -1, cmd)
}

func (s *Service) postCash(ctx context.Context, scope string, kind domain.EntryKind, refType, eventType string, sign int64, cmd DepositCmd) (*EntryResult, error) {
	if cmd.Amount.Amount() <= 0 {
		return nil, fmt.Errorf("%w: amount must be positive", domain.ErrValidation)
	}
	var out EntryResult
	err := s.runIdempotent(ctx, scope, cmd.Idem, &out, func(ctx context.Context, tx pgx.Tx) (any, error) {
		now := s.clock.Now()
		settleID, err := s.repo.SystemAccountID(ctx, tx, domain.SystemSettlement, cmd.Amount.Currency())
		if err != nil {
			return nil, err
		}
		locked, err := s.repo.LockAccounts(ctx, tx, []uuid.UUID{cmd.AccountID, settleID})
		if err != nil {
			return nil, err
		}
		acc := locked[cmd.AccountID]
		if err := checkOwner(cmd.OwnerID, acc); err != nil {
			return nil, err
		}
		if acc.Currency.Code != cmd.Amount.Currency().Code {
			return nil, domain.ErrCurrencyMismatch
		}
		settle := locked[settleID]
		amount := cmd.Amount.Amount()
		var customerAmt int64
		if sign > 0 {
			if err := acc.CanCredit(); err != nil {
				return nil, err
			}
			customerAmt = amount
		} else {
			if err := acc.CanDebit(amount); err != nil {
				return nil, err
			}
			customerAmt = -amount
		}
		entry := &domain.JournalEntry{
			ID:            s.ids.New(),
			Kind:          kind,
			ReferenceType: refType,
			ReferenceID:   cmd.Idem.Key,
			Description:   cmd.ExternalRef,
			CreatedAt:     now,
			Postings: []domain.Posting{
				{AccountID: settleID, Amount: -customerAmt, Currency: cmd.Amount.Currency()},
				{AccountID: acc.ID, Amount: customerAmt, Currency: cmd.Amount.Currency()},
			},
		}
		if err := entry.Apply(locked, now); err != nil {
			return nil, err
		}
		if err := s.repo.InsertEntry(ctx, tx, entry); err != nil {
			return nil, err
		}
		if err := s.repo.UpdateBalances(ctx, tx, acc, settle); err != nil {
			return nil, err
		}
		if err := s.addEvent(ctx, tx, eventType, "account", acc.ID.String(), accountMoney{
			AccountID:    acc.ID.String(),
			OwnerID:      acc.OwnerID.String(),
			EntryID:      entry.ID.String(),
			Amount:       amount,
			Currency:     acc.Currency.Code,
			BalanceAfter: acc.Balance,
			EntryKind:    string(kind),
		}); err != nil {
			return nil, err
		}
		return &EntryResult{EntryID: entry.ID, Account: acc}, nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *Service) CreateHold(ctx context.Context, cmd CreateHoldCmd) (*domain.Hold, error) {
	if cmd.ReferenceID == "" || cmd.Amount.Amount() <= 0 {
		return nil, fmt.Errorf("%w: hold reference and amount are required", domain.ErrValidation)
	}
	ttl := cmd.TTL
	if ttl == 0 {
		ttl = domain.DefaultHoldTTL
	}
	if ttl < 0 || ttl > domain.MaxHoldTTL {
		return nil, fmt.Errorf("%w: hold ttl out of range", domain.ErrValidation)
	}
	var out domain.Hold
	err := s.runIdempotent(ctx, scopeCreateHold, cmd.Idem, &out, func(ctx context.Context, tx pgx.Tx) (any, error) {
		now := s.clock.Now()
		locked, err := s.repo.LockAccounts(ctx, tx, []uuid.UUID{cmd.AccountID})
		if err != nil {
			return nil, err
		}
		acc := locked[cmd.AccountID]
		if err := checkOwner(cmd.OwnerID, acc); err != nil {
			return nil, err
		}
		if acc.Currency.Code != cmd.Amount.Currency().Code {
			return nil, domain.ErrCurrencyMismatch
		}
		existing, err := s.repo.FindHold(ctx, tx, acc.ID, cmd.ReferenceID)
		if err != nil && !errors.Is(err, domain.ErrHoldNotFound) {
			return nil, err
		}
		if existing != nil {
			if existing.Amount != cmd.Amount.Amount() {
				return nil, fmt.Errorf("%w: hold amount does not match the existing reference", domain.ErrValidation)
			}
			return existing, nil
		}
		if err := acc.Reserve(cmd.Amount.Amount(), now); err != nil {
			return nil, err
		}
		hold := &domain.Hold{
			ID:          s.ids.New(),
			AccountID:   acc.ID,
			Amount:      cmd.Amount.Amount(),
			Status:      domain.HoldActive,
			ReferenceID: cmd.ReferenceID,
			ExpiresAt:   now.Add(ttl),
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		if err := s.repo.CreateHold(ctx, tx, hold); err != nil {
			return nil, err
		}
		if err := s.repo.UpdateBalances(ctx, tx, acc); err != nil {
			return nil, err
		}
		if err := s.addEvent(ctx, tx, "hold.created", "hold", hold.ID.String(), holdCreated{
			HoldID:      hold.ID.String(),
			AccountID:   acc.ID.String(),
			OwnerID:     acc.OwnerID.String(),
			Amount:      hold.Amount,
			Currency:    acc.Currency.Code,
			ReferenceID: hold.ReferenceID,
			ExpiresAt:   hold.ExpiresAt,
		}); err != nil {
			return nil, err
		}
		return hold, nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *Service) CaptureHold(ctx context.Context, cmd CaptureHoldCmd) (*CaptureResult, error) {
	if cmd.DestAmount.Amount() <= 0 || cmd.ReferenceID == "" {
		return nil, fmt.Errorf("%w: dest amount and reference are required", domain.ErrValidation)
	}
	var out CaptureResult
	err := s.runIdempotent(ctx, scopeCaptureHold, cmd.Idem, &out, func(ctx context.Context, tx pgx.Tx) (any, error) {
		now := s.clock.Now()
		hold, err := s.repo.LockHold(ctx, tx, cmd.HoldID)
		if err != nil {
			return nil, err
		}
		if hold.Status != domain.HoldActive {
			return nil, domain.ErrHoldNotActive
		}
		if hold.IsExpired(now) {
			return nil, domain.ErrHoldExpired
		}
		dest, err := s.repo.GetAccount(ctx, tx, cmd.DestAccountID)
		if err != nil {
			return nil, err
		}
		if dest.Currency.Code != cmd.DestAmount.Currency().Code {
			return nil, domain.ErrCurrencyMismatch
		}
		ids := []uuid.UUID{hold.AccountID, dest.ID}
		var fxSrcID, fxDstID uuid.UUID
		srcPeek, err := s.repo.GetAccount(ctx, tx, hold.AccountID)
		if err != nil {
			return nil, err
		}
		if srcPeek.Currency.Code != dest.Currency.Code {
			fxSrcID, err = s.repo.SystemAccountID(ctx, tx, domain.SystemFX, srcPeek.Currency)
			if err != nil {
				return nil, err
			}
			fxDstID, err = s.repo.SystemAccountID(ctx, tx, domain.SystemFX, dest.Currency)
			if err != nil {
				return nil, err
			}
			ids = append(ids, fxSrcID, fxDstID)
		} else if cmd.DestAmount.Amount() != hold.Amount {
			return nil, fmt.Errorf("%w: dest amount must equal the hold", domain.ErrValidation)
		}
		locked, err := s.repo.LockAccounts(ctx, tx, ids)
		if err != nil {
			return nil, err
		}
		src := locked[hold.AccountID]
		dst := locked[dest.ID]
		if err := checkOwner(cmd.OwnerID, src); err != nil {
			return nil, err
		}
		if dst.Currency.Code != cmd.DestAmount.Currency().Code {
			return nil, domain.ErrCurrencyMismatch
		}
		if err := src.Unreserve(hold.Amount, now); err != nil {
			return nil, err
		}
		entry := &domain.JournalEntry{
			ID:            s.ids.New(),
			Kind:          domain.EntryTransfer,
			ReferenceType: "transfer",
			ReferenceID:   cmd.ReferenceID,
			Description:   cmd.Description,
			CreatedAt:     now,
			Postings: domain.TransferPostings(
				src.ID, dst.ID,
				money.New(hold.Amount, src.Currency),
				cmd.DestAmount,
				fxSrcID, fxDstID,
			),
		}
		if err := entry.Apply(locked, now); err != nil {
			return nil, err
		}
		// journal_entries must exist before holds.journal_entry_id points at it.
		if err := s.repo.InsertEntry(ctx, tx, entry); err != nil {
			return nil, err
		}
		if err := hold.Capture(entry.ID, now); err != nil {
			return nil, err
		}
		if err := s.repo.UpdateHold(ctx, tx, hold); err != nil {
			return nil, err
		}
		changed := make([]*domain.Account, 0, len(locked))
		for _, a := range locked {
			changed = append(changed, a)
		}
		if err := s.repo.UpdateBalances(ctx, tx, changed...); err != nil {
			return nil, err
		}
		if err := s.addEvent(ctx, tx, "hold.captured", "hold", hold.ID.String(), holdCaptured{
			HoldID:    hold.ID.String(),
			AccountID: src.ID.String(),
			EntryID:   entry.ID.String(),
		}); err != nil {
			return nil, err
		}
		if err := s.addEvent(ctx, tx, "account.debited", "account", src.ID.String(), accountMoney{
			AccountID:    src.ID.String(),
			OwnerID:      src.OwnerID.String(),
			EntryID:      entry.ID.String(),
			Amount:       hold.Amount,
			Currency:     src.Currency.Code,
			BalanceAfter: src.Balance,
			EntryKind:    string(domain.EntryTransfer),
		}); err != nil {
			return nil, err
		}
		if err := s.addEvent(ctx, tx, "account.credited", "account", dst.ID.String(), accountMoney{
			AccountID:    dst.ID.String(),
			OwnerID:      dst.OwnerID.String(),
			EntryID:      entry.ID.String(),
			Amount:       cmd.DestAmount.Amount(),
			Currency:     dst.Currency.Code,
			BalanceAfter: dst.Balance,
			EntryKind:    string(domain.EntryTransfer),
		}); err != nil {
			return nil, err
		}
		return &CaptureResult{Hold: hold, EntryID: entry.ID}, nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *Service) ReleaseHold(ctx context.Context, cmd ReleaseHoldCmd) (*domain.Hold, error) {
	var out domain.Hold
	err := s.runIdempotent(ctx, scopeReleaseHold, cmd.Idem, &out, func(ctx context.Context, tx pgx.Tx) (any, error) {
		now := s.clock.Now()
		hold, err := s.repo.LockHold(ctx, tx, cmd.HoldID)
		if err != nil {
			return nil, err
		}
		if hold.Status == domain.HoldReleased || hold.Status == domain.HoldExpired {
			return hold, nil
		}
		if hold.Status != domain.HoldActive {
			return nil, domain.ErrHoldNotActive
		}
		locked, err := s.repo.LockAccounts(ctx, tx, []uuid.UUID{hold.AccountID})
		if err != nil {
			return nil, err
		}
		acc := locked[hold.AccountID]
		if err := checkOwner(cmd.OwnerID, acc); err != nil {
			return nil, err
		}
		if err := acc.Unreserve(hold.Amount, now); err != nil {
			return nil, err
		}
		if _, err := hold.Release(now); err != nil {
			return nil, err
		}
		if err := s.repo.UpdateHold(ctx, tx, hold); err != nil {
			return nil, err
		}
		if err := s.repo.UpdateBalances(ctx, tx, acc); err != nil {
			return nil, err
		}
		if err := s.addEvent(ctx, tx, "hold.released", "hold", hold.ID.String(), holdReleased{
			HoldID:    hold.ID.String(),
			AccountID: acc.ID.String(),
			Reason:    "released",
		}); err != nil {
			return nil, err
		}
		return hold, nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *Service) Statement(ctx context.Context, owner uuid.UUID, f StatementFilter) ([]StatementLine, error) {
	var lines []StatementLine
	err := s.tx.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		acc, err := s.repo.GetAccount(ctx, tx, f.AccountID)
		if err != nil {
			return err
		}
		if err := checkOwner(owner, acc); err != nil {
			return err
		}
		lines, err = s.repo.Statement(ctx, tx, f)
		return err
	})
	if err != nil {
		return nil, err
	}
	return lines, nil
}

// ExpireHolds releases up to batch expired holds. Account locks are taken in one
// ordered LockAccounts call so two batches cannot deadlock on account order.
func (s *Service) ExpireHolds(ctx context.Context, batch int) (int, error) {
	if batch <= 0 {
		return 0, nil
	}
	var n int
	err := s.tx.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		now := s.clock.Now()
		holds, err := s.repo.LockExpiredHolds(ctx, tx, now, batch)
		if err != nil {
			return err
		}
		if len(holds) == 0 {
			return nil
		}
		ids := make([]uuid.UUID, len(holds))
		for i, h := range holds {
			ids[i] = h.AccountID
		}
		locked, err := s.repo.LockAccounts(ctx, tx, ids)
		if err != nil {
			return err
		}
		for _, h := range holds {
			acc := locked[h.AccountID]
			if err := acc.Unreserve(h.Amount, now); err != nil {
				return err
			}
			if err := h.Expire(now); err != nil {
				return err
			}
			if err := s.repo.UpdateHold(ctx, tx, h); err != nil {
				return err
			}
			if err := s.addEvent(ctx, tx, "hold.released", "hold", h.ID.String(), holdReleased{
				HoldID:    h.ID.String(),
				AccountID: acc.ID.String(),
				Reason:    "expired",
			}); err != nil {
				return err
			}
		}
		changed := make([]*domain.Account, 0, len(locked))
		for _, a := range locked {
			changed = append(changed, a)
		}
		if err := s.repo.UpdateBalances(ctx, tx, changed...); err != nil {
			return err
		}
		n = len(holds)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

func checkOwner(owner uuid.UUID, acc *domain.Account) error {
	if owner == uuid.Nil {
		return nil
	}
	return acc.CheckOwner(owner)
}
