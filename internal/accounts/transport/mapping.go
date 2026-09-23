package transport

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	accountsv1 "github.com/IgorMirkhanov/ledger-core/gen/ledger/accounts/v1"
	"github.com/IgorMirkhanov/ledger-core/internal/accounts/domain"
	"github.com/IgorMirkhanov/ledger-core/internal/accounts/service"
	"github.com/IgorMirkhanov/ledger-core/internal/money"
)

func parseUUID(raw, what string) (uuid.UUID, error) {
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, fmt.Errorf("%w: %s", domain.ErrValidation, what)
	}
	return id, nil
}

func parseMoney(m *accountsv1.Money) (money.Money, error) {
	if m == nil {
		return money.Money{}, fmt.Errorf("%w: amount", domain.ErrValidation)
	}
	amt, err := money.NewPositive(m.GetAmount(), m.GetCurrency())
	if err != nil {
		return money.Money{}, fmt.Errorf("%w: %s", domain.ErrValidation, err.Error())
	}
	return amt, nil
}

func parsePageToken(token string) (int64, error) {
	if token == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0, fmt.Errorf("%w: page_token", domain.ErrValidation)
	}
	id, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("%w: page_token", domain.ErrValidation)
	}
	return id, nil
}

func formatPageToken(postingID int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(postingID, 10)))
}

func pageSize(n int32) (int, error) {
	if n == 0 {
		return 50, nil
	}
	if n < 1 || n > 200 {
		return 0, fmt.Errorf("%w: page_size", domain.ErrValidation)
	}
	return int(n), nil
}

func protoTime(ts *timestamppb.Timestamp) *time.Time {
	if ts == nil {
		return nil
	}
	t := ts.AsTime()
	return &t
}

func accountPB(a *domain.Account) *accountsv1.Account {
	owner := ""
	if a.OwnerID != uuid.Nil {
		owner = a.OwnerID.String()
	}
	return &accountsv1.Account{
		Id:        a.ID.String(),
		OwnerId:   owner,
		Currency:  a.Currency.Code,
		Status:    statusPB(a.Status),
		Balance:   a.Balance,
		Held:      a.Held,
		Available: a.Available(),
		Version:   a.Version,
		CreatedAt: timestamppb.New(a.CreatedAt),
		UpdatedAt: timestamppb.New(a.UpdatedAt),
	}
}

func statusPB(s domain.AccountStatus) accountsv1.AccountStatus {
	switch string(s) {
	case "active":
		return accountsv1.AccountStatus_ACCOUNT_STATUS_ACTIVE
	case "frozen":
		return accountsv1.AccountStatus_ACCOUNT_STATUS_FROZEN
	case "closed":
		return accountsv1.AccountStatus_ACCOUNT_STATUS_CLOSED
	default:
		return accountsv1.AccountStatus_ACCOUNT_STATUS_UNSPECIFIED
	}
}

func holdPB(h *domain.Hold, cur money.Currency) *accountsv1.Hold {
	entry := ""
	if h.JournalEntryID != uuid.Nil {
		entry = h.JournalEntryID.String()
	}
	return &accountsv1.Hold{
		Id:             h.ID.String(),
		AccountId:      h.AccountID.String(),
		Amount:         &accountsv1.Money{Amount: h.Amount, Currency: cur.Code},
		Status:         holdStatusPB(h.Status),
		ReferenceId:    h.ReferenceID,
		ExpiresAt:      timestamppb.New(h.ExpiresAt),
		JournalEntryId: entry,
	}
}

func holdStatusPB(s domain.HoldStatus) accountsv1.HoldStatus {
	switch s {
	case domain.HoldActive:
		return accountsv1.HoldStatus_HOLD_STATUS_ACTIVE
	case domain.HoldCaptured:
		return accountsv1.HoldStatus_HOLD_STATUS_CAPTURED
	case domain.HoldReleased:
		return accountsv1.HoldStatus_HOLD_STATUS_RELEASED
	case domain.HoldExpired:
		return accountsv1.HoldStatus_HOLD_STATUS_EXPIRED
	default:
		return accountsv1.HoldStatus_HOLD_STATUS_UNSPECIFIED
	}
}

func linesPB(lines []service.StatementLine, limit int) ([]*accountsv1.StatementLine, string) {
	out := make([]*accountsv1.StatementLine, len(lines))
	for i, line := range lines {
		out[i] = &accountsv1.StatementLine{
			PostingId:    line.PostingID,
			EntryId:      line.EntryID.String(),
			Kind:         kindPB(line.Kind),
			Amount:       line.Amount,
			BalanceAfter: line.BalanceAfter,
			Description:  line.Description,
			CreatedAt:    timestamppb.New(line.CreatedAt),
		}
	}
	next := ""
	if limit > 0 && len(lines) == limit {
		next = formatPageToken(lines[len(lines)-1].PostingID)
	}
	return out, next
}

func kindPB(k domain.EntryKind) accountsv1.EntryKind {
	switch k {
	case domain.EntryDeposit:
		return accountsv1.EntryKind_ENTRY_KIND_DEPOSIT
	case domain.EntryWithdrawal:
		return accountsv1.EntryKind_ENTRY_KIND_WITHDRAWAL
	case domain.EntryTransfer:
		return accountsv1.EntryKind_ENTRY_KIND_TRANSFER
	case domain.EntryReversal:
		return accountsv1.EntryKind_ENTRY_KIND_REVERSAL
	default:
		return accountsv1.EntryKind_ENTRY_KIND_UNSPECIFIED
	}
}
