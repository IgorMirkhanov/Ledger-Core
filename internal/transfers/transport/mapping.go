package transport

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	transfersv1 "github.com/IgorMirkhanov/ledger-core/gen/ledger/transfers/v1"
	"github.com/IgorMirkhanov/ledger-core/internal/money"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/domain"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/service"
)

func parseUUID(raw, what string) (uuid.UUID, error) {
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, fmt.Errorf("%w: %s", domain.ErrValidation, what)
	}
	return id, nil
}

func parseCurrency(code, what string) (money.Currency, error) {
	cur, err := money.ParseCurrency(code)
	if err != nil {
		return money.Currency{}, fmt.Errorf("%w: %s", domain.ErrValidation, what)
	}
	return cur, nil
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

type listToken struct {
	T  time.Time `json:"t"`
	ID string    `json:"id"`
}

func parsePageToken(token string) (*service.ListCursor, error) {
	if token == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, fmt.Errorf("%w: page_token", domain.ErrValidation)
	}
	var tok listToken
	if err := json.Unmarshal(raw, &tok); err != nil {
		return nil, fmt.Errorf("%w: page_token", domain.ErrValidation)
	}
	id, err := uuid.Parse(tok.ID)
	if err != nil {
		return nil, fmt.Errorf("%w: page_token", domain.ErrValidation)
	}
	return &service.ListCursor{CreatedAt: tok.T, ID: id}, nil
}

func formatPageToken(c service.ListCursor) (string, error) {
	raw, err := json.Marshal(listToken{T: c.CreatedAt, ID: c.ID.String()})
	if err != nil {
		return "", fmt.Errorf("transfers: page token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func transferPB(t *domain.Transfer) *transfersv1.Transfer {
	pb := &transfersv1.Transfer{
		Id:              t.ID.String(),
		OwnerId:         t.OwnerID.String(),
		SourceAccountId: t.SourceAccountID.String(),
		DestAccountId:   t.DestAccountID.String(),
		Amount:          t.Amount.Amount(),
		Currency:        t.Amount.Currency().Code,
		DestAmount:      t.DestAmount.Amount(),
		DestCurrency:    t.DestAmount.Currency().Code,
		Status:          statusPB(t.Status),
		FailureCode:     t.FailureCode,
		FailureReason:   t.FailureReason,
		CreatedAt:       timestamppb.New(t.CreatedAt),
		UpdatedAt:       timestamppb.New(t.UpdatedAt),
	}
	if t.FXRate != nil {
		pb.FxRate = t.FXRate.FloatString(10)
	}
	if t.CompletedAt != nil {
		pb.CompletedAt = timestamppb.New(*t.CompletedAt)
	}
	return pb
}

func statusPB(s domain.Status) transfersv1.TransferStatus {
	switch s {
	case domain.StatusCreated:
		return transfersv1.TransferStatus_TRANSFER_STATUS_CREATED
	case domain.StatusFundsHeld:
		return transfersv1.TransferStatus_TRANSFER_STATUS_FUNDS_HELD
	case domain.StatusCompleted:
		return transfersv1.TransferStatus_TRANSFER_STATUS_COMPLETED
	case domain.StatusCompensating:
		return transfersv1.TransferStatus_TRANSFER_STATUS_COMPENSATING
	case domain.StatusFailed:
		return transfersv1.TransferStatus_TRANSFER_STATUS_FAILED
	default:
		return transfersv1.TransferStatus_TRANSFER_STATUS_UNSPECIFIED
	}
}
