// Package transport maps gRPC requests to service commands and domain errors to gRPC statuses.
// No business logic here. Spec: docs/cursor/prompts/05-accounts-service-grpc.md.
package transport

import (
	"google.golang.org/grpc/codes"

	accountsv1 "github.com/IgorMirkhanov/ledger-core/gen/ledger/accounts/v1"
	"github.com/IgorMirkhanov/ledger-core/internal/accounts/domain"
	"github.com/IgorMirkhanov/ledger-core/internal/accounts/service"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/grpcx"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/idempotency"
)

const ErrorDomain = "ledger.accounts"

// Errors maps domain errors to gRPC codes. The ErrorInfo.reason is the sentinel text.
var Errors = grpcx.ErrorMapping{
	domain.ErrAccountNotFound:   codes.NotFound,
	domain.ErrHoldNotFound:      codes.NotFound,
	domain.ErrInsufficientFunds: codes.FailedPrecondition,
	domain.ErrAccountNotActive:  codes.FailedPrecondition,
	domain.ErrHoldNotActive:     codes.FailedPrecondition,
	domain.ErrHoldExpired:       codes.FailedPrecondition,
	domain.ErrCurrencyMismatch:  codes.FailedPrecondition,
	domain.ErrNotAccountOwner:   codes.PermissionDenied,
	domain.ErrValidation:        codes.InvalidArgument,
	idempotency.ErrKeyReused:    codes.InvalidArgument,
	idempotency.ErrKeyMissing:   codes.InvalidArgument,
	idempotency.ErrKeyInvalid:   codes.InvalidArgument,
	service.ErrNotImplemented:   codes.Unimplemented,
}

// Handler implements accountsv1.AccountsServiceServer.
// Unimplemented methods fall through to UnimplementedAccountsServiceServer.
type Handler struct {
	accountsv1.UnimplementedAccountsServiceServer
	svc *service.Service
}

func NewHandler(svc *service.Service) *Handler { return &Handler{svc: svc} }
