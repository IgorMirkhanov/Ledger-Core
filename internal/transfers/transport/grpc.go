// Package transport maps gRPC requests of TransfersService to the saga service.
// Spec: docs/cursor/prompts/07-transfers-grpc-e2e.md.
package transport

import (
	"google.golang.org/grpc/codes"

	transfersv1 "github.com/IgorMirkhanov/ledger-core/gen/ledger/transfers/v1"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/grpcx"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/idempotency"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/domain"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/service"
)

const ErrorDomain = "ledger.transfers"

var Errors = grpcx.ErrorMapping{
	domain.ErrNotFound:        codes.NotFound,
	domain.ErrSameAccount:     codes.InvalidArgument,
	domain.ErrValidation:      codes.InvalidArgument,
	domain.ErrFXRateNotFound:  codes.FailedPrecondition,
	idempotency.ErrKeyReused:  codes.InvalidArgument,
	idempotency.ErrKeyMissing: codes.InvalidArgument,
	idempotency.ErrKeyInvalid: codes.InvalidArgument,
	service.ErrNotImplemented: codes.Unimplemented,
}

type Handler struct {
	transfersv1.UnimplementedTransfersServiceServer
	svc *service.Service
}

func NewHandler(svc *service.Service) *Handler { return &Handler{svc: svc} }
