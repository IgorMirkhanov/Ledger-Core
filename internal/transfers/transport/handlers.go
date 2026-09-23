package transport

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	transfersv1 "github.com/IgorMirkhanov/ledger-core/gen/ledger/transfers/v1"
	"github.com/IgorMirkhanov/ledger-core/internal/money"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/grpcx"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/idempotency"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/domain"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/service"
)

func (h *Handler) CreateTransfer(ctx context.Context, req *transfersv1.CreateTransferRequest) (*transfersv1.CreateTransferResponse, error) {
	cmd, err := h.createCmd(ctx, req)
	if err != nil {
		return nil, statusErr(err)
	}
	ctx, replayed := idempotency.WithReplayTracker(ctx)
	tr, err := h.svc.CreateTransfer(ctx, cmd)
	if err != nil {
		return nil, statusErr(err)
	}
	if err := replayHeader(ctx, replayed); err != nil {
		return nil, statusErr(err)
	}
	return &transfersv1.CreateTransferResponse{Transfer: transferPB(tr)}, nil
}

func (h *Handler) GetTransfer(ctx context.Context, req *transfersv1.GetTransferRequest) (*transfersv1.GetTransferResponse, error) {
	owner, err := grpcx.OwnerID(ctx)
	if err != nil {
		return nil, statusErr(err)
	}
	id, err := parseUUID(req.GetTransferId(), "transfer_id")
	if err != nil {
		return nil, statusErr(err)
	}
	tr, err := h.svc.GetTransfer(ctx, owner, id)
	if err != nil {
		return nil, statusErr(err)
	}
	return &transfersv1.GetTransferResponse{Transfer: transferPB(tr)}, nil
}

func (h *Handler) ListTransfers(ctx context.Context, req *transfersv1.ListTransfersRequest) (*transfersv1.ListTransfersResponse, error) {
	owner, err := grpcx.OwnerID(ctx)
	if err != nil {
		return nil, statusErr(err)
	}
	limit, err := pageSize(req.GetPageSize())
	if err != nil {
		return nil, statusErr(err)
	}
	cursor, err := parsePageToken(req.GetPageToken())
	if err != nil {
		return nil, statusErr(err)
	}
	list, next, err := h.svc.ListTransfers(ctx, owner, cursor, limit)
	if err != nil {
		return nil, statusErr(err)
	}
	out := make([]*transfersv1.Transfer, len(list))
	for i, tr := range list {
		out[i] = transferPB(tr)
	}
	resp := &transfersv1.ListTransfersResponse{Transfers: out}
	if next != nil {
		token, err := formatPageToken(*next)
		if err != nil {
			return nil, statusErr(err)
		}
		resp.NextPageToken = token
	}
	return resp, nil
}

func (h *Handler) createCmd(ctx context.Context, req *transfersv1.CreateTransferRequest) (service.CreateTransferCmd, error) {
	owner, err := grpcx.OwnerID(ctx)
	if err != nil {
		return service.CreateTransferCmd{}, err
	}
	key := grpcx.IdempotencyKey(ctx)
	if err := idempotency.ValidateKey(key); err != nil {
		return service.CreateTransferCmd{}, err
	}
	src, err := parseUUID(req.GetSourceAccountId(), "source_account_id")
	if err != nil {
		return service.CreateTransferCmd{}, err
	}
	dst, err := parseUUID(req.GetDestAccountId(), "dest_account_id")
	if err != nil {
		return service.CreateTransferCmd{}, err
	}
	amount, err := money.NewPositive(req.GetAmount(), req.GetCurrency())
	if err != nil {
		return service.CreateTransferCmd{}, fmt.Errorf("%w: %s", domain.ErrValidation, err.Error())
	}
	var destCur money.Currency
	if req.GetDestCurrency() != "" {
		destCur, err = parseCurrency(req.GetDestCurrency(), "dest_currency")
		if err != nil {
			return service.CreateTransferCmd{}, err
		}
	}
	sum, err := idempotency.HashRequest(req)
	if err != nil {
		return service.CreateTransferCmd{}, err
	}
	return service.CreateTransferCmd{
		IdemKey:      key,
		RequestHash:  sum,
		OwnerID:      owner,
		SourceID:     src,
		DestID:       dst,
		Amount:       amount,
		DestCurrency: destCur,
		Description:  req.GetDescription(),
	}, nil
}

func replayHeader(ctx context.Context, replayed func() bool) error {
	if replayed != nil && replayed() {
		return grpc.SetHeader(ctx, metadata.Pairs("idempotent-replayed", "true"))
	}
	return nil
}

func statusErr(err error) error {
	if err == nil {
		return nil
	}
	var biz *service.BusinessError
	if errors.As(err, &biz) {
		return businessStatus(biz)
	}
	return grpcx.ToStatus(err, ErrorDomain, Errors)
}

func businessStatus(biz *service.BusinessError) error {
	code := codes.FailedPrecondition
	switch biz.Code {
	case domain.FailureAccountNotFound:
		code = codes.NotFound
	case domain.FailureNotAccountOwner:
		code = codes.PermissionDenied
	case domain.FailureCurrencyMismatch:
		code = codes.InvalidArgument
	}
	st, err := status.New(code, biz.Error()).WithDetails(&errdetails.ErrorInfo{
		Reason: biz.Code,
		Domain: ErrorDomain,
	})
	if err != nil {
		return status.Error(code, biz.Error())
	}
	return st.Err()
}
