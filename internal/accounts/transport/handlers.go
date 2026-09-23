package transport

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	accountsv1 "github.com/IgorMirkhanov/ledger-core/gen/ledger/accounts/v1"
	"github.com/IgorMirkhanov/ledger-core/internal/accounts/domain"
	"github.com/IgorMirkhanov/ledger-core/internal/accounts/service"
	"github.com/IgorMirkhanov/ledger-core/internal/money"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/grpcx"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/idempotency"
)

type prepared struct {
	ctx      context.Context
	replayed func() bool
	owner    uuid.UUID
	idem     service.Idem
}

func (h *Handler) CreateAccount(ctx context.Context, req *accountsv1.CreateAccountRequest) (*accountsv1.CreateAccountResponse, error) {
	cur, err := money.ParseCurrency(req.GetCurrency())
	if err != nil {
		return nil, statusErr(fmt.Errorf("%w: currency", domain.ErrValidation))
	}
	call, err := h.prepare(ctx, req, false)
	if err != nil {
		return nil, statusErr(err)
	}
	acc, err := h.svc.CreateAccount(call.ctx, service.CreateAccountCmd{
		Idem:     call.idem,
		OwnerID:  call.owner,
		Currency: cur,
	})
	if err := h.finish(ctx, call.replayed, err); err != nil {
		return nil, err
	}
	return &accountsv1.CreateAccountResponse{Account: accountPB(acc)}, nil
}

func (h *Handler) GetAccount(ctx context.Context, req *accountsv1.GetAccountRequest) (*accountsv1.GetAccountResponse, error) {
	id, err := parseUUID(req.GetAccountId(), "account_id")
	if err != nil {
		return nil, statusErr(err)
	}
	owner, err := accountOwner(ctx)
	if err != nil {
		return nil, statusErr(err)
	}
	acc, err := h.svc.GetAccount(ctx, owner, id)
	if err != nil {
		return nil, statusErr(err)
	}
	return &accountsv1.GetAccountResponse{Account: accountPB(acc)}, nil
}

func (h *Handler) ListAccounts(ctx context.Context, _ *accountsv1.ListAccountsRequest) (*accountsv1.ListAccountsResponse, error) {
	owner, err := grpcx.OwnerID(ctx)
	if err != nil {
		return nil, err
	}
	list, err := h.svc.ListAccounts(ctx, owner)
	if err != nil {
		return nil, statusErr(err)
	}
	out := make([]*accountsv1.Account, len(list))
	for i, a := range list {
		out[i] = accountPB(a)
	}
	return &accountsv1.ListAccountsResponse{Accounts: out}, nil
}

func (h *Handler) GetStatement(ctx context.Context, req *accountsv1.GetStatementRequest) (*accountsv1.GetStatementResponse, error) {
	id, err := parseUUID(req.GetAccountId(), "account_id")
	if err != nil {
		return nil, statusErr(err)
	}
	limit, err := pageSize(req.GetPageSize())
	if err != nil {
		return nil, statusErr(err)
	}
	before, err := parsePageToken(req.GetPageToken())
	if err != nil {
		return nil, statusErr(err)
	}
	owner, err := grpcx.OwnerID(ctx)
	if err != nil {
		return nil, err
	}
	lines, err := h.svc.Statement(ctx, owner, service.StatementFilter{
		AccountID: id,
		BeforeID:  before,
		From:      protoTime(req.GetFrom()),
		To:        protoTime(req.GetTo()),
		Limit:     limit,
	})
	if err != nil {
		return nil, statusErr(err)
	}
	pb, next := linesPB(lines, limit)
	return &accountsv1.GetStatementResponse{Lines: pb, NextPageToken: next}, nil
}

func (h *Handler) Deposit(ctx context.Context, req *accountsv1.DepositRequest) (*accountsv1.DepositResponse, error) {
	res, call, err := h.postCash(ctx, req, req.GetAccountId(), req.GetAmount(), req.GetExternalRef(), true)
	if err != nil {
		return nil, err
	}
	if err := h.finish(ctx, call.replayed, nil); err != nil {
		return nil, err
	}
	return &accountsv1.DepositResponse{JournalEntryId: res.EntryID.String(), Account: accountPB(res.Account)}, nil
}

func (h *Handler) Withdraw(ctx context.Context, req *accountsv1.WithdrawRequest) (*accountsv1.WithdrawResponse, error) {
	res, call, err := h.postCash(ctx, req, req.GetAccountId(), req.GetAmount(), req.GetExternalRef(), false)
	if err != nil {
		return nil, err
	}
	if err := h.finish(ctx, call.replayed, nil); err != nil {
		return nil, err
	}
	return &accountsv1.WithdrawResponse{JournalEntryId: res.EntryID.String(), Account: accountPB(res.Account)}, nil
}

func (h *Handler) postCash(ctx context.Context, req proto.Message, accountID string, amount *accountsv1.Money, external string, deposit bool) (*service.EntryResult, prepared, error) {
	var zero prepared
	id, err := parseUUID(accountID, "account_id")
	if err != nil {
		return nil, zero, statusErr(err)
	}
	amt, err := parseMoney(amount)
	if err != nil {
		return nil, zero, statusErr(err)
	}
	call, err := h.prepare(ctx, req, false)
	if err != nil {
		return nil, zero, statusErr(err)
	}
	cmd := service.DepositCmd{
		Idem:        call.idem,
		OwnerID:     call.owner,
		AccountID:   id,
		Amount:      amt,
		ExternalRef: external,
	}
	var res *service.EntryResult
	if deposit {
		res, err = h.svc.Deposit(call.ctx, cmd)
	} else {
		res, err = h.svc.Withdraw(call.ctx, cmd)
	}
	if err != nil {
		return nil, zero, statusErr(err)
	}
	return res, call, nil
}

func (h *Handler) CreateHold(ctx context.Context, req *accountsv1.CreateHoldRequest) (*accountsv1.CreateHoldResponse, error) {
	id, err := parseUUID(req.GetAccountId(), "account_id")
	if err != nil {
		return nil, statusErr(err)
	}
	amt, err := parseMoney(req.GetAmount())
	if err != nil {
		return nil, statusErr(err)
	}
	if req.GetReferenceId() == "" {
		return nil, statusErr(fmt.Errorf("%w: reference_id", domain.ErrValidation))
	}
	call, err := h.prepare(ctx, req, true)
	if err != nil {
		return nil, statusErr(err)
	}
	hold, err := h.svc.CreateHold(call.ctx, service.CreateHoldCmd{
		Idem:        call.idem,
		OwnerID:     call.owner,
		AccountID:   id,
		Amount:      amt,
		ReferenceID: req.GetReferenceId(),
		TTL:         durationOrZero(req.GetTtl()),
	})
	if err := h.finish(ctx, call.replayed, err); err != nil {
		return nil, err
	}
	return &accountsv1.CreateHoldResponse{Hold: holdPB(hold, amt.Currency())}, nil
}

func (h *Handler) CaptureHold(ctx context.Context, req *accountsv1.CaptureHoldRequest) (*accountsv1.CaptureHoldResponse, error) {
	holdID, err := parseUUID(req.GetHoldId(), "hold_id")
	if err != nil {
		return nil, statusErr(err)
	}
	destID, err := parseUUID(req.GetDestAccountId(), "dest_account_id")
	if err != nil {
		return nil, statusErr(err)
	}
	amt, err := parseMoney(req.GetDestAmount())
	if err != nil {
		return nil, statusErr(err)
	}
	if req.GetReferenceId() == "" {
		return nil, statusErr(fmt.Errorf("%w: reference_id", domain.ErrValidation))
	}
	call, err := h.prepare(ctx, req, true)
	if err != nil {
		return nil, statusErr(err)
	}
	res, err := h.svc.CaptureHold(call.ctx, service.CaptureHoldCmd{
		Idem:          call.idem,
		OwnerID:       call.owner,
		HoldID:        holdID,
		DestAccountID: destID,
		DestAmount:    amt,
		ReferenceID:   req.GetReferenceId(),
		Description:   req.GetDescription(),
	})
	if err := h.finish(ctx, call.replayed, err); err != nil {
		return nil, err
	}
	cur, err := h.accountCurrency(call.ctx, call.owner, res.Hold.AccountID)
	if err != nil {
		return nil, statusErr(err)
	}
	return &accountsv1.CaptureHoldResponse{
		Hold:           holdPB(res.Hold, cur),
		JournalEntryId: res.EntryID.String(),
	}, nil
}

func (h *Handler) ReleaseHold(ctx context.Context, req *accountsv1.ReleaseHoldRequest) (*accountsv1.ReleaseHoldResponse, error) {
	holdID, err := parseUUID(req.GetHoldId(), "hold_id")
	if err != nil {
		return nil, statusErr(err)
	}
	call, err := h.prepare(ctx, req, true)
	if err != nil {
		return nil, statusErr(err)
	}
	hold, err := h.svc.ReleaseHold(call.ctx, service.ReleaseHoldCmd{
		Idem:    call.idem,
		OwnerID: call.owner,
		HoldID:  holdID,
		Reason:  req.GetReason(),
	})
	if err := h.finish(ctx, call.replayed, err); err != nil {
		return nil, err
	}
	cur, err := h.accountCurrency(call.ctx, call.owner, hold.AccountID)
	if err != nil {
		return nil, statusErr(err)
	}
	return &accountsv1.ReleaseHoldResponse{Hold: holdPB(hold, cur)}, nil
}

func (h *Handler) accountCurrency(ctx context.Context, owner, accountID uuid.UUID) (money.Currency, error) {
	acc, err := h.svc.GetAccount(ctx, owner, accountID)
	if err != nil {
		return money.Currency{}, err
	}
	return acc.Currency, nil
}

func durationOrZero(d *durationpb.Duration) time.Duration {
	if d == nil {
		return 0
	}
	return d.AsDuration()
}

func (h *Handler) prepare(ctx context.Context, req proto.Message, internal bool) (prepared, error) {
	var out prepared
	var err error
	if internal {
		out.owner, err = transfersOwner(ctx)
	} else {
		out.owner, err = grpcx.OwnerID(ctx)
	}
	if err != nil {
		return prepared{}, err
	}
	key := grpcx.IdempotencyKey(ctx)
	if err := idempotency.ValidateKey(key); err != nil {
		return prepared{}, err
	}
	principal := out.owner.String()
	if internal {
		principal = "svc:transfers"
	}
	sum, err := idempotency.HashRequest(req)
	if err != nil {
		return prepared{}, err
	}
	out.ctx, out.replayed = idempotency.WithReplayTracker(ctx)
	out.idem = service.Idem{Key: idempotency.Namespace(principal, key), RequestHash: sum}
	return out, nil
}

// accountOwner lets transfers read a foreign dest account (no x-owner-id → uuid.Nil, CheckOwner skipped).
// A customer call still requires x-owner-id.
func accountOwner(ctx context.Context) (uuid.UUID, error) {
	if grpcx.Caller(ctx) == "transfers" {
		return transfersOwner(ctx)
	}
	return grpcx.OwnerID(ctx)
}

func transfersOwner(ctx context.Context) (uuid.UUID, error) {
	if grpcx.Caller(ctx) != "transfers" {
		return uuid.Nil, errCallerNotTransfers
	}
	md, _ := metadata.FromIncomingContext(ctx)
	if len(md.Get(grpcx.MDOwnerID)) == 0 {
		return uuid.Nil, nil
	}
	return grpcx.OwnerID(ctx)
}

func (h *Handler) finish(ctx context.Context, replayed func() bool, err error) error {
	if err != nil {
		return statusErr(err)
	}
	if replayed != nil && replayed() {
		if herr := grpc.SetHeader(ctx, metadata.Pairs("idempotent-replayed", "true")); herr != nil {
			return statusErr(herr)
		}
	}
	return nil
}

func statusErr(err error) error {
	if err == nil {
		return nil
	}
	return grpcx.ToStatus(err, ErrorDomain, Errors)
}
