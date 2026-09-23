// Package accountsclient calls the accounts gRPC service for the transfer saga.
// A business refusal is *service.BusinessError; every other failure, including an open
// circuit breaker, is transient and safe to retry with the same idempotency key.
package accountsclient

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/sony/gobreaker/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	accountsv1 "github.com/IgorMirkhanov/ledger-core/gen/ledger/accounts/v1"
	"github.com/IgorMirkhanov/ledger-core/internal/money"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/grpcx"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/service"
)

const callTimeout = 3 * time.Second

// Client implements service.AccountsClient.
type Client struct {
	api  accountsv1.AccountsServiceClient
	conn *grpc.ClientConn
	cb   *gobreaker.CircuitBreaker[struct{}]
}

func New(addr string) (*Client, error) {
	conn, err := grpcx.Dial(addr)
	if err != nil {
		return nil, fmt.Errorf("accounts client: %w", err)
	}
	return newClient(accountsv1.NewAccountsServiceClient(conn), conn), nil
}

// NewFromAPI builds a client over an existing stub. Tests use it with bufconn.
func NewFromAPI(api accountsv1.AccountsServiceClient) *Client {
	return newClient(api, nil)
}

func newClient(api accountsv1.AccountsServiceClient, conn *grpc.ClientConn) *Client {
	cb := gobreaker.NewCircuitBreaker[struct{}](gobreaker.Settings{
		Name:    "accounts",
		Timeout: 5 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 5
		},
		IsSuccessful: func(err error) bool {
			if err == nil {
				return true
			}
			var biz *service.BusinessError
			return errors.As(err, &biz)
		},
		IsExcluded: func(err error) bool {
			return errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled
		},
	})
	return &Client{api: api, conn: conn, cb: cb}
}

func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *Client) CreateHold(ctx context.Context, idemKey string, accountID uuid.UUID, amount money.Money, referenceID string, ttl time.Duration) (uuid.UUID, error) {
	var holdID uuid.UUID
	err := c.do(ctx, func(ctx context.Context) error {
		resp, err := c.api.CreateHold(outgoing(ctx, idemKey, uuid.Nil), &accountsv1.CreateHoldRequest{
			AccountId:   accountID.String(),
			Amount:      &accountsv1.Money{Amount: amount.Amount(), Currency: amount.Currency().Code},
			ReferenceId: referenceID,
			Ttl:         durationpb.New(ttl),
		})
		if err != nil {
			return classify(err)
		}
		id, err := uuid.Parse(resp.GetHold().GetId())
		if err != nil {
			return fmt.Errorf("accounts: hold id: %w", err)
		}
		holdID = id
		return nil
	})
	return holdID, err
}

func (c *Client) CaptureHold(ctx context.Context, idemKey string, holdID, destAccountID uuid.UUID, destAmount money.Money, referenceID, description string) (uuid.UUID, error) {
	var entryID uuid.UUID
	err := c.do(ctx, func(ctx context.Context) error {
		resp, err := c.api.CaptureHold(outgoing(ctx, idemKey, uuid.Nil), &accountsv1.CaptureHoldRequest{
			HoldId:        holdID.String(),
			DestAccountId: destAccountID.String(),
			DestAmount:    &accountsv1.Money{Amount: destAmount.Amount(), Currency: destAmount.Currency().Code},
			Description:   description,
			ReferenceId:   referenceID,
		})
		if err != nil {
			return classify(err)
		}
		id, err := uuid.Parse(resp.GetJournalEntryId())
		if err != nil {
			return fmt.Errorf("accounts: journal entry id: %w", err)
		}
		entryID = id
		return nil
	})
	return entryID, err
}

func (c *Client) ReleaseHold(ctx context.Context, idemKey string, holdID uuid.UUID, reason string) error {
	return c.do(ctx, func(ctx context.Context) error {
		_, err := c.api.ReleaseHold(outgoing(ctx, idemKey, uuid.Nil), &accountsv1.ReleaseHoldRequest{
			HoldId: holdID.String(),
			Reason: reason,
		})
		if err != nil {
			return classify(err)
		}
		return nil
	})
}

func (c *Client) GetAccountInfo(ctx context.Context, ownerID, accountID uuid.UUID) (service.AccountInfo, error) {
	var info service.AccountInfo
	err := c.do(ctx, func(ctx context.Context) error {
		resp, err := c.api.GetAccount(outgoing(ctx, "", ownerID), &accountsv1.GetAccountRequest{
			AccountId: accountID.String(),
		})
		if err != nil {
			return classify(err)
		}
		acc := resp.GetAccount()
		id, err := uuid.Parse(acc.GetId())
		if err != nil {
			return fmt.Errorf("accounts: account id: %w", err)
		}
		cur, err := money.ParseCurrency(acc.GetCurrency())
		if err != nil {
			return fmt.Errorf("accounts: currency: %w", err)
		}
		info = service.AccountInfo{
			ID:       id,
			Currency: cur,
			Active:   acc.GetStatus() == accountsv1.AccountStatus_ACCOUNT_STATUS_ACTIVE,
		}
		if raw := acc.GetOwnerId(); raw != "" {
			owner, err := uuid.Parse(raw)
			if err != nil {
				return fmt.Errorf("accounts: owner id: %w", err)
			}
			info.OwnerID = owner
		}
		return nil
	})
	return info, err
}

func (c *Client) do(ctx context.Context, fn func(context.Context) error) error {
	ctx, cancel := withTimeout(ctx)
	defer cancel()
	_, err := c.cb.Execute(func() (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
	if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
		return fmt.Errorf("accounts: circuit open: %w", err)
	}
	return err
}

func withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if dl, ok := ctx.Deadline(); ok && !dl.After(time.Now().Add(callTimeout)) {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, callTimeout)
}

// outgoing sets transfers metadata. ownerID is sent only when it is not uuid.Nil
// (source-account check). Hold calls and dest lookups omit it.
func outgoing(ctx context.Context, idemKey string, ownerID uuid.UUID) context.Context {
	pairs := []string{grpcx.MDCaller, "transfers"}
	if idemKey != "" {
		pairs = append(pairs, grpcx.MDIdempotencyKey, idemKey)
	}
	if rid := grpcx.RequestID(ctx); rid != "" {
		pairs = append(pairs, grpcx.MDRequestID, rid)
	}
	if ownerID != uuid.Nil {
		pairs = append(pairs, grpcx.MDOwnerID, ownerID.String())
	}
	return metadata.AppendToOutgoingContext(ctx, pairs...)
}

func classify(err error) error {
	if err == nil {
		return nil
	}
	switch status.Code(err) {
	case codes.FailedPrecondition, codes.NotFound, codes.PermissionDenied, codes.InvalidArgument:
		reason := grpcx.Reason(err)
		if reason == "" {
			reason = status.Code(err).String()
		}
		return &service.BusinessError{Code: reason, Message: status.Convert(err).Message()}
	default:
		return fmt.Errorf("accounts: %w", err)
	}
}

var _ service.AccountsClient = (*Client)(nil)
