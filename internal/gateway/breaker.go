package gateway

import (
	"context"
	"errors"
	"time"

	"github.com/sony/gobreaker/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	accountsv1 "github.com/IgorMirkhanov/ledger-core/gen/ledger/accounts/v1"
	transfersv1 "github.com/IgorMirkhanov/ledger-core/gen/ledger/transfers/v1"
)

type breakingAccounts struct {
	inner AccountsAPI
	cb    *gobreaker.CircuitBreaker[struct{}]
}

func withAccountsBreaker(inner AccountsAPI) AccountsAPI {
	return &breakingAccounts{inner: inner, cb: newUpstreamBreaker("accounts")}
}

func (c *breakingAccounts) CreateAccount(ctx context.Context, in *accountsv1.CreateAccountRequest, opts ...grpc.CallOption) (*accountsv1.CreateAccountResponse, error) {
	var out *accountsv1.CreateAccountResponse
	err := c.run(func() error {
		var err error
		out, err = c.inner.CreateAccount(ctx, in, opts...)
		return err
	})
	return out, err
}

func (c *breakingAccounts) GetAccount(ctx context.Context, in *accountsv1.GetAccountRequest, opts ...grpc.CallOption) (*accountsv1.GetAccountResponse, error) {
	var out *accountsv1.GetAccountResponse
	err := c.run(func() error {
		var err error
		out, err = c.inner.GetAccount(ctx, in, opts...)
		return err
	})
	return out, err
}

func (c *breakingAccounts) ListAccounts(ctx context.Context, in *accountsv1.ListAccountsRequest, opts ...grpc.CallOption) (*accountsv1.ListAccountsResponse, error) {
	var out *accountsv1.ListAccountsResponse
	err := c.run(func() error {
		var err error
		out, err = c.inner.ListAccounts(ctx, in, opts...)
		return err
	})
	return out, err
}

func (c *breakingAccounts) GetStatement(ctx context.Context, in *accountsv1.GetStatementRequest, opts ...grpc.CallOption) (*accountsv1.GetStatementResponse, error) {
	var out *accountsv1.GetStatementResponse
	err := c.run(func() error {
		var err error
		out, err = c.inner.GetStatement(ctx, in, opts...)
		return err
	})
	return out, err
}

func (c *breakingAccounts) Deposit(ctx context.Context, in *accountsv1.DepositRequest, opts ...grpc.CallOption) (*accountsv1.DepositResponse, error) {
	var out *accountsv1.DepositResponse
	err := c.run(func() error {
		var err error
		out, err = c.inner.Deposit(ctx, in, opts...)
		return err
	})
	return out, err
}

func (c *breakingAccounts) Withdraw(ctx context.Context, in *accountsv1.WithdrawRequest, opts ...grpc.CallOption) (*accountsv1.WithdrawResponse, error) {
	var out *accountsv1.WithdrawResponse
	err := c.run(func() error {
		var err error
		out, err = c.inner.Withdraw(ctx, in, opts...)
		return err
	})
	return out, err
}

func (c *breakingAccounts) run(fn func() error) error {
	_, err := c.cb.Execute(func() (struct{}, error) { return struct{}{}, fn() })
	if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
		return status.Error(codes.Unavailable, "circuit open")
	}
	return err
}

type breakingTransfers struct {
	inner TransfersAPI
	cb    *gobreaker.CircuitBreaker[struct{}]
}

func withTransfersBreaker(inner TransfersAPI) TransfersAPI {
	return &breakingTransfers{inner: inner, cb: newUpstreamBreaker("transfers")}
}

func (c *breakingTransfers) CreateTransfer(ctx context.Context, in *transfersv1.CreateTransferRequest, opts ...grpc.CallOption) (*transfersv1.CreateTransferResponse, error) {
	var out *transfersv1.CreateTransferResponse
	err := c.run(func() error {
		var err error
		out, err = c.inner.CreateTransfer(ctx, in, opts...)
		return err
	})
	return out, err
}

func (c *breakingTransfers) GetTransfer(ctx context.Context, in *transfersv1.GetTransferRequest, opts ...grpc.CallOption) (*transfersv1.GetTransferResponse, error) {
	var out *transfersv1.GetTransferResponse
	err := c.run(func() error {
		var err error
		out, err = c.inner.GetTransfer(ctx, in, opts...)
		return err
	})
	return out, err
}

func (c *breakingTransfers) ListTransfers(ctx context.Context, in *transfersv1.ListTransfersRequest, opts ...grpc.CallOption) (*transfersv1.ListTransfersResponse, error) {
	var out *transfersv1.ListTransfersResponse
	err := c.run(func() error {
		var err error
		out, err = c.inner.ListTransfers(ctx, in, opts...)
		return err
	})
	return out, err
}

func (c *breakingTransfers) run(fn func() error) error {
	_, err := c.cb.Execute(func() (struct{}, error) { return struct{}{}, fn() })
	if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
		return status.Error(codes.Unavailable, "circuit open")
	}
	return err
}

func newUpstreamBreaker(name string) *gobreaker.CircuitBreaker[struct{}] {
	return gobreaker.NewCircuitBreaker[struct{}](gobreaker.Settings{
		Name:    name,
		Timeout: 5 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 5
		},
		IsSuccessful: func(err error) bool {
			if err == nil {
				return true
			}
			switch status.Code(err) {
			case codes.InvalidArgument, codes.NotFound, codes.FailedPrecondition, codes.PermissionDenied, codes.Unauthenticated:
				return true
			default:
				return false
			}
		},
		IsExcluded: func(err error) bool {
			return errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled
		},
	})
}

// WrapUpstreams adds circuit breakers around gRPC clients.
func WrapUpstreams(accounts AccountsAPI, transfers TransfersAPI) (AccountsAPI, TransfersAPI) {
	if accounts != nil {
		accounts = withAccountsBreaker(accounts)
	}
	if transfers != nil {
		transfers = withTransfersBreaker(transfers)
	}
	return accounts, transfers
}
