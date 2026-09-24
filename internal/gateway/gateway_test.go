package gateway_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	accountsv1 "github.com/IgorMirkhanov/ledger-core/gen/ledger/accounts/v1"
	"github.com/IgorMirkhanov/ledger-core/internal/gateway"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/grpcx"
)

func TestGateway_MetadataIgnoresClientSpoofing(t *testing.T) {
	secret := []byte("test-secret")
	owner := uuid.Must(uuid.NewV7())
	token, err := gateway.IssueToken(secret, owner, time.Now().UTC())
	require.NoError(t, err)

	fake := &fakeAccounts{}
	h := gateway.NewRouter(gateway.Options{
		API:       &gateway.API{Accounts: fake, AppEnv: "local"},
		JWTSecret: secret,
		AppEnv:    "local",
	})
	body := `{"currency":"USD"}`
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/accounts", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Idempotency-Key", "k1")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Caller", "transfers")
	req.Header.Set("X-Owner-Id", uuid.Must(uuid.NewV7()).String())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, []string{owner.String()}, fake.md.Get(grpcx.MDOwnerID))
	require.Empty(t, fake.md.Get(grpcx.MDCaller))
	require.Equal(t, []string{"k1"}, fake.md.Get(grpcx.MDIdempotencyKey))
}

func TestGateway_AmountMustBePositiveString(t *testing.T) {
	secret := []byte("test-secret")
	owner := uuid.Must(uuid.NewV7())
	token, err := gateway.IssueToken(secret, owner, time.Now().UTC())
	require.NoError(t, err)
	fake := &fakeAccounts{}
	h := gateway.NewRouter(gateway.Options{
		API:       &gateway.API{Accounts: fake, AppEnv: "local"},
		JWTSecret: secret,
		AppEnv:    "local",
	})
	cases := []string{
		`{"amount":0,"currency":"USD"}`,
		`{"amount":"0","currency":"USD"}`,
		`{"amount":"-5","currency":"USD"}`,
		`{"amount":"+5","currency":"USD"}`,
		`{"amount":"1.5","currency":"USD"}`,
		`{"amount":"1e3","currency":"USD"}`,
		`{"amount":" 5","currency":"USD"}`,
	}
	for _, body := range cases {
		req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/accounts/"+uuid.Must(uuid.NewV7()).String()+"/deposits", bytes.NewBufferString(body))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Idempotency-Key", uuid.Must(uuid.NewV7()).String())
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		require.Equal(t, http.StatusBadRequest, rec.Code, body)
		var p map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &p))
		require.Equal(t, "VALIDATION_FAILED", p["code"], body)
	}
}

func TestGateway_Unauthorized(t *testing.T) {
	h := gateway.NewRouter(gateway.Options{JWTSecret: []byte("s"), AppEnv: "local"})
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/accounts", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestGateway_RateLimitAndFailOpen(t *testing.T) {
	mr := miniredis.RunT(t)
	addr := mr.Addr()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	secret := []byte("s")
	owner := uuid.Must(uuid.NewV7())
	token, err := gateway.IssueToken(secret, owner, time.Now().UTC())
	require.NoError(t, err)
	fake := &fakeAccounts{}
	h := gateway.NewRouter(gateway.Options{
		API:       &gateway.API{Accounts: fake, AppEnv: "local"},
		Redis:     rdb,
		RateLimit: 1,
		JWTSecret: secret,
		AppEnv:    "local",
	})
	do := func() *httptest.ResponseRecorder {
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/accounts", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	require.Equal(t, http.StatusOK, do().Code)
	require.Equal(t, http.StatusOK, do().Code) // burst = 2×RPS
	third := do()
	require.Equal(t, http.StatusTooManyRequests, third.Code)
	require.NotEmpty(t, third.Header().Get("Retry-After"))

	mr.Close()
	failOpen := gateway.NewRouter(gateway.Options{
		API:       &gateway.API{Accounts: fake, AppEnv: "local"},
		Redis:     redis.NewClient(&redis.Options{Addr: addr}),
		RateLimit: 1,
		JWTSecret: secret,
		AppEnv:    "local",
	})
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/accounts", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	failOpen.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestGateway_GrpcErrorMapping(t *testing.T) {
	secret := []byte("s")
	owner := uuid.Must(uuid.NewV7())
	token, err := gateway.IssueToken(secret, owner, time.Now().UTC())
	require.NoError(t, err)
	fake := &fakeAccounts{err: status.Error(codes.FailedPrecondition, "no money")}
	// inject ErrorInfo via Reason helper — status without details maps to FAILED_PRECONDITION
	h := gateway.NewRouter(gateway.Options{
		API:       &gateway.API{Accounts: fake, AppEnv: "local"},
		JWTSecret: secret,
		AppEnv:    "local",
	})
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/accounts/"+uuid.Must(uuid.NewV7()).String(), nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
}

type fakeAccounts struct {
	md  metadata.MD
	err error
}

func (f *fakeAccounts) CreateAccount(ctx context.Context, _ *accountsv1.CreateAccountRequest, _ ...grpc.CallOption) (*accountsv1.CreateAccountResponse, error) {
	f.md, _ = metadata.FromOutgoingContext(ctx)
	if f.err != nil {
		return nil, f.err
	}
	now := timestamppb.Now()
	return &accountsv1.CreateAccountResponse{Account: &accountsv1.Account{
		Id: uuid.Must(uuid.NewV7()).String(), Currency: "USD", Status: accountsv1.AccountStatus_ACCOUNT_STATUS_ACTIVE,
		CreatedAt: now, UpdatedAt: now,
	}}, nil
}

func (f *fakeAccounts) GetAccount(ctx context.Context, _ *accountsv1.GetAccountRequest, _ ...grpc.CallOption) (*accountsv1.GetAccountResponse, error) {
	f.md, _ = metadata.FromOutgoingContext(ctx)
	if f.err != nil {
		return nil, f.err
	}
	now := timestamppb.Now()
	return &accountsv1.GetAccountResponse{Account: &accountsv1.Account{
		Id: uuid.Must(uuid.NewV7()).String(), Currency: "USD", Status: accountsv1.AccountStatus_ACCOUNT_STATUS_ACTIVE,
		CreatedAt: now, UpdatedAt: now,
	}}, nil
}

func (f *fakeAccounts) ListAccounts(ctx context.Context, _ *accountsv1.ListAccountsRequest, _ ...grpc.CallOption) (*accountsv1.ListAccountsResponse, error) {
	f.md, _ = metadata.FromOutgoingContext(ctx)
	return &accountsv1.ListAccountsResponse{}, f.err
}

func (f *fakeAccounts) GetStatement(context.Context, *accountsv1.GetStatementRequest, ...grpc.CallOption) (*accountsv1.GetStatementResponse, error) {
	return &accountsv1.GetStatementResponse{}, f.err
}

func (f *fakeAccounts) Deposit(context.Context, *accountsv1.DepositRequest, ...grpc.CallOption) (*accountsv1.DepositResponse, error) {
	return nil, f.err
}

func (f *fakeAccounts) Withdraw(context.Context, *accountsv1.WithdrawRequest, ...grpc.CallOption) (*accountsv1.WithdrawResponse, error) {
	return nil, f.err
}
