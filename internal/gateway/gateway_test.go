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
		API:          &gateway.API{Accounts: fake, AppEnv: "local"},
		Redis:        rdb,
		RateLimitRPS: 1,
		RateLimitIP:  1000,
		JWTSecret:    secret,
		AppEnv:       "local",
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
		API:          &gateway.API{Accounts: fake, AppEnv: "local"},
		Redis:        redis.NewClient(&redis.Options{Addr: addr}),
		RateLimitRPS: 1,
		RateLimitIP:  1000,
		JWTSecret:    secret,
		AppEnv:       "local",
	})
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/accounts", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	failOpen.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestGateway_IPRateLimitBeforeAuth(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	h := gateway.NewRouter(gateway.Options{
		API:          &gateway.API{Accounts: &fakeAccounts{}, AppEnv: "local"},
		Redis:        rdb,
		RateLimitRPS: 1000,
		RateLimitIP:  1,
		JWTSecret:    []byte("s"),
		AppEnv:       "local",
	})
	got429 := false
	for i := 0; i < 300; i++ {
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/accounts", nil)
		req.Header.Set("Authorization", "Bearer garbage-token")
		req.RemoteAddr = "203.0.113.10:54321"
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		switch rec.Code {
		case http.StatusTooManyRequests:
			got429 = true
		case http.StatusUnauthorized:
			// expected until IP limit trips
		default:
			t.Fatalf("unexpected status %d", rec.Code)
		}
	}
	require.True(t, got429, "IP limiter before Auth should return 429 for junk tokens")
}

func TestClientIP_TrustedProxies(t *testing.T) {
	trusted, err := gateway.ParseTrustedProxies("10.0.0.0/8,192.168.0.0/16")
	require.NoError(t, err)

	cases := []struct {
		name    string
		remote  string
		xff     string
		trusted *gateway.TrustedProxies
		want    string
	}{
		{
			name:    "no trusted: ignore XFF",
			remote:  "203.0.113.10:1",
			xff:     "198.51.100.1, 10.0.0.2",
			trusted: nil,
			want:    "203.0.113.10",
		},
		{
			name:    "empty trusted: ignore XFF",
			remote:  "203.0.113.10:1",
			xff:     "198.51.100.1",
			trusted: &gateway.TrustedProxies{},
			want:    "203.0.113.10",
		},
		{
			name:    "trusted remote: walk right-to-left",
			remote:  "10.0.0.5:1",
			xff:     "198.51.100.99, 203.0.113.50, 10.0.0.2",
			trusted: trusted,
			want:    "203.0.113.50",
		},
		{
			name:    "untrusted remote: ignore XFF",
			remote:  "203.0.113.10:1",
			xff:     "198.51.100.1, 10.0.0.2",
			trusted: trusted,
			want:    "203.0.113.10",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
			req.RemoteAddr = tc.remote
			if tc.xff != "" {
				req.Header.Set("X-Forwarded-For", tc.xff)
			}
			require.Equal(t, tc.want, gateway.ClientIP(req, tc.trusted))
		})
	}
}

func TestFormatFXRate(t *testing.T) {
	require.Equal(t, "", gateway.FormatFXRate(""))
	require.Equal(t, "90", gateway.FormatFXRate("90.000"))
	require.Equal(t, "0.011", gateway.FormatFXRate("0.01100"))
	require.Equal(t, "90", gateway.FormatFXRate("90"))
}

func TestGateway_StatementJSON(t *testing.T) {
	secret := []byte("s")
	owner := uuid.Must(uuid.NewV7())
	token, err := gateway.IssueToken(secret, owner, time.Now().UTC())
	require.NoError(t, err)
	now := timestamppb.Now()
	fake := &fakeAccounts{statement: &accountsv1.GetStatementResponse{
		Lines: []*accountsv1.StatementLine{{
			PostingId:    42,
			EntryId:      uuid.Must(uuid.NewV7()).String(),
			Kind:         accountsv1.EntryKind_ENTRY_KIND_TRANSFER,
			Amount:       900000,
			BalanceAfter: 900000,
			Description:  "fx",
			CreatedAt:    now,
		}},
	}}
	h := gateway.NewRouter(gateway.Options{
		API:       &gateway.API{Accounts: fake, AppEnv: "local"},
		JWTSecret: secret,
		AppEnv:    "local",
	})
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/accounts/"+uuid.Must(uuid.NewV7()).String()+"/statement", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	lines := body["lines"].([]any)
	require.Len(t, lines, 1)
	line := lines[0].(map[string]any)
	require.Equal(t, "42", line["posting_id"])
	require.Equal(t, "transfer", line["kind"])
}

func TestGateway_GrpcErrorMapping(t *testing.T) {
	secret := []byte("s")
	owner := uuid.Must(uuid.NewV7())
	token, err := gateway.IssueToken(secret, owner, time.Now().UTC())
	require.NoError(t, err)
	fake := &fakeAccounts{err: status.Error(codes.FailedPrecondition, "no money")}
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
	md        metadata.MD
	err       error
	statement *accountsv1.GetStatementResponse
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
	if f.err != nil {
		return nil, f.err
	}
	if f.statement != nil {
		return f.statement, nil
	}
	return &accountsv1.GetStatementResponse{}, nil
}

func (f *fakeAccounts) Deposit(context.Context, *accountsv1.DepositRequest, ...grpc.CallOption) (*accountsv1.DepositResponse, error) {
	return nil, f.err
}

func (f *fakeAccounts) Withdraw(context.Context, *accountsv1.WithdrawRequest, ...grpc.CallOption) (*accountsv1.WithdrawResponse, error) {
	return nil, f.err
}
