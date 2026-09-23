package accountsclient

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sony/gobreaker/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	accountsv1 "github.com/IgorMirkhanov/ledger-core/gen/ledger/accounts/v1"
	"github.com/IgorMirkhanov/ledger-core/internal/money"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/grpcx"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/service"
)

func TestClient_MetadataAndBusinessError(t *testing.T) {
	owner := uuid.Must(uuid.NewV7())
	accountID := uuid.Must(uuid.NewV7())
	holdID := uuid.Must(uuid.NewV7())
	fake := &fakeAccounts{holdID: holdID.String(), ownerID: owner.String(), accountID: accountID.String()}
	client := startClient(t, fake)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(grpcx.MDRequestID, "req-1"))
	usd := mustUSD(t)
	got, err := client.CreateHold(ctx, "transfer:1:hold", accountID, money.New(100, usd), "ref", 15*time.Minute)
	require.NoError(t, err)
	require.Equal(t, holdID, got)
	require.Equal(t, "transfers", fake.last.Get(grpcx.MDCaller)[0])
	require.Equal(t, "transfer:1:hold", fake.last.Get(grpcx.MDIdempotencyKey)[0])
	require.Equal(t, "req-1", fake.last.Get(grpcx.MDRequestID)[0])
	require.Empty(t, fake.last.Get(grpcx.MDOwnerID))

	info, err := client.GetAccountInfo(ctx, owner, accountID)
	require.NoError(t, err)
	require.Equal(t, owner, info.OwnerID)
	require.True(t, info.Active)
	require.Equal(t, "USD", info.Currency.Code)
	require.Equal(t, []string{owner.String()}, fake.last.Get(grpcx.MDOwnerID))
	require.Empty(t, fake.last.Get(grpcx.MDIdempotencyKey))

	_, err = client.GetAccountInfo(ctx, uuid.Nil, accountID)
	require.NoError(t, err)
	require.Empty(t, fake.last.Get(grpcx.MDOwnerID))

	fake.err = businessStatus(t, codes.FailedPrecondition, "INSUFFICIENT_FUNDS", "no money")
	_, err = client.CreateHold(ctx, "transfer:1:hold", accountID, money.New(100, usd), "ref", time.Minute)
	var biz *service.BusinessError
	require.ErrorAs(t, err, &biz)
	require.Equal(t, "INSUFFICIENT_FUNDS", biz.Code)
}

func TestClient_CircuitOpensAfterFiveTransient(t *testing.T) {
	fake := &fakeAccounts{err: status.Error(codes.Unavailable, "down")}
	client := startClient(t, fake)
	usd := mustUSD(t)
	id := uuid.Must(uuid.NewV7())
	for range 5 {
		_, err := client.CreateHold(context.Background(), "k", id, money.New(1, usd), "r", time.Minute)
		require.Error(t, err)
		var biz *service.BusinessError
		require.NotErrorAs(t, err, &biz)
	}
	require.Equal(t, int32(5), fake.calls.Load())

	_, err := client.CreateHold(context.Background(), "k", id, money.New(1, usd), "r", time.Minute)
	require.ErrorIs(t, err, gobreaker.ErrOpenState)
	require.Equal(t, int32(5), fake.calls.Load())
}

func TestClient_BusinessErrorDoesNotTripBreaker(t *testing.T) {
	fake := &fakeAccounts{err: status.Error(codes.Unavailable, "down")}
	client := startClient(t, fake)
	usd := mustUSD(t)
	id := uuid.Must(uuid.NewV7())
	for range 4 {
		_, err := client.CreateHold(context.Background(), "k", id, money.New(1, usd), "r", time.Minute)
		require.Error(t, err)
	}
	fake.err = businessStatus(t, codes.NotFound, "ACCOUNT_NOT_FOUND", "missing")
	_, err := client.CreateHold(context.Background(), "k", id, money.New(1, usd), "r", time.Minute)
	var biz *service.BusinessError
	require.ErrorAs(t, err, &biz)

	fake.err = status.Error(codes.Unavailable, "down")
	for range 4 {
		_, err := client.CreateHold(context.Background(), "k", id, money.New(1, usd), "r", time.Minute)
		require.Error(t, err)
		require.NotErrorIs(t, err, gobreaker.ErrOpenState)
	}
	require.Equal(t, int32(9), fake.calls.Load())
}

func TestClient_CallTimeoutIsThreeSeconds(t *testing.T) {
	fake := &fakeAccounts{block: true}
	client := startClient(t, fake)
	start := time.Now()
	_, err := client.GetAccountInfo(context.Background(), uuid.Nil, uuid.Must(uuid.NewV7()))
	require.Error(t, err)
	require.Less(t, time.Since(start), 4*time.Second)
	var biz *service.BusinessError
	require.NotErrorAs(t, err, &biz)
}

type fakeAccounts struct {
	accountsv1.UnimplementedAccountsServiceServer
	calls     atomic.Int32
	last      metadata.MD
	err       error
	block     bool
	holdID    string
	ownerID   string
	accountID string
}

func (f *fakeAccounts) CreateHold(ctx context.Context, _ *accountsv1.CreateHoldRequest) (*accountsv1.CreateHoldResponse, error) {
	return &accountsv1.CreateHoldResponse{Hold: &accountsv1.Hold{Id: f.holdID}}, f.hit(ctx)
}

func (f *fakeAccounts) GetAccount(ctx context.Context, _ *accountsv1.GetAccountRequest) (*accountsv1.GetAccountResponse, error) {
	if err := f.hit(ctx); err != nil {
		return nil, err
	}
	return &accountsv1.GetAccountResponse{Account: &accountsv1.Account{
		Id:       f.accountID,
		OwnerId:  f.ownerID,
		Currency: "USD",
		Status:   accountsv1.AccountStatus_ACCOUNT_STATUS_ACTIVE,
	}}, nil
}

func (f *fakeAccounts) hit(ctx context.Context) error {
	f.calls.Add(1)
	md, _ := metadata.FromIncomingContext(ctx)
	f.last = md
	if f.block {
		<-ctx.Done()
		return status.FromContextError(ctx.Err()).Err()
	}
	return f.err
}

func startClient(t *testing.T, fake *fakeAccounts) *Client {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	accountsv1.RegisterAccountsServiceServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return newClient(accountsv1.NewAccountsServiceClient(conn), nil)
}

func businessStatus(t *testing.T, code codes.Code, reason, msg string) error {
	t.Helper()
	st, err := status.New(code, msg).WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: "ledger.accounts"})
	require.NoError(t, err)
	return st.Err()
}

func mustUSD(t *testing.T) money.Currency {
	t.Helper()
	cur, err := money.ParseCurrency("USD")
	require.NoError(t, err)
	return cur
}
