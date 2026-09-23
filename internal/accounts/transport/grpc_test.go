package transport

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	accountsv1 "github.com/IgorMirkhanov/ledger-core/gen/ledger/accounts/v1"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/grpcx"
)

func TestGRPC_ErrorInfoReason(t *testing.T) {
	client := startBufClient(t, NewHandler(nil))
	_, err := client.GetAccount(context.Background(), &accountsv1.GetAccountRequest{AccountId: "not-a-uuid"})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Equal(t, "VALIDATION_FAILED", grpcx.Reason(err))
}

func TestGRPC_CreateHoldWithoutCallerIsDenied(t *testing.T) {
	client := startBufClient(t, NewHandler(nil))
	_, err := client.CreateHold(context.Background(), &accountsv1.CreateHoldRequest{
		AccountId:   "0192f7a4-6b1e-7c3d-9a2b-1f2e3d4c5b6a",
		Amount:      &accountsv1.Money{Amount: 100, Currency: "USD"},
		ReferenceId: "tr-1",
	})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.Equal(t, "CALLER_NOT_TRANSFERS", grpcx.Reason(err))
}

func startBufClient(t *testing.T, h *Handler) accountsv1.AccountsServiceClient {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	accountsv1.RegisterAccountsServiceServer(srv, h)
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
	return accountsv1.NewAccountsServiceClient(conn)
}
