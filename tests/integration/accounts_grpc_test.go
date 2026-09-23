//go:build integration

package integration

import (
	"context"
	"net"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	accountsv1 "github.com/IgorMirkhanov/ledger-core/gen/ledger/accounts/v1"
	"github.com/IgorMirkhanov/ledger-core/internal/accounts/service"
	"github.com/IgorMirkhanov/ledger-core/internal/accounts/transport"
	"github.com/IgorMirkhanov/ledger-core/migrations"
	"github.com/IgorMirkhanov/ledger-core/tests/integration/testenv"
)

func TestGRPC_SameKeyTwoOwnersAndReplayHeader(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	client := startAccountsClient(t, newAccountsService(pool))

	var ids []string
	for range 2 {
		owner := uuid.Must(uuid.NewV7())
		callCtx := metadata.AppendToOutgoingContext(ctx,
			"x-owner-id", owner.String(),
			"idempotency-key", "1",
		)
		created, err := client.CreateAccount(callCtx, &accountsv1.CreateAccountRequest{Currency: "USD"})
		require.NoError(t, err)
		ids = append(ids, created.GetAccount().GetId())

		dep := &accountsv1.DepositRequest{
			AccountId: created.GetAccount().GetId(),
			Amount:    &accountsv1.Money{Amount: 100, Currency: "USD"},
		}
		first, err := client.Deposit(callCtx, dep)
		require.NoError(t, err)
		require.Equal(t, int64(100), first.GetAccount().GetBalance())

		var hdr metadata.MD
		second, err := client.Deposit(callCtx, dep, grpc.Header(&hdr))
		require.NoError(t, err)
		require.Equal(t, first.GetJournalEntryId(), second.GetJournalEntryId())
		require.Equal(t, []string{"true"}, hdr.Get("idempotent-replayed"))
		require.Equal(t, int64(100), second.GetAccount().GetBalance())
	}
	require.NotEqual(t, ids[0], ids[1])
}

func startAccountsClient(t *testing.T, svc *service.Service) accountsv1.AccountsServiceClient {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	accountsv1.RegisterAccountsServiceServer(srv, transport.NewHandler(svc))
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
