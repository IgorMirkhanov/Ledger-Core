package transport

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	transfersv1 "github.com/IgorMirkhanov/ledger-core/gen/ledger/transfers/v1"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/grpcx"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/domain"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/service"
)

func TestGRPC_CreateTransferValidation(t *testing.T) {
	client := startBufClient(t, NewHandler(nil))
	ctx := metadata.AppendToOutgoingContext(context.Background(), grpcx.MDOwnerID, uuid.Must(uuid.NewV7()).String())

	_, err := client.CreateTransfer(ctx, &transfersv1.CreateTransferRequest{})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Equal(t, "IDEMPOTENCY_KEY_MISSING", grpcx.Reason(err))

	ctx = metadata.AppendToOutgoingContext(ctx, grpcx.MDIdempotencyKey, "1")
	_, err = client.CreateTransfer(ctx, &transfersv1.CreateTransferRequest{
		SourceAccountId: "not-a-uuid",
		DestAccountId:   uuid.Must(uuid.NewV7()).String(),
		Amount:          100,
		Currency:        "USD",
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Equal(t, "VALIDATION_FAILED", grpcx.Reason(err))
}

func TestGRPC_ListPageSize(t *testing.T) {
	client := startBufClient(t, NewHandler(nil))
	ctx := metadata.AppendToOutgoingContext(context.Background(), grpcx.MDOwnerID, uuid.Must(uuid.NewV7()).String())
	_, err := client.ListTransfers(ctx, &transfersv1.ListTransfersRequest{PageSize: 201})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Equal(t, "VALIDATION_FAILED", grpcx.Reason(err))
}

func TestStatusErr_BusinessErrorReason(t *testing.T) {
	err := statusErr(&service.BusinessError{Code: domain.FailureNotAccountOwner, Message: "foreign"})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.Equal(t, domain.FailureNotAccountOwner, grpcx.Reason(err))

	err = statusErr(&service.BusinessError{Code: domain.FailureAccountNotFound, Message: "missing"})
	require.Equal(t, codes.NotFound, status.Code(err))
	require.Equal(t, domain.FailureAccountNotFound, grpcx.Reason(err))
}

func TestPageTokenRoundTrip(t *testing.T) {
	cur := service.ListCursor{CreatedAt: time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC), ID: uuid.Must(uuid.NewV7())}
	token, err := formatPageToken(cur)
	require.NoError(t, err)
	got, err := parsePageToken(token)
	require.NoError(t, err)
	require.True(t, cur.CreatedAt.Equal(got.CreatedAt))
	require.Equal(t, cur.ID, got.ID)
	empty, err := parsePageToken("")
	require.NoError(t, err)
	require.Nil(t, empty)
}

func startBufClient(t *testing.T, h *Handler) transfersv1.TransfersServiceClient {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	transfersv1.RegisterTransfersServiceServer(srv, h)
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
	return transfersv1.NewTransfersServiceClient(conn)
}
