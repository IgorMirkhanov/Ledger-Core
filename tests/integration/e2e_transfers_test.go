//go:build integration

package integration

import (
	"context"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	accountsv1 "github.com/IgorMirkhanov/ledger-core/gen/ledger/accounts/v1"
	accountsvc "github.com/IgorMirkhanov/ledger-core/internal/accounts/service"
	accounttransport "github.com/IgorMirkhanov/ledger-core/internal/accounts/transport"
	"github.com/IgorMirkhanov/ledger-core/internal/money"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/idempotency"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/outbox"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/postgres"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/accountsclient"
	transferdomain "github.com/IgorMirkhanov/ledger-core/internal/transfers/domain"
	transferrepo "github.com/IgorMirkhanov/ledger-core/internal/transfers/repository"
	transfersvc "github.com/IgorMirkhanov/ledger-core/internal/transfers/service"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/worker"
	"github.com/IgorMirkhanov/ledger-core/migrations"
	"github.com/IgorMirkhanov/ledger-core/tests/integration/testenv"
)

func TestE2E_SameCurrencyCompleted(t *testing.T) {
	ctx := testContext(t)
	st := startTransferStack(t)
	rub := mustCurrency(t, "RUB")
	owner := uuid.Must(uuid.NewV7())
	src := fundAccount(t, ctx, st.accounts, owner, rub, 1_000)
	dst := openAccount(t, ctx, st.accounts, owner, rub)

	tr := mustTransfer(t, ctx, st.transfers, owner, src.ID, dst.ID, money.New(400, rub), rub)
	require.Equal(t, transferdomain.StatusCompleted, tr.Status)
	require.Equal(t, int64(600), accountBalance(t, ctx, st.accountsPool, src.ID))
	require.Equal(t, int64(400), accountBalance(t, ctx, st.accountsPool, dst.ID))
	require.Equal(t, 2, postingCount(t, ctx, st.accountsPool, tr.JournalEntryID))
	require.Equal(t, 1, outboxCount(t, ctx, st.transfersPool, "transfer.completed"))
	require.Equal(t, 1, outboxCount(t, ctx, st.accountsPool, "hold.captured"))
	require.Equal(t, int64(0), currencySum(t, ctx, st.accountsPool, "RUB"))
}

func TestE2E_FXFourPostings(t *testing.T) {
	ctx := testContext(t)
	st := startTransferStack(t)
	usd := mustCurrency(t, "USD")
	rub := mustCurrency(t, "RUB")
	owner := uuid.Must(uuid.NewV7())
	src := fundAccount(t, ctx, st.accounts, owner, usd, 10_000)
	dst := openAccount(t, ctx, st.accounts, uuid.Must(uuid.NewV7()), rub)

	tr := mustTransfer(t, ctx, st.transfers, owner, src.ID, dst.ID, money.New(10_000, usd), rub)
	require.Equal(t, transferdomain.StatusCompleted, tr.Status)
	require.Equal(t, int64(900_000), tr.DestAmount.Amount())
	require.Equal(t, 4, postingCount(t, ctx, st.accountsPool, tr.JournalEntryID))
	require.Equal(t, int64(0), currencySum(t, ctx, st.accountsPool, "USD"))
	require.Equal(t, int64(0), currencySum(t, ctx, st.accountsPool, "RUB"))
	require.Equal(t, int64(900_000), accountBalance(t, ctx, st.accountsPool, dst.ID))
}

func TestE2E_InsufficientFunds(t *testing.T) {
	ctx := testContext(t)
	st := startTransferStack(t)
	rub := mustCurrency(t, "RUB")
	owner := uuid.Must(uuid.NewV7())
	src := openAccount(t, ctx, st.accounts, owner, rub)
	dst := openAccount(t, ctx, st.accounts, owner, rub)

	tr := mustTransfer(t, ctx, st.transfers, owner, src.ID, dst.ID, money.New(100, rub), rub)
	require.Equal(t, transferdomain.StatusFailed, tr.Status)
	require.Equal(t, transferdomain.FailureInsufficientFunds, tr.FailureCode)
	require.Equal(t, int64(0), holdCount(t, ctx, st.accountsPool))
	require.Equal(t, int64(0), accountBalance(t, ctx, st.accountsPool, src.ID))
}

func TestE2E_ClosedDestCompensates(t *testing.T) {
	ctx := testContext(t)
	st := startTransferStack(t)
	rub := mustCurrency(t, "RUB")
	owner := uuid.Must(uuid.NewV7())
	src := fundAccount(t, ctx, st.accounts, owner, rub, 1_000)
	dst := openAccount(t, ctx, st.accounts, owner, rub)
	_, err := st.accountsPool.Exec(ctx, `UPDATE accounts SET status = 'closed' WHERE id = $1`, dst.ID)
	require.NoError(t, err)

	tr := mustTransfer(t, ctx, st.transfers, owner, src.ID, dst.ID, money.New(400, rub), rub)
	require.Equal(t, transferdomain.StatusFailed, tr.Status)
	require.Equal(t, transferdomain.FailureAccountNotActive, tr.FailureCode)
	require.Equal(t, int64(1_000), accountBalance(t, ctx, st.accountsPool, src.ID))
	require.Equal(t, int64(0), accountBalance(t, ctx, st.accountsPool, dst.ID))
	require.Equal(t, int64(0), holdsByStatus(t, ctx, st.accountsPool, "active"))
	require.Equal(t, int64(1), holdsByStatus(t, ctx, st.accountsPool, "released"))
}

func TestE2E_FrozenDestCompletes(t *testing.T) {
	ctx := testContext(t)
	st := startTransferStack(t)
	rub := mustCurrency(t, "RUB")
	owner := uuid.Must(uuid.NewV7())
	src := fundAccount(t, ctx, st.accounts, owner, rub, 1_000)
	dst := openAccount(t, ctx, st.accounts, owner, rub)
	_, err := st.accountsPool.Exec(ctx, `UPDATE accounts SET status = 'frozen' WHERE id = $1`, dst.ID)
	require.NoError(t, err)

	tr := mustTransfer(t, ctx, st.transfers, owner, src.ID, dst.ID, money.New(400, rub), rub)
	require.Equal(t, transferdomain.StatusCompleted, tr.Status)
	require.Equal(t, int64(600), accountBalance(t, ctx, st.accountsPool, src.ID))
	require.Equal(t, int64(400), accountBalance(t, ctx, st.accountsPool, dst.ID))
}

func TestE2E_FrozenSourceRejectedAtHold(t *testing.T) {
	ctx := testContext(t)
	st := startTransferStack(t)
	rub := mustCurrency(t, "RUB")
	owner := uuid.Must(uuid.NewV7())
	src := fundAccount(t, ctx, st.accounts, owner, rub, 1_000)
	dst := openAccount(t, ctx, st.accounts, owner, rub)
	_, err := st.accountsPool.Exec(ctx, `UPDATE accounts SET status = 'frozen' WHERE id = $1`, src.ID)
	require.NoError(t, err)

	tr := mustTransfer(t, ctx, st.transfers, owner, src.ID, dst.ID, money.New(400, rub), rub)
	require.Equal(t, transferdomain.StatusFailed, tr.Status)
	require.Equal(t, transferdomain.FailureAccountNotActive, tr.FailureCode)
	require.Equal(t, int64(1_000), accountBalance(t, ctx, st.accountsPool, src.ID))
	require.Equal(t, int64(0), holdCount(t, ctx, st.accountsPool))

	var step, outcome string
	require.NoError(t, st.transfersPool.QueryRow(ctx, `
		SELECT step, outcome FROM transfer_steps WHERE transfer_id = $1`, tr.ID).Scan(&step, &outcome))
	require.Equal(t, "hold", step)
	require.Equal(t, "business_error", outcome)
}

func TestE2E_LostCaptureResponseIsRecovered(t *testing.T) {
	ctx := testContext(t)
	st := startTransferStack(t)
	rub := mustCurrency(t, "RUB")
	owner := uuid.Must(uuid.NewV7())
	src := fundAccount(t, ctx, st.accounts, owner, rub, 1_000)
	dst := openAccount(t, ctx, st.accounts, owner, rub)

	wrapped := &dropCapture{next: st.client}
	tsvc := newTransfersService(st.transfersPool, wrapped)
	tr := mustTransfer(t, ctx, tsvc, owner, src.ID, dst.ID, money.New(250, rub), rub)
	require.Equal(t, transferdomain.StatusFundsHeld, tr.Status)

	runCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	go func() { _ = worker.NewRecovery(tsvc, 50*time.Millisecond).Run(runCtx) }()

	require.Eventually(t, func() bool {
		got, err := tsvc.GetTransfer(ctx, owner, tr.ID)
		return err == nil && got.Status == transferdomain.StatusCompleted
	}, 5*time.Second, 50*time.Millisecond)

	var entries int
	require.NoError(t, st.accountsPool.QueryRow(ctx, `
		SELECT count(*) FROM journal_entries WHERE reference_type = 'transfer' AND reference_id = $1`,
		tr.ID.String()).Scan(&entries))
	require.Equal(t, 1, entries)
	require.Equal(t, int64(750), accountBalance(t, ctx, st.accountsPool, src.ID))
	require.Equal(t, int64(250), accountBalance(t, ctx, st.accountsPool, dst.ID))
}

func TestE2E_ParallelTransfersConserveMoney(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	st := startTransferStack(t)
	usd := mustCurrency(t, "USD")
	owner := uuid.Must(uuid.NewV7())

	const (
		nAccounts  = 20
		initial    = int64(50_000)
		nTransfers = 200
	)
	ids := make([]uuid.UUID, nAccounts)
	for i := range ids {
		ids[i] = fundAccount(t, ctx, st.accounts, owner, usd, initial).ID
	}

	var wg sync.WaitGroup
	var firstErr atomic.Value
	wg.Add(nTransfers)
	for i := range nTransfers {
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(i+1), 0x9e3779b97f4a7c15))
			a := rng.IntN(nAccounts)
			b := rng.IntN(nAccounts - 1)
			if b >= a {
				b++
			}
			amount := int64(rng.IntN(200) + 1)
			key := uuid.Must(uuid.NewV7()).String()
			_, err := st.transfers.CreateTransfer(ctx, transfersvc.CreateTransferCmd{
				IdemKey:      key,
				RequestHash:  []byte(key),
				OwnerID:      owner,
				SourceID:     ids[a],
				DestID:       ids[b],
				Amount:       money.New(amount, usd),
				DestCurrency: usd,
			})
			if err != nil {
				firstErr.CompareAndSwap(nil, err)
			}
		}()
	}
	wg.Wait()
	require.NoError(t, errFrom(&firstErr))

	var open int
	require.NoError(t, st.transfersPool.QueryRow(ctx, `
		SELECT count(*) FROM transfers WHERE status NOT IN ('completed', 'failed')`).Scan(&open))
	require.Equal(t, 0, open)
	require.Equal(t, nAccounts*initial, customerSum(t, ctx, st.accountsPool, owner))
	require.Equal(t, int64(0), currencySum(t, ctx, st.accountsPool, "USD"))
	require.Equal(t, int64(0), holdsByStatus(t, ctx, st.accountsPool, "active"))
}

type transferStack struct {
	accountsPool  *pgxpool.Pool
	transfersPool *pgxpool.Pool
	accounts      *accountsvc.Service
	transfers     *transfersvc.Service
	client        *accountsclient.Client
}

func startTransferStack(t *testing.T) transferStack {
	t.Helper()
	accountsPool := testenv.MigratedPool(t, migrations.Accounts())
	transfersPool := testenv.MigratedPool(t, migrations.Transfers())
	accounts := newAccountsService(accountsPool)
	client := startAccountsGRPC(t, accounts)
	return transferStack{
		accountsPool:  accountsPool,
		transfersPool: transfersPool,
		accounts:      accounts,
		transfers:     newTransfersService(transfersPool, client),
		client:        client,
	}
}

func newTransfersService(pool *pgxpool.Pool, accounts transfersvc.AccountsClient) *transfersvc.Service {
	return transfersvc.New(
		transferrepo.New(),
		postgres.NewTxManager(pool),
		idempotency.NewStore(),
		outbox.NewWriter("transfers"),
		accounts,
		utcClock{},
	)
}

type utcClock struct{}

func (utcClock) Now() time.Time { return time.Now().UTC() }

func startAccountsGRPC(t *testing.T, svc *accountsvc.Service) *accountsclient.Client {
	t.Helper()
	lis := bufconn.Listen(8 * 1024 * 1024)
	srv := grpc.NewServer()
	accountsv1.RegisterAccountsServiceServer(srv, accounttransport.NewHandler(svc))
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
	return accountsclient.NewFromAPI(accountsv1.NewAccountsServiceClient(conn))
}

func mustTransfer(t *testing.T, ctx context.Context, svc *transfersvc.Service, owner, src, dst uuid.UUID, amount money.Money, dest money.Currency) *transferdomain.Transfer {
	t.Helper()
	key := uuid.Must(uuid.NewV7()).String()
	tr, err := svc.CreateTransfer(ctx, transfersvc.CreateTransferCmd{
		IdemKey:      key,
		RequestHash:  []byte(key),
		OwnerID:      owner,
		SourceID:     src,
		DestID:       dst,
		Amount:       amount,
		DestCurrency: dest,
	})
	require.NoError(t, err)
	return tr
}

func accountBalance(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) int64 {
	t.Helper()
	var balance int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT balance FROM accounts WHERE id = $1`, id).Scan(&balance))
	return balance
}

func postingCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, entryID uuid.UUID) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM postings WHERE entry_id = $1`, entryID).Scan(&n))
	return n
}

func outboxCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventType string) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE event_type = $1`, eventType).Scan(&n))
	return n
}

func holdCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int64 {
	t.Helper()
	return holdsByStatus(t, ctx, pool, "")
}

func holdsByStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, status string) int64 {
	t.Helper()
	var n int64
	var err error
	if status == "" {
		err = pool.QueryRow(ctx, `SELECT count(*) FROM holds`).Scan(&n)
	} else {
		err = pool.QueryRow(ctx, `SELECT count(*) FROM holds WHERE status = $1::hold_status`, status).Scan(&n)
	}
	require.NoError(t, err)
	return n
}

func currencySum(t *testing.T, ctx context.Context, pool *pgxpool.Pool, currency string) int64 {
	t.Helper()
	var sum int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT COALESCE(sum(balance), 0) FROM accounts WHERE currency = $1`, currency).Scan(&sum))
	return sum
}

func customerSum(t *testing.T, ctx context.Context, pool *pgxpool.Pool, owner uuid.UUID) int64 {
	t.Helper()
	var sum int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT COALESCE(sum(balance), 0) FROM accounts WHERE owner_id = $1`, owner).Scan(&sum))
	return sum
}

func errFrom(v *atomic.Value) error {
	err, _ := v.Load().(error)
	return err
}

// dropCapture performs the capture, then hides the first success so the saga must retry.
type dropCapture struct {
	next    transfersvc.AccountsClient
	dropped atomic.Bool
}

func (d *dropCapture) CreateHold(ctx context.Context, idemKey string, accountID uuid.UUID, amount money.Money, referenceID string, ttl time.Duration) (uuid.UUID, error) {
	return d.next.CreateHold(ctx, idemKey, accountID, amount, referenceID, ttl)
}

func (d *dropCapture) CaptureHold(ctx context.Context, idemKey string, holdID, destAccountID uuid.UUID, destAmount money.Money, referenceID, description string) (uuid.UUID, error) {
	id, err := d.next.CaptureHold(ctx, idemKey, holdID, destAccountID, destAmount, referenceID, description)
	if err != nil {
		return uuid.Nil, err
	}
	if d.dropped.CompareAndSwap(false, true) {
		return uuid.Nil, status.Error(codes.Unavailable, "lost response")
	}
	return id, nil
}

func (d *dropCapture) ReleaseHold(ctx context.Context, idemKey string, holdID uuid.UUID, reason string) error {
	return d.next.ReleaseHold(ctx, idemKey, holdID, reason)
}

func (d *dropCapture) GetAccountInfo(ctx context.Context, ownerID, accountID uuid.UUID) (transfersvc.AccountInfo, error) {
	return d.next.GetAccountInfo(ctx, ownerID, accountID)
}
