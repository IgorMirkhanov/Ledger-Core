package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	accountsv1 "github.com/IgorMirkhanov/ledger-core/gen/ledger/accounts/v1"
	transfersv1 "github.com/IgorMirkhanov/ledger-core/gen/ledger/transfers/v1"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/grpcx"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/httpx"
)

// AccountsAPI is the subset of AccountsServiceClient used by the gateway.
type AccountsAPI interface {
	CreateAccount(ctx context.Context, in *accountsv1.CreateAccountRequest, opts ...grpc.CallOption) (*accountsv1.CreateAccountResponse, error)
	GetAccount(ctx context.Context, in *accountsv1.GetAccountRequest, opts ...grpc.CallOption) (*accountsv1.GetAccountResponse, error)
	ListAccounts(ctx context.Context, in *accountsv1.ListAccountsRequest, opts ...grpc.CallOption) (*accountsv1.ListAccountsResponse, error)
	GetStatement(ctx context.Context, in *accountsv1.GetStatementRequest, opts ...grpc.CallOption) (*accountsv1.GetStatementResponse, error)
	Deposit(ctx context.Context, in *accountsv1.DepositRequest, opts ...grpc.CallOption) (*accountsv1.DepositResponse, error)
	Withdraw(ctx context.Context, in *accountsv1.WithdrawRequest, opts ...grpc.CallOption) (*accountsv1.WithdrawResponse, error)
}

// TransfersAPI is the subset of TransfersServiceClient used by the gateway.
type TransfersAPI interface {
	CreateTransfer(ctx context.Context, in *transfersv1.CreateTransferRequest, opts ...grpc.CallOption) (*transfersv1.CreateTransferResponse, error)
	GetTransfer(ctx context.Context, in *transfersv1.GetTransferRequest, opts ...grpc.CallOption) (*transfersv1.GetTransferResponse, error)
	ListTransfers(ctx context.Context, in *transfersv1.ListTransfersRequest, opts ...grpc.CallOption) (*transfersv1.ListTransfersResponse, error)
}

// API holds dependencies for HTTP handlers.
type API struct {
	Accounts  AccountsAPI
	Transfers TransfersAPI
	JWTSecret []byte
	Upstream  time.Duration
	AppEnv    string
}

type amountField struct{ v int64 }

func (a *amountField) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || data[0] != '"' {
		return fmt.Errorf("amount must be a JSON string")
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	n, err := ParseAmountString(s)
	if err != nil {
		return err
	}
	a.v = n
	return nil
}

func (a amountField) MarshalJSON() ([]byte, error) {
	return json.Marshal(FormatAmount(a.v))
}

func decodeJSON(r *http.Request, dst any) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, 64<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("unexpected trailing data")
	}
	return nil
}

func (a *API) upstream(ctx context.Context) (context.Context, context.CancelFunc) {
	ttl := a.Upstream
	if ttl <= 0 {
		ttl = 5 * time.Second
	}
	return context.WithTimeout(ctx, ttl)
}

// outgoing builds a fresh gRPC context with only gateway-owned metadata.
// Client headers such as X-Caller are never forwarded.
func (a *API) outgoing(ctx context.Context, r *http.Request) (context.Context, error) {
	uid, ok := UserID(ctx)
	if !ok {
		return nil, errUnauthenticated
	}
	pairs := []string{
		grpcx.MDOwnerID, uid.String(),
		grpcx.MDRequestID, RequestID(ctx),
	}
	if key := r.Header.Get("Idempotency-Key"); key != "" {
		pairs = append(pairs, grpcx.MDIdempotencyKey, key)
	}
	return metadata.NewOutgoingContext(ctx, metadata.Pairs(pairs...)), nil
}

var errUnauthenticated = status.Error(codes.Unauthenticated, "unauthenticated")

func (a *API) callOpts(hdr *metadata.MD) []grpc.CallOption {
	if hdr == nil {
		return nil
	}
	return []grpc.CallOption{grpc.Header(hdr)}
}

func setReplayHeader(w http.ResponseWriter, hdr metadata.MD) {
	if v := hdr.Get("idempotent-replayed"); len(v) > 0 && v[0] == "true" {
		w.Header().Set("Idempotent-Replayed", "true")
	}
}

func (a *API) CreateAccount(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Currency string `json:"currency"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeProblem(w, r, http.StatusBadRequest, "VALIDATION_FAILED", "Invalid body")
		return
	}
	ctx, cancel := a.upstream(r.Context())
	defer cancel()
	out, err := a.outgoing(ctx, r)
	if err != nil {
		grpcToProblem(w, r, err)
		return
	}
	var hdr metadata.MD
	resp, err := a.Accounts.CreateAccount(out, &accountsv1.CreateAccountRequest{Currency: body.Currency}, a.callOpts(&hdr)...)
	if err != nil {
		grpcToProblem(w, r, err)
		return
	}
	setReplayHeader(w, hdr)
	httpx.WriteJSON(w, http.StatusCreated, accountJSON(resp.GetAccount()))
}

func (a *API) ListAccounts(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := a.upstream(r.Context())
	defer cancel()
	out, err := a.outgoing(ctx, r)
	if err != nil {
		grpcToProblem(w, r, err)
		return
	}
	resp, err := a.Accounts.ListAccounts(out, &accountsv1.ListAccountsRequest{})
	if err != nil {
		grpcToProblem(w, r, err)
		return
	}
	list := make([]map[string]any, 0, len(resp.GetAccounts()))
	for _, acc := range resp.GetAccounts() {
		list = append(list, accountJSON(acc))
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"accounts": list})
}

func (a *API) GetAccount(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	ctx, cancel := a.upstream(r.Context())
	defer cancel()
	out, err := a.outgoing(ctx, r)
	if err != nil {
		grpcToProblem(w, r, err)
		return
	}
	resp, err := a.Accounts.GetAccount(out, &accountsv1.GetAccountRequest{AccountId: id})
	if err != nil {
		grpcToProblem(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, accountJSON(resp.GetAccount()))
}

func (a *API) GetStatement(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	req := &accountsv1.GetStatementRequest{
		AccountId: chi.URLParam(r, "id"),
		PageToken: q.Get("cursor"),
	}
	if lim := q.Get("limit"); lim != "" {
		var n int32
		if _, err := fmt.Sscanf(lim, "%d", &n); err == nil {
			req.PageSize = n
		}
	}
	if from := q.Get("from"); from != "" {
		if t, err := time.Parse(time.RFC3339, from); err == nil {
			req.From = timestamppb.New(t)
		}
	}
	if to := q.Get("to"); to != "" {
		if t, err := time.Parse(time.RFC3339, to); err == nil {
			req.To = timestamppb.New(t)
		}
	}
	ctx, cancel := a.upstream(r.Context())
	defer cancel()
	out, err := a.outgoing(ctx, r)
	if err != nil {
		grpcToProblem(w, r, err)
		return
	}
	resp, err := a.Accounts.GetStatement(out, req)
	if err != nil {
		grpcToProblem(w, r, err)
		return
	}
	lines := make([]map[string]any, 0, len(resp.GetLines()))
	for _, l := range resp.GetLines() {
		kind := strings.ToLower(strings.TrimPrefix(l.GetKind().String(), "ENTRY_KIND_"))
		lines = append(lines, map[string]any{
			"posting_id":    strconv.FormatInt(l.GetPostingId(), 10),
			"entry_id":      l.GetEntryId(),
			"kind":          kind,
			"amount":        FormatAmount(l.GetAmount()),
			"balance_after": FormatAmount(l.GetBalanceAfter()),
			"description":   l.GetDescription(),
			"created_at":    l.GetCreatedAt().AsTime().UTC().Format(time.RFC3339Nano),
		})
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"lines":           lines,
		"next_page_token": resp.GetNextPageToken(),
	})
}

func (a *API) Deposit(w http.ResponseWriter, r *http.Request) {
	a.postCash(w, r, true)
}

func (a *API) Withdraw(w http.ResponseWriter, r *http.Request) {
	a.postCash(w, r, false)
}

func (a *API) postCash(w http.ResponseWriter, r *http.Request, deposit bool) {
	var body struct {
		Amount      amountField `json:"amount"`
		Currency    string      `json:"currency"`
		ExternalRef string      `json:"external_ref"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeProblem(w, r, http.StatusBadRequest, "VALIDATION_FAILED", "Invalid body")
		return
	}
	ctx, cancel := a.upstream(r.Context())
	defer cancel()
	out, err := a.outgoing(ctx, r)
	if err != nil {
		grpcToProblem(w, r, err)
		return
	}
	money := &accountsv1.Money{Amount: body.Amount.v, Currency: body.Currency}
	var hdr metadata.MD
	var entryID string
	var acc *accountsv1.Account
	if deposit {
		resp, err := a.Accounts.Deposit(out, &accountsv1.DepositRequest{
			AccountId: chi.URLParam(r, "id"), Amount: money, ExternalRef: body.ExternalRef,
		}, a.callOpts(&hdr)...)
		if err != nil {
			grpcToProblem(w, r, err)
			return
		}
		entryID, acc = resp.GetJournalEntryId(), resp.GetAccount()
	} else {
		resp, err := a.Accounts.Withdraw(out, &accountsv1.WithdrawRequest{
			AccountId: chi.URLParam(r, "id"), Amount: money, ExternalRef: body.ExternalRef,
		}, a.callOpts(&hdr)...)
		if err != nil {
			grpcToProblem(w, r, err)
			return
		}
		entryID, acc = resp.GetJournalEntryId(), resp.GetAccount()
	}
	setReplayHeader(w, hdr)
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{
		"journal_entry_id": entryID,
		"account":          accountJSON(acc),
	})
}

func (a *API) CreateTransfer(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SourceAccountID string      `json:"source_account_id"`
		DestAccountID   string      `json:"dest_account_id"`
		Amount          amountField `json:"amount"`
		Currency        string      `json:"currency"`
		DestCurrency    string      `json:"dest_currency"`
		Description     string      `json:"description"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeProblem(w, r, http.StatusBadRequest, "VALIDATION_FAILED", "Invalid body")
		return
	}
	ctx, cancel := a.upstream(r.Context())
	defer cancel()
	out, err := a.outgoing(ctx, r)
	if err != nil {
		grpcToProblem(w, r, err)
		return
	}
	var hdr metadata.MD
	resp, err := a.Transfers.CreateTransfer(out, &transfersv1.CreateTransferRequest{
		SourceAccountId: body.SourceAccountID,
		DestAccountId:   body.DestAccountID,
		Amount:          body.Amount.v,
		Currency:        body.Currency,
		DestCurrency:    body.DestCurrency,
		Description:     body.Description,
	}, a.callOpts(&hdr)...)
	if err != nil {
		grpcToProblem(w, r, err)
		return
	}
	setReplayHeader(w, hdr)
	tr := resp.GetTransfer()
	code := http.StatusCreated
	switch tr.GetStatus() {
	case transfersv1.TransferStatus_TRANSFER_STATUS_CREATED,
		transfersv1.TransferStatus_TRANSFER_STATUS_FUNDS_HELD,
		transfersv1.TransferStatus_TRANSFER_STATUS_COMPENSATING:
		code = http.StatusAccepted
		w.Header().Set("Location", "/v1/transfers/"+tr.GetId())
	}
	httpx.WriteJSON(w, code, transferJSON(tr))
}

func (a *API) GetTransfer(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := a.upstream(r.Context())
	defer cancel()
	out, err := a.outgoing(ctx, r)
	if err != nil {
		grpcToProblem(w, r, err)
		return
	}
	resp, err := a.Transfers.GetTransfer(out, &transfersv1.GetTransferRequest{TransferId: chi.URLParam(r, "id")})
	if err != nil {
		grpcToProblem(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, transferJSON(resp.GetTransfer()))
}

func (a *API) ListTransfers(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	req := &transfersv1.ListTransfersRequest{PageToken: q.Get("cursor")}
	if lim := q.Get("limit"); lim != "" {
		var n int32
		if _, err := fmt.Sscanf(lim, "%d", &n); err == nil {
			req.PageSize = n
		}
	}
	ctx, cancel := a.upstream(r.Context())
	defer cancel()
	out, err := a.outgoing(ctx, r)
	if err != nil {
		grpcToProblem(w, r, err)
		return
	}
	resp, err := a.Transfers.ListTransfers(out, req)
	if err != nil {
		grpcToProblem(w, r, err)
		return
	}
	list := make([]map[string]any, 0, len(resp.GetTransfers()))
	for _, tr := range resp.GetTransfers() {
		list = append(list, transferJSON(tr))
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"transfers":       list,
		"next_page_token": resp.GetNextPageToken(),
	})
}

func (a *API) DevToken(w http.ResponseWriter, r *http.Request) {
	if a.AppEnv != "local" {
		writeProblem(w, r, http.StatusNotFound, "NOT_FOUND", "Not found")
		return
	}
	var body struct {
		UserID string `json:"user_id"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&body)
	}
	id := uuid.Must(uuid.NewV7())
	if body.UserID != "" {
		parsed, err := uuid.Parse(body.UserID)
		if err != nil {
			writeProblem(w, r, http.StatusBadRequest, "VALIDATION_FAILED", "Invalid user_id")
			return
		}
		id = parsed
	}
	token, err := IssueToken(a.JWTSecret, id, time.Now().UTC())
	if err != nil {
		writeProblem(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "Internal error")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"token": token, "user_id": id.String()})
}

func accountJSON(a *accountsv1.Account) map[string]any {
	if a == nil {
		return nil
	}
	status := strings.ToLower(strings.TrimPrefix(a.GetStatus().String(), "ACCOUNT_STATUS_"))
	return map[string]any{
		"id":         a.GetId(),
		"owner_id":   a.GetOwnerId(),
		"currency":   a.GetCurrency(),
		"status":     status,
		"balance":    FormatAmount(a.GetBalance()),
		"held":       FormatAmount(a.GetHeld()),
		"available":  FormatAmount(a.GetAvailable()),
		"version":    a.GetVersion(),
		"created_at": a.GetCreatedAt().AsTime().UTC().Format(time.RFC3339Nano),
		"updated_at": a.GetUpdatedAt().AsTime().UTC().Format(time.RFC3339Nano),
	}
}

func transferJSON(t *transfersv1.Transfer) map[string]any {
	if t == nil {
		return nil
	}
	status := strings.ToLower(strings.TrimPrefix(t.GetStatus().String(), "TRANSFER_STATUS_"))
	out := map[string]any{
		"id":                t.GetId(),
		"owner_id":          t.GetOwnerId(),
		"source_account_id": t.GetSourceAccountId(),
		"dest_account_id":   t.GetDestAccountId(),
		"amount":            FormatAmount(t.GetAmount()),
		"currency":          t.GetCurrency(),
		"status":            status,
		"created_at":        t.GetCreatedAt().AsTime().UTC().Format(time.RFC3339Nano),
		"updated_at":        t.GetUpdatedAt().AsTime().UTC().Format(time.RFC3339Nano),
	}
	if t.GetDestAmount() != 0 {
		out["dest_amount"] = FormatAmount(t.GetDestAmount())
	}
	if t.GetDestCurrency() != "" {
		out["dest_currency"] = t.GetDestCurrency()
	}
	if fx := FormatFXRate(t.GetFxRate()); fx != "" {
		out["fx_rate"] = fx
	}
	if c := t.GetFailureCode(); c != "" {
		out["failure_code"] = c
	}
	if r := t.GetFailureReason(); r != "" {
		out["failure_reason"] = r
	}
	if t.GetCompletedAt() != nil {
		out["completed_at"] = t.GetCompletedAt().AsTime().UTC().Format(time.RFC3339Nano)
	}
	return out
}
