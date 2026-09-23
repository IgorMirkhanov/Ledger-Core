package domain

import "errors"

// Business errors. Transport maps them to gRPC codes + ErrorInfo.reason
// (see api/proto/ledger/accounts/v1/accounts.proto).
var (
	ErrAccountNotFound   = errors.New("ACCOUNT_NOT_FOUND")
	ErrHoldNotFound      = errors.New("HOLD_NOT_FOUND")
	ErrInsufficientFunds = errors.New("INSUFFICIENT_FUNDS")
	ErrAccountNotActive  = errors.New("ACCOUNT_NOT_ACTIVE")
	ErrHoldNotActive     = errors.New("HOLD_NOT_ACTIVE")
	ErrHoldExpired       = errors.New("HOLD_EXPIRED")
	ErrCurrencyMismatch  = errors.New("CURRENCY_MISMATCH")
	ErrNotAccountOwner   = errors.New("NOT_ACCOUNT_OWNER")
	ErrValidation        = errors.New("VALIDATION_FAILED")
	ErrUnbalancedEntry   = errors.New("UNBALANCED_ENTRY") // programming error, never a client error
)
