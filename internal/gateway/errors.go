package gateway

import (
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/grpcx"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/httpx"
)

func grpcToProblem(w http.ResponseWriter, r *http.Request, err error) {
	st, ok := status.FromError(err)
	if !ok {
		writeProblem(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "Internal error")
		return
	}
	reason := grpcx.Reason(err)
	httpStatus, code, title := mapGRPC(st.Code(), reason)
	p := httpx.Problem{
		Type:      "https://ledger-core.dev/errors/" + kebab(code),
		Title:     title,
		Status:    httpStatus,
		Code:      code,
		RequestID: RequestID(r.Context()),
	}
	if httpStatus == http.StatusServiceUnavailable {
		w.Header().Set("Retry-After", "1")
	}
	httpx.WriteProblem(w, p)
}

func mapGRPC(code codes.Code, reason string) (httpStatus int, errCode, title string) {
	switch code {
	case codes.InvalidArgument:
		switch reason {
		case "IDEMPOTENCY_KEY_REUSED":
			return http.StatusUnprocessableEntity, reason, "Idempotency key reused"
		case "IDEMPOTENCY_KEY_MISSING":
			return http.StatusBadRequest, reason, "Idempotency key missing"
		case "SAME_ACCOUNT":
			return http.StatusBadRequest, reason, "Same account"
		default:
			if reason == "" {
				reason = "VALIDATION_FAILED"
			}
			return http.StatusBadRequest, reason, "Validation failed"
		}
	case codes.Unauthenticated:
		return http.StatusUnauthorized, "UNAUTHENTICATED", "Unauthenticated"
	case codes.PermissionDenied:
		// Hide foreign resources.
		return http.StatusNotFound, "NOT_FOUND", "Not found"
	case codes.NotFound:
		if reason == "" {
			reason = "NOT_FOUND"
		}
		return http.StatusNotFound, reason, "Not found"
	case codes.FailedPrecondition:
		if reason == "" {
			reason = "FAILED_PRECONDITION"
		}
		return http.StatusUnprocessableEntity, reason, titleFromReason(reason)
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests, "RATE_LIMITED", "Too many requests"
	case codes.Unavailable:
		return http.StatusServiceUnavailable, "UNAVAILABLE", "Service unavailable"
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout, "DEADLINE_EXCEEDED", "Upstream timeout"
	default:
		return http.StatusInternalServerError, "INTERNAL_ERROR", "Internal error"
	}
}

func titleFromReason(reason string) string {
	switch reason {
	case "INSUFFICIENT_FUNDS":
		return "Insufficient funds"
	case "ACCOUNT_NOT_ACTIVE":
		return "Account not active"
	case "CURRENCY_MISMATCH":
		return "Currency mismatch"
	case "FX_RATE_NOT_FOUND":
		return "FX rate not found"
	default:
		return reason
	}
}
