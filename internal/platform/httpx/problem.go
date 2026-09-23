package httpx

import (
	"encoding/json"
	"net/http"
)

// Problem is an RFC 7807 error response (application/problem+json).
type Problem struct {
	Type      string `json:"type"`             // e.g. "https://ledger-core.dev/errors/insufficient-funds"
	Title     string `json:"title"`            // short human-readable summary
	Status    int    `json:"status"`           // HTTP status
	Detail    string `json:"detail,omitempty"` // human-readable details
	Code      string `json:"code"`             // machine-readable, e.g. INSUFFICIENT_FUNDS
	RequestID string `json:"request_id,omitempty"`
}

// WriteProblem writes p as application/problem+json.
func WriteProblem(w http.ResponseWriter, p Problem) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(p.Status)
	_ = json.NewEncoder(w).Encode(p)
}

// WriteJSON writes v with the given status.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
