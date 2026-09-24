package notifications

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/outbox"
)

const (
	TemplateDepositReceived  = "deposit_received"
	TemplateTransferSent     = "transfer_sent"
	TemplateTransferReceived = "transfer_received"
	TemplateTransferFailed   = "transfer_failed"

	ChannelEmail = "email"
)

// Draft is a notification to insert after a successfully claimed event.
type Draft struct {
	UserID   uuid.UUID
	Channel  string
	Template string
	Payload  json.RawMessage
}

// MapEvent maps a ledger event to a notification draft.
// A nil draft means the event is acknowledged but produces no notification
// (unknown type or non-notifying variant).
func MapEvent(env outbox.Envelope) (*Draft, error) {
	switch env.EventType {
	case "account.credited":
		var data struct {
			AccountID    string `json:"account_id"`
			OwnerID      string `json:"owner_id"`
			EntryID      string `json:"entry_id"`
			Amount       int64  `json:"amount"`
			Currency     string `json:"currency"`
			BalanceAfter int64  `json:"balance_after"`
			EntryKind    string `json:"entry_kind"`
		}
		if err := json.Unmarshal(env.Data, &data); err != nil {
			return nil, fmt.Errorf("account.credited data: %w", err)
		}
		owner, err := uuid.Parse(data.OwnerID)
		if err != nil {
			return nil, fmt.Errorf("account.credited owner_id: %w", err)
		}
		switch data.EntryKind {
		case "deposit":
			return draft(owner, TemplateDepositReceived, env.Data)
		case "transfer":
			return draft(owner, TemplateTransferReceived, env.Data)
		default:
			return nil, nil
		}
	case "transfer.completed":
		var data struct {
			TransferID     string `json:"transfer_id"`
			OwnerID        string `json:"owner_id"`
			JournalEntryID string `json:"journal_entry_id"`
			Amount         int64  `json:"amount"`
			Currency       string `json:"currency"`
			DestAmount     int64  `json:"dest_amount"`
			DestCurrency   string `json:"dest_currency"`
		}
		if err := json.Unmarshal(env.Data, &data); err != nil {
			return nil, fmt.Errorf("transfer.completed data: %w", err)
		}
		owner, err := uuid.Parse(data.OwnerID)
		if err != nil {
			return nil, fmt.Errorf("transfer.completed owner_id: %w", err)
		}
		return draft(owner, TemplateTransferSent, env.Data)
	case "transfer.failed":
		var data struct {
			TransferID    string `json:"transfer_id"`
			OwnerID       string `json:"owner_id"`
			FailureCode   string `json:"failure_code"`
			FailureReason string `json:"failure_reason"`
		}
		if err := json.Unmarshal(env.Data, &data); err != nil {
			return nil, fmt.Errorf("transfer.failed data: %w", err)
		}
		owner, err := uuid.Parse(data.OwnerID)
		if err != nil {
			return nil, fmt.Errorf("transfer.failed owner_id: %w", err)
		}
		return draft(owner, TemplateTransferFailed, env.Data)
	default:
		// Unknown event_type: claim in inbox, skip notification (forward compatibility).
		return nil, nil
	}
}

func draft(user uuid.UUID, tmpl string, payload json.RawMessage) (*Draft, error) {
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	return &Draft{
		UserID:   user,
		Channel:  ChannelEmail,
		Template: tmpl,
		Payload:  payload,
	}, nil
}
