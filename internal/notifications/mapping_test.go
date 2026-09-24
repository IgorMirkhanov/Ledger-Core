package notifications_test

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/IgorMirkhanov/ledger-core/internal/notifications"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/outbox"
)

func TestMapEvent(t *testing.T) {
	owner := uuid.Must(uuid.NewV7())
	cases := []struct {
		name     string
		event    string
		data     any
		wantTmpl string
		wantNil  bool
	}{
		{
			name:  "deposit_received",
			event: "account.credited",
			data: map[string]any{
				"account_id": uuid.Must(uuid.NewV7()).String(), "owner_id": owner.String(),
				"entry_id": uuid.Must(uuid.NewV7()).String(), "amount": 100, "currency": "USD",
				"balance_after": 100, "entry_kind": "deposit",
			},
			wantTmpl: notifications.TemplateDepositReceived,
		},
		{
			name:  "transfer_received",
			event: "account.credited",
			data: map[string]any{
				"account_id": uuid.Must(uuid.NewV7()).String(), "owner_id": owner.String(),
				"entry_id": uuid.Must(uuid.NewV7()).String(), "amount": 50, "currency": "RUB",
				"balance_after": 50, "entry_kind": "transfer",
			},
			wantTmpl: notifications.TemplateTransferReceived,
		},
		{
			name:  "credited withdrawal skipped",
			event: "account.credited",
			data: map[string]any{
				"account_id": uuid.Must(uuid.NewV7()).String(), "owner_id": owner.String(),
				"entry_id": uuid.Must(uuid.NewV7()).String(), "amount": 1, "currency": "USD",
				"balance_after": 1, "entry_kind": "withdrawal",
			},
			wantNil: true,
		},
		{
			name:  "transfer_sent",
			event: "transfer.completed",
			data: map[string]any{
				"transfer_id": uuid.Must(uuid.NewV7()).String(), "owner_id": owner.String(),
				"journal_entry_id": uuid.Must(uuid.NewV7()).String(),
				"amount":           100, "currency": "USD", "dest_amount": 9000, "dest_currency": "RUB",
			},
			wantTmpl: notifications.TemplateTransferSent,
		},
		{
			name:  "transfer_failed",
			event: "transfer.failed",
			data: map[string]any{
				"transfer_id": uuid.Must(uuid.NewV7()).String(), "owner_id": owner.String(),
				"failure_code": "INSUFFICIENT_FUNDS", "failure_reason": "no money",
			},
			wantTmpl: notifications.TemplateTransferFailed,
		},
		{
			name:    "unknown event",
			event:   "account.opened",
			data:    map[string]any{"account_id": "x", "owner_id": owner.String(), "currency": "USD"},
			wantNil: true,
		},
		{
			name:    "future event type",
			event:   "account.something_new",
			data:    map[string]any{},
			wantNil: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.data)
			require.NoError(t, err)
			d, err := notifications.MapEvent(outbox.Envelope{
				EventID:   uuid.Must(uuid.NewV7()),
				EventType: tc.event,
				Data:      raw,
			})
			require.NoError(t, err)
			if tc.wantNil {
				require.Nil(t, d)
				return
			}
			require.NotNil(t, d)
			require.Equal(t, tc.wantTmpl, d.Template)
			require.Equal(t, owner, d.UserID)
			require.Equal(t, notifications.ChannelEmail, d.Channel)
		})
	}
}
