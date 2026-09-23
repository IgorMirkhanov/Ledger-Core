package service

type transferCreated struct {
	TransferID      string `json:"transfer_id"`
	OwnerID         string `json:"owner_id"`
	SourceAccountID string `json:"source_account_id"`
	DestAccountID   string `json:"dest_account_id"`
	Amount          int64  `json:"amount"`
	Currency        string `json:"currency"`
	DestAmount      int64  `json:"dest_amount"`
	DestCurrency    string `json:"dest_currency"`
}

type transferCompleted struct {
	TransferID     string `json:"transfer_id"`
	OwnerID        string `json:"owner_id"`
	JournalEntryID string `json:"journal_entry_id"`
	Amount         int64  `json:"amount"`
	Currency       string `json:"currency"`
	DestAmount     int64  `json:"dest_amount"`
	DestCurrency   string `json:"dest_currency"`
}

type transferFailed struct {
	TransferID    string `json:"transfer_id"`
	OwnerID       string `json:"owner_id"`
	FailureCode   string `json:"failure_code"`
	FailureReason string `json:"failure_reason"`
}
