package transactions

import (
	"testing"
	"time"
)

func TestTransactionValidate(t *testing.T) {
	txn := &Transaction{
		UserID:          "user-1",
		TransactionType: TransactionTypeBet,
		Amount:          10,
		Timestamp:       time.Now().UTC(),
	}
	if err := txn.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	txn.UserID = ""
	if err := txn.Validate(); err == nil {
		t.Fatalf("expected error for empty user_id")
	}
}
