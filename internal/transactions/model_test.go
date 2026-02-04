package transactions

import (
	"strings"
	"testing"
	"time"
)

func TestTransactionValidate(t *testing.T) {
	validTime := time.Now().UTC()

	t.Run("valid bet transaction", func(t *testing.T) {
		txn := &Transaction{
			UserID:          "user-1",
			TransactionType: TransactionTypeBet,
			Amount:          10,
			Timestamp:       validTime,
		}
		if err := txn.Validate(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("valid win transaction", func(t *testing.T) {
		txn := &Transaction{
			UserID:          "user-2",
			TransactionType: TransactionTypeWin,
			Amount:          5.5,
			Timestamp:       validTime,
		}
		if err := txn.Validate(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("empty user_id", func(t *testing.T) {
		txn := &Transaction{
			UserID:          "",
			TransactionType: TransactionTypeBet,
			Amount:          10,
			Timestamp:       validTime,
		}
		err := txn.Validate()
		if err == nil {
			t.Fatal("expected error for empty user_id")
		}
		if err.Error() != "user_id is required" {
			t.Errorf("expected 'user_id is required', got %q", err.Error())
		}
	})

	t.Run("whitespace-only user_id", func(t *testing.T) {
		txn := &Transaction{
			UserID:          "   \t  ",
			TransactionType: TransactionTypeBet,
			Amount:          10,
			Timestamp:       validTime,
		}
		if err := txn.Validate(); err == nil {
			t.Fatal("expected error for whitespace-only user_id")
		}
	})

	t.Run("invalid transaction_type", func(t *testing.T) {
		txn := &Transaction{
			UserID:          "user-1",
			TransactionType: "deposit",
			Amount:          10,
			Timestamp:       validTime,
		}
		err := txn.Validate()
		if err == nil {
			t.Fatal("expected error for invalid transaction_type")
		}
		if !strings.Contains(err.Error(), "transaction_type") {
			t.Errorf("expected error about transaction_type, got %q", err.Error())
		}
	})

	t.Run("zero amount", func(t *testing.T) {
		txn := &Transaction{
			UserID:          "user-1",
			TransactionType: TransactionTypeBet,
			Amount:          0,
			Timestamp:       validTime,
		}
		err := txn.Validate()
		if err == nil {
			t.Fatal("expected error for zero amount")
		}
		if err.Error() != "amount must be greater than zero" {
			t.Errorf("expected 'amount must be greater than zero', got %q", err.Error())
		}
	})

	t.Run("negative amount", func(t *testing.T) {
		txn := &Transaction{
			UserID:          "user-1",
			TransactionType: TransactionTypeBet,
			Amount:          -1,
			Timestamp:       validTime,
		}
		if err := txn.Validate(); err == nil {
			t.Fatal("expected error for negative amount")
		}
	})

	t.Run("zero timestamp", func(t *testing.T) {
		txn := &Transaction{
			UserID:          "user-1",
			TransactionType: TransactionTypeBet,
			Amount:          10,
			Timestamp:       time.Time{},
		}
		err := txn.Validate()
		if err == nil {
			t.Fatal("expected error for zero timestamp")
		}
		if err.Error() != "timestamp is required" {
			t.Errorf("expected 'timestamp is required', got %q", err.Error())
		}
	})
}

func TestTransactionMessage_ToTransaction(t *testing.T) {
	ts := time.Date(2025, 2, 4, 12, 0, 0, 0, time.UTC)
	msg := &TransactionMessage{
		UserID:          "user-99",
		TransactionType: TransactionTypeWin,
		Amount:          42.5,
		Timestamp:       ts,
	}
	txn := msg.ToTransaction("msg-id-123")
	if txn == nil {
		t.Fatal("ToTransaction returned nil")
	}
	if txn.MessageID != "msg-id-123" {
		t.Errorf("MessageID: want %q, got %q", "msg-id-123", txn.MessageID)
	}
	if txn.UserID != msg.UserID {
		t.Errorf("UserID: want %q, got %q", msg.UserID, txn.UserID)
	}
	if txn.TransactionType != msg.TransactionType {
		t.Errorf("TransactionType: want %v, got %v", msg.TransactionType, txn.TransactionType)
	}
	if txn.Amount != msg.Amount {
		t.Errorf("Amount: want %v, got %v", msg.Amount, txn.Amount)
	}
	if !txn.Timestamp.Equal(msg.Timestamp) {
		t.Errorf("Timestamp: want %v, got %v", msg.Timestamp, txn.Timestamp)
	}
	if txn.ID != 0 {
		t.Errorf("ID should be zero, got %d", txn.ID)
	}
}

func TestGenerateMessageID(t *testing.T) {
	t.Run("uses key when non-empty", func(t *testing.T) {
		key := []byte("my-key")
		value := []byte(`{"user_id":"u1"}`)
		id := GenerateMessageID(key, value, 100)
		if id != "my-key" {
			t.Errorf("expected message ID %q, got %q", "my-key", id)
		}
	})

	t.Run("hashes value and offset when key is empty", func(t *testing.T) {
		value := []byte(`{"user_id":"u1"}`)
		id1 := GenerateMessageID(nil, value, 0)
		id2 := GenerateMessageID([]byte{}, value, 0)
		if id1 != id2 {
			t.Errorf("nil key and empty key should produce same ID: %q vs %q", id1, id2)
		}
		if len(id1) != 64 {
			t.Errorf("sha256 hex length should be 64, got %d", len(id1))
		}
	})

	t.Run("different offset produces different id", func(t *testing.T) {
		value := []byte("same")
		id1 := GenerateMessageID(nil, value, 1)
		id2 := GenerateMessageID(nil, value, 2)
		if id1 == id2 {
			t.Error("different offsets should produce different IDs")
		}
	})

	t.Run("different value produces different id", func(t *testing.T) {
		id1 := GenerateMessageID(nil, []byte("a"), 0)
		id2 := GenerateMessageID(nil, []byte("b"), 0)
		if id1 == id2 {
			t.Error("different values should produce different IDs")
		}
	})
}

func TestDecodeMessage(t *testing.T) {
	t.Run("valid payload", func(t *testing.T) {
		payload := []byte(`{"user_id":"u1","transaction_type":"bet","amount":10.5,"timestamp":"2025-02-04T12:00:00Z"}`)
		msg, err := DecodeMessage(payload)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if msg == nil {
			t.Fatal("expected non-nil message")
		}
		if msg.UserID != "u1" {
			t.Errorf("UserID: want u1, got %q", msg.UserID)
		}
		if msg.TransactionType != TransactionTypeBet {
			t.Errorf("TransactionType: want bet, got %q", msg.TransactionType)
		}
		if msg.Amount != 10.5 {
			t.Errorf("Amount: want 10.5, got %v", msg.Amount)
		}
	})

	t.Run("invalid JSON", func(t *testing.T) {
		payload := []byte(`{invalid json`)
		msg, err := DecodeMessage(payload)
		if err == nil {
			t.Fatal("expected error for invalid JSON")
		}
		if msg != nil {
			t.Errorf("expected nil message on error, got %+v", msg)
		}
	})

	t.Run("empty payload", func(t *testing.T) {
		payload := []byte(``)
		msg, err := DecodeMessage(payload)
		if err == nil {
			t.Fatal("expected error for empty payload")
		}
		if msg != nil {
			t.Errorf("expected nil message on error, got %+v", msg)
		}
	})

	t.Run("win type decodes", func(t *testing.T) {
		payload := []byte(`{"user_id":"u2","transaction_type":"win","amount":5,"timestamp":"2025-02-04T12:00:00Z"}`)
		msg, err := DecodeMessage(payload)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if msg.TransactionType != TransactionTypeWin {
			t.Errorf("TransactionType: want win, got %q", msg.TransactionType)
		}
	})
}
