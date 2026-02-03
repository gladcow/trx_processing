package transactions

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type TransactionType string

const (
	TransactionTypeBet TransactionType = "bet"
	TransactionTypeWin TransactionType = "win"
)

type Transaction struct {
	ID              int64           `json:"id"`
	MessageID       string          `json:"message_id,omitempty"`
	UserID          string          `json:"user_id"`
	TransactionType TransactionType `json:"transaction_type"`
	Amount          float64         `json:"amount"`
	Timestamp       time.Time       `json:"timestamp"`
}

func (t *Transaction) Validate() error {
	if strings.TrimSpace(t.UserID) == "" {
		return errors.New("user_id is required")
	}
	if t.TransactionType != TransactionTypeBet && t.TransactionType != TransactionTypeWin {
		return errors.New("transaction_type must be bet or win")
	}
	if t.Amount <= 0 {
		return errors.New("amount must be greater than zero")
	}
	if t.Timestamp.IsZero() {
		return errors.New("timestamp is required")
	}
	return nil
}

type TransactionMessage struct {
	UserID          string          `json:"user_id"`
	TransactionType TransactionType `json:"transaction_type"`
	Amount          float64         `json:"amount"`
	Timestamp       time.Time       `json:"timestamp"`
}

func (m *TransactionMessage) ToTransaction(messageID string) *Transaction {
	return &Transaction{
		MessageID:       messageID,
		UserID:          m.UserID,
		TransactionType: m.TransactionType,
		Amount:          m.Amount,
		Timestamp:       m.Timestamp,
	}
}

// GenerateMessageID creates a unique message ID from Kafka message key or content+offset
func GenerateMessageID(key []byte, value []byte, offset int64) string {
	if len(key) > 0 {
		return string(key)
	}
	// Generate hash from content + offset for idempotency
	hash := sha256.Sum256(append(value, []byte(fmt.Sprintf("%d", offset))...))
	return hex.EncodeToString(hash[:])
}

func DecodeMessage(payload []byte) (*TransactionMessage, error) {
	var msg TransactionMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}
