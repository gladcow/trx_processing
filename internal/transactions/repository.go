package transactions

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"

	"casino_trxes/internal/logger"
)

// Pool abstracts Exec and Query for the repository (satisfied by *pgxpool.Pool and mocks).
type Pool interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type Repository interface {
	BatchInsert(ctx context.Context, txns []Transaction) error
	List(ctx context.Context, filter Filter) ([]Transaction, *string, error)
}

type Filter struct {
	UserID          string
	TransactionType string
	Limit           int
	CursorTimestamp time.Time
	CursorID        int64
}

// EncodeCursor encodes (timestamp, id) for cursor-based pagination. Format: RFC3339Nano_id.
func EncodeCursor(ts time.Time, id int64) string {
	return ts.UTC().Format(time.RFC3339Nano) + "_" + strconv.FormatInt(id, 10)
}

// DecodeCursor decodes a cursor string into (timestamp, id). Returns zero time and 0 id on parse error.
func DecodeCursor(s string) (time.Time, int64, error) {
	idx := strings.LastIndex(s, "_")
	if idx <= 0 {
		return time.Time{}, 0, fmt.Errorf("invalid cursor format")
	}
	ts, err := time.Parse(time.RFC3339Nano, s[:idx])
	if err != nil {
		return time.Time{}, 0, err
	}
	id, err := strconv.ParseInt(s[idx+1:], 10, 64)
	if err != nil {
		return time.Time{}, 0, err
	}
	return ts, id, nil
}

type PostgresRepository struct {
	pool Pool
}

func NewPostgresRepository(pool Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

func (r *PostgresRepository) BatchInsert(ctx context.Context, txns []Transaction) error {
	if len(txns) == 0 {
		return nil
	}

	// Validate all transactions first
	for i := range txns {
		if err := txns[i].Validate(); err != nil {
			logger.Warnf("transactions: invalid transaction at index %d: %v", i, err)
			return fmt.Errorf("transaction at index %d: %w", i, err)
		}
	}
	logger.Infof("transactions: batch insert %d records", len(txns))

	// Build batch insert query
	query := `INSERT INTO transactions (message_id, user_id, transaction_type, amount, timestamp)
			  VALUES `
	args := make([]any, 0, len(txns)*5)
	placeholders := make([]string, 0, len(txns))

	for i, txn := range txns {
		placeholders = append(placeholders, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d)",
			i*5+1, i*5+2, i*5+3, i*5+4, i*5+5))
		args = append(args, txn.MessageID, txn.UserID, string(txn.TransactionType), txn.Amount, txn.Timestamp)
	}

	query += strings.Join(placeholders, ", ")
	query += " ON CONFLICT (message_id) DO NOTHING"

	_, err := r.pool.Exec(ctx, query, args...)
	if err != nil {
		logger.Errorf("transactions: batch insert failed: %v", err)
		return fmt.Errorf("batch insert: %w", err)
	}

	return nil
}

func (r *PostgresRepository) List(ctx context.Context, filter Filter) ([]Transaction, *string, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	query := `SELECT id, message_id, user_id, transaction_type, amount, timestamp FROM transactions`
	clauses := make([]string, 0, 4)
	args := make([]any, 0, 6)
	if strings.TrimSpace(filter.UserID) != "" {
		args = append(args, filter.UserID)
		clauses = append(clauses, fmt.Sprintf("user_id = $%d", len(args)))
	}
	if filter.TransactionType != "" && filter.TransactionType != "all" {
		args = append(args, filter.TransactionType)
		clauses = append(clauses, fmt.Sprintf("transaction_type = $%d", len(args)))
	}
	if !filter.CursorTimestamp.IsZero() {
		args = append(args, filter.CursorTimestamp, filter.CursorID)
		clauses = append(clauses, fmt.Sprintf("(timestamp, id) < ($%d, $%d)", len(args)-1, len(args)))
	}
	if len(clauses) > 0 {
		query = query + " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY timestamp DESC, id DESC"
	args = append(args, limit+1)
	query += fmt.Sprintf(" LIMIT $%d", len(args))

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		logger.Errorf("transactions: list query failed: %v", err)
		return nil, nil, err
	}
	defer rows.Close()

	transactions := []Transaction{}
	for rows.Next() {
		var txn Transaction
		var ttype string
		var messageID sql.NullString
		if err := rows.Scan(&txn.ID, &messageID, &txn.UserID, &ttype, &txn.Amount, &txn.Timestamp); err != nil {
			return nil, nil, err
		}
		if messageID.Valid {
			txn.MessageID = messageID.String
		}
		txn.TransactionType = TransactionType(ttype)
		transactions = append(transactions, txn)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	var nextCursor *string
	if len(transactions) > limit {
		cursor := EncodeCursor(transactions[limit-1].Timestamp, transactions[limit-1].ID)
		nextCursor = &cursor
		transactions = transactions[:limit]
	}
	return transactions, nextCursor, nil
}

var _ Repository = (*PostgresRepository)(nil)
