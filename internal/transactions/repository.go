package transactions

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository interface {
	Insert(ctx context.Context, txn *Transaction) error
	BatchInsert(ctx context.Context, txns []Transaction) error
	List(ctx context.Context, filter Filter) ([]Transaction, error)
}

type Filter struct {
	UserID          string
	TransactionType string
}

type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

func (r *PostgresRepository) Insert(ctx context.Context, txn *Transaction) error {
	if txn == nil {
		return fmt.Errorf("transaction is nil")
	}
	if err := txn.Validate(); err != nil {
		return err
	}
	_, err := r.pool.Exec(
		ctx,
		`INSERT INTO transactions (message_id, user_id, transaction_type, amount, timestamp)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (message_id) DO NOTHING`,
		txn.MessageID,
		txn.UserID,
		string(txn.TransactionType),
		txn.Amount,
		txn.Timestamp,
	)
	return err
}

func (r *PostgresRepository) BatchInsert(ctx context.Context, txns []Transaction) error {
	if len(txns) == 0 {
		return nil
	}

	// Validate all transactions first
	for i := range txns {
		if err := txns[i].Validate(); err != nil {
			return fmt.Errorf("transaction at index %d: %w", i, err)
		}
	}

	// Use a transaction to ensure atomicity
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

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

	_, err = tx.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("batch insert: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (r *PostgresRepository) List(ctx context.Context, filter Filter) ([]Transaction, error) {
	query := `SELECT id, message_id, user_id, transaction_type, amount, timestamp FROM transactions`
	clauses := make([]string, 0, 2)
	args := make([]any, 0, 2)
	if strings.TrimSpace(filter.UserID) != "" {
		args = append(args, filter.UserID)
		clauses = append(clauses, fmt.Sprintf("user_id = $%d", len(args)))
	}
	if filter.TransactionType != "" && filter.TransactionType != "all" {
		args = append(args, filter.TransactionType)
		clauses = append(clauses, fmt.Sprintf("transaction_type = $%d", len(args)))
	}
	if len(clauses) > 0 {
		query = query + " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY timestamp DESC"

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	transactions := []Transaction{}
	for rows.Next() {
		var txn Transaction
		var ttype string
		var messageID sql.NullString
		if err := rows.Scan(&txn.ID, &messageID, &txn.UserID, &ttype, &txn.Amount, &txn.Timestamp); err != nil {
			return nil, err
		}
		if messageID.Valid {
			txn.MessageID = messageID.String
		}
		txn.TransactionType = TransactionType(ttype)
		transactions = append(transactions, txn)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return transactions, nil
}

var _ Repository = (*PostgresRepository)(nil)
