package transactions

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"casino_trxes/internal/db"

	"github.com/driftprogramming/pgxpoolmock"
	"github.com/golang/mock/gomock"
	"github.com/jackc/pgconn"
)

// --- Unit tests with pgxpoolmock ---

func TestPostgresRepository_BatchInsert_EmptySlice(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPool := pgxpoolmock.NewMockPgxPool(ctrl)
	repo := NewPostgresRepository(mockPool)
	ctx := context.Background()

	// No Exec call expected
	err := repo.BatchInsert(ctx, nil)
	if err != nil {
		t.Fatalf("BatchInsert(nil): %v", err)
	}
	err = repo.BatchInsert(ctx, []Transaction{})
	if err != nil {
		t.Fatalf("BatchInsert(empty): %v", err)
	}
}

func TestPostgresRepository_BatchInsert_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPool := pgxpoolmock.NewMockPgxPool(ctrl)
	repo := NewPostgresRepository(mockPool)
	ctx := context.Background()
	now := time.Now().UTC()

	txn := Transaction{
		MessageID:       "msg-1",
		UserID:          "user-1",
		TransactionType: TransactionTypeBet,
		Amount:          10,
		Timestamp:       now,
	}
	query := `INSERT INTO transactions (message_id, user_id, transaction_type, amount, timestamp)
			  VALUES ($1, $2, $3, $4, $5) ON CONFLICT (message_id) DO NOTHING`
	mockPool.EXPECT().
		Exec(gomock.Any(), query, "msg-1", "user-1", "bet", 10.0, gomock.Any()).
		Return(pgconn.CommandTag([]byte("INSERT 0 1")), nil)

	err := repo.BatchInsert(ctx, []Transaction{txn})
	if err != nil {
		t.Fatalf("BatchInsert: %v", err)
	}
}

func TestPostgresRepository_BatchInsert_MultipleRows(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPool := pgxpoolmock.NewMockPgxPool(ctrl)
	repo := NewPostgresRepository(mockPool)
	ctx := context.Background()
	now := time.Now().UTC()

	txns := []Transaction{
		{MessageID: "m1", UserID: "u1", TransactionType: TransactionTypeBet, Amount: 10, Timestamp: now},
		{MessageID: "m2", UserID: "u2", TransactionType: TransactionTypeWin, Amount: 20, Timestamp: now},
	}
	query := `INSERT INTO transactions (message_id, user_id, transaction_type, amount, timestamp)
			  VALUES ($1, $2, $3, $4, $5), ($6, $7, $8, $9, $10) ON CONFLICT (message_id) DO NOTHING`
	mockPool.EXPECT().
		Exec(gomock.Any(), query, "m1", "u1", "bet", 10.0, gomock.Any(), "m2", "u2", "win", 20.0, gomock.Any()).
		Return(pgconn.CommandTag([]byte("INSERT 0 2")), nil)

	err := repo.BatchInsert(ctx, txns)
	if err != nil {
		t.Fatalf("BatchInsert: %v", err)
	}
}

func TestPostgresRepository_BatchInsert_ValidationError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPool := pgxpoolmock.NewMockPgxPool(ctrl)
	repo := NewPostgresRepository(mockPool)
	ctx := context.Background()
	now := time.Now().UTC()

	// Invalid: empty UserID - no Exec call
	txns := []Transaction{
		{MessageID: "m1", UserID: "", TransactionType: TransactionTypeBet, Amount: 10, Timestamp: now},
	}
	err := repo.BatchInsert(ctx, txns)
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "index") {
		t.Errorf("error should mention index: %q", err.Error())
	}
}

func TestPostgresRepository_BatchInsert_ExecError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPool := pgxpoolmock.NewMockPgxPool(ctrl)
	repo := NewPostgresRepository(mockPool)
	ctx := context.Background()
	now := time.Now().UTC()
	execErr := errors.New("db error")

	txn := Transaction{
		MessageID:       "msg-1",
		UserID:          "user-1",
		TransactionType: TransactionTypeBet,
		Amount:          10,
		Timestamp:       now,
	}
	mockPool.EXPECT().
		Exec(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(pgconn.CommandTag(nil), execErr)

	err := repo.BatchInsert(ctx, []Transaction{txn})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "batch insert") {
		t.Errorf("expected wrapped error containing 'batch insert', got %q", err.Error())
	}
}

func TestPostgresRepository_List_NoFilter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPool := pgxpoolmock.NewMockPgxPool(ctrl)
	repo := NewPostgresRepository(mockPool)
	ctx := context.Background()

	columns := []string{"id", "message_id", "user_id", "transaction_type", "amount", "timestamp"}
	ts := time.Date(2025, 2, 4, 12, 0, 0, 0, time.UTC)
	pgxRows := pgxpoolmock.NewRows(columns).
		AddRow(int64(1), sql.NullString{String: "msg-1", Valid: true}, "user-1", "bet", 10.5, ts).
		ToPgxRows()

	query := `SELECT id, message_id, user_id, transaction_type, amount, timestamp FROM transactions ORDER BY timestamp DESC`
	mockPool.EXPECT().
		Query(gomock.Any(), query).
		Return(pgxRows, nil)

	rows, err := repo.List(ctx, Filter{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0].ID != 1 || rows[0].MessageID != "msg-1" || rows[0].UserID != "user-1" ||
		rows[0].TransactionType != TransactionTypeBet || rows[0].Amount != 10.5 {
		t.Errorf("unexpected row: %+v", rows[0])
	}
}

func TestPostgresRepository_List_FilterByUserID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPool := pgxpoolmock.NewMockPgxPool(ctrl)
	repo := NewPostgresRepository(mockPool)
	ctx := context.Background()

	columns := []string{"id", "message_id", "user_id", "transaction_type", "amount", "timestamp"}
	ts := time.Date(2025, 2, 4, 12, 0, 0, 0, time.UTC)
	pgxRows := pgxpoolmock.NewRows(columns).
		AddRow(int64(1), sql.NullString{String: "msg-1", Valid: true}, "user-1", "bet", 10.0, ts).
		ToPgxRows()

	query := `SELECT id, message_id, user_id, transaction_type, amount, timestamp FROM transactions WHERE user_id = $1 ORDER BY timestamp DESC`
	mockPool.EXPECT().
		Query(gomock.Any(), query, "user-1").
		Return(pgxRows, nil)

	rows, err := repo.List(ctx, Filter{UserID: "user-1"})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(rows) != 1 || rows[0].UserID != "user-1" {
		t.Errorf("unexpected rows: %+v", rows)
	}
}

func TestPostgresRepository_List_FilterByTransactionType(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPool := pgxpoolmock.NewMockPgxPool(ctrl)
	repo := NewPostgresRepository(mockPool)
	ctx := context.Background()

	columns := []string{"id", "message_id", "user_id", "transaction_type", "amount", "timestamp"}
	ts := time.Date(2025, 2, 4, 12, 0, 0, 0, time.UTC)
	pgxRows := pgxpoolmock.NewRows(columns).
		AddRow(int64(1), sql.NullString{String: "msg-1", Valid: true}, "user-1", "win", 25.0, ts).
		ToPgxRows()

	query := `SELECT id, message_id, user_id, transaction_type, amount, timestamp FROM transactions WHERE transaction_type = $1 ORDER BY timestamp DESC`
	mockPool.EXPECT().
		Query(gomock.Any(), query, "win").
		Return(pgxRows, nil)

	rows, err := repo.List(ctx, Filter{TransactionType: "win"})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(rows) != 1 || rows[0].TransactionType != TransactionTypeWin {
		t.Errorf("unexpected rows: %+v", rows)
	}
}

func TestPostgresRepository_List_BothFilters(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPool := pgxpoolmock.NewMockPgxPool(ctrl)
	repo := NewPostgresRepository(mockPool)
	ctx := context.Background()

	columns := []string{"id", "message_id", "user_id", "transaction_type", "amount", "timestamp"}
	ts := time.Date(2025, 2, 4, 12, 0, 0, 0, time.UTC)
	pgxRows := pgxpoolmock.NewRows(columns).
		AddRow(int64(1), sql.NullString{String: "msg-1", Valid: true}, "user-1", "bet", 10.0, ts).
		ToPgxRows()

	query := `SELECT id, message_id, user_id, transaction_type, amount, timestamp FROM transactions WHERE user_id = $1 AND transaction_type = $2 ORDER BY timestamp DESC`
	mockPool.EXPECT().
		Query(gomock.Any(), query, "user-1", "bet").
		Return(pgxRows, nil)

	rows, err := repo.List(ctx, Filter{UserID: "user-1", TransactionType: "bet"})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(rows))
	}
}

func TestPostgresRepository_List_EmptyResult(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPool := pgxpoolmock.NewMockPgxPool(ctrl)
	repo := NewPostgresRepository(mockPool)
	ctx := context.Background()

	columns := []string{"id", "message_id", "user_id", "transaction_type", "amount", "timestamp"}
	pgxRows := pgxpoolmock.NewRows(columns).ToPgxRows()

	query := `SELECT id, message_id, user_id, transaction_type, amount, timestamp FROM transactions ORDER BY timestamp DESC`
	mockPool.EXPECT().
		Query(gomock.Any(), query).
		Return(pgxRows, nil)

	rows, err := repo.List(ctx, Filter{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rows))
	}
}

func TestPostgresRepository_List_QueryError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPool := pgxpoolmock.NewMockPgxPool(ctrl)
	repo := NewPostgresRepository(mockPool)
	ctx := context.Background()
	queryErr := errors.New("query failed")

	query := `SELECT id, message_id, user_id, transaction_type, amount, timestamp FROM transactions ORDER BY timestamp DESC`
	mockPool.EXPECT().
		Query(gomock.Any(), query).
		Return(nil, queryErr)

	rows, err := repo.List(ctx, Filter{})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, queryErr) {
		t.Errorf("expected queryErr, got %v", err)
	}
	if rows != nil {
		t.Errorf("expected nil rows on error, got %v", rows)
	}
}

func TestPostgresRepositoryListFilters(t *testing.T) {
	postgresURL := os.Getenv("POSTGRES_URL")
	if postgresURL == "" {
		t.Skip("POSTGRES_URL not set")
	}

	ctx := context.Background()
	pool, err := db.Connect(ctx, postgresURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	root, err := projectRoot()
	if err != nil {
		t.Fatalf("project root: %v", err)
	}
	if err := db.ApplyMigrations(ctx, pool, filepath.Join(root, "migrations")); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if _, err := pool.Exec(ctx, "DELETE FROM transactions"); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	repo := NewPostgresRepository(pool)
	now := time.Now().UTC()
	if err := repo.BatchInsert(ctx, []Transaction{
		{
			MessageID:       "msg-1",
			UserID:          "user-1",
			TransactionType: TransactionTypeBet,
			Amount:          10,
			Timestamp:       now.Add(-time.Minute),
		},
		{
			MessageID:       "msg-2",
			UserID:          "user-2",
			TransactionType: TransactionTypeWin,
			Amount:          25,
			Timestamp:       now,
		},
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	rows, err := repo.List(ctx, Filter{UserID: "user-1", TransactionType: "all"})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 1 || rows[0].UserID != "user-1" {
		t.Fatalf("unexpected rows: %+v", rows)
	}

	rows, err = repo.List(ctx, Filter{TransactionType: "win"})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 1 || rows[0].TransactionType != TransactionTypeWin {
		t.Fatalf("unexpected rows: %+v", rows)
	}
}

func TestPostgresRepositoryBatchInsert(t *testing.T) {
	postgresURL := os.Getenv("POSTGRES_URL")
	if postgresURL == "" {
		t.Skip("POSTGRES_URL not set")
	}

	ctx := context.Background()
	pool, err := db.Connect(ctx, postgresURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	root, err := projectRoot()
	if err != nil {
		t.Fatalf("project root: %v", err)
	}
	if err := db.ApplyMigrations(ctx, pool, filepath.Join(root, "migrations")); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if _, err := pool.Exec(ctx, "DELETE FROM transactions"); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	repo := NewPostgresRepository(pool)
	now := time.Now().UTC()
	txns := []Transaction{
		{MessageID: "batch-msg-1", UserID: "user-1", TransactionType: TransactionTypeBet, Amount: 10, Timestamp: now},
		{MessageID: "batch-msg-2", UserID: "user-2", TransactionType: TransactionTypeWin, Amount: 20, Timestamp: now},
		{MessageID: "batch-msg-3", UserID: "user-3", TransactionType: TransactionTypeBet, Amount: 30, Timestamp: now},
	}

	if err := repo.BatchInsert(ctx, txns); err != nil {
		t.Fatalf("batch insert: %v", err)
	}

	rows, err := repo.List(ctx, Filter{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

func TestPostgresRepositoryIdempotency(t *testing.T) {
	postgresURL := os.Getenv("POSTGRES_URL")
	if postgresURL == "" {
		t.Skip("POSTGRES_URL not set")
	}

	ctx := context.Background()
	pool, err := db.Connect(ctx, postgresURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	root, err := projectRoot()
	if err != nil {
		t.Fatalf("project root: %v", err)
	}
	if err := db.ApplyMigrations(ctx, pool, filepath.Join(root, "migrations")); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if _, err := pool.Exec(ctx, "DELETE FROM transactions"); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	repo := NewPostgresRepository(pool)
	now := time.Now().UTC()
	// Test batch insert idempotency
	txns := []Transaction{
		{MessageID: "duplicate-msg-1", UserID: "user-1", TransactionType: TransactionTypeBet, Amount: 10, Timestamp: now},
		{MessageID: "batch-msg-new", UserID: "user-2", TransactionType: TransactionTypeWin, Amount: 20, Timestamp: now},
	}

	if err := repo.BatchInsert(ctx, txns); err != nil {
		t.Fatalf("batch insert with duplicate: %v", err)
	}
	if err := repo.BatchInsert(ctx, txns); err != nil {
		t.Fatalf("batch insert with duplicate: %v", err)
	}

	// Should have 2 rows total
	rows, err := repo.List(ctx, Filter{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func projectRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", os.ErrNotExist
		}
		dir = parent
	}
}
