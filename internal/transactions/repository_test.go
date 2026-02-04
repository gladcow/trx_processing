package transactions

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"casino_trxes/internal/db"
)

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
