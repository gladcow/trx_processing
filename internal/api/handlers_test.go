package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"casino_trxes/internal/transactions"
)

type repoStub struct {
	filter      transactions.Filter
	result      []transactions.Transaction
	nextCursor  *string
	err         error
}

func (r *repoStub) Insert(ctx context.Context, txn *transactions.Transaction) error {
	return nil
}

func (r *repoStub) BatchInsert(ctx context.Context, txns []transactions.Transaction) error {
	return nil
}

func (r *repoStub) List(ctx context.Context, filter transactions.Filter) ([]transactions.Transaction, *string, error) {
	r.filter = filter
	return r.result, r.nextCursor, r.err
}

func TestListTransactionsDefaultAll(t *testing.T) {
	now := time.Now().UTC()
	repo := &repoStub{
		result: []transactions.Transaction{
			{ID: 1, UserID: "user-1", TransactionType: transactions.TransactionTypeBet, Amount: 10, Timestamp: now},
		},
	}
	server := NewServer(repo, 100)

	req := httptest.NewRequest(http.MethodGet, "/transactions?user_id=user-1", nil)
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var payload listTransactionsResponse
	if err := json.NewDecoder(rec.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(payload.Items) != 1 || payload.Items[0].UserID != "user-1" {
		t.Fatalf("unexpected payload: %+v", payload)
	}
	if repo.filter.TransactionType != "all" {
		t.Fatalf("expected filter all, got %s", repo.filter.TransactionType)
	}
	if repo.filter.Limit != 100 {
		t.Fatalf("expected limit 100 (default), got %d", repo.filter.Limit)
	}
}

func TestListTransactionsInvalidType(t *testing.T) {
	repo := &repoStub{}
	server := NewServer(repo, 100)

	req := httptest.NewRequest(http.MethodGet, "/transactions?transaction_type=bad", nil)
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}
}

func TestListTransactionsInvalidCursor(t *testing.T) {
	repo := &repoStub{}
	server := NewServer(repo, 100)

	req := httptest.NewRequest(http.MethodGet, "/transactions?cursor=invalid", nil)
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}
}

func TestListTransactionsLimitCapped(t *testing.T) {
	repo := &repoStub{result: []transactions.Transaction{}}
	server := NewServer(repo, 50)

	req := httptest.NewRequest(http.MethodGet, "/transactions?limit=999", nil)
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	if repo.filter.Limit != 50 {
		t.Fatalf("expected limit capped at 50, got %d", repo.filter.Limit)
	}
}

func TestListTransactionsWithNextCursor(t *testing.T) {
	now := time.Now().UTC()
	next := "2025-02-05T11:00:00Z_42"
	repo := &repoStub{
		result: []transactions.Transaction{
			{ID: 1, UserID: "u1", TransactionType: transactions.TransactionTypeBet, Amount: 10, Timestamp: now},
		},
		nextCursor: &next,
	}
	server := NewServer(repo, 100)

	req := httptest.NewRequest(http.MethodGet, "/transactions?limit=10", nil)
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	var payload listTransactionsResponse
	if err := json.NewDecoder(rec.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.NextCursor != next {
		t.Fatalf("expected next_cursor %q, got %q", next, payload.NextCursor)
	}
	if len(payload.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(payload.Items))
	}
}
