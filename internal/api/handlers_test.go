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
	filter transactions.Filter
	result []transactions.Transaction
	err    error
}

func (r *repoStub) Insert(ctx context.Context, txn *transactions.Transaction) error {
	return nil
}

func (r *repoStub) BatchInsert(ctx context.Context, txns []transactions.Transaction) error {
	return nil
}

func (r *repoStub) List(ctx context.Context, filter transactions.Filter) ([]transactions.Transaction, error) {
	r.filter = filter
	return r.result, r.err
}

func TestListTransactionsDefaultAll(t *testing.T) {
	now := time.Now().UTC()
	repo := &repoStub{
		result: []transactions.Transaction{
			{ID: 1, UserID: "user-1", TransactionType: transactions.TransactionTypeBet, Amount: 10, Timestamp: now},
		},
	}
	server := NewServer(repo)

	req := httptest.NewRequest(http.MethodGet, "/transactions?user_id=user-1", nil)
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var payload []transactions.Transaction
	if err := json.NewDecoder(rec.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(payload) != 1 || payload[0].UserID != "user-1" {
		t.Fatalf("unexpected payload: %+v", payload)
	}
	if repo.filter.TransactionType != "all" {
		t.Fatalf("expected filter all, got %s", repo.filter.TransactionType)
	}
}

func TestListTransactionsInvalidType(t *testing.T) {
	repo := &repoStub{}
	server := NewServer(repo)

	req := httptest.NewRequest(http.MethodGet, "/transactions?transaction_type=bad", nil)
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}
}
