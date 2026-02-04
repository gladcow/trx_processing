package api

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"casino_trxes/internal/logger"
	"casino_trxes/internal/transactions"
)

type Server struct {
	repo transactions.Repository
}

func NewServer(repo transactions.Repository) *Server {
	return &Server{repo: repo}
}

func (s *Server) Routes() http.Handler {
	r := chi.NewRouter()
	r.Get("/transactions", s.handleListTransactions)
	return r
}

func (s *Server) handleListTransactions(w http.ResponseWriter, r *http.Request) {
	userID := strings.TrimSpace(r.URL.Query().Get("user_id"))
	txType := strings.TrimSpace(r.URL.Query().Get("transaction_type"))
	if txType == "" {
		txType = "all"
	}
	if txType != "all" && txType != string(transactions.TransactionTypeBet) && txType != string(transactions.TransactionTypeWin) {
		logger.Warnf("api: invalid transaction_type %q", txType)
		writeError(w, http.StatusBadRequest, "transaction_type must be bet, win, or all")
		return
	}

	logger.Infof("api: list transactions user_id=%q transaction_type=%s", userID, txType)
	items, err := s.repo.List(r.Context(), transactions.Filter{
		UserID:          userID,
		TransactionType: txType,
	})
	if err != nil {
		logger.Errorf("api: list transactions failed: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to fetch transactions")
		return
	}

	logger.Infof("api: list transactions returned %d items", len(items))
	writeJSON(w, http.StatusOK, items)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
