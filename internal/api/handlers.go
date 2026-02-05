package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"casino_trxes/internal/logger"
	"casino_trxes/internal/transactions"
)

type Server struct {
	repo         transactions.Repository
	listMaxLimit int
}

func NewServer(repo transactions.Repository, listMaxLimit int) *Server {
	return &Server{repo: repo, listMaxLimit: listMaxLimit}
}

func (s *Server) Routes() http.Handler {
	r := chi.NewRouter()
	r.Get("/transactions", s.handleListTransactions)
	return r
}

type listTransactionsResponse struct {
	Items      []transactions.Transaction `json:"items"`
	NextCursor string                    `json:"next_cursor,omitempty"`
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

	limit := s.listMaxLimit
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > s.listMaxLimit {
		limit = s.listMaxLimit
	}
	if limit < 1 {
		limit = 1
	}

	var cursorTimestamp time.Time
	var cursorID int64
	if raw := strings.TrimSpace(r.URL.Query().Get("cursor")); raw != "" {
		ts, id, err := transactions.DecodeCursor(raw)
		if err != nil {
			logger.Warnf("api: invalid cursor %q: %v", raw, err)
			writeError(w, http.StatusBadRequest, "invalid cursor")
			return
		}
		cursorTimestamp = ts
		cursorID = id
	}

	filter := transactions.Filter{
		UserID:          userID,
		TransactionType: txType,
		Limit:           limit,
		CursorTimestamp: cursorTimestamp,
		CursorID:        cursorID,
	}

	logger.Infof("api: list transactions user_id=%q transaction_type=%s limit=%d", userID, txType, limit)
	items, nextCursor, err := s.repo.List(r.Context(), filter)
	if err != nil {
		logger.Errorf("api: list transactions failed: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to fetch transactions")
		return
	}

	logger.Infof("api: list transactions returned %d items", len(items))
	resp := listTransactionsResponse{Items: items}
	if nextCursor != nil {
		resp.NextCursor = *nextCursor
	}
	writeJSON(w, http.StatusOK, resp)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
