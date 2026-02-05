# Transaction Management System

This service consumes bet/win transactions from Kafka, stores them in PostgreSQL, and exposes an HTTP API to query transactions.
In local tests service can consume up to 45k txes/sec 

## Local Setup
1. Start dependencies:
   - `docker compose up -d`
2. Run the service:
   - `go run ./cmd/server`

To generate a stream of random transactions into Kafka (e.g. for load testing), run the test utility until Ctrl+C:
   - `go run ./cmd/tx_generator` (uses `KAFKA_BROKERS` and `KAFKA_TOPIC` from env, same defaults as above)

## Configuration
Environment variables:
- `HTTP_ADDR` (default `:8080`)
- `KAFKA_BROKERS` (default `localhost:9092`)
- `KAFKA_TOPIC` (default `casino.transactions`)
- `KAFKA_GROUP_ID` (default `casino_txn_consumer`)
- `POSTGRES_URL` (default `postgres://postgres:postgres@localhost:5432/casino?sslmode=disable`)
- `MIGRATIONS_PATH` (default `migrations`)
- `HTTP_READ_TIMEOUT` (default `10s`)
- `HTTP_WRITE_TIMEOUT` (default `10s`)
- `LIST_TRANSACTIONS_MAX_LIMIT` (default `100`) — max page size for list transactions

## Message Format
Kafka messages are JSON:
```json
{
  "user_id": "user-123",
  "transaction_type": "bet",
  "amount": 12.5,
  "timestamp": "2026-01-29T12:34:56Z"
}
```

## API
`GET /transactions` — list transactions with cursor-based pagination.

Query params:
- `user_id` (optional)
- `transaction_type` (`bet`, `win`, or `all`, default `all`)
- `limit` (optional, capped by `LIST_TRANSACTIONS_MAX_LIMIT`)
- `cursor` (optional, from previous response’s `next_cursor` for next page)

Response: `{"items": [...], "next_cursor": "..." }`. `next_cursor` is omitted on the last page.

Example:
```
curl "http://localhost:8080/transactions?user_id=user-123&limit=20"
```

## Tests
Unit tests run normally:
```
go test ./... -cover
```

Integration tests for the repository run when `POSTGRES_URL` is set and reachable:
```
POSTGRES_URL=postgres://postgres:postgres@localhost:5432/casino?sslmode=disable go test ./internal/transactions -run TestPostgresRepositoryListFilters
```
