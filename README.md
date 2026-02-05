# Transaction Management System

This service consumes bet/win transactions from Kafka, stores them in PostgreSQL, and exposes an HTTP API to query transactions.

## Local Setup
1. Start dependencies:
   - `docker compose up -d`
2. Create the Kafka topic:
   - `docker compose exec kafka kafka-topics --create --topic casino.transactions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1`
3. Run the service:
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
`GET /transactions`

Query params:
- `user_id` (optional)
- `transaction_type` (`bet`, `win`, or `all`, default `all`)

Example:
```
curl "http://localhost:8080/transactions?user_id=user-123&transaction_type=bet"
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
