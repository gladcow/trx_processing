CREATE TABLE IF NOT EXISTS transactions (
    id BIGSERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    transaction_type TEXT NOT NULL CHECK (transaction_type IN ('bet', 'win')),
    amount DOUBLE PRECISION NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    message_id TEXT NOT NULL UNIQUE
);

CREATE INDEX IF NOT EXISTS transactions_user_id_idx ON transactions (user_id);
CREATE INDEX IF NOT EXISTS transactions_type_idx ON transactions (transaction_type);
CREATE UNIQUE INDEX IF NOT EXISTS transactions_message_id_idx ON transactions (message_id) WHERE message_id IS NOT NULL;

