-- idempotency_keys: request deduplication with 24h TTL

CREATE TABLE idempotency_keys (
  key                TEXT PRIMARY KEY,
  response_card_id   UUID,
  response_payload   JSONB,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_idempotency_keys_created ON idempotency_keys(created_at) WHERE created_at > now() - INTERVAL '1 day';
