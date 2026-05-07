-- idempotency_keys: request deduplication with 24h TTL

CREATE TABLE idempotency_keys (
  key                TEXT PRIMARY KEY,
  response_card_id   UUID,
  response_payload   JSONB,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Plain (non-partial) index. A periodic cleanup job runs `DELETE FROM
-- idempotency_keys WHERE created_at < now() - INTERVAL '1 day'`; partial-index
-- predicates can't reference `now()` because it isn't immutable.
CREATE INDEX idx_idempotency_keys_created ON idempotency_keys(created_at);
