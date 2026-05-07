-- event_log: transactional outbox for JetStream dispatch

CREATE TABLE event_log (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  board_id      UUID NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
  event_type    TEXT NOT NULL,
  payload       JSONB NOT NULL,
  nats_seq      BIGINT,
  dispatched_at TIMESTAMPTZ,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_event_log_board ON event_log(board_id);
CREATE INDEX idx_event_log_dispatched ON event_log(dispatched_at) WHERE dispatched_at IS NULL;
