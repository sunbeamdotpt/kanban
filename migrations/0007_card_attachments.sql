-- Ported from apps/kanban-old/server/migrate.ts migration 7
-- card_attachments: file uploads associated with cards

CREATE TABLE card_attachments (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  card_id     UUID NOT NULL REFERENCES cards(id) ON DELETE CASCADE,
  filename    TEXT NOT NULL,
  mimetype    TEXT NOT NULL DEFAULT 'application/octet-stream',
  size        BIGINT NOT NULL DEFAULT 0,
  s3_key      TEXT NOT NULL,
  uploaded_by TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_card_attachments_card ON card_attachments(card_id);
