-- card_assignees: join table for OIDC subjects assigned to cards

CREATE TABLE card_assignees (
  card_id     UUID NOT NULL REFERENCES cards(id) ON DELETE CASCADE,
  subject     TEXT NOT NULL,
  assigned_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (card_id, subject)
);

CREATE INDEX idx_card_assignees_subject ON card_assignees(subject);
