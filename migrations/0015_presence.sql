-- presence: user activity tracking per board

CREATE TABLE presence (
  board_id      UUID NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
  subject       TEXT NOT NULL,
  last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (board_id, subject)
);

CREATE INDEX idx_presence_board ON presence(board_id);
