-- SPDX-License-Identifier: AGPL-3.0-or-later
-- Ported from apps/kanban-old/server/migrate.ts migration 6
-- cards: work items with extended fields (revision, ref, blocking, milestones, cover, completed_at)

CREATE TABLE cards (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id    UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  column_id     UUID NOT NULL REFERENCES columns(id) ON DELETE CASCADE,
  board_id      UUID NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
  ref           TEXT NOT NULL,
  title         TEXT NOT NULL,
  description   TEXT DEFAULT '',
  position      INT NOT NULL DEFAULT 0,
  priority      TEXT DEFAULT 'medium' CHECK (priority IN ('low', 'medium', 'high', 'urgent')),
  due_date      TIMESTAMPTZ,
  completed_at  TIMESTAMPTZ,
  blocked       BOOLEAN NOT NULL DEFAULT false,
  cover         TEXT,
  milestone_id  UUID,
  revision      BIGINT NOT NULL DEFAULT 0,
  created_by    TEXT NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (project_id, ref)
);

CREATE INDEX idx_cards_column ON cards(column_id);
CREATE INDEX idx_cards_board ON cards(board_id);
CREATE INDEX idx_cards_project ON cards(project_id);
CREATE INDEX idx_cards_milestone ON cards(milestone_id);
