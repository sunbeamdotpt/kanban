-- SPDX-License-Identifier: AGPL-3.0-or-later
-- AggregatedBoard (meta board) tables.
--
-- An aggregated board is a view spanning an explicit list of source boards.
-- It does not own cards; cards remain in their source boards and projects.

CREATE TABLE aggregated_boards (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   TEXT NOT NULL,
  name        TEXT NOT NULL,
  description TEXT,
  icon        TEXT,
  created_by  TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_aggregated_boards_tenant ON aggregated_boards(tenant_id);

CREATE TABLE aggregated_board_sources (
  aggregated_board_id UUID NOT NULL REFERENCES aggregated_boards(id) ON DELETE CASCADE,
  tenant_id           TEXT NOT NULL,
  board_id            UUID NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
  position            INT NOT NULL DEFAULT 0,
  added_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (aggregated_board_id, board_id)
);

CREATE INDEX idx_aggregated_board_sources_board ON aggregated_board_sources(board_id);
CREATE INDEX idx_aggregated_board_sources_tenant ON aggregated_board_sources(tenant_id);

-- Mirror table for Keto tuples, same pattern as project_members.
CREATE TABLE aggregated_board_members (
  aggregated_board_id UUID NOT NULL REFERENCES aggregated_boards(id) ON DELETE CASCADE,
  tenant_id           TEXT NOT NULL,
  subject             TEXT NOT NULL,
  relation            TEXT NOT NULL CHECK (relation IN ('owner', 'admin', 'editor', 'viewer')),
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (aggregated_board_id, subject)
);

CREATE INDEX idx_aggregated_board_members_subject ON aggregated_board_members(subject);
CREATE INDEX idx_aggregated_board_members_tenant ON aggregated_board_members(tenant_id);
