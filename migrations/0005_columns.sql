-- SPDX-License-Identifier: AGPL-3.0-or-later
-- Ported from apps/kanban-old/server/migrate.ts migration 5
-- columns: swim-lanes within a board

CREATE TABLE columns (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   TEXT NOT NULL,
  board_id    UUID NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
  title       TEXT NOT NULL,
  position    INT NOT NULL DEFAULT 0,
  accent      TEXT,
  wip_limit   INT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_columns_board ON columns(board_id);
CREATE INDEX idx_columns_tenant ON columns(tenant_id);
