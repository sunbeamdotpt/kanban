-- SPDX-License-Identifier: AGPL-3.0-or-later
-- Ported from apps/kanban-old/server/migrate.ts migration 4
-- boards: top-level container for columns and cards

CREATE TABLE boards (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   TEXT NOT NULL,
  project_id  UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  name        TEXT NOT NULL,
  slug        TEXT NOT NULL,
  description TEXT DEFAULT '',
  icon        TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (project_id, slug)
);

CREATE INDEX idx_boards_project ON boards(project_id);
CREATE INDEX idx_boards_tenant ON boards(tenant_id);
