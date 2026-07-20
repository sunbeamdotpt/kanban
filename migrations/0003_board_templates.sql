-- SPDX-License-Identifier: AGPL-3.0-or-later
-- Ported from apps/kanban-old/server/migrate.ts migration 3
-- board_templates: reusable column presets (global or project-scoped)

CREATE TABLE board_templates (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   TEXT NOT NULL,
  name        TEXT NOT NULL,
  description TEXT DEFAULT '',
  columns     JSONB NOT NULL DEFAULT '[]',
  created_by  TEXT,
  is_global   BOOLEAN NOT NULL DEFAULT false,
  project_id  UUID REFERENCES projects(id) ON DELETE CASCADE,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_board_templates_project ON board_templates(project_id) WHERE is_global = false;
CREATE INDEX idx_board_templates_tenant ON board_templates(tenant_id);
