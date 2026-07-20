-- SPDX-License-Identifier: AGPL-3.0-or-later
-- Ported from apps/kanban-old/server/migrate.ts migration 2
-- project_members: access control and role assignments per project

CREATE TABLE project_members (
  project_id  UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  tenant_id   TEXT NOT NULL,
  user_id     TEXT NOT NULL,
  role        TEXT NOT NULL DEFAULT 'viewer',
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (project_id, user_id)
);

CREATE INDEX idx_project_members_user ON project_members(user_id);
CREATE INDEX idx_project_members_tenant ON project_members(tenant_id, project_id, user_id);
