-- SPDX-License-Identifier: AGPL-3.0-or-later
-- Ported from apps/kanban-old/server/migrate.ts migration 1
-- projects: core project entity and pgcrypto extension

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE projects (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   TEXT NOT NULL,
  name        TEXT NOT NULL,
  slug        TEXT NOT NULL,
  description TEXT DEFAULT '',
  owner_id    TEXT NOT NULL,
  visibility  TEXT NOT NULL DEFAULT 'private',
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, slug)
);

CREATE INDEX idx_projects_tenant_id ON projects(tenant_id);
CREATE INDEX idx_projects_tenant_slug ON projects(tenant_id, slug);
