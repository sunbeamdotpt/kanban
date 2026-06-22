-- Reusable card templates (global or project-scoped).

CREATE TABLE card_templates (
  id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id           UUID REFERENCES projects(id) ON DELETE CASCADE,
  name                 TEXT NOT NULL,
  description          TEXT DEFAULT '',
  title                TEXT DEFAULT '',
  default_description  TEXT DEFAULT '',
  label_names          TEXT[] NOT NULL DEFAULT '{}',
  checklist_items      JSONB NOT NULL DEFAULT '[]',
  created_by           TEXT,
  is_global            BOOLEAN NOT NULL DEFAULT false,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_card_templates_project ON card_templates(project_id) WHERE is_global = false;

-- Board templates gained an updated_at column after their initial migration.
ALTER TABLE board_templates ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();
