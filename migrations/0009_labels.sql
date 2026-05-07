-- labels: project-scoped label catalog with style tokens

CREATE TABLE labels (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id  UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  name        TEXT NOT NULL,
  style       TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (project_id, name)
);

CREATE INDEX idx_labels_project ON labels(project_id);

-- card_labels: join table for labels on cards
CREATE TABLE card_labels (
  card_id     UUID NOT NULL REFERENCES cards(id) ON DELETE CASCADE,
  label_id    UUID NOT NULL REFERENCES labels(id) ON DELETE CASCADE,
  PRIMARY KEY (card_id, label_id)
);

CREATE INDEX idx_card_labels_label ON card_labels(label_id);
