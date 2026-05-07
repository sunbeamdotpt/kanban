-- project_ref_counter: advisory-lock allocator for card refs (MF-3)
-- Do NOT seed rows in this migration; CardService.CreateCard lazy-creates them

CREATE TABLE project_ref_counter (
  project_id  UUID PRIMARY KEY REFERENCES projects(id) ON DELETE CASCADE,
  prefix      TEXT NOT NULL,
  next_seq    INT NOT NULL DEFAULT 1
);

CREATE INDEX idx_project_ref_counter_project ON project_ref_counter(project_id);
