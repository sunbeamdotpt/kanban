-- Add visibility levels to boards: private, internal, public.

ALTER TABLE boards
  ADD COLUMN visibility TEXT NOT NULL DEFAULT 'private';

CREATE INDEX idx_boards_visibility ON boards(project_id, visibility);
