-- SPDX-License-Identifier: AGPL-3.0-or-later
-- template seed: global Kanban templates (ported from kanban-old migration 8)

INSERT INTO board_templates (name, description, columns, is_global, created_by)
VALUES
  ('Kanban', 'Standard kanban workflow', '[
    {"title": "Backlog", "position": 0, "accent": "slate"},
    {"title": "To Do", "position": 1, "accent": "blue"},
    {"title": "In Progress", "position": 2, "accent": "amber"},
    {"title": "Review", "position": 3, "accent": "purple"},
    {"title": "Done", "position": 4, "accent": "green"}
  ]', true, null),
  ('Sprint', 'Agile sprint board', '[
    {"title": "Sprint Backlog", "position": 0, "accent": "slate"},
    {"title": "In Progress", "position": 1, "accent": "amber"},
    {"title": "Testing", "position": 2, "accent": "purple"},
    {"title": "Done", "position": 3, "accent": "green"}
  ]', true, null),
  ('Simple', 'Minimal three-column board', '[
    {"title": "To Do", "position": 0, "accent": "blue"},
    {"title": "Doing", "position": 1, "accent": "amber"},
    {"title": "Done", "position": 2, "accent": "green"}
  ]', true, null)
ON CONFLICT DO NOTHING;
