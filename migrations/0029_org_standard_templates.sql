-- SPDX-License-Identifier: AGPL-3.0-or-later
-- org-standard global templates (KANBAN-007)
-- Global templates cannot be created via API, so the org standard ships as a
-- seed. Board template: no Backlog, no Archived — To Do / In Progress /
-- Review / Done. Card templates: feature, bug, chore.
-- ids are explicit ULIDs: since 0026 the id columns are TEXT with no default.

INSERT INTO board_templates (id, tenant_id, name, description, columns, is_global, created_by)
VALUES
  ('KSNAQWV3M8RRVGRB03V0215953', 'system', 'standard', 'Org-standard workflow', '[
    {"title": "To Do", "position": 0, "accent": "blue"},
    {"title": "In Progress", "position": 1, "accent": "amber"},
    {"title": "Review", "position": 2, "accent": "purple"},
    {"title": "Done", "position": 3, "accent": "green"}
  ]', true, null)
ON CONFLICT DO NOTHING;

INSERT INTO card_templates (id, tenant_id, project_id, name, description, title, default_description, is_global, created_by)
VALUES
  ('AQM9Y81KGB6MQ3Q27Z72V12WDV', 'system', null, 'feature', 'Feature with Given/When/Then acceptance criteria', '', '## Acceptance Criteria

**Given** <precondition>

**When** <action>

**Then** <expected outcome>', true, null),
  ('ZFV6D6484C46GZSJQAKWTJ70GW', 'system', null, 'bug', 'Bug report with expected/actual behavior, repro, and impact', '', '## Expected

## Actual

## Repro

## Impact', true, null),
  ('DW0719C2ZHR65NRBCMS17KWAPR', 'system', null, 'chore', 'One-off maintenance task', '', 'Describe the chore and its definition of done.', true, null)
ON CONFLICT DO NOTHING;
