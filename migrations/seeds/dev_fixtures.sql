-- SPDX-License-Identifier: AGPL-3.0-or-later
-- Seed source: /tmp/kanban-mockup-extracted/src/data.jsx (mockup PROJECTS const)
-- Run via: psql $DATABASE_URL -f apps/kanban/migrations/seeds/dev_fixtures.sql
-- Idempotent on re-run via ON CONFLICT DO NOTHING.

-- =====================================================================
-- PEOPLE / PROJECT MEMBERS
-- =====================================================================
-- The mockup PEOPLE are seeded as project_members with deterministic user_id mapping.
-- Each person is added as 'viewer' to all 4 projects, plus 'editor' to projects where they're listed in members.

INSERT INTO project_members (project_id, tenant_id, user_id, role, created_at)
VALUES
  -- beam-ui members: sp, mc, ak, mb, lr (plus all 8 people as viewers)
  ('550e8400-e29b-41d4-a716-446655440001', 'dev-tenant', 'sp', 'editor', now()),
  ('550e8400-e29b-41d4-a716-446655440001', 'dev-tenant', 'mc', 'editor', now()),
  ('550e8400-e29b-41d4-a716-446655440001', 'dev-tenant', 'ak', 'editor', now()),
  ('550e8400-e29b-41d4-a716-446655440001', 'dev-tenant', 'mb', 'editor', now()),
  ('550e8400-e29b-41d4-a716-446655440001', 'dev-tenant', 'lr', 'editor', now()),
  ('550e8400-e29b-41d4-a716-446655440001', 'dev-tenant', 'jc', 'viewer', now()),
  ('550e8400-e29b-41d4-a716-446655440001', 'dev-tenant', 'tr', 'viewer', now()),
  ('550e8400-e29b-41d4-a716-446655440001', 'dev-tenant', 'in', 'viewer', now()),

  -- sol-ai members: jc, tr, mc, in, sp (plus all 8 people as viewers)
  ('550e8400-e29b-41d4-a716-446655440002', 'dev-tenant', 'jc', 'editor', now()),
  ('550e8400-e29b-41d4-a716-446655440002', 'dev-tenant', 'tr', 'editor', now()),
  ('550e8400-e29b-41d4-a716-446655440002', 'dev-tenant', 'mc', 'editor', now()),
  ('550e8400-e29b-41d4-a716-446655440002', 'dev-tenant', 'in', 'editor', now()),
  ('550e8400-e29b-41d4-a716-446655440002', 'dev-tenant', 'sp', 'editor', now()),
  ('550e8400-e29b-41d4-a716-446655440002', 'dev-tenant', 'ak', 'viewer', now()),
  ('550e8400-e29b-41d4-a716-446655440002', 'dev-tenant', 'mb', 'viewer', now()),
  ('550e8400-e29b-41d4-a716-446655440002', 'dev-tenant', 'lr', 'viewer', now()),

  -- marathon members: lr, tr, ak, in (plus all 8 people as viewers)
  ('550e8400-e29b-41d4-a716-446655440003', 'dev-tenant', 'lr', 'editor', now()),
  ('550e8400-e29b-41d4-a716-446655440003', 'dev-tenant', 'tr', 'editor', now()),
  ('550e8400-e29b-41d4-a716-446655440003', 'dev-tenant', 'ak', 'editor', now()),
  ('550e8400-e29b-41d4-a716-446655440003', 'dev-tenant', 'in', 'editor', now()),
  ('550e8400-e29b-41d4-a716-446655440003', 'dev-tenant', 'sp', 'viewer', now()),
  ('550e8400-e29b-41d4-a716-446655440003', 'dev-tenant', 'mc', 'viewer', now()),
  ('550e8400-e29b-41d4-a716-446655440003', 'dev-tenant', 'mb', 'viewer', now()),
  ('550e8400-e29b-41d4-a716-446655440003', 'dev-tenant', 'jc', 'viewer', now()),

  -- studios members: sp, mb, jc (plus all 8 people as viewers)
  ('550e8400-e29b-41d4-a716-446655440004', 'dev-tenant', 'sp', 'editor', now()),
  ('550e8400-e29b-41d4-a716-446655440004', 'dev-tenant', 'mb', 'editor', now()),
  ('550e8400-e29b-41d4-a716-446655440004', 'dev-tenant', 'jc', 'editor', now()),
  ('550e8400-e29b-41d4-a716-446655440004', 'dev-tenant', 'mc', 'viewer', now()),
  ('550e8400-e29b-41d4-a716-446655440004', 'dev-tenant', 'ak', 'viewer', now()),
  ('550e8400-e29b-41d4-a716-446655440004', 'dev-tenant', 'tr', 'viewer', now()),
  ('550e8400-e29b-41d4-a716-446655440004', 'dev-tenant', 'lr', 'viewer', now()),
  ('550e8400-e29b-41d4-a716-446655440004', 'dev-tenant', 'in', 'viewer', now())
ON CONFLICT DO NOTHING;

-- =====================================================================
-- PROJECTS (4)
-- =====================================================================
INSERT INTO projects (id, tenant_id, name, slug, prefix, description, owner_id, visibility, created_at, updated_at)
VALUES
  ('550e8400-e29b-41d4-a716-446655440001', 'dev-tenant', 'Beam UI', 'beam-ui', 'BEAM', 'The Sunbeam component library and design language showcase.', 'sp', 'private', now(), now()),
  ('550e8400-e29b-41d4-a716-446655440002', 'dev-tenant', 'Sol', 'sol-ai', 'SOL', 'In-house AI agent — chat UI, tool use, evals.', 'jc', 'private', now(), now()),
  ('550e8400-e29b-41d4-a716-446655440003', 'dev-tenant', 'Marathon', 'marathon', 'MAR', 'Offline-first game engine SDK.', 'lr', 'private', now(), now()),
  ('550e8400-e29b-41d4-a716-446655440004', 'dev-tenant', 'Studios Site', 'studios', 'SITE', 'Public marketing + docs at sunbeam.pt.', 'sp', 'private', now(), now())
ON CONFLICT DO NOTHING;

-- =====================================================================
-- MILESTONES
-- =====================================================================
INSERT INTO milestones (id, tenant_id, project_id, title, due, created_at, updated_at)
VALUES
  -- beam-ui v2
  ('550e8400-e29b-41d4-a716-446655450001', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', 'v2.0 Release', '2026-06-15'::timestamptz, now(), now()),
  ('550e8400-e29b-41d4-a716-446655450002', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', 'Hardening Sprint', '2026-05-15'::timestamptz, now(), now()),

  -- sol-ai beta
  ('550e8400-e29b-41d4-a716-446655450003', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440002', 'Sol Beta', '2026-05-22'::timestamptz, now(), now()),

  -- marathon 1.0
  ('550e8400-e29b-41d4-a716-446655450004', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440003', 'Marathon 1.0', '2026-09-01'::timestamptz, now(), now()),

  -- studios beta
  ('550e8400-e29b-41d4-a716-446655450005', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440004', 'Sol Beta', '2026-05-22'::timestamptz, now(), now())
ON CONFLICT DO NOTHING;

-- =====================================================================
-- LABELS
-- =====================================================================
-- All projects share a common label set
INSERT INTO labels (id, tenant_id, project_id, name, style, created_at)
VALUES
  -- beam-ui labels
  ('550e8400-e29b-41d4-a716-446655460001', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', 'feature', 'orange', now()),
  ('550e8400-e29b-41d4-a716-446655460002', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', 'bug', 'rust', now()),
  ('550e8400-e29b-41d4-a716-446655460003', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', 'design', 'gold', now()),
  ('550e8400-e29b-41d4-a716-446655460004', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', 'a11y', 'gold', now()),
  ('550e8400-e29b-41d4-a716-446655460005', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', 'docs', 'sand', now()),
  ('550e8400-e29b-41d4-a716-446655460006', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', 'refactor', 'sand', now()),
  ('550e8400-e29b-41d4-a716-446655460007', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', 'ui', 'gold', now()),
  ('550e8400-e29b-41d4-a716-446655460008', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', 'performance', 'green', now()),
  ('550e8400-e29b-41d4-a716-446655460009', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', 'infra', 'olive', now()),
  ('550e8400-e29b-41d4-a716-446655460010', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', 'chore', 'ink', now()),

  -- sol-ai labels
  ('550e8400-e29b-41d4-a716-446655460011', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440002', 'feature', 'orange', now()),
  ('550e8400-e29b-41d4-a716-446655460012', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440002', 'bug', 'rust', now()),
  ('550e8400-e29b-41d4-a716-446655460013', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440002', 'research', 'purple', now()),
  ('550e8400-e29b-41d4-a716-446655460014', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440002', 'infra', 'olive', now()),
  ('550e8400-e29b-41d4-a716-446655460015', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440002', 'ui', 'gold', now()),
  ('550e8400-e29b-41d4-a716-446655460016', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440002', 'design', 'gold', now()),
  ('550e8400-e29b-41d4-a716-446655460017', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440002', 'ai', 'orange', now()),
  ('550e8400-e29b-41d4-a716-446655460018', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440002', 'performance', 'green', now()),
  ('550e8400-e29b-41d4-a716-446655460019', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440002', 'api', 'olive', now()),

  -- marathon labels
  ('550e8400-e29b-41d4-a716-446655460020', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440003', 'feature', 'orange', now()),
  ('550e8400-e29b-41d4-a716-446655460021', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440003', 'performance', 'green', now()),
  ('550e8400-e29b-41d4-a716-446655460022', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440003', 'refactor', 'sand', now()),
  ('550e8400-e29b-41d4-a716-446655460023', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440003', 'infra', 'olive', now()),

  -- studios labels
  ('550e8400-e29b-41d4-a716-446655460024', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440004', 'feature', 'orange', now()),
  ('550e8400-e29b-41d4-a716-446655460025', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440004', 'design', 'gold', now()),
  ('550e8400-e29b-41d4-a716-446655460026', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440004', 'docs', 'sand', now()),
  ('550e8400-e29b-41d4-a716-446655460027', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440004', 'ai', 'orange', now())
ON CONFLICT DO NOTHING;

-- =====================================================================
-- BOARDS
-- =====================================================================
INSERT INTO boards (id, tenant_id, project_id, name, slug, description, icon, created_at, updated_at)
VALUES
  -- beam-ui boards
  ('550e8400-e29b-41d4-a716-446655470001', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', 'Components', 'components', 'Day-to-day work on the component catalogue — new primitives, fixes, accessibility passes.', 'view_kanban', now(), now()),
  ('550e8400-e29b-41d4-a716-446655470002', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', 'v2 Migration', 'v2-migration', 'Customer-facing migration tooling, breaking change docs, codemods.', 'route', now(), now()),

  -- sol-ai boards
  ('550e8400-e29b-41d4-a716-446655470003', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440002', 'Roadmap', 'roadmap', 'High-level Sol features tracking toward the public beta.', 'rocket_launch', now(), now()),
  ('550e8400-e29b-41d4-a716-446655470004', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440002', 'Bugs', 'sol-bugs', 'Triage and field bugs reported during alpha.', 'bug_report', now(), now()),

  -- marathon boards
  ('550e8400-e29b-41d4-a716-446655470005', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440003', 'Engine', 'engine', 'Core engine work — renderer, audio, scene graph.', 'memory', now(), now()),

  -- studios boards
  ('550e8400-e29b-41d4-a716-446655470006', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440004', 'Content', 'content', 'Blog posts, case studies, page revamps.', 'edit_note', now(), now())
ON CONFLICT DO NOTHING;

-- =====================================================================
-- COLUMNS
-- =====================================================================
-- Standard template for most boards
INSERT INTO columns (id, tenant_id, board_id, title, position, accent, wip_limit, created_at, updated_at)
VALUES
  -- beam-ui / components
  ('550e8400-e29b-41d4-a716-446655480001', 'dev-tenant', '550e8400-e29b-41d4-a716-446655470001', 'Backlog', 0, '#7f6315', NULL, now(), now()),
  ('550e8400-e29b-41d4-a716-446655480002', 'dev-tenant', '550e8400-e29b-41d4-a716-446655470001', 'Up next', 1, '#ffa110', 5, now(), now()),
  ('550e8400-e29b-41d4-a716-446655480003', 'dev-tenant', '550e8400-e29b-41d4-a716-446655470001', 'In progress', 2, '#fa520f', 3, now(), now()),
  ('550e8400-e29b-41d4-a716-446655480004', 'dev-tenant', '550e8400-e29b-41d4-a716-446655470001', 'In review', 3, '#7e22ce', 4, now(), now()),
  ('550e8400-e29b-41d4-a716-446655480005', 'dev-tenant', '550e8400-e29b-41d4-a716-446655470001', 'Done', 4, '#15803d', NULL, now(), now()),

  -- beam-ui / v2-migration
  ('550e8400-e29b-41d4-a716-446655480006', 'dev-tenant', '550e8400-e29b-41d4-a716-446655470002', 'Backlog', 0, '#7f6315', NULL, now(), now()),
  ('550e8400-e29b-41d4-a716-446655480007', 'dev-tenant', '550e8400-e29b-41d4-a716-446655470002', 'Up next', 1, '#ffa110', 5, now(), now()),
  ('550e8400-e29b-41d4-a716-446655480008', 'dev-tenant', '550e8400-e29b-41d4-a716-446655470002', 'In progress', 2, '#fa520f', 3, now(), now()),
  ('550e8400-e29b-41d4-a716-446655480009', 'dev-tenant', '550e8400-e29b-41d4-a716-446655470002', 'In review', 3, '#7e22ce', 4, now(), now()),
  ('550e8400-e29b-41d4-a716-446655480010', 'dev-tenant', '550e8400-e29b-41d4-a716-446655470002', 'Done', 4, '#15803d', NULL, now(), now()),

  -- sol-ai / roadmap
  ('550e8400-e29b-41d4-a716-446655480011', 'dev-tenant', '550e8400-e29b-41d4-a716-446655470003', 'Backlog', 0, '#7f6315', NULL, now(), now()),
  ('550e8400-e29b-41d4-a716-446655480012', 'dev-tenant', '550e8400-e29b-41d4-a716-446655470003', 'Up next', 1, '#ffa110', 5, now(), now()),
  ('550e8400-e29b-41d4-a716-446655480013', 'dev-tenant', '550e8400-e29b-41d4-a716-446655470003', 'In progress', 2, '#fa520f', 3, now(), now()),
  ('550e8400-e29b-41d4-a716-446655480014', 'dev-tenant', '550e8400-e29b-41d4-a716-446655470003', 'In review', 3, '#7e22ce', 4, now(), now()),
  ('550e8400-e29b-41d4-a716-446655480015', 'dev-tenant', '550e8400-e29b-41d4-a716-446655470003', 'Done', 4, '#15803d', NULL, now(), now()),

  -- sol-ai / sol-bugs
  ('550e8400-e29b-41d4-a716-446655480016', 'dev-tenant', '550e8400-e29b-41d4-a716-446655470004', 'Backlog', 0, '#7f6315', NULL, now(), now()),
  ('550e8400-e29b-41d4-a716-446655480017', 'dev-tenant', '550e8400-e29b-41d4-a716-446655470004', 'Up next', 1, '#ffa110', 5, now(), now()),
  ('550e8400-e29b-41d4-a716-446655480018', 'dev-tenant', '550e8400-e29b-41d4-a716-446655470004', 'In progress', 2, '#fa520f', 3, now(), now()),
  ('550e8400-e29b-41d4-a716-446655480019', 'dev-tenant', '550e8400-e29b-41d4-a716-446655470004', 'In review', 3, '#7e22ce', 4, now(), now()),
  ('550e8400-e29b-41d4-a716-446655480020', 'dev-tenant', '550e8400-e29b-41d4-a716-446655470004', 'Done', 4, '#15803d', NULL, now(), now()),

  -- marathon / engine
  ('550e8400-e29b-41d4-a716-446655480021', 'dev-tenant', '550e8400-e29b-41d4-a716-446655470005', 'Backlog', 0, '#7f6315', NULL, now(), now()),
  ('550e8400-e29b-41d4-a716-446655480022', 'dev-tenant', '550e8400-e29b-41d4-a716-446655470005', 'Up next', 1, '#ffa110', 5, now(), now()),
  ('550e8400-e29b-41d4-a716-446655480023', 'dev-tenant', '550e8400-e29b-41d4-a716-446655470005', 'In progress', 2, '#fa520f', 3, now(), now()),
  ('550e8400-e29b-41d4-a716-446655480024', 'dev-tenant', '550e8400-e29b-41d4-a716-446655470005', 'In review', 3, '#7e22ce', 4, now(), now()),
  ('550e8400-e29b-41d4-a716-446655480025', 'dev-tenant', '550e8400-e29b-41d4-a716-446655470005', 'Done', 4, '#15803d', NULL, now(), now()),

  -- studios / content (custom columns)
  ('550e8400-e29b-41d4-a716-446655480026', 'dev-tenant', '550e8400-e29b-41d4-a716-446655470006', 'Ideas', 0, '#7f6315', NULL, now(), now()),
  ('550e8400-e29b-41d4-a716-446655480027', 'dev-tenant', '550e8400-e29b-41d4-a716-446655470006', 'Drafting', 1, '#ffa110', 4, now(), now()),
  ('550e8400-e29b-41d4-a716-446655480028', 'dev-tenant', '550e8400-e29b-41d4-a716-446655470006', 'Editing', 2, '#fa520f', 3, now(), now()),
  ('550e8400-e29b-41d4-a716-446655480029', 'dev-tenant', '550e8400-e29b-41d4-a716-446655470006', 'Published', 3, '#15803d', NULL, now(), now())
ON CONFLICT DO NOTHING;

-- =====================================================================
-- CARDS - BEAM UI / COMPONENTS
-- =====================================================================
INSERT INTO cards (id, tenant_id, project_id, board_id, column_id, ref, title, description, position, priority, due_date, milestone_id, comments_count, attachments_count, checklist_done, checklist_total, blocked, cover, completed_at, created_by, created_at, updated_at)
VALUES
  -- Backlog
  ('550e8400-e29b-41d4-a716-446655490001', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', '550e8400-e29b-41d4-a716-446655470001', '550e8400-e29b-41d4-a716-446655480001', 'BEAM-204', 'Implement live-reload toggle for the showcase', '', 0, 'medium', NULL, '550e8400-e29b-41d4-a716-446655450001', 3, 0, 0, 4, false, NULL, NULL, 'sp', now(), now()),
  ('550e8400-e29b-41d4-a716-446655490002', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', '550e8400-e29b-41d4-a716-446655470001', '550e8400-e29b-41d4-a716-446655480001', 'BEAM-198', 'Audit accessibility across all components', '', 1, 'high', '2026-05-30'::timestamptz, '550e8400-e29b-41d4-a716-446655450001', 12, 2, 4, 18, false, NULL, NULL, 'sp', now(), now()),
  ('550e8400-e29b-41d4-a716-446655490003', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', '550e8400-e29b-41d4-a716-446655470001', '550e8400-e29b-41d4-a716-446655480001', 'BEAM-195', 'Write upgrade guide for v2', '', 2, 'medium', NULL, '550e8400-e29b-41d4-a716-446655450001', 1, 1, 1, 6, false, NULL, NULL, 'sp', now(), now()),
  ('550e8400-e29b-41d4-a716-446655490004', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', '550e8400-e29b-41d4-a716-446655470001', '550e8400-e29b-41d4-a716-446655480001', 'BEAM-191', 'Tree view → keyboard navigation', '', 3, 'medium', NULL, '550e8400-e29b-41d4-a716-446655450001', 0, 0, 0, 0, false, NULL, NULL, 'sp', now(), now()),

  -- Up next
  ('550e8400-e29b-41d4-a716-446655490005', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', '550e8400-e29b-41d4-a716-446655470001', '550e8400-e29b-41d4-a716-446655480002', 'BEAM-210', 'Refactor the Splitter component', '', 0, 'medium', '2026-05-12'::timestamptz, '550e8400-e29b-41d4-a716-446655450001', 4, 0, 2, 7, false, NULL, NULL, 'sp', now(), now()),
  ('550e8400-e29b-41d4-a716-446655490006', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', '550e8400-e29b-41d4-a716-446655470001', '550e8400-e29b-41d4-a716-446655480002', 'BEAM-208', 'Add color picker dark mode support', '', 1, 'low', NULL, '550e8400-e29b-41d4-a716-446655450001', 2, 1, 0, 3, false, 'linear-gradient(135deg, #1f1f1f, #fa520f)', NULL, 'sp', now(), now()),

  -- In progress
  ('550e8400-e29b-41d4-a716-446655490007', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', '550e8400-e29b-41d4-a716-446655470001', '550e8400-e29b-41d4-a716-446655480003', 'BEAM-215', 'Combobox: support virtualised options for 10k+ items', '', 0, 'high', '2026-05-08'::timestamptz, '550e8400-e29b-41d4-a716-446655450001', 8, 0, 5, 9, false, NULL, NULL, 'sp', now(), now()),
  ('550e8400-e29b-41d4-a716-446655490008', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', '550e8400-e29b-41d4-a716-446655470001', '550e8400-e29b-41d4-a716-446655480003', 'BEAM-207', 'Fix modals closing on outside click during text selection', '', 1, 'urgent', '2026-05-03'::timestamptz, '550e8400-e29b-41d4-a716-446655450002', 6, 3, 0, 0, false, NULL, NULL, 'sp', now(), now()),
  ('550e8400-e29b-41d4-a716-446655490009', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', '550e8400-e29b-41d4-a716-446655470001', '550e8400-e29b-41d4-a716-446655480003', 'BEAM-203', 'Calendar: support week-start configuration', '', 2, 'low', NULL, '550e8400-e29b-41d4-a716-446655450001', 1, 0, 1, 4, false, NULL, NULL, 'sp', now(), now()),

  -- In review
  ('550e8400-e29b-41d4-a716-446655490010', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', '550e8400-e29b-41d4-a716-446655470001', '550e8400-e29b-41d4-a716-446655480004', 'BEAM-216', 'Toast component: API simplification', '', 0, 'medium', NULL, '550e8400-e29b-41d4-a716-446655450001', 9, 1, 3, 3, false, 'linear-gradient(135deg, #ffe295, #fa520f)', NULL, 'sp', now(), now()),
  ('550e8400-e29b-41d4-a716-446655490011', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', '550e8400-e29b-41d4-a716-446655470001', '550e8400-e29b-41d4-a716-446655480004', 'BEAM-211', 'Dialog focus trap regression', '', 1, 'high', NULL, '550e8400-e29b-41d4-a716-446655450002', 4, 1, 0, 0, true, NULL, NULL, 'sp', now(), now()),

  -- Done
  ('550e8400-e29b-41d4-a716-446655490012', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', '550e8400-e29b-41d4-a716-446655470001', '550e8400-e29b-41d4-a716-446655480005', 'BEAM-201', 'Spinner — replace with sinusoidal worm motion', '', 0, 'low', NULL, '550e8400-e29b-41d4-a716-446655450001', 2, 0, 5, 5, false, NULL, '2026-04-28'::timestamptz, 'sp', now(), now()),
  ('550e8400-e29b-41d4-a716-446655490013', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', '550e8400-e29b-41d4-a716-446655470001', '550e8400-e29b-41d4-a716-446655480005', 'BEAM-199', 'Avatar stack: support overflow indicator', '', 1, 'low', NULL, '550e8400-e29b-41d4-a716-446655450001', 1, 0, 4, 4, false, NULL, '2026-04-26'::timestamptz, 'sp', now(), now())
ON CONFLICT DO NOTHING;

-- =====================================================================
-- CARDS - BEAM UI / V2 MIGRATION
-- =====================================================================
INSERT INTO cards (id, tenant_id, project_id, board_id, column_id, ref, title, description, position, priority, due_date, milestone_id, comments_count, attachments_count, checklist_done, checklist_total, blocked, cover, completed_at, created_by, created_at, updated_at)
VALUES
  -- Backlog
  ('550e8400-e29b-41d4-a716-446655490014', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', '550e8400-e29b-41d4-a716-446655470002', '550e8400-e29b-41d4-a716-446655480006', 'BEAM-220', 'Codemod: rename Box → Frame', '', 0, 'high', NULL, '550e8400-e29b-41d4-a716-446655450001', 2, 0, 0, 5, false, NULL, NULL, 'sp', now(), now()),
  ('550e8400-e29b-41d4-a716-446655490015', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', '550e8400-e29b-41d4-a716-446655470002', '550e8400-e29b-41d4-a716-446655480006', 'BEAM-221', 'Migration playground in docs', '', 1, 'medium', NULL, '550e8400-e29b-41d4-a716-446655450001', 0, 0, 0, 0, false, NULL, NULL, 'sp', now(), now()),

  -- Up next
  ('550e8400-e29b-41d4-a716-446655490016', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', '550e8400-e29b-41d4-a716-446655470002', '550e8400-e29b-41d4-a716-446655480007', 'BEAM-225', 'Deprecation notices for v1 props', '', 0, 'medium', NULL, '550e8400-e29b-41d4-a716-446655450001', 1, 0, 0, 12, false, NULL, NULL, 'sp', now(), now()),

  -- In progress
  ('550e8400-e29b-41d4-a716-446655490017', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', '550e8400-e29b-41d4-a716-446655470002', '550e8400-e29b-41d4-a716-446655480008', 'BEAM-228', 'Breaking changes catalogue', '', 0, 'high', '2026-05-20'::timestamptz, '550e8400-e29b-41d4-a716-446655450001', 5, 0, 8, 22, false, NULL, NULL, 'sp', now(), now()),

  -- Done
  ('550e8400-e29b-41d4-a716-446655490018', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440001', '550e8400-e29b-41d4-a716-446655470002', '550e8400-e29b-41d4-a716-446655480010', 'BEAM-218', 'Set up v2 release branch + CI', '', 0, 'medium', NULL, '550e8400-e29b-41d4-a716-446655450001', 0, 0, 3, 3, false, NULL, '2026-04-22'::timestamptz, 'sp', now(), now())
ON CONFLICT DO NOTHING;

-- =====================================================================
-- CARDS - SOL AI / ROADMAP
-- =====================================================================
INSERT INTO cards (id, tenant_id, project_id, board_id, column_id, ref, title, description, position, priority, due_date, milestone_id, comments_count, attachments_count, checklist_done, checklist_total, blocked, cover, completed_at, created_by, created_at, updated_at)
VALUES
  -- Backlog
  ('550e8400-e29b-41d4-a716-446655490019', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440002', '550e8400-e29b-41d4-a716-446655470003', '550e8400-e29b-41d4-a716-446655480011', 'SOL-104', 'Multi-modal: image input pipeline', '', 0, 'high', NULL, '550e8400-e29b-41d4-a716-446655450003', 14, 4, 2, 11, false, 'linear-gradient(135deg, #ff8a00, #c084fc)', NULL, 'sp', now(), now()),
  ('550e8400-e29b-41d4-a716-446655490020', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440002', '550e8400-e29b-41d4-a716-446655470003', '550e8400-e29b-41d4-a716-446655480011', 'SOL-098', 'Agent eval harness — pass@k metrics', '', 1, 'high', NULL, '550e8400-e29b-41d4-a716-446655450003', 8, 1, 3, 8, false, NULL, NULL, 'sp', now(), now()),
  ('550e8400-e29b-41d4-a716-446655490021', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440002', '550e8400-e29b-41d4-a716-446655470003', '550e8400-e29b-41d4-a716-446655480011', 'SOL-091', 'Streaming markdown renderer for chat', '', 2, 'medium', NULL, '550e8400-e29b-41d4-a716-446655450003', 3, 0, 0, 4, false, NULL, NULL, 'sp', now(), now()),

  -- Up next
  ('550e8400-e29b-41d4-a716-446655490022', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440002', '550e8400-e29b-41d4-a716-446655470003', '550e8400-e29b-41d4-a716-446655480012', 'SOL-107', 'Tool use: filesystem MCP connector', '', 0, 'high', '2026-05-12'::timestamptz, '550e8400-e29b-41d4-a716-446655450003', 6, 1, 1, 6, false, NULL, NULL, 'sp', now(), now()),
  ('550e8400-e29b-41d4-a716-446655490023', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440002', '550e8400-e29b-41d4-a716-446655470003', '550e8400-e29b-41d4-a716-446655480012', 'SOL-106', 'Conversation history pagination', '', 1, 'medium', NULL, '550e8400-e29b-41d4-a716-446655450003', 2, 0, 0, 3, false, NULL, NULL, 'sp', now(), now()),

  -- In progress
  ('550e8400-e29b-41d4-a716-446655490024', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440002', '550e8400-e29b-41d4-a716-446655470003', '550e8400-e29b-41d4-a716-446655480013', 'SOL-110', 'Slash commands menu — design + impl', '', 0, 'high', '2026-05-09'::timestamptz, '550e8400-e29b-41d4-a716-446655450003', 11, 3, 4, 7, false, 'linear-gradient(135deg, #ffd06a, #fa520f)', NULL, 'sp', now(), now()),
  ('550e8400-e29b-41d4-a716-446655490025', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440002', '550e8400-e29b-41d4-a716-446655470003', '550e8400-e29b-41d4-a716-446655480013', 'SOL-109', 'Token cost meter in conversation', '', 1, 'medium', NULL, '550e8400-e29b-41d4-a716-446655450003', 4, 0, 2, 5, false, NULL, NULL, 'sp', now(), now()),

  -- In review
  ('550e8400-e29b-41d4-a716-446655490026', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440002', '550e8400-e29b-41d4-a716-446655470003', '550e8400-e29b-41d4-a716-446655480014', 'SOL-103', 'Prompt template gallery', '', 0, 'medium', NULL, '550e8400-e29b-41d4-a716-446655450003', 7, 2, 6, 6, false, NULL, NULL, 'sp', now(), now()),

  -- Done
  ('550e8400-e29b-41d4-a716-446655490027', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440002', '550e8400-e29b-41d4-a716-446655470003', '550e8400-e29b-41d4-a716-446655480015', 'SOL-088', 'Foundation: streaming response handler', '', 0, 'high', NULL, '550e8400-e29b-41d4-a716-446655450003', 9, 0, 8, 8, false, NULL, '2026-04-18'::timestamptz, 'sp', now(), now()),
  ('550e8400-e29b-41d4-a716-446655490028', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440002', '550e8400-e29b-41d4-a716-446655470003', '550e8400-e29b-41d4-a716-446655480015', 'SOL-085', 'Auth + rate limit middleware', '', 1, 'high', NULL, '550e8400-e29b-41d4-a716-446655450003', 2, 0, 4, 4, false, NULL, '2026-04-15'::timestamptz, 'sp', now(), now())
ON CONFLICT DO NOTHING;

-- =====================================================================
-- CARDS - SOL AI / BUGS
-- =====================================================================
INSERT INTO cards (id, tenant_id, project_id, board_id, column_id, ref, title, description, position, priority, due_date, milestone_id, comments_count, attachments_count, checklist_done, checklist_total, blocked, cover, completed_at, created_by, created_at, updated_at)
VALUES
  -- Backlog
  ('550e8400-e29b-41d4-a716-446655490029', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440002', '550e8400-e29b-41d4-a716-446655470004', '550e8400-e29b-41d4-a716-446655480016', 'SOL-201', 'Code blocks lose highlighting after long stream', '', 0, 'medium', NULL, NULL, 3, 1, 0, 0, false, NULL, NULL, 'sp', now(), now()),
  ('550e8400-e29b-41d4-a716-446655490030', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440002', '550e8400-e29b-41d4-a716-446655470004', '550e8400-e29b-41d4-a716-446655480016', 'SOL-202', 'Tool calls with long args truncated in UI', '', 1, 'low', NULL, NULL, 1, 0, 0, 0, false, NULL, NULL, 'sp', now(), now()),

  -- Up next
  ('550e8400-e29b-41d4-a716-446655490031', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440002', '550e8400-e29b-41d4-a716-446655470004', '550e8400-e29b-41d4-a716-446655480017', 'SOL-205', 'Conversation export: JSON missing metadata', '', 0, 'medium', NULL, NULL, 2, 0, 0, 2, false, NULL, NULL, 'sp', now(), now()),

  -- In progress
  ('550e8400-e29b-41d4-a716-446655490032', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440002', '550e8400-e29b-41d4-a716-446655470004', '550e8400-e29b-41d4-a716-446655480018', 'SOL-208', 'Race: tool result before tool call message', '', 0, 'urgent', '2026-05-04'::timestamptz, NULL, 16, 2, 1, 4, false, NULL, NULL, 'sp', now(), now()),

  -- In review
  ('550e8400-e29b-41d4-a716-446655490033', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440002', '550e8400-e29b-41d4-a716-446655470004', '550e8400-e29b-41d4-a716-446655480019', 'SOL-204', 'Network failure → infinite spinner', '', 0, 'high', NULL, NULL, 5, 1, 2, 2, false, NULL, NULL, 'sp', now(), now()),

  -- Done
  ('550e8400-e29b-41d4-a716-446655490034', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440002', '550e8400-e29b-41d4-a716-446655470004', '550e8400-e29b-41d4-a716-446655480020', 'SOL-198', 'Markdown lists render with extra margin', '', 0, 'low', NULL, NULL, 1, 0, 1, 1, false, NULL, '2026-04-21'::timestamptz, 'sp', now(), now())
ON CONFLICT DO NOTHING;

-- =====================================================================
-- CARDS - MARATHON / ENGINE
-- =====================================================================
INSERT INTO cards (id, tenant_id, project_id, board_id, column_id, ref, title, description, position, priority, due_date, milestone_id, comments_count, attachments_count, checklist_done, checklist_total, blocked, cover, completed_at, created_by, created_at, updated_at)
VALUES
  -- Backlog
  ('550e8400-e29b-41d4-a716-446655490035', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440003', '550e8400-e29b-41d4-a716-446655470005', '550e8400-e29b-41d4-a716-446655480021', 'MAR-070', 'WebGPU backend behind feature flag', '', 0, 'high', NULL, '550e8400-e29b-41d4-a716-446655450004', 9, 0, 0, 14, false, NULL, NULL, 'sp', now(), now()),
  ('550e8400-e29b-41d4-a716-446655490036', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440003', '550e8400-e29b-41d4-a716-446655470005', '550e8400-e29b-41d4-a716-446655480021', 'MAR-068', 'Particle system: GPU instancing', '', 1, 'medium', NULL, '550e8400-e29b-41d4-a716-446655450004', 4, 0, 1, 6, false, NULL, NULL, 'sp', now(), now()),
  ('550e8400-e29b-41d4-a716-446655490037', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440003', '550e8400-e29b-41d4-a716-446655470005', '550e8400-e29b-41d4-a716-446655480021', 'MAR-065', 'Procedural terrain — chunk LOD', '', 2, 'medium', NULL, '550e8400-e29b-41d4-a716-446655450004', 3, 1, 0, 0, false, 'linear-gradient(135deg, #15803d, #ffa110)', NULL, 'sp', now(), now()),

  -- Up next
  ('550e8400-e29b-41d4-a716-446655490038', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440003', '550e8400-e29b-41d4-a716-446655470005', '550e8400-e29b-41d4-a716-446655480022', 'MAR-072', 'Audio: spatialized music beds', '', 0, 'low', NULL, '550e8400-e29b-41d4-a716-446655450004', 0, 0, 0, 4, false, NULL, NULL, 'sp', now(), now()),

  -- In progress
  ('550e8400-e29b-41d4-a716-446655490039', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440003', '550e8400-e29b-41d4-a716-446655470005', '550e8400-e29b-41d4-a716-446655480023', 'MAR-075', 'Asset pipeline: WebP + AVIF support', '', 0, 'high', '2026-05-18'::timestamptz, '550e8400-e29b-41d4-a716-446655450004', 5, 0, 3, 7, false, NULL, NULL, 'sp', now(), now()),

  -- In review
  ('550e8400-e29b-41d4-a716-446655490040', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440003', '550e8400-e29b-41d4-a716-446655470005', '550e8400-e29b-41d4-a716-446655480024', 'MAR-066', 'Scene graph traversal optimisation', '', 0, 'medium', NULL, '550e8400-e29b-41d4-a716-446655450004', 12, 0, 4, 4, false, NULL, NULL, 'sp', now(), now()),

  -- Done
  ('550e8400-e29b-41d4-a716-446655490041', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440003', '550e8400-e29b-41d4-a716-446655470005', '550e8400-e29b-41d4-a716-446655480025', 'MAR-060', 'IndexedDB save layer', '', 0, 'high', NULL, '550e8400-e29b-41d4-a716-446655450004', 3, 0, 5, 5, false, NULL, '2026-04-12'::timestamptz, 'sp', now(), now())
ON CONFLICT DO NOTHING;

-- =====================================================================
-- CARDS - STUDIOS / CONTENT
-- =====================================================================
INSERT INTO cards (id, tenant_id, project_id, board_id, column_id, ref, title, description, position, priority, due_date, milestone_id, comments_count, attachments_count, checklist_done, checklist_total, blocked, cover, completed_at, created_by, created_at, updated_at)
VALUES
  -- Ideas
  ('550e8400-e29b-41d4-a716-446655490042', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440004', '550e8400-e29b-41d4-a716-446655470006', '550e8400-e29b-41d4-a716-446655480026', 'SITE-031', 'Case study — Beam UI at Estafeta', '', 0, 'medium', NULL, NULL, 1, 0, 0, 0, false, NULL, NULL, 'sp', now(), now()),
  ('550e8400-e29b-41d4-a716-446655490043', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440004', '550e8400-e29b-41d4-a716-446655470006', '550e8400-e29b-41d4-a716-446655480026', 'SITE-029', 'Why warm colors? A design treatise', '', 1, 'low', NULL, NULL, 0, 0, 0, 0, false, NULL, NULL, 'sp', now(), now()),

  -- Drafting
  ('550e8400-e29b-41d4-a716-446655490044', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440004', '550e8400-e29b-41d4-a716-446655470006', '550e8400-e29b-41d4-a716-446655480027', 'SITE-035', 'Announcing Sol — public beta post', '', 0, 'high', '2026-05-22'::timestamptz, '550e8400-e29b-41d4-a716-446655450005', 7, 2, 3, 6, false, NULL, NULL, 'sp', now(), now()),
  ('550e8400-e29b-41d4-a716-446655490045', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440004', '550e8400-e29b-41d4-a716-446655470006', '550e8400-e29b-41d4-a716-446655480027', 'SITE-034', 'Designing for offline-first apps', '', 1, 'low', NULL, NULL, 0, 0, 1, 5, false, NULL, NULL, 'sp', now(), now()),

  -- Editing
  ('550e8400-e29b-41d4-a716-446655490046', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440004', '550e8400-e29b-41d4-a716-446655470006', '550e8400-e29b-41d4-a716-446655480028', 'SITE-033', 'Iconography in Beam — a rationale', '', 0, 'medium', NULL, NULL, 4, 1, 4, 5, false, NULL, NULL, 'sp', now(), now()),

  -- Published
  ('550e8400-e29b-41d4-a716-446655490047', 'dev-tenant', '550e8400-e29b-41d4-a716-446655440004', '550e8400-e29b-41d4-a716-446655470006', '550e8400-e29b-41d4-a716-446655480029', 'SITE-028', 'Hello, Sunbeam Studios', '', 0, 'low', NULL, NULL, 12, 0, 1, 1, false, NULL, '2026-04-04'::timestamptz, 'sp', now(), now())
ON CONFLICT DO NOTHING;

-- =====================================================================
-- CARD LABELS (join table)
-- =====================================================================
INSERT INTO card_labels (card_id, label_id)
VALUES
  -- BEAM-204: feature, design
  ('550e8400-e29b-41d4-a716-446655490001', '550e8400-e29b-41d4-a716-446655460001'),
  ('550e8400-e29b-41d4-a716-446655490001', '550e8400-e29b-41d4-a716-446655460003'),
  -- BEAM-198: a11y, docs
  ('550e8400-e29b-41d4-a716-446655490002', '550e8400-e29b-41d4-a716-446655460004'),
  ('550e8400-e29b-41d4-a716-446655490002', '550e8400-e29b-41d4-a716-446655460005'),
  -- BEAM-195: docs
  ('550e8400-e29b-41d4-a716-446655490003', '550e8400-e29b-41d4-a716-446655460005'),
  -- BEAM-191: feature, a11y
  ('550e8400-e29b-41d4-a716-446655490004', '550e8400-e29b-41d4-a716-446655460001'),
  ('550e8400-e29b-41d4-a716-446655490004', '550e8400-e29b-41d4-a716-446655460004'),
  -- BEAM-210: refactor, ui
  ('550e8400-e29b-41d4-a716-446655490005', '550e8400-e29b-41d4-a716-446655460006'),
  ('550e8400-e29b-41d4-a716-446655490005', '550e8400-e29b-41d4-a716-446655460007'),
  -- BEAM-208: ui, design
  ('550e8400-e29b-41d4-a716-446655490006', '550e8400-e29b-41d4-a716-446655460007'),
  ('550e8400-e29b-41d4-a716-446655490006', '550e8400-e29b-41d4-a716-446655460003'),
  -- BEAM-215: feature, performance
  ('550e8400-e29b-41d4-a716-446655490007', '550e8400-e29b-41d4-a716-446655460001'),
  ('550e8400-e29b-41d4-a716-446655490007', '550e8400-e29b-41d4-a716-446655460008'),
  -- BEAM-207: bug
  ('550e8400-e29b-41d4-a716-446655490008', '550e8400-e29b-41d4-a716-446655460002'),
  -- BEAM-203: feature
  ('550e8400-e29b-41d4-a716-446655490009', '550e8400-e29b-41d4-a716-446655460001'),
  -- BEAM-216: refactor, feature
  ('550e8400-e29b-41d4-a716-446655490010', '550e8400-e29b-41d4-a716-446655460006'),
  ('550e8400-e29b-41d4-a716-446655490010', '550e8400-e29b-41d4-a716-446655460001'),
  -- BEAM-211: bug, a11y
  ('550e8400-e29b-41d4-a716-446655490011', '550e8400-e29b-41d4-a716-446655460002'),
  ('550e8400-e29b-41d4-a716-446655490011', '550e8400-e29b-41d4-a716-446655460004'),
  -- BEAM-201: design
  ('550e8400-e29b-41d4-a716-446655490012', '550e8400-e29b-41d4-a716-446655460003'),
  -- BEAM-199: feature
  ('550e8400-e29b-41d4-a716-446655490013', '550e8400-e29b-41d4-a716-446655460001'),
  -- BEAM-220: feature, infra
  ('550e8400-e29b-41d4-a716-446655490014', '550e8400-e29b-41d4-a716-446655460001'),
  ('550e8400-e29b-41d4-a716-446655490014', '550e8400-e29b-41d4-a716-446655460009'),
  -- BEAM-221: docs, feature
  ('550e8400-e29b-41d4-a716-446655490015', '550e8400-e29b-41d4-a716-446655460005'),
  ('550e8400-e29b-41d4-a716-446655490015', '550e8400-e29b-41d4-a716-446655460001'),
  -- BEAM-225: chore
  ('550e8400-e29b-41d4-a716-446655490016', '550e8400-e29b-41d4-a716-446655460010'),
  -- BEAM-228: docs
  ('550e8400-e29b-41d4-a716-446655490017', '550e8400-e29b-41d4-a716-446655460005'),
  -- BEAM-218: infra
  ('550e8400-e29b-41d4-a716-446655490018', '550e8400-e29b-41d4-a716-446655460009'),
  -- SOL-104: feature, ai
  ('550e8400-e29b-41d4-a716-446655490019', '550e8400-e29b-41d4-a716-446655460011'),
  ('550e8400-e29b-41d4-a716-446655490019', '550e8400-e29b-41d4-a716-446655460017'),
  -- SOL-098: research, infra
  ('550e8400-e29b-41d4-a716-446655490020', '550e8400-e29b-41d4-a716-446655460013'),
  ('550e8400-e29b-41d4-a716-446655490020', '550e8400-e29b-41d4-a716-446655460014'),
  -- SOL-091: feature, ui
  ('550e8400-e29b-41d4-a716-446655490021', '550e8400-e29b-41d4-a716-446655460011'),
  ('550e8400-e29b-41d4-a716-446655490021', '550e8400-e29b-41d4-a716-446655460015'),
  -- SOL-107: feature, ai
  ('550e8400-e29b-41d4-a716-446655490022', '550e8400-e29b-41d4-a716-446655460011'),
  ('550e8400-e29b-41d4-a716-446655490022', '550e8400-e29b-41d4-a716-446655460017'),
  -- SOL-106: feature, performance
  ('550e8400-e29b-41d4-a716-446655490023', '550e8400-e29b-41d4-a716-446655460011'),
  ('550e8400-e29b-41d4-a716-446655490023', '550e8400-e29b-41d4-a716-446655460018'),
  -- SOL-110: feature, design
  ('550e8400-e29b-41d4-a716-446655490024', '550e8400-e29b-41d4-a716-446655460011'),
  ('550e8400-e29b-41d4-a716-446655490024', '550e8400-e29b-41d4-a716-446655460016'),
  -- SOL-109: feature, ui
  ('550e8400-e29b-41d4-a716-446655490025', '550e8400-e29b-41d4-a716-446655460011'),
  ('550e8400-e29b-41d4-a716-446655490025', '550e8400-e29b-41d4-a716-446655460015'),
  -- SOL-103: feature
  ('550e8400-e29b-41d4-a716-446655490026', '550e8400-e29b-41d4-a716-446655460011'),
  -- SOL-088: infra, ai
  ('550e8400-e29b-41d4-a716-446655490027', '550e8400-e29b-41d4-a716-446655460014'),
  ('550e8400-e29b-41d4-a716-446655490027', '550e8400-e29b-41d4-a716-446655460017'),
  -- SOL-085: infra
  ('550e8400-e29b-41d4-a716-446655490028', '550e8400-e29b-41d4-a716-446655460014'),
  -- SOL-201: bug, ui
  ('550e8400-e29b-41d4-a716-446655490029', '550e8400-e29b-41d4-a716-446655460012'),
  ('550e8400-e29b-41d4-a716-446655490029', '550e8400-e29b-41d4-a716-446655460015'),
  -- SOL-202: bug, ui
  ('550e8400-e29b-41d4-a716-446655490030', '550e8400-e29b-41d4-a716-446655460012'),
  ('550e8400-e29b-41d4-a716-446655490030', '550e8400-e29b-41d4-a716-446655460015'),
  -- SOL-205: bug, api
  ('550e8400-e29b-41d4-a716-446655490031', '550e8400-e29b-41d4-a716-446655460012'),
  ('550e8400-e29b-41d4-a716-446655490031', '550e8400-e29b-41d4-a716-446655460019'),
  -- SOL-208: bug, ai
  ('550e8400-e29b-41d4-a716-446655490032', '550e8400-e29b-41d4-a716-446655460012'),
  ('550e8400-e29b-41d4-a716-446655490032', '550e8400-e29b-41d4-a716-446655460017'),
  -- SOL-204: bug
  ('550e8400-e29b-41d4-a716-446655490033', '550e8400-e29b-41d4-a716-446655460012'),
  -- SOL-198: bug, ui
  ('550e8400-e29b-41d4-a716-446655490034', '550e8400-e29b-41d4-a716-446655460012'),
  ('550e8400-e29b-41d4-a716-446655490034', '550e8400-e29b-41d4-a716-446655460015'),
  -- MAR-070: feature, performance
  ('550e8400-e29b-41d4-a716-446655490035', '550e8400-e29b-41d4-a716-446655460020'),
  ('550e8400-e29b-41d4-a716-446655490035', '550e8400-e29b-41d4-a716-446655460021'),
  -- MAR-068: performance
  ('550e8400-e29b-41d4-a716-446655490036', '550e8400-e29b-41d4-a716-446655460021'),
  -- MAR-065: feature
  ('550e8400-e29b-41d4-a716-446655490037', '550e8400-e29b-41d4-a716-446655460020'),
  -- MAR-072: feature
  ('550e8400-e29b-41d4-a716-446655490038', '550e8400-e29b-41d4-a716-446655460020'),
  -- MAR-075: infra, performance
  ('550e8400-e29b-41d4-a716-446655490039', '550e8400-e29b-41d4-a716-446655460023'),
  ('550e8400-e29b-41d4-a716-446655490039', '550e8400-e29b-41d4-a716-446655460021'),
  -- MAR-066: performance, refactor
  ('550e8400-e29b-41d4-a716-446655490040', '550e8400-e29b-41d4-a716-446655460021'),
  ('550e8400-e29b-41d4-a716-446655490040', '550e8400-e29b-41d4-a716-446655460022'),
  -- MAR-060: feature, infra
  ('550e8400-e29b-41d4-a716-446655490041', '550e8400-e29b-41d4-a716-446655460020'),
  ('550e8400-e29b-41d4-a716-446655490041', '550e8400-e29b-41d4-a716-446655460023'),
  -- SITE-031: feature
  ('550e8400-e29b-41d4-a716-446655490042', '550e8400-e29b-41d4-a716-446655460024'),
  -- SITE-029: design
  ('550e8400-e29b-41d4-a716-446655490043', '550e8400-e29b-41d4-a716-446655460025'),
  -- SITE-035: feature, ai
  ('550e8400-e29b-41d4-a716-446655490044', '550e8400-e29b-41d4-a716-446655460024'),
  ('550e8400-e29b-41d4-a716-446655490044', '550e8400-e29b-41d4-a716-446655460027'),
  -- SITE-034: design
  ('550e8400-e29b-41d4-a716-446655490045', '550e8400-e29b-41d4-a716-446655460025'),
  -- SITE-033: design, docs
  ('550e8400-e29b-41d4-a716-446655490046', '550e8400-e29b-41d4-a716-446655460025'),
  ('550e8400-e29b-41d4-a716-446655490046', '550e8400-e29b-41d4-a716-446655460026'),
  -- SITE-028: docs
  ('550e8400-e29b-41d4-a716-446655490047', '550e8400-e29b-41d4-a716-446655460026')
ON CONFLICT DO NOTHING;

-- =====================================================================
-- CARD ASSIGNEES (join table)
-- =====================================================================
INSERT INTO card_assignees (card_id, tenant_id, subject)
VALUES
  -- BEAM-204: sp
  ('550e8400-e29b-41d4-a716-446655490001', 'dev-tenant', 'sp'),
  -- BEAM-198: mc, sp
  ('550e8400-e29b-41d4-a716-446655490002', 'dev-tenant', 'mc'),
  ('550e8400-e29b-41d4-a716-446655490002', 'dev-tenant', 'sp'),
  -- BEAM-195: ak
  ('550e8400-e29b-41d4-a716-446655490003', 'dev-tenant', 'ak'),
  -- BEAM-191: lr
  ('550e8400-e29b-41d4-a716-446655490004', 'dev-tenant', 'lr'),
  -- BEAM-210: ak
  ('550e8400-e29b-41d4-a716-446655490005', 'dev-tenant', 'ak'),
  -- BEAM-208: mb
  ('550e8400-e29b-41d4-a716-446655490006', 'dev-tenant', 'mb'),
  -- BEAM-215: mc, lr
  ('550e8400-e29b-41d4-a716-446655490007', 'dev-tenant', 'mc'),
  ('550e8400-e29b-41d4-a716-446655490007', 'dev-tenant', 'lr'),
  -- BEAM-207: mc
  ('550e8400-e29b-41d4-a716-446655490008', 'dev-tenant', 'mc'),
  -- BEAM-203: ak
  ('550e8400-e29b-41d4-a716-446655490009', 'dev-tenant', 'ak'),
  -- BEAM-216: sp
  ('550e8400-e29b-41d4-a716-446655490010', 'dev-tenant', 'sp'),
  -- BEAM-211: mc
  ('550e8400-e29b-41d4-a716-446655490011', 'dev-tenant', 'mc'),
  -- BEAM-201: sp
  ('550e8400-e29b-41d4-a716-446655490012', 'dev-tenant', 'sp'),
  -- BEAM-199: lr
  ('550e8400-e29b-41d4-a716-446655490013', 'dev-tenant', 'lr'),
  -- BEAM-220: ak
  ('550e8400-e29b-41d4-a716-446655490014', 'dev-tenant', 'ak'),
  -- BEAM-221: sp
  ('550e8400-e29b-41d4-a716-446655490015', 'dev-tenant', 'sp'),
  -- BEAM-225: mc
  ('550e8400-e29b-41d4-a716-446655490016', 'dev-tenant', 'mc'),
  -- BEAM-228: ak, sp
  ('550e8400-e29b-41d4-a716-446655490017', 'dev-tenant', 'ak'),
  ('550e8400-e29b-41d4-a716-446655490017', 'dev-tenant', 'sp'),
  -- BEAM-218: lr
  ('550e8400-e29b-41d4-a716-446655490018', 'dev-tenant', 'lr'),
  -- SOL-104: tr, mc
  ('550e8400-e29b-41d4-a716-446655490019', 'dev-tenant', 'tr'),
  ('550e8400-e29b-41d4-a716-446655490019', 'dev-tenant', 'mc'),
  -- SOL-098: in
  ('550e8400-e29b-41d4-a716-446655490020', 'dev-tenant', 'in'),
  -- SOL-091: sp
  ('550e8400-e29b-41d4-a716-446655490021', 'dev-tenant', 'sp'),
  -- SOL-107: mc
  ('550e8400-e29b-41d4-a716-446655490022', 'dev-tenant', 'mc'),
  -- SOL-106: tr
  ('550e8400-e29b-41d4-a716-446655490023', 'dev-tenant', 'tr'),
  -- SOL-110: sp, jc
  ('550e8400-e29b-41d4-a716-446655490024', 'dev-tenant', 'sp'),
  ('550e8400-e29b-41d4-a716-446655490024', 'dev-tenant', 'jc'),
  -- SOL-109: tr
  ('550e8400-e29b-41d4-a716-446655490025', 'dev-tenant', 'tr'),
  -- SOL-103: jc
  ('550e8400-e29b-41d4-a716-446655490026', 'dev-tenant', 'jc'),
  -- SOL-088: mc
  ('550e8400-e29b-41d4-a716-446655490027', 'dev-tenant', 'mc'),
  -- SOL-085: tr
  ('550e8400-e29b-41d4-a716-446655490028', 'dev-tenant', 'tr'),
  -- SOL-201: sp
  ('550e8400-e29b-41d4-a716-446655490029', 'dev-tenant', 'sp'),
  -- SOL-202: tr
  ('550e8400-e29b-41d4-a716-446655490030', 'dev-tenant', 'tr'),
  -- SOL-205: mc
  ('550e8400-e29b-41d4-a716-446655490031', 'dev-tenant', 'mc'),
  -- SOL-208: mc, tr
  ('550e8400-e29b-41d4-a716-446655490032', 'dev-tenant', 'mc'),
  ('550e8400-e29b-41d4-a716-446655490032', 'dev-tenant', 'tr'),
  -- SOL-204: tr
  ('550e8400-e29b-41d4-a716-446655490033', 'dev-tenant', 'tr'),
  -- SOL-198: sp
  ('550e8400-e29b-41d4-a716-446655490034', 'dev-tenant', 'sp'),
  -- MAR-070: lr
  ('550e8400-e29b-41d4-a716-446655490035', 'dev-tenant', 'lr'),
  -- MAR-068: tr
  ('550e8400-e29b-41d4-a716-446655490036', 'dev-tenant', 'tr'),
  -- MAR-065: ak
  ('550e8400-e29b-41d4-a716-446655490037', 'dev-tenant', 'ak'),
  -- MAR-072: in
  ('550e8400-e29b-41d4-a716-446655490038', 'dev-tenant', 'in'),
  -- MAR-075: lr, tr
  ('550e8400-e29b-41d4-a716-446655490039', 'dev-tenant', 'lr'),
  ('550e8400-e29b-41d4-a716-446655490039', 'dev-tenant', 'tr'),
  -- MAR-066: lr
  ('550e8400-e29b-41d4-a716-446655490040', 'dev-tenant', 'lr'),
  -- MAR-060: tr
  ('550e8400-e29b-41d4-a716-446655490041', 'dev-tenant', 'tr'),
  -- SITE-031: jc
  ('550e8400-e29b-41d4-a716-446655490042', 'dev-tenant', 'jc'),
  -- SITE-029: sp
  ('550e8400-e29b-41d4-a716-446655490043', 'dev-tenant', 'sp'),
  -- SITE-035: jc, sp
  ('550e8400-e29b-41d4-a716-446655490044', 'dev-tenant', 'jc'),
  ('550e8400-e29b-41d4-a716-446655490044', 'dev-tenant', 'sp'),
  -- SITE-034: mb
  ('550e8400-e29b-41d4-a716-446655490045', 'dev-tenant', 'mb'),
  -- SITE-033: mb, sp
  ('550e8400-e29b-41d4-a716-446655490046', 'dev-tenant', 'mb'),
  ('550e8400-e29b-41d4-a716-446655490046', 'dev-tenant', 'sp'),
  -- SITE-028: jc
  ('550e8400-e29b-41d4-a716-446655490047', 'dev-tenant', 'jc')
ON CONFLICT DO NOTHING;
