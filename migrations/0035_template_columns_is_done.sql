-- SPDX-License-Identifier: AGPL-3.0-or-later
-- Mark the Done column is_done in the seeded global board templates
-- (KANBAN-033).
--
-- Column.is_done (0034) opts a column into completed_at stamping, but the
-- four seeded global templates (standard, Kanban, Sprint, Simple) predate the
-- flag, so boards built from them never stamp completions without a manual
-- column update. Template columns are stored as a JSONB array; rewrite the
-- array of every global template, flagging completion-titled entries.
-- Title matching mirrors the production backfill of 2026-07-31
-- ('done' | 'completed' | 'complete'). Project-scoped custom templates are
-- untouched: their owners can now express is_done explicitly via
-- TemplateColumn.is_done. Idempotent by construction (flagging an already
-- flagged entry is a no-op).

UPDATE board_templates
SET columns = (
  SELECT jsonb_agg(
    CASE
      WHEN lower(col->>'title') IN ('done', 'completed', 'complete')
        THEN col || '{"is_done": true}'::jsonb
      ELSE col
    END
    ORDER BY ord
  )
  FROM jsonb_array_elements(columns) WITH ORDINALITY AS t(col, ord)
)
WHERE is_global
  AND EXISTS (
    SELECT 1
    FROM jsonb_array_elements(columns) AS c
    WHERE lower(c->>'title') IN ('done', 'completed', 'complete')
  );
