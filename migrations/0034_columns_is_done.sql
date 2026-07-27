-- SPDX-License-Identifier: AGPL-3.0-or-later
-- is_done marker on columns (KANBAN-023).
--
-- Moving a card into a done-marked column sets completed_at; moving it out
-- clears it. Milestone completion stats count completed_at, so board-driven
-- workflows now advance milestone progress. Existing columns default to
-- false; clients mark their done columns via UpdateColumn (update_mask
-- "is_done").

ALTER TABLE columns ADD COLUMN is_done BOOLEAN NOT NULL DEFAULT false;
