-- SPDX-License-Identifier: AGPL-3.0-or-later
-- Backfill prefix for projects created after 0019_projects_prefix.
-- create_project never wrote the prefix column, so projects created via the
-- RPC have prefix = '' and mint card refs like "-001" instead of "TRI-001".
-- Derive the same fallback 0019 used: first 4 chars of slug, uppercased.

UPDATE projects
SET prefix = UPPER(SUBSTRING(slug, 1, 4))
WHERE prefix = '';
