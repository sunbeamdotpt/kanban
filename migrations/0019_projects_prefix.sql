-- Add prefix column to projects for card-ref allocation (MF-3, Stage 3c)
-- Default to upper-case slug prefix (first 4 chars of slug, uppercased).
-- Existing rows get a derived prefix; new rows should set it explicitly.

ALTER TABLE projects
  ADD COLUMN IF NOT EXISTS prefix TEXT NOT NULL DEFAULT '';

-- Back-fill existing rows from their slug (first 4 chars, upper-cased).
UPDATE projects
SET prefix = UPPER(SUBSTRING(slug, 1, 4))
WHERE prefix = '';

-- Ensure prefix is non-empty for future rows — enforce at app level.
