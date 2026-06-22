-- SPDX-License-Identifier: AGPL-3.0-or-later
-- Add visibility levels to aggregated boards: private, internal, public.

ALTER TABLE aggregated_boards
  ADD COLUMN visibility TEXT NOT NULL DEFAULT 'private';

CREATE INDEX idx_agg_boards_visibility ON aggregated_boards(visibility);
