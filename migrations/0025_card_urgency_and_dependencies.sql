-- SPDX-License-Identifier: AGPL-3.0-or-later
-- Add native Postgres enums for priority/urgency, a card urgency column,
-- and a join table for card dependencies.

CREATE TYPE card_priority AS ENUM ('low', 'medium', 'high', 'urgent');
CREATE TYPE card_urgency AS ENUM ('low', 'medium', 'high', 'critical');

ALTER TABLE cards DROP CONSTRAINT IF EXISTS cards_priority_check;
ALTER TABLE cards ALTER COLUMN priority DROP DEFAULT;
ALTER TABLE cards ALTER COLUMN priority TYPE card_priority USING priority::card_priority;
ALTER TABLE cards ALTER COLUMN priority SET DEFAULT 'medium';

ALTER TABLE cards
  ADD COLUMN urgency card_urgency NOT NULL DEFAULT 'medium';

CREATE TABLE card_dependencies (
  card_id            UUID NOT NULL REFERENCES cards(id) ON DELETE CASCADE,
  depends_on_card_id UUID NOT NULL REFERENCES cards(id) ON DELETE CASCADE,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (card_id, depends_on_card_id),
  CHECK (card_id != depends_on_card_id)
);

CREATE INDEX idx_card_dependencies_depends_on
  ON card_dependencies(depends_on_card_id);
