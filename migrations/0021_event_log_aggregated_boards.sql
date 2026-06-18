-- event_log: support aggregated-board events alongside regular board events.
--
-- AggregatedBoard mutations also use the transactional outbox, but they are
-- not rows in the `boards` table. We therefore store the routing object id in
-- a separate nullable `aggregated_board_id` column and let the dispatcher pick
-- whichever id is present.

ALTER TABLE event_log
    ADD COLUMN aggregated_board_id UUID REFERENCES aggregated_boards(id) ON DELETE CASCADE;

ALTER TABLE event_log
    ALTER COLUMN board_id DROP NOT NULL;

ALTER TABLE event_log
    ADD CONSTRAINT event_log_one_object_id
    CHECK (board_id IS NOT NULL OR aggregated_board_id IS NOT NULL);

CREATE INDEX idx_event_log_aggregated_board ON event_log(aggregated_board_id);
