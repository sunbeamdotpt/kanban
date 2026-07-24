-- SPDX-License-Identifier: AGPL-3.0-or-later
-- event_log: project-scoped outbox events.
--
-- Project mutations (member changes, project field updates, board lifecycle)
-- also use the transactional outbox, but they have no board context. The
-- routing object id goes into a nullable `project_id` column (TEXT, matching
-- the post-0026 ULID/legacy-UUID string form of every other id column) and
-- the dispatcher publishes those rows to kanban.project.<id>.events.

ALTER TABLE event_log
    ADD COLUMN project_id TEXT REFERENCES projects(id) ON DELETE CASCADE;

ALTER TABLE event_log
    DROP CONSTRAINT event_log_one_object_id;

ALTER TABLE event_log
    ADD CONSTRAINT event_log_one_object_id
    CHECK (board_id IS NOT NULL OR aggregated_board_id IS NOT NULL OR project_id IS NOT NULL);

CREATE INDEX idx_event_log_project ON event_log(project_id);
