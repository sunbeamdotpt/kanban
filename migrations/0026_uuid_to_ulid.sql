-- SPDX-License-Identifier: AGPL-3.0-or-later
-- Schema migration: widen all identifier columns from UUID to TEXT.
--
-- Existing deployments keep their current identifier values (now stored as
-- strings); the application accepts both legacy UUIDs and new ULIDs. New
-- identifiers are generated as ULIDs by the application, so the gen_random_uuid
-- defaults are removed to avoid accidentally minting UUIDs.

BEGIN;

-- Drop foreign keys so column type changes do not fail on type mismatches.
ALTER TABLE boards DROP CONSTRAINT IF EXISTS boards_project_id_fkey;
ALTER TABLE columns DROP CONSTRAINT IF EXISTS columns_board_id_fkey;
ALTER TABLE cards DROP CONSTRAINT IF EXISTS cards_project_id_fkey;
ALTER TABLE cards DROP CONSTRAINT IF EXISTS cards_column_id_fkey;
ALTER TABLE cards DROP CONSTRAINT IF EXISTS cards_board_id_fkey;
ALTER TABLE card_attachments DROP CONSTRAINT IF EXISTS card_attachments_card_id_fkey;
ALTER TABLE milestones DROP CONSTRAINT IF EXISTS milestones_project_id_fkey;
ALTER TABLE labels DROP CONSTRAINT IF EXISTS labels_project_id_fkey;
ALTER TABLE card_labels DROP CONSTRAINT IF EXISTS card_labels_card_id_fkey;
ALTER TABLE card_labels DROP CONSTRAINT IF EXISTS card_labels_label_id_fkey;
ALTER TABLE card_assignees DROP CONSTRAINT IF EXISTS card_assignees_card_id_fkey;
ALTER TABLE comments DROP CONSTRAINT IF EXISTS comments_card_id_fkey;
ALTER TABLE checklist_items DROP CONSTRAINT IF EXISTS checklist_items_card_id_fkey;
ALTER TABLE github_links DROP CONSTRAINT IF EXISTS github_links_card_id_fkey;
ALTER TABLE board_templates DROP CONSTRAINT IF EXISTS board_templates_project_id_fkey;
ALTER TABLE card_templates DROP CONSTRAINT IF EXISTS card_templates_project_id_fkey;
ALTER TABLE aggregated_board_sources DROP CONSTRAINT IF EXISTS aggregated_board_sources_aggregated_board_id_fkey;
ALTER TABLE aggregated_board_sources DROP CONSTRAINT IF EXISTS aggregated_board_sources_board_id_fkey;
ALTER TABLE aggregated_board_members DROP CONSTRAINT IF EXISTS aggregated_board_members_aggregated_board_id_fkey;
ALTER TABLE event_log DROP CONSTRAINT IF EXISTS event_log_board_id_fkey;
ALTER TABLE event_log DROP CONSTRAINT IF EXISTS event_log_aggregated_board_id_fkey;
ALTER TABLE presence DROP CONSTRAINT IF EXISTS presence_board_id_fkey;
ALTER TABLE project_ref_counter DROP CONSTRAINT IF EXISTS project_ref_counter_project_id_fkey;
ALTER TABLE project_members DROP CONSTRAINT IF EXISTS project_members_project_id_fkey;
ALTER TABLE card_dependencies DROP CONSTRAINT IF EXISTS card_dependencies_card_id_fkey;
ALTER TABLE card_dependencies DROP CONSTRAINT IF EXISTS card_dependencies_depends_on_card_id_fkey;

-- Drop the self-referential check so the two UUID columns can be widened independently.
ALTER TABLE card_dependencies DROP CONSTRAINT IF EXISTS card_dependencies_check;

-- Core entity tables
ALTER TABLE projects ALTER COLUMN id TYPE TEXT USING id::TEXT;
ALTER TABLE projects ALTER COLUMN id DROP DEFAULT;

ALTER TABLE boards ALTER COLUMN id TYPE TEXT USING id::TEXT;
ALTER TABLE boards ALTER COLUMN id DROP DEFAULT;
ALTER TABLE boards ALTER COLUMN project_id TYPE TEXT USING project_id::TEXT;

ALTER TABLE columns ALTER COLUMN id TYPE TEXT USING id::TEXT;
ALTER TABLE columns ALTER COLUMN id DROP DEFAULT;
ALTER TABLE columns ALTER COLUMN board_id TYPE TEXT USING board_id::TEXT;

ALTER TABLE cards ALTER COLUMN id TYPE TEXT USING id::TEXT;
ALTER TABLE cards ALTER COLUMN id DROP DEFAULT;
ALTER TABLE cards ALTER COLUMN project_id TYPE TEXT USING project_id::TEXT;
ALTER TABLE cards ALTER COLUMN column_id TYPE TEXT USING column_id::TEXT;
ALTER TABLE cards ALTER COLUMN board_id TYPE TEXT USING board_id::TEXT;
ALTER TABLE cards ALTER COLUMN milestone_id TYPE TEXT USING milestone_id::TEXT;

ALTER TABLE card_attachments ALTER COLUMN id TYPE TEXT USING id::TEXT;
ALTER TABLE card_attachments ALTER COLUMN id DROP DEFAULT;
ALTER TABLE card_attachments ALTER COLUMN card_id TYPE TEXT USING card_id::TEXT;

ALTER TABLE milestones ALTER COLUMN id TYPE TEXT USING id::TEXT;
ALTER TABLE milestones ALTER COLUMN id DROP DEFAULT;
ALTER TABLE milestones ALTER COLUMN project_id TYPE TEXT USING project_id::TEXT;

ALTER TABLE labels ALTER COLUMN id TYPE TEXT USING id::TEXT;
ALTER TABLE labels ALTER COLUMN id DROP DEFAULT;
ALTER TABLE labels ALTER COLUMN project_id TYPE TEXT USING project_id::TEXT;

ALTER TABLE comments ALTER COLUMN id TYPE TEXT USING id::TEXT;
ALTER TABLE comments ALTER COLUMN id DROP DEFAULT;
ALTER TABLE comments ALTER COLUMN card_id TYPE TEXT USING card_id::TEXT;

ALTER TABLE checklist_items ALTER COLUMN id TYPE TEXT USING id::TEXT;
ALTER TABLE checklist_items ALTER COLUMN id DROP DEFAULT;
ALTER TABLE checklist_items ALTER COLUMN card_id TYPE TEXT USING card_id::TEXT;

ALTER TABLE github_links ALTER COLUMN id TYPE TEXT USING id::TEXT;
ALTER TABLE github_links ALTER COLUMN id DROP DEFAULT;
ALTER TABLE github_links ALTER COLUMN card_id TYPE TEXT USING card_id::TEXT;

-- Templates
ALTER TABLE board_templates ALTER COLUMN id TYPE TEXT USING id::TEXT;
ALTER TABLE board_templates ALTER COLUMN id DROP DEFAULT;
ALTER TABLE board_templates ALTER COLUMN project_id TYPE TEXT USING project_id::TEXT;

ALTER TABLE card_templates ALTER COLUMN id TYPE TEXT USING id::TEXT;
ALTER TABLE card_templates ALTER COLUMN id DROP DEFAULT;
ALTER TABLE card_templates ALTER COLUMN project_id TYPE TEXT USING project_id::TEXT;

-- Aggregated boards
ALTER TABLE aggregated_boards ALTER COLUMN id TYPE TEXT USING id::TEXT;
ALTER TABLE aggregated_boards ALTER COLUMN id DROP DEFAULT;

ALTER TABLE aggregated_board_sources ALTER COLUMN aggregated_board_id TYPE TEXT USING aggregated_board_id::TEXT;
ALTER TABLE aggregated_board_sources ALTER COLUMN board_id TYPE TEXT USING board_id::TEXT;

ALTER TABLE aggregated_board_members ALTER COLUMN aggregated_board_id TYPE TEXT USING aggregated_board_id::TEXT;

-- Event log and idempotency
ALTER TABLE event_log ALTER COLUMN id TYPE TEXT USING id::TEXT;
ALTER TABLE event_log ALTER COLUMN id DROP DEFAULT;
ALTER TABLE event_log ALTER COLUMN board_id TYPE TEXT USING board_id::TEXT;
ALTER TABLE event_log ALTER COLUMN aggregated_board_id TYPE TEXT USING aggregated_board_id::TEXT;

ALTER TABLE idempotency_keys ALTER COLUMN response_card_id TYPE TEXT USING response_card_id::TEXT;

-- Cross-reference and membership tables
ALTER TABLE project_members ALTER COLUMN project_id TYPE TEXT USING project_id::TEXT;

ALTER TABLE card_labels ALTER COLUMN card_id TYPE TEXT USING card_id::TEXT;
ALTER TABLE card_labels ALTER COLUMN label_id TYPE TEXT USING label_id::TEXT;

ALTER TABLE card_assignees ALTER COLUMN card_id TYPE TEXT USING card_id::TEXT;

ALTER TABLE presence ALTER COLUMN board_id TYPE TEXT USING board_id::TEXT;

ALTER TABLE project_ref_counter ALTER COLUMN project_id TYPE TEXT USING project_id::TEXT;

-- Card dependencies (added in migration 0025)
ALTER TABLE card_dependencies ALTER COLUMN card_id TYPE TEXT USING card_id::TEXT;
ALTER TABLE card_dependencies ALTER COLUMN depends_on_card_id TYPE TEXT USING depends_on_card_id::TEXT;

-- Restore foreign keys.
ALTER TABLE boards ADD CONSTRAINT boards_project_id_fkey FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE;
ALTER TABLE columns ADD CONSTRAINT columns_board_id_fkey FOREIGN KEY (board_id) REFERENCES boards(id) ON DELETE CASCADE;
ALTER TABLE cards ADD CONSTRAINT cards_project_id_fkey FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE;
ALTER TABLE cards ADD CONSTRAINT cards_column_id_fkey FOREIGN KEY (column_id) REFERENCES columns(id) ON DELETE CASCADE;
ALTER TABLE cards ADD CONSTRAINT cards_board_id_fkey FOREIGN KEY (board_id) REFERENCES boards(id) ON DELETE CASCADE;
ALTER TABLE card_attachments ADD CONSTRAINT card_attachments_card_id_fkey FOREIGN KEY (card_id) REFERENCES cards(id) ON DELETE CASCADE;
ALTER TABLE milestones ADD CONSTRAINT milestones_project_id_fkey FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE;
ALTER TABLE labels ADD CONSTRAINT labels_project_id_fkey FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE;
ALTER TABLE card_labels ADD CONSTRAINT card_labels_card_id_fkey FOREIGN KEY (card_id) REFERENCES cards(id) ON DELETE CASCADE;
ALTER TABLE card_labels ADD CONSTRAINT card_labels_label_id_fkey FOREIGN KEY (label_id) REFERENCES labels(id) ON DELETE CASCADE;
ALTER TABLE card_assignees ADD CONSTRAINT card_assignees_card_id_fkey FOREIGN KEY (card_id) REFERENCES cards(id) ON DELETE CASCADE;
ALTER TABLE comments ADD CONSTRAINT comments_card_id_fkey FOREIGN KEY (card_id) REFERENCES cards(id) ON DELETE CASCADE;
ALTER TABLE checklist_items ADD CONSTRAINT checklist_items_card_id_fkey FOREIGN KEY (card_id) REFERENCES cards(id) ON DELETE CASCADE;
ALTER TABLE github_links ADD CONSTRAINT github_links_card_id_fkey FOREIGN KEY (card_id) REFERENCES cards(id);
ALTER TABLE board_templates ADD CONSTRAINT board_templates_project_id_fkey FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE;
ALTER TABLE card_templates ADD CONSTRAINT card_templates_project_id_fkey FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE;
ALTER TABLE aggregated_board_sources ADD CONSTRAINT aggregated_board_sources_aggregated_board_id_fkey FOREIGN KEY (aggregated_board_id) REFERENCES aggregated_boards(id) ON DELETE CASCADE;
ALTER TABLE aggregated_board_sources ADD CONSTRAINT aggregated_board_sources_board_id_fkey FOREIGN KEY (board_id) REFERENCES boards(id) ON DELETE CASCADE;
ALTER TABLE aggregated_board_members ADD CONSTRAINT aggregated_board_members_aggregated_board_id_fkey FOREIGN KEY (aggregated_board_id) REFERENCES aggregated_boards(id) ON DELETE CASCADE;
ALTER TABLE event_log ADD CONSTRAINT event_log_board_id_fkey FOREIGN KEY (board_id) REFERENCES boards(id) ON DELETE CASCADE;
ALTER TABLE event_log ADD CONSTRAINT event_log_aggregated_board_id_fkey FOREIGN KEY (aggregated_board_id) REFERENCES aggregated_boards(id) ON DELETE CASCADE;
ALTER TABLE presence ADD CONSTRAINT presence_board_id_fkey FOREIGN KEY (board_id) REFERENCES boards(id) ON DELETE CASCADE;
ALTER TABLE project_ref_counter ADD CONSTRAINT project_ref_counter_project_id_fkey FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE;
ALTER TABLE project_members ADD CONSTRAINT project_members_project_id_fkey FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE;
ALTER TABLE card_dependencies ADD CONSTRAINT card_dependencies_card_id_fkey FOREIGN KEY (card_id) REFERENCES cards(id) ON DELETE CASCADE;
ALTER TABLE card_dependencies ADD CONSTRAINT card_dependencies_depends_on_card_id_fkey FOREIGN KEY (depends_on_card_id) REFERENCES cards(id) ON DELETE CASCADE;

-- Restore the self-referential check.
ALTER TABLE card_dependencies ADD CONSTRAINT card_dependencies_check CHECK (card_id != depends_on_card_id);

COMMIT;
