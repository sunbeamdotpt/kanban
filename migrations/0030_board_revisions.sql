-- SPDX-License-Identifier: AGPL-3.0-or-later
-- Board revision counters.
--
-- Every mutation that writes an outbox event for a board bumps the board's
-- revision in the same transaction (see src/event_log.rs). The outbox
-- dispatcher copies the value into BoardEventEnvelope.board_revision so
-- clients can cache-bust at board granularity.

ALTER TABLE boards
    ADD COLUMN revision BIGINT NOT NULL DEFAULT 0;

ALTER TABLE aggregated_boards
    ADD COLUMN revision BIGINT NOT NULL DEFAULT 0;
