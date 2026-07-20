-- SPDX-License-Identifier: AGPL-3.0-or-later
-- card_assignees: join table for OIDC subjects assigned to cards

CREATE TABLE card_assignees (
  tenant_id   TEXT NOT NULL,
  card_id     UUID NOT NULL REFERENCES cards(id) ON DELETE CASCADE,
  subject     TEXT NOT NULL,
  assigned_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (card_id, subject)
);

CREATE INDEX idx_card_assignees_subject ON card_assignees(subject);
CREATE INDEX idx_card_assignees_tenant ON card_assignees(tenant_id);
