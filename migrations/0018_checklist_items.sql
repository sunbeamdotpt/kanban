-- SPDX-License-Identifier: AGPL-3.0-or-later
-- checklist_items: per-card ordered checklist (Stage 3c)

CREATE TABLE checklist_items (
  id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id  TEXT NOT NULL,
  card_id    UUID NOT NULL REFERENCES cards(id) ON DELETE CASCADE,
  text       TEXT NOT NULL,
  done       BOOLEAN NOT NULL DEFAULT false,
  position   INT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_checklist_items_card ON checklist_items(card_id);
CREATE INDEX idx_checklist_items_tenant ON checklist_items(tenant_id);
