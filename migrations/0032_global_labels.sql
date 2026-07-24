-- SPDX-License-Identifier: AGPL-3.0-or-later
-- labels: allow global (tenant-wide) label catalog entries.
--
-- A label with project_id IS NULL is global: visible in every project of the
-- tenant via ListLabels. The FK to projects is unaffected (NULL is allowed),
-- and card_labels joins keep working unchanged.
--
-- The old UNIQUE (tenant_id, project_id, name) constraint cannot express
-- uniqueness for NULL project_id (NULLs compare distinct), so it is replaced
-- by two partial unique indexes, one per scope.

ALTER TABLE labels
    ALTER COLUMN project_id DROP NOT NULL;

ALTER TABLE labels
    DROP CONSTRAINT labels_tenant_id_project_id_name_key;

CREATE UNIQUE INDEX labels_project_name_unique
    ON labels (tenant_id, project_id, name)
    WHERE project_id IS NOT NULL;

CREATE UNIQUE INDEX labels_global_name_unique
    ON labels (tenant_id, name)
    WHERE project_id IS NULL;
