-- SPDX-License-Identifier: AGPL-3.0-or-later
-- Canonicalize seeded global template IDs (KANBAN-026, KANBAN-022).
--
-- 0029 seeded four IDs whose first Crockford character exceeds the ULID
-- timestamp range (only 0-7 is canonical). The application's Id type decodes
-- them by masking the overflow bits and re-encodes canonically, so List
-- returned IDs ('7FV6…') that Get could not resolve against the stored values
-- ('ZFV6…') and 404'd. Rewrite the stored values to their canonical forms so
-- the stored string and the API string are identical.
--
-- Also reseed the three legacy UUID board templates from 0017 with ULIDs so
-- every seeded global template exposes a ULID (KANBAN-022). Legacy UUIDs on
-- project-scoped templates remain valid read-side: the Id type round-trips
-- them. No foreign keys reference template IDs, so a plain UPDATE is safe.

UPDATE card_templates SET id = '2QM9Y81KGB6MQ3Q27Z72V12WDV' WHERE id = 'AQM9Y81KGB6MQ3Q27Z72V12WDV';
UPDATE card_templates SET id = '7FV6D6484C46GZSJQAKWTJ70GW' WHERE id = 'ZFV6D6484C46GZSJQAKWTJ70GW';
UPDATE card_templates SET id = '5W0719C2ZHR65NRBCMS17KWAPR' WHERE id = 'DW0719C2ZHR65NRBCMS17KWAPR';
UPDATE board_templates SET id = '3SNAQWV3M8RRVGRB03V0215953' WHERE id = 'KSNAQWV3M8RRVGRB03V0215953';

UPDATE board_templates SET id = '01KYJRZ3WA3GYYH1CP088WTP27'
 WHERE id LIKE '%-%' AND tenant_id = 'system' AND name = 'Kanban' AND is_global;
UPDATE board_templates SET id = '01KYJRZ3WA42FVTBTDQGV39XJZ'
 WHERE id LIKE '%-%' AND tenant_id = 'system' AND name = 'Sprint' AND is_global;
UPDATE board_templates SET id = '01KYJRZ3WA0GMX3F9HDF5P6M5X'
 WHERE id LIKE '%-%' AND tenant_id = 'system' AND name = 'Simple' AND is_global;
