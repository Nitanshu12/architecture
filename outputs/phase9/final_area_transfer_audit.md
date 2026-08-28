# Phase 9: Final Area Transfer Audit

Source events: 1550

Event summary rows: 1550

Unique events represented in transfer table: 1550

Transfer relationships: 1550

Measured relationships: 76

Normalized relationships: 311

Unmeasured relationships: 1022

Low coverage relationships: 7

Questionable relationships: 0

Duplicate relationships: 0

Negative overlap values: 0 (MUST BE 0)

NULL measurement statuses: 0 (MUST BE 0)

Area calculation mismatches: 0 (MUST BE 0)

Random validation failures: 0 (MUST BE 0)

## Validations Performed
- All 1550 source events are correctly populated in `event_register` and `event_area_summary`.
- `overlap_excess` is calculated at the source group level and replaces negative raw differentials with proper UNION coverage subtraction.
- Missing spatial data is assigned `UNMEASURED` cleanly (without dropping records via `LEFT JOIN`).
- Overlapping snapshots (`MULTIPLE_VINTAGES`) have been resolved by picking the latest `snapshot_id` using a strict `QUALIFY ROW_NUMBER() = 1` deduplication step.
