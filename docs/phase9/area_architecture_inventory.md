# Area Architecture Inventory

## Identity & Temporal Tracking
- **Source District / Target District**: Identified via `canonical_key` (e.g., `IND-000429`). The `canonical_key_registry` stores stable identity across years.
- **Parent / Child**: Identified through `district_relationship` where `from_ck` is the parent and `to_ck` is the child.
- **Event**: `boundary_event` captures administrative events (`event_id`, `event_type`, `event_date_est`, `split_case`). Events link to lineages via `supporting_event_id` in `district_relationship`.
- **Snapshot**: A snapshot (`fact_district_snapshot`) captures a specific `canonical_key` at a specific `time_sk` (year). `snapshot_sk` is the primary key.
- **Year**: `dim_time` provides the `year_sk` which links to `time_sk` in snapshots.

## Area & Spatial Accounting
- **Geometry Area (km²)**: Calculated directly from PostGIS geography `ST_Area_Spheroid/1e6` and stored in `geometry_observation.area_sqkm`.
- **Spatial Overlap**: `geometric_crosswalk` computes actual geometric intersections (`intersection_sqkm`).
- **Statistical Crosswalk**: `statistical_crosswalk` provides the `statistical_weight`, `was_normalized` flag, and `coverage_score`.
- **Lineage without Geometry (UNMEASURED)**: Captured in `statistical_crosswalk` with `weighting_method = 'UNMEASURED'` and `geo_xwalk_id = NULL`.

## Necessary Aggregations
To prevent exporting fragmented polygon intersections, spatial overlaps (`geometric_crosswalk`) must be aggregated up to the `from_snapshot_id` and `to_snapshot_id` level, and subsequently mapped to the `canonical_key` level for district-to-district relationships, which form the basis for `event_area_accounting`.
