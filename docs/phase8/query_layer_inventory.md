# Phase 8 Query Layer Inventory

This document details the logical views comprising the Query / Access Layer on top of the Gold warehouse.

| View Name | Purpose | Grain | Primary Key | Important Fields | Scientific Meaning |
|---|---|---|---|---|---|
| `vw_district_identity` | Central Canonical Key registry exposing the lifespan and stability of districts. | One Canonical District Identity | `canonical_key` | `identity_status`, `first_observed_year`, `last_observed_year` | Defines the ontological identity of a district across time regardless of physical footprint changes. |
| `vw_district_snapshot` | Flattened spatio-temporal observations of districts. | One Snapshot | `snapshot_id` | `canonical_key`, `year`, `geometry`, `area` | Represents the best available physical footprint of an identity at a discrete time. |
| `vw_district_lineage` | Administrative parent-child relationships linking canonical identities across time. | One Administrative Relationship | `relationship_id` | `parent_ck`, `child_ck`, `relationship_type` | Encodes the legal/administrative continuity between districts (e.g., SPLIT, MERGED). Does NOT encode spatial measurement. |
| `vw_boundary_events` | Flattened view of administrative events driving the lineage changes. | One Administrative Event | `event_id` | `event_type`, `effective_year`, `confidence` | The historical cause for changes in identity/geometry. |
| `vw_statistical_crosswalk` | Master matrix mapping source snapshots to target footprints over time. | One Source-Target Allocation | `from_snapshot_id`, `to_snapshot_id` | `statistical_weight`, `overlap_excess`, `crosswalk_status` | The authoritative map for harmonizing data, encoding both explicit geometric overlaps and derived apportionment rules. |
| `vw_usable_crosswalk` | Scientifically safe subset of the crosswalk filtering out invalid/unmeasured pairs. | One Safely Measured Allocation | `from_snapshot_id`, `to_snapshot_id` | `crosswalk_status` | The recommended subset for econometric / demographic harmonization (Status = MEASURED or MEASURED_NORMALIZED). |
| `vw_crosswalk_quality` | Snapshot-level diagnostic rollups of crosswalk allocations. | One Source Snapshot | `source_snapshot_id` | `overlap_excess`, `scientific_risk` | Provides metadata on the amount of distortion/repair a snapshot required to achieve harmonization. |
| `vw_validation_summary` | Summary of all data quality/validation rule evaluations. | One Rule per Run | `validation_run_id`, `rule_id` | `passed`, `failed`, `warning_count` | System health and integrity check results. |
