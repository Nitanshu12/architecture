# Area Transfer Matrix Definition (Revised)

## Overview
The Area Transfer Matrix is an analytical data product providing a cross-tabulated representation of spatial relationships between source district snapshots and target district snapshots. 

## Structure (Long-Form)
The Area Transfer Matrix in this system is strictly provided in **LONG-FORM**.
- **Rows**: One source district (`from_ck`) to target district (`to_ck`) transfer event.
- **Values**: `area_transferred_km2`, `area_received_km2`, `area_relinquished_km2`.

## Why Long-Form?
Generating a wide matrix of 4,000+ columns (one per snapshot) exposes computational intermediate geometries rather than meaningful district facts. The long-form matrix (`area_transfer_matrix.parquet`) allows robust filtering by `event_id` and cleanly preserves metadata like `crosswalk_status`, `coverage_score`, and `overlap_excess`.

## Usage
If a wide matrix format is necessary (e.g., for linear algebra or Excel heatmaps), the long-form matrix can be pivoted dynamically at query-time using the API (`get_area_transfer_matrix`) for a specific year transition. The system does not statically export national wide matrices spanning all historical snapshots.
