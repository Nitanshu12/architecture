# Phase 0 Findings

## 1. Discoveries
- **Spatial Data:** We have 8 Stanford GPKG files (1951-2021) and 1 SOI GPKG (2025). The SOI dataset is in LCC projection and needs CRS transformation to EPSG:4326. Stanford datasets are mostly EPSG:4326.
- **Events Data:** We have 1550 rows in `district_evolution_master.csv`. It contains `effective_year` but no exact dates.
- **Attributes:** The Stanford datasets have highly variable schemas per year (e.g., 9 attributes in 1951, 170 attributes in 2001).

## 2. Confirmed Items
- The source data is immutable and intact.
- The pipeline can successfully be built following the v0.3 architecture.

## 3. Ambiguities & Conflicts
- **Temporal Precision:** Architecture demands DATE types for all temporal fields. The events data only provides `effective_year`. This requires mapping to a January 1st date with a `YEAR` precision enum to satisfy the architecture.
- **Split Classification:** Architecture v0.3 introduced `CLEAN_SPLIT` vs `CARVE_OUT`. The event data may not explicitly state which case a split is. This must be inferred or reviewed.
- **OD-01:** The open decision regarding reclassification CK rules is acknowledged and isolated.

## 4. Proposed Implementation Plan
- **Phase 1:** Implement Bronze Layer (Data loading and immutable storage, schema normalization).
- **Phase 2:** Implement Silver Layer (Geometry validation, date parsing, standardized names).
- **Phase 3:** Implement Gold Identity & Core (CK generation, snapshots, temporal model).
- **Phase 4:** Implement Gold Events & Lineage.
- **Phase 5:** Implement Gold Spatial & Harmonization.

## 5. Blocking Issues
- OD-01 (Reclassification CK Rule) remains an open decision in the architecture, but it only blocks final canonicalization for specific edge cases. We can proceed with Phase 1 (Bronze).
