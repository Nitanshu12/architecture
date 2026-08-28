# Phase 8 Data Product Catalog

This catalog details the physical analytical datasets produced in `data/products/` for downstream use.

## Core Products

### 1. district_master (.parquet/.csv)
- **Grain**: One Canonical District Identity
- **Description**: Master dimension defining the ontological lifespan of districts.
- **Scientific Meaning**: Represents continuous administrative entities regardless of minor footprint adjustments.

### 2. district_snapshot (.parquet/.csv)
- **Grain**: One Geometry Observation
- **Description**: Explicit spatio-temporal footprints containing `WKB`/`WKT` geometry objects.
- **Scientific Meaning**: Represents the physical boundary asserted by a historical dataset.

### 3. district_lineage (.parquet/.csv)
- **Grain**: One Relationship
- **Description**: Relational mapping of how districts split, formed, or merged over time.
- **Scientific Meaning**: Encodes legal continuity, NOT spatial allocation.

### 4. boundary_events (.parquet/.csv)
- **Grain**: One Event
- **Description**: Administrative orders dictating boundary reorganizations.
- **Scientific Meaning**: The underlying temporal causality of lineage changes.

### 5. statistical_crosswalk (.parquet/.csv)
- **Grain**: One Source-to-Target Allocation
- **Description**: Authoritative harmonic mapping bridging historical boundaries to target boundaries. Contains all raw variables, coverages, and weights.
- **Scientific Meaning**: Defines apportionment assumptions, explicitly differentiating raw spatial evidence from derived weights. UNMEASURED relationships are preserved as `NULL` weights.
- **Unsafe Use Cases**: Aggregating variables using UNMEASURED or LOW_COVERAGE crosswalks without contextual handling.

### 6. usable_crosswalk (.parquet/.csv)
- **Grain**: One Scientifically Safely Measured Allocation
- **Description**: A derived subset of `statistical_crosswalk` filtering out unmeasured or invalid geometries based on a deterministic policy (`MEASURED`, `MEASURED_NORMALIZED`).
- **Recommended Use**: Directly joining with tabular indicator data for harmonization tasks.

### 7. crosswalk_quality (.parquet/.csv)
- **Grain**: One Source Snapshot
- **Description**: Provides snapshot-level diagnostic roll-ups such as aggregate overlap excess and scientific risk.
- **Scientific Meaning**: Measures the level of distortion or topological error in the underlying GIS data.

### 8. validation_summary (.parquet/.csv)
- **Grain**: One Validation Rule Result
- **Description**: Evaluation outputs of the data quality pipeline.
