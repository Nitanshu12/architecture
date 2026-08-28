# Data Contracts

## 1. Stanford Historical District Boundaries (1951-2021)
- **Source:** Stanford GPKG datasets
- **Source PK Candidate:** Varies by year (`C_CODE51`, `C_CODE61`, `DIS01_ID`, `pc11_d_id`, `JID`)
- **Semantic Meaning:** Representing historical district boundaries for census years
- **Nullability:** Missing values exist across attributes; geometry must not be null
- **Temporal Meaning:** Represents boundaries valid around the census year (Observation Time)
- **Geometry Meaning:** Polygon/MultiPolygon of the district extent
- **Transformation Requirements:** 
  - Ensure all geometries are MultiPolygon (ST_Multi)
  - Ensure EPSG:4326
  - Generate source_pk from dataset + internal ID if no stable PK exists.

## 2. Survey of India (SOI) District Boundaries (2025)
- **Source:** SOI GPKG (`2025.gpkg`)
- **Source PK Candidate:** `DISTRICT_L` or combination of State + District name
- **Temporal Meaning:** Modern boundaries (2025)
- **Geometry Meaning:** High-precision boundaries in LCC projection
- **Transformation Requirements:** 
  - Reproject from LCC to EPSG:4326
  - Handle mixed Polygon/MultiPolygon (cast to MultiPolygon)

## 3. Administrative Events (CSV)
- **Source:** `district_evolution_master.csv`
- **Source PK Candidate:** `district_id` combined with `event_type` and `effective_year`
- **Temporal Meaning:** `effective_year` represents the year the event occurred. Must be parsed to DATE with `YEAR` precision.
- **Semantic Meaning:** Represents historical splits, merges, and administrative changes.
- **Transformation Requirements:**
  - Map `event_type` to Architecture Taxonomy (e.g., FORMATION, SPLIT, MERGE).
  - Map `effective_year` to `event_date_est` with `YEAR` precision.
