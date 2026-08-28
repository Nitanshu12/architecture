# Phase 9 Area Units Guide

## The Standard Analytical Unit
To prevent analytical errors, the authoritative standard unit for area in the District Evolution Intelligence System is **square kilometres (km²)**. 

No area field is exposed to researchers without an explicit unit suffix. Fields like `AREA` or `TOT_AREA` are intentionally omitted from Gold data products to prevent ambiguity.

## Exposed Fields
In the data products (`district_area_timeseries.parquet`, `area_reconciliation.csv`, etc.), you will find the following fields:

### 1. `area_km2` (or `geometry_area_km2`)
- **Origin**: Computed directly from the spatial polygon using `ST_Area_Spheroid` on the WGS 84 ellipsoid.
- **Meaning**: The actual, physical geographic extent of the digitized map boundary in square kilometres.
- **Usage**: Use this for spatial normalizations (e.g., calculating population density based on the mapped boundaries).

### 2. `source_area_value`
- **Origin**: Extracted verbatim from the tabular attributes of the historical source dataset (e.g., the `TOT_AREA` field in the 1991 Stanford shapefile).
- **Meaning**: The administrative area reported by historical officials (e.g., the Census of India).
- **Usage**: Use this when matching against historical statistical tables, or when studying administrative reporting errors.

### 3. `source_area_unit`
- **Meaning**: The unit associated with `source_area_value` (e.g., "km2"). 

## Magnitudes Explained
For context, here is how magnitudes of square kilometres map to real-world Indian districts:

- **1 km²**: Roughly the size of a very large neighbourhood or a very small urban ward. (No districts are this small).
- **10 km²**: The size of a very small urban administrative unit (e.g., New Delhi municipal council area is roughly 42 km²).
- **100 km²**: The size of a dense urban district (e.g., Chennai district is ~426 km²).
- **1,000 km²**: A small-to-average rural Indian district (e.g., Howrah is ~1,467 km²).
- **10,000 km²**: A very large district (e.g., Anantapur was ~19,130 km² before reorganization).
- **45,000 km²**: The largest districts in India (e.g., Kachchh in Gujarat is ~45,674 km²).
