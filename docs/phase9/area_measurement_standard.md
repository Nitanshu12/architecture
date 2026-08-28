# Area Measurement Standard

All researcher-facing area products explicitly utilize `km²`.

### Specification
- **Unit of Measurement**: Square Kilometers (`km²`)
- **Methodology**: `ST_Area_Spheroid` (Calculated using equal-area geodesic math over the WGS84 spheroid directly within PostGIS/DuckDB Spatial).
- **Prohibited Data**: Raw CRS-dependent planar units (e.g., degree²) are fundamentally excluded from the `event_area_summary` and `event_area_accounting` API surfaces.
- **Tolerance**: 0.001 km²
- **NULL Representation**: Missing spatial overlap or geometric observations are recorded as `NULL`. Zero `0.0` is strictly reserved for empirically verified zero-area intersections.
