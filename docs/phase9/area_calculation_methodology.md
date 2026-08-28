# Phase 9 Area Calculation Methodology

## The Requirement for True Surface Area
Historical district geometries in this project span the entirety of the Indian subcontinent (latitudes 8°N to 37°N, longitudes 68°E to 97°E). Representing the precise territorial footprint of districts across this vast expanse poses significant cartographic challenges.

The architecture explicitly rejects the use of a single planar projection (such as EPSG:24378 - Kalianpur 1975 / India zone IIa) for calculating India-wide historical areas. While planar projections are necessary for localized rendering or specific regional studies, imposing a single planar projection over a 3-million square kilometre landmass introduces severe peripheral area distortions (e.g., inflating or deflating the area of Kashmir or Kerala depending on the standard parallels chosen).

## Chosen Methodology: Spheroidal Geodesic Area
Instead of using a projected coordinate system, we calculate area using geodesic geometry directly on the reference ellipsoid.

**Formula**: `ST_Area_Spheroid(geom)`
**CRS/Ellipsoid**: WGS 84 (EPSG:4326)
**Geometry Preprocessing**: Raw geometries are transformed to EPSG:4326 and repaired (`ST_MakeValid`) before calculations are made. MultiPolygons are preserved natively.
**Unit Conversion**: The result of `ST_Area_Spheroid` is in square meters (m²). This is divided by `1,000,000.0` to yield exact square kilometres (km²).
**Precision**: Calculated and stored as `NUMERIC(12,4)` (four decimal places of precision).
**Treatment of Invalid Geometry**: Invalid geometries are repaired in the Silver layer. If repair fails, area is not calculated (has_geometry = FALSE).
**Treatment of Repaired Geometry**: The area delta introduced by repairs is monitored. If the repair alters the area by > 0.1%, a `WARNING` is raised in the pipeline.

## Implementation Standard
This logic is implemented during the Bronze-to-Silver transformation and materialised into the Gold layer within the `silver.geometry_observation` table as the `area_sqkm` field. 

This field acts as the immutable physical truth for the snapshot, explicitly segregated from census-reported textual area fields.
