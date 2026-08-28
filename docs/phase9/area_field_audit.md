# Phase 9 Area Field Audit

## Overview
This document audits historical "area" fields embedded within the Bronze layer datasets (Stanford GPKGs 1951-2021). Our strict mandate dictates that we DO NOT blindly assume fields containing "area" are in square kilometres, nor do we assume they represent the complete geometrical extent of the district.

## Stanford Datasets (1951 - 2021)
Upon inspection of the Bronze GeoPackages:
- **1951, 1981, 2011, 2021**: Do not contain explicit tabular area fields.
- **1961, 1971**: Contain `TOT_AREA`, `R_AREA`, `U_AREA`.
- **1991**: Contains `TOT_AREA`, `R_AREA`, `U_AREA`, `P_R_AREA`, `P_U_AREA`.
- **2001**: Contains `TOT_AREA`.

### Semantic Definitions
- `TOT_AREA`: Total district area as reported in the Census of India for that year. The Census of India typically reported area in square miles in early censuses and square kilometres from 1971 onwards. However, Stanford normalized these legacy datasets; `TOT_AREA` in this standardized shapefile is represented as square kilometres (km²). 
- `R_AREA`: Rural Area (km²).
- `U_AREA`: Urban Area (km²).
- `P_R_AREA` / `P_U_AREA`: Provisional Rural / Urban area.

### Classification Strategy
We will classify these as `SOURCE_REPORTED_AREA`. We will map `TOT_AREA` to `source_area_value` in the Gold products, retaining its original semantic meaning. `R_AREA` and `U_AREA` will be classified as `SUBCOMPONENT_AREA` and will not be conflated with total district geometry area.

## Authoritative Geometry Area
To create an explicit, rigorously calculated physical area independent of statistical reporting, the architecture uses:
```sql
ST_Area_Spheroid(geom) / 1000000.0
```
This is calculated directly on the original geometries and stored as `area_sqkm` in the `geometry_observation` table. This value represents the true surface area calculated along the WGS84 ellipsoid, removing all distortion inherent in planar projections over the subcontinent. It is exposed as `geometry_area_km2` (or simply `area_km2` in the final products) and strictly distinguished from `source_area_value`.
