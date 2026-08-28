# Area Conceptual Model

## Architecture
The system follows a strict computational pipeline:

1. **Historical Geometries**: Raw spatial polygons (Bronze layer).
2. **Internal Spatial Intersection**: DuckDB / PostGIS `ST_Intersection` computes pairwise geometric overlaps. This yields computational intermediates (polygon fragments).
3. **Event/District Aggregation**: Fragments are grouped by `event_id`, `from_ck`, and `to_ck` to determine true district territorial shifts.
4. **District Area Accounting**: The `event_area_accounting` and `district_area_ledger` map these changes logically (e.g., Area Retained, Area Relinquished, Net Change).
5. **Research Data Products**: Parquet and CSV files at the district/event grain ready for econometric, historical, or demographic analysis.

## Worked Example: A Carve-Out

### Setup
- **Parent**: District A (Before = 1,000 km²)
- **Children**: District B (300 km²) & District C (250 km²)
- **Measured Spatial Intersection**:
  - A $\rightarrow$ B = 280 km²
  - A $\rightarrow$ C = 230 km²
  - A (After) = 490 km² (Area Retained)

### Accounting Output
- **A retained**: 490 km²
- **A relinquished**: 510 km² (280 + 230)
- **B received**: 280 km²
- **C received**: 230 km²
- **Conservation error / Unaccounted**: 
  - $1,000 - (490 + 280 + 230) = 0$
  - The system is perfectly conserved.

If B and C were drawn such that their combined geometries overlapped by 20 km² over A's original territory, `overlap_area_km2` would flag the 20 km² discrepancy instead of silently inflating transferred area.
