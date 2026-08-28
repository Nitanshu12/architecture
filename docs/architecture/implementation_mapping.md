# Implementation Mapping

| ARCHITECTURE COMPONENT | CODE MODULE | DATABASE OBJECT | TEST SUITE | OUTPUT DATASET |
|---|---|---|---|---|
| Bronze Ingestion | `src/bronze/loader.py` | `bronze.*` | `tests/integration/test_bronze.py` | GPKG/CSV extracts |
| Geometry Validation | `src/silver/geometry/validation.py` | `silver.geometry_observation` | `tests/spatial/` | Validated Geometries |
| CK Allocation | `src/identity/ck_registry.py` | `gold_core.canonical_key_registry` | `tests/unit/test_identity.py` | CK Registry |
| Snapshot Generation | `src/core/snapshots.py` | `gold_core.fact_district_snapshot` | `tests/temporal/` | Snapshots |
| Lineage DAG | `src/lineage/graph.py` | `gold_events.district_relationship` | `tests/lineage/` | Lineage Edges |
