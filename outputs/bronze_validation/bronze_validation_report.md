# Bronze Layer Validation Report

**Run ID:** `bb63b44d-56fe-4e01-bb9c-788af9f94da1`
**Pipeline Version:** 0.3.0
**Started:** 2026-08-18T04:43:51.602146+00:00
**Completed:** 2026-08-18T04:43:54.444060+00:00
**Stage:** bronze

---

## 1. Ingestion Summary

| Metric | Value |
|---|---|
| Total records ingested | 11 |
| Successful | 10 |
| Quarantined | 1 |
| Errors | 0 |
| Total rows ingested | 6174 |

## 2. Dataset Detail

| Dataset | Layer | Records | Geometries | Status | Output |
|---|---|---|---|---|---|
| stanford | districts_1951 | 316 | 316 | SUCCESS | stanford_districts_1951_1951.geoparquet |
| stanford | 1951 | 2 | 2 | QUARANTINED | stanford_1951_1951.geoparquet |
| stanford | districts_1961 | 341 | 341 | SUCCESS | stanford_districts_1961_1961.geoparquet |
| stanford | districts_1971 | 360 | 360 | SUCCESS | stanford_districts_1971_1971.geoparquet |
| stanford | districts_1981 | 426 | 426 | SUCCESS | stanford_districts_1981_1981.geoparquet |
| stanford | districts_1991 | 468 | 468 | SUCCESS | stanford_districts_1991_1991.geoparquet |
| stanford | districts_2001 | 595 | 595 | SUCCESS | stanford_districts_2001_2001.geoparquet |
| stanford | districts_2011 | 641 | 641 | SUCCESS | stanford_districts_2011_2011.geoparquet |
| stanford | districts_2021 | 735 | 735 | SUCCESS | stanford_districts_2021_2021.geoparquet |
| soi | India_District_Boundary | 742 | 742 | SUCCESS | soi_India_District_Boundary_2025.geoparquet |
| events | CSV | 1550 | N/A | SUCCESS | master.parquet |

## 3. Source PK Validation

| Dataset | Year | PK Column | Rows | Unique | Nulls | Valid | Issues |
|---|---|---|---|---|---|---|---|
| stanford | 1951 | C_CODE51 | 316 | 316 | 0 | ✓ | None |
| stanford | 1961 | C_CODE61 | 341 | 341 | 0 | ✓ | None |
| stanford | 1971 | C_CODE71 | 360 | 360 | 0 | ✓ | None |
| stanford | 1981 | C_CODE81 | 426 | 426 | 0 | ✓ | None |
| stanford | 1991 | DISTRICT_9 | 468 | 468 | 0 | ✓ | None |
| stanford | 2001 | DIS01_ID | 595 | 595 | 0 | ✓ | None |
| stanford | 2011 | pc11_d_id | 641 | 641 | 0 | ✓ | None |
| stanford | 2021 | JID | 735 | 735 | 0 | ✓ | None |
| soi | 2025 | DISTRICT_L | 742 | 742 | 0 | ✓ | None |

## 4. Geometry Validity Status

> **NOTE:** Geometry validity is RECORDED only. No geometry was modified or repaired. Original source geometry is preserved exactly. Repair is a Silver-layer operation producing a derived artifact with full provenance tracking.

| Dataset | Layer | Total | Valid | Invalid | Empty | Multi | CRS | Types |
|---|---|---|---|---|---|---|---|---|
| stanford | districts_1951 | 316 | 316 | 0 | 0 | 316 | EPSG:4326 | MultiPolygon |
| stanford | 1951 | 2 | 1 | 1 | 0 | 0 | EPSG:4326 | Polygon |
| stanford | districts_1961 | 341 | 341 | 0 | 0 | 341 | EPSG:4326 | MultiPolygon |
| stanford | districts_1971 | 360 | 360 | 0 | 0 | 360 | EPSG:4326 | MultiPolygon |
| stanford | districts_1981 | 426 | 426 | 0 | 0 | 426 | EPSG:4326 | MultiPolygon |
| stanford | districts_1991 | 468 | 468 | 0 | 0 | 468 | EPSG:4326 | MultiPolygon |
| stanford | districts_2001 | 595 | 595 | 0 | 0 | 595 | EPSG:4326 | MultiPolygon |
| stanford | districts_2011 | 641 | 641 | 0 | 0 | 641 | EPSG:4326 | MultiPolygon |
| stanford | districts_2021 | 735 | 735 | 0 | 0 | 735 | EPSG:4326 | MultiPolygon |
| soi | India_District_Boundary | 742 | 734 | 8 | 0 | 111 | PROJCS["LCC_WGS84",GEOGCS["WGS | MultiPolygon, Polygon |

## 5. Quarantined Layers

### stanford / 1951
- **Records:** 2
- **Reason:** Unknown 2-feature layer with synthetic geometries (bbox 0,0 to 10,10). Only 2 rows. Semantics unestablished. Do NOT merge with primary layer or discard.

- **Output:** `stanford_1951_1951.geoparquet`
- **Action:** Do NOT merge with primary layer or discard. Semantics must be established before inclusion.


## 6. Source Immutability Verification

All source file checksums recorded in the ingest manifest.
Source files were NOT modified during ingestion.

| Source File | SHA-256 (first 32 chars) |
|---|---|
| 1951.gpkg | `e3c292fe9f1a7e6acfb7897b825082b4...` |
| 1961.gpkg | `e1408f7bd2440b2f0943b405ebebbb7b...` |
| 1971.gpkg | `a16520de40f9970f33acf0c86ac2e511...` |
| 1981.gpkg | `56037a4b0740771667d90d693b10831f...` |
| 1991.gpkg | `825542f7e797058c6dd8d847d038d208...` |
| 2001.gpkg | `7b7b8c36f8ca6c76ec16bb06cf780204...` |
| 2011.gpkg | `5975a3616e9f04e6a7167f098375711c...` |
| 2021.gpkg | `e75e4d80900206e1757da69b1e702736...` |
| 2025.gpkg | `9a106a7c8c343b1644f0715adf8322ac...` |
| district_evolution_master.csv | `0056e375b576c643c717ace9c41b02bb...` |

## 7. Events Data Notes

- `effective_year` is preserved as an **observed integer year**. It has NOT been converted to a DATE.
- Any downstream DATE representation must be explicitly labelled as estimated/representational and must NEVER be interpreted as an exact event date.
- Source event types (SPLIT, NEW_DISTRICT, RENAME) are preserved as-is. Mapping to Architecture §9 taxonomy is a downstream (Silver/Gold) operation.
- Ambiguous split cases (CLEAN_SPLIT vs CARVE_OUT) are NOT inferred in Bronze. They must be quarantined for review in the event classification stage.
- `district_id` is NOT a unique event PK — it identifies a district. A synthetic `_bronze_row_id` has been assigned for row-level tracking.

## 8. Architectural Compliance Checklist

| Requirement | Status |
|---|---|
| Source files unmodified (immutable) | ✓ |
| All original source fields preserved | ✓ |
| SHA-256 checksums computed and recorded | ✓ |
| Source PKs validated for uniqueness/nullability | ✓ |
| Geometry validity recorded (NOT repaired) | ✓ |
| Bronze metadata columns added | ✓ |
| Outputs persisted as GeoParquet/Parquet | ✓ |
| Ingest manifest generated | ✓ |
| Quarantine layer isolated | ✓ |
| effective_year preserved as integer | ✓ |
| SOI CRS preserved (NOT reprojected) | ✓ |
| No CKs generated | ✓ |
| No lineage generated | ✓ |
| No snapshots generated | ✓ |
| No harmonization performed | ✓ |
| OD-01 remains quarantined | ✓ |

## 9. Phase 1 Completion Gate

- [x] All datasets ingested without errors
- [x] All source PKs validated
- [x] Manifests and checksums generated
- [x] Bronze outputs persisted
- [x] Quarantine layers isolated
- [x] Validation report produced
- [x] No source data modified
- [x] No CKs, lineage, or harmonization generated

**PHASE 1 GATE: PASSED** — Bronze layer ready for Silver.
