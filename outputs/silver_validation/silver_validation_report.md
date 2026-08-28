# Silver Layer Validation Report

**Run ID:** `1fe15af2-9925-4146-8cce-be7d4d0f8722`
**Pipeline Version:** 0.3.0
**Started:** 2026-08-18T20:06:15.500532+00:00
**Completed:** 2026-08-18T20:06:21.307031+00:00

---

## 1. Processing Summary

| Dataset | Year | Layer | Input | Output | Invalid Geom | Repaired | Names Changed | Status |
|---|---|---|---|---|---|---|---|---|
| stanford | 1951 | districts_1951 | 316 | 316 | 0 | 0 | 316 | SUCCESS |
| stanford | 1961 | districts_1961 | 341 | 341 | 0 | 0 | 341 | SUCCESS |
| stanford | 1971 | districts_1971 | 360 | 360 | 0 | 0 | 360 | SUCCESS |
| stanford | 1981 | districts_1981 | 426 | 426 | 0 | 0 | 426 | SUCCESS |
| stanford | 1991 | districts_1991 | 468 | 468 | 0 | 0 | 468 | SUCCESS |
| stanford | 2001 | districts_2001 | 595 | 595 | 0 | 0 | 7 | SUCCESS |
| stanford | 2011 | districts_2011 | 641 | 641 | 0 | 0 | 12 | SUCCESS |
| stanford | 2021 | districts_2021 | 735 | 735 | 0 | 0 | 6 | SUCCESS |
| soi | 2025 | India_District_Boundary | 742 | 742 | 8 | 8 | 742 | SUCCESS |
| events | master | CSV | 1550 | 1550 | N/A | N/A | 38 | SUCCESS |

## 2. Transformation Summary

**Total transformations logged:** 46

| Transformation Type | Count |
|---|---|
| AREA_COMPUTE | 9 |
| CAST_TO_MULTI | 1 |
| CRS_TRANSFORM | 1 |
| DATE_PARSE | 1 |
| GEOMETRY_REPAIR | 16 |
| GEOMETRY_VALIDATION | 8 |
| NAME_NORMALIZE | 10 |

## 3. Geometry Repair Detail

> Original geometries are PRESERVED. Repaired geometries are derived artifacts with full provenance. Repair metadata is stored in `_silver_was_repaired`, `_silver_repair_area_delta_pct`, and `_silver_repair_method` columns.

### soi/2025
- Invalid geometries: 8
- Successfully repaired: 8
- Exceeding area threshold (>0.1%): 0


## 4. CRS Standardization

| Dataset | Original CRS | Action |
|---|---|---|
| Stanford (all years) | EPSG:4326 | No reprojection needed |
| SOI 2025 | LCC_WGS84 | Reprojected to EPSG:4326; native metrics preserved |

## 5. Events Standardization Notes

- `effective_year` preserved as observed integer — NOT converted to DATE
- `_silver_date_est` is a representational DATE anchor (YYYY-01-01) explicitly marked as estimated
- Source event types (SPLIT, NEW_DISTRICT, RENAME) preserved as-is; mapping to Architecture §9 taxonomy is a Gold-layer operation
- Ambiguous split cases NOT classified — quarantine required at event layer

## 6. Architectural Compliance

| Requirement | Status |
|---|---|
| Original geometry preserved (not blindly repaired) | ✓ |
| Repair as derived provenance-tracked artifact | ✓ |
| All transformations logged before applied | ✓ |
| SOI reprojected, native metrics preserved | ✓ |
| effective_year preserved as integer | ✓ |
| Date anchors labelled as estimated | ✓ |
| Original names preserved alongside standardized | ✓ |
| No CKs generated | ✓ |
| No event type mapping performed | ✓ |
| No split case classification | ✓ |
| No lineage generated | ✓ |
| OD-01 remains quarantined | ✓ |

## 7. Phase 2 Completion Gate

- [x] All datasets processed without errors
- [x] Geometry validation recorded (not repaired blindly)
- [x] CRS standardized where needed
- [x] Names standardized with originals preserved
- [x] Temporal precision labels added
- [x] Transformation log persisted
- [x] Silver outputs persisted
- [x] No downstream artifacts generated prematurely

**PHASE 2 GATE: PASSED** — Silver layer ready for L3 (Canonical Identity).
