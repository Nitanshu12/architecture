# Phase 9.1 — Independent Audit of Decadal Area Accounting

## Scope

Read-only audit of revised products, Silver geometries, Gold CK/lineage/event tables, the legacy intersection archive, and previous Phase 9 reports. No pipeline or product-generating code was changed.

## Quantitative findings

- Long-form observations: 4,381; unique usable observations in the wide matrix: 4,166.
- CK/vintage ambiguity: 105 groups / 215 long-form rows. Source PK/year duplicates: 0. Null, zero, and negative areas: 0.
- CK registry coverage: 19 of 964 registered CKs have no matrix row. `district_area_change.csv` has the revised 4,194-row decadal schema, but its 3,278-row Parquet counterpart is stale and has a different schema; the product pair is invalid.
- Independent EPSG:6933 recalculation: 36 districts across 6 regions; 0 tolerance exceptions.
- Bronze and Silver inventories contain no null or empty geometries; 8 area observations carry an explicit Silver repair/derived flag.
- Census CSV values and NULL semantics exactly match unique long-form observations. Workbook sheets are ['District Area by Census', 'Metadata & Definitions']; the required `area_quality` sheet is missing and eight repaired observations are not marked there.
- Temporal alignment: {'MODERATE': 1265, 'GOOD': 234, 'EXCELLENT': 30, 'NO_PRE_OBSERVATION': 21}. Strict bracketing has 21 events without a pre-observation; events dated at observed vintages use a 20-year surrounding span.
- Clean split conservation: 50 calculable, 1 PASS and 49 FAIL.
- Carve-out conservation: 430 positive-loss comparisons, 118 PASS, 15 minor, 39 material, and 258 FAIL.
- Multi-target event/source groups: 0; MERGE events: 0. The multi-child, target-overlap, and merge tests are not testable—not PASS. Zero observed overlap is tautological for single-target groups.
- Multi-target overlap distribution: n=0; median, p90, p95, p99, and maximum are not applicable. The stored overlap diagnostic must not be interpreted as empirical validation.
- Nulls: source area 459; target area 692; parent loss 673; allocated transfer 1,076; conservation error 1,058. `null_reason_audit.csv` assigns each null an explicit reason without converting it to zero.
- Old vs new: 480 comparable positive-transfer groups. Absolute percent difference median 6.98%, mean 47.84%, p90 85.93%, p95 100.00%, p99 985.95%, max 1668.97%.
- Official 2025 same-source comparison: 648 matches, median absolute discrepancy 3.705%, maximum 4.010%. This compares geodesic and planar calculations of the same SOI geometry, not independent historical truth.
- Conservation accuracy among 480 measurable transfer comparisons: median 0.926, p90 1.000, minimum -0.862. Negative values mean the discrepancy exceeds the expected change.

## Taxonomy and reproducibility

Gold event taxonomy: NEW_DISTRICT=487, RENAME=134, SPLIT=929. Gold lineage taxonomy: FORMED_FROM=456, SPLIT_FROM=95. Events with multiple lineage relationships: 0.
The exported event-register and lineage Parquet products serialize event IDs as binary values while revised accounting uses UUID strings, so researcher-facing product joins are not reliable. Previous Phase 9 reports also conflict with the revised product's status counts and should not be treated as audit evidence.

## Top 10 scientifically important issues

1. External conservation failure in 307/480 measurable transfer groups, including 49/50 clean splits.
2. The district-area-change CSV and Parquet products disagree in both schema and row count, so a researcher cannot know which product is authoritative.
3. No multi-child lineage case exists, so the intended allocation method is untested.
4. No MERGE event exists, so merge behavior is untested.
5. 1,058/1,550 accounting rows are UNMEASURED; no numeric transfer should be inferred from them.
6. 105 CK/vintage assignments are ambiguous and 19 registered CKs have no area-matrix observation.
7. Workbook contract failure: `area_quality` is absent and repaired values are not marked.
8. Temporal mismatch: 21 events lack a pre-vintage, with 20-year spans around census-dated events.
9. The claimed zero-overlap result has no eligible multi-target test case; it does not validate overlap logic.
10. Binary versus UUID event identifiers and stale all-PASS Phase 9 reports break cross-product provenance and misstate audit evidence.

## Required conclusion

The decadal-area-change formulation is conceptually more interpretable than raw intersections: it makes the area constraint explicit, and the 6.98% median raw-versus-change divergence shows raw intersection is not a stable proxy. It is not yet scientifically validated as a territory-transfer estimator. External child-area conservation fails in most measurable cases, while multi-target, overlap, and merge behavior have no test data.

The next methodology should retain the decadal change as a bound, but require complete event-specific successor sets and an independent conservation check before publishing a transfer. Incomplete cases should remain bounded/unmeasured. Add verified multi-child and merge cases before making allocation or overlap claims.

## PHASE 9.1 AUDIT RESULT

| Dimension | Result |
|---|---|
| Architecture | FAIL |
| District Area Foundation | CONDITIONAL |
| Event Accounting | FAIL |
| Conservation | FAIL |
| Temporal Alignment | CONDITIONAL |
| Spatial Evidence | CONDITIONAL |
| Scientific Validity | FAIL |
