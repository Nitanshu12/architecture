# Phase 9 Validation and Reconciliation

## Validation Rules
The Area Accounting system enforces the following constraints:

1. **No Negative Area**: All area values (`area_before`, `area_after`, `area_transferred`) must be $\ge 0$.
2. **Standard Units**: All fields strictly end in `_km2` representing square kilometers.
3. **Unique Grain**: The `event_area_accounting` dataset contains exactly one row per source-target district pair per event.
4. **No Polygon Fragments**: Computational intermediates (e.g. geometric overlap fragments) are never exposed directly; they are always aggregated to the district/event level.
5. **Reconciliation**:
   - `total_area_transferred_out` matches `sum(area_relinquished)`.
   - `area_before = area_retained + sum(area_transferred) + unaccounted_area`.

## Troubleshooting Discrepancies
If the conservation error is high:
- **Shared Overlap**: Target boundaries may overlap. This is identified by `overlap_area_km2 > 0`.
- **Unaccounted Area**: If source territory disappears, it will be captured in `unaccounted_area_km2`.
- **Measurement Status**: Check if the relationship was `UNMEASURED` (lineage without geometry) or `MEASURED`.

## Reproducibility
Phase 9 execution processes dual-pass generation. Cryptographic checksums (MD5) are generated for the data products on each run to verify absolute determinism in spatial extraction and aggregation. The results are logged in `outputs/phase9/reproducibility_report.md`.
