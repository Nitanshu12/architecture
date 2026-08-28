# Revised Phase 9 Area Validation

## Method status

This is the decadal-evidence Phase 9 product.  It treats census/source
vintages as observations surrounding an event, uses the observed parent area
change as the transfer constraint, and uses intersections solely as allocation
evidence.  It does not export intersection fragments and does not represent an
observed geometry as the exact event-date boundary.

## Input evidence

- Geometry observations: 4,381
- Vintages: 1951, 1961, 1971, 1981, 1991, 2001, 2011, 2021, 2025
- Events in summary: 1,550
- Accounting relationships/placeholders: 1,550
- Explicitly preserved legacy files: present in the legacy archive (or unavailable before this build)

Geometry status counts: `{"AMBIGUOUS_DUPLICATE_CK_VINTAGE": 215, "VALID_OBSERVED": 4158, "VALID_REPAIRED_DERIVED_ARTIFACT": 8}`.

## Tolerances

- Absolute tolerance: 5.0 km²
- Relative tolerance: 0.5% of the expected comparison area
- Overlap classes: no overlap < 0.5%; minor < 2.0%; material < 5.0%; severe otherwise.

## Results

Measurement status counts: `{"MEASURED": 479, "MEASURED_NO_SPATIAL_EVIDENCE": 6, "MEASURED_PARENT_GAIN": 7, "UNMEASURED": 1058}`.

Validation status counts: `{"CONSERVATION_FAILURE": 301, "CONSERVATION_PASS": 119, "MATERIAL_DISCREPANCY": 39, "MINOR_DISCREPANCY": 15, "NO_SPATIAL_ALLOCATION_EVIDENCE": 6, "PARENT_GAIN_NO_TRANSFER": 7, "RENAME_AREA_INCONSISTENCY": 3, "RENAME_AREA_STABLE": 2, "UNMEASURED": 1058}`.

Event/source groups with material or severe overlap: 0.

## 2025 Survey of India comparison

The comparison uses the locally available SOI benchmark where district and
state names match. It is a method comparison (geodesic area versus the
benchmark's source geometry calculation), not a historical benchmark.

- Matched 2025 observations: 648
- Mean absolute percentage difference: 3.2267%
- Median absolute percentage difference: 3.7047%
- Maximum percentage difference: 4.0098%

## Interpretation

`UNMEASURED` means the project does not possess the lineage or unique observed
geometry needed for the stated measurement. A blank matrix value means no
usable observation, never zero area. `MEASURED_NO_SPATIAL_EVIDENCE` means the
areas exist but no intersection supports an allocation; it is not a fabricated
transfer. `RENAME_AREA_INCONSISTENCY` is a diagnostic, not a reassignment of
territory.
