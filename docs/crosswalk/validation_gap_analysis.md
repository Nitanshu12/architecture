# Validation Gap Analysis

## Validation Distinctions
The current validation framework effectively evaluates constraints, but these distinct concepts must not be conflated:
- **TECHNICAL VALIDITY**: Schema enforcement, referential integrity, and data types.
- **MATHEMATICAL VALIDITY**: Ensuring bounds, such as `SUM(statistical_weight) <= 1.001`.
- **SPATIAL VALIDITY**: Topological correctness of footprints.
- **SCIENTIFIC VALIDITY**: Whether the apportionment coefficient accurately reflects historical reality.
- **ANALYTICAL SUITABILITY**: Whether the crosswalk meets the assumptions required for a specific downstream analysis.

## Identified Gaps
The most critical gap is that a rule such as:
`SUM(statistical_weight) <= 1.001`
is necessary for the current allocation model, but is **not sufficient to establish scientific correctness**. 

The validation engine guarantees mathematical validity, but does not evaluate whether a mathematically perfect sum hides major boundary inconsistencies or overlapping target coverage. The current validation framework should NOT be described as proving scientific correctness of every crosswalk.

## Recommended Schema Enhancements
To improve provenance and support scientific validity checks, we recommend enhancing the `statistical_crosswalk` schema to persist these machine-readable diagnostic fields:
- `was_normalized` (Currently present)
- `normalization_method`
- `raw_weight_sum`
- `normalized_weight_sum`
- `overlap_excess`
- `coverage_score` (Currently present)
- `normalization_assumption`

This would allow analytical queries to filter based on the magnitude of the mathematical transformations applied to the raw spatial evidence without silently losing the original state of the measurement.
