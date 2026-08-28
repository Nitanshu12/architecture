# Sample Case Studies

This document provides deterministic audits of statistical crosswalks across different classifications.

## Case A: CLEAN
- **Source District**: IND-000156 (1991)
- **Raw Weights Sum**: 0.997
- **Normalized Weights Sum**: 0.997
- **Coverage**: 99.7%
- **Overlap Excess**: 0.00
- **Scientific Interpretation**: A stable district boundary where geometric intersections safely approximate 1-to-1 administrative identity without normalization.

## Case B: NORMALIZED
- **Source District**: IND-000692 
- **Raw Weights Sum**: 2.64
- **Normalized Weights Sum**: 1.00
- **Overlap Excess**: 1.64
- **Scientific Interpretation**: Raw area weights significantly exceeded 1.0, requiring mathematical normalization to function as a derived apportionment coefficient. 

## Case C: HIGH_OVERLAP
- **Source District**: IND-003907
- **Raw Weights Sum**: 5.32
- **Coverage**: 100%
- **Overlap Excess**: 4.32
- **Scientific Interpretation**: The targets exhibit severe OVERLAPPING TARGET COVERAGE. The cause could be digitization artifacts, source-boundary inconsistency, or duplicate geometries, requiring explicit distribution assumptions.

## Case D: LOW_COVERAGE
- **Source District**: IND-000239
- **Raw Weights Sum**: 0.002
- **Coverage**: 0.2%
- **Overlap Excess**: 0.00
- **Scientific Interpretation**: Minimal geometric intersection exists. Requires investigation into potential temporal mismatch or source boundary errors before analytical use.

## Case E: UNMEASURED
- **Source District**: IND-000004
- **Target District**: IND-003996
- **Normalized Weight**: NULL
- **Reason**: Lineage evidence exists, but spatial measurement is unavailable.
- **Scientific Interpretation**: Administrative relationship is documented, but without spatial measurement, it cannot be treated as a zero weight, full weight, or measured allocation. The statistical weight correctly remains NULL.

## Case F: GEOMETRY_REPAIRED
- **Source District**: IND-002243
- **Raw Weights Sum**: 4.01
- **Overlap Excess**: 3.01
- **Scientific Interpretation**: Target footprints may have been subjected to repair algorithms that inadvertently created overlaps. Normalization mathematically allocates this, but does not repair the underlying geometry.
