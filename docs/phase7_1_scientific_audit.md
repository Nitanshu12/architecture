# Phase 7.1 — Scientific Audit Report

## Audit Scope
This scientific audit assessed the methodological validity of the Gold statistical crosswalks, focusing on the 2,338 snapshots where raw intersection sums exceeded the expected upper bound and were normalized.

## Summary Statistics
1. **Number of measured crosswalks**: 39,692
2. **Number of UNMEASURED crosswalks**: 6,696
3. **Number of normalized crosswalks**: 28,289 (across 2,338 snapshots)
4. **Number of raw overlap cases**: 2,338
5. **Number of low-coverage cases**: 334
6. **Median and percentile coverage**: Median = 1.6%, p25 = 0.3%, p75 = 42.5% (for the low coverage subset)
7. **Maximum raw weight sum**: 24.25
8. **Maximum overlap excess**: 23.25
9. **Cases where normalization materially changes weights**: All 2,338 normalized snapshots exhibit material changes to raw allocation fractions.
10. **Potentially scientifically invalid cases**: 334 low-coverage cases and high-excess overlap cases requiring strict methodological assumptions.

## Methodological Decision: Normalization

QUESTION: Is normalizing raw area weights to sum to 1.0 scientifically justified for the 2,338 affected cases?

**METHODOLOGICAL DECISION: NORMALIZATION CONDITIONALLY ACCEPTED**

Normalization is acceptable as a derived apportionment procedure ONLY under the explicit assumption that the observed overlap represents an allocation error/artifact rather than legitimate overlapping administrative jurisdictions, and that proportional redistribution according to intersection size is the intended allocation rule. Normalization does NOT repair the underlying geometry. 

The diagnostics establish that these cases exhibit **OVERLAPPING TARGET COVERAGE**, but this does not definitively prove they are topological artifacts. Causes such as source-boundary inconsistency, temporal mismatch, or legitimate overlapping units remain open unless independently proven.

## Crosswalk Status Classification
Crosswalk snapshots should be classified into the following statuses for downstream use:
- **MEASURED**: Spatial intersections safely approximate allocation.
- **MEASURED_NORMALIZED**: Conditionally accepted as a derived apportionment coefficient.
- **UNMEASURED**: Lineage exists but measurement is unavailable. Correctly kept as NULL.
- **LOW_COVERAGE**: Requires review for temporal or vintage mismatches.
- **QUESTIONABLE**: Extreme overlap excess or inconsistency.
- **REJECTED**: Scientifically invalid for allocation.

## Final Conclusion
Phase 7.1 establishes methodological conditions for the use of normalized crosswalks; it does not establish that every normalized crosswalk is historically or spatially correct.
