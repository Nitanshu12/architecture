# Crosswalk Weight Semantics

This document defines the strict semantics of `statistical_weight` within the District Evolution Intelligence System.

## Definition

In the current architecture, `statistical_weight` is explicitly defined as **A DERIVED APPORTIONMENT COEFFICIENT**. It is NOT an observed geometric property.

### Explicit Distinctions
- `intersection_area`: The absolute spatial area (e.g., in square meters) of the physical overlap between source and target footprints.
- `raw_area_weight`: The raw geometric proportion (`intersection_area` / `source_area`). This is an empirical spatial measurement bound by GIS dataset realities.
- `coverage_score`: The fraction of the source area covered by the geometric union of all target intersections. A measure of mapping completeness.
- `statistical_weight`: A derived apportionment coefficient designed to allocate extensive variables (like population) into target footprints.
- `distribution_assumption`: The declared epistemological assumption governing how the `statistical_weight` was derived from the raw spatial measurement.

## Scientific Interpretation
The current crosswalk employs a uniform distribution assumption. It must be explicitly stated that this is a **methodological baseline**, not an empirically observed distribution. 

When applied, the `statistical_weight` guarantees that harmonized aggregates will mathematically balance, but this does not inherently prove that historical populations lived precisely according to the derived apportionment.
