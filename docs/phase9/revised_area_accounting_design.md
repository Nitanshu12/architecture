# Revised Area Accounting Design

## Decision

Phase 9 is rebuilt as an **observed-vintage area accounting** layer.  The unit of
evidence is a district geometry at a known source vintage, not an inferred
event-date boundary and not an intersection fragment.  The accounting order is:

`observed source-area change → territory available for allocation → spatial allocation evidence → conservation diagnostic`

This design supersedes the intersection-led Phase 9 transfer outputs.  Those
outputs are retained unchanged in `data/products/legacy_phase9_intersection_method_20260820/` when the revised pipeline first runs.

## Existing architecture and findings

The Gold database has 4,381 `geometry_observation` records, 4,351 district
snapshots, 1,550 `boundary_event` records, and 551 `district_relationship`
records.  The observed geometry vintages are 1951, 1961, 1971, 1981, 1991,
2001, 2011, 2021, and 2025.  Stanford supplies the first eight vintages and
Survey of India supplies 2025.

The data audit undertaken for this design found:

- all retained Gold geometries are non-empty, valid and positive area at audit
  time;
- no duplicate exact WKB geometries occur within a source vintage;
- duplicate *canonical-key/vintage* assignments do occur, and are recorded as
  ambiguous rather than chosen arbitrarily for researcher-facing analysis;
- multipart geometries are common and are retained as MultiPolygons; and
- eight 2025 source records have a documented, derived Silver repair history.

`district_relationship` currently contains `FORMED_FROM` and `SPLIT_FROM`
relationships only.  The event register contains `NEW_DISTRICT`, `SPLIT`, and
`RENAME`; no merge event has current evidence.  The implementation nevertheless
contains explicit MERGE semantics, so a future event type can be measured
without changing the method.  Event participants give a source/target role pair
for 871 events.  Where an explicit lineage relationship is absent, a
one-predecessor/one-successor participant pair is used only as a clearly marked
`EVENT_PARTICIPANT_PAIR` relationship; it never creates area values.

## Why the former method is unsuitable

The prior Phase 9 code used `intersection_area_km2` and precomputed crosswalk
weights as `area_transferred_km2`, and subsequently normalized overlap-heavy
crosswalks.  That confuses a comparison between two observed vintages with an
event-date measurement.  It can also make the sum of intersections exceed the
area available from the parent.  It is therefore retained only as a historical
audit artifact, never as the new transfer estimate.

## Foundational district-area observations

`district_area_timeseries` is a long-form table with one row per Gold geometry
observation.  It reports `area_km2` calculated deterministically from the
stored WGS 84 geometry using `pyproj.Geod(ellps="WGS84")` and
`geometry_area_perimeter`, divided by 1,000,000.  It retains the method
(`PYPROJ_GEOD_WGS84_GEODESIC`) and CRS (`EPSG:4326 / WGS84 ellipsoid`);
degree-squared areas are not exported.  This deliberately handles valid
multipart geometries for which DuckDB's spheroidal helper returns a null area.

Source names, standardised names and state are read from the corresponding
Silver derived geometry artifact.  Bronze remains untouched.  `is_observed` is
true for all rows, while `is_derived` means that Silver recorded a geometry
repair.  A repair is never relabelled as an original Bronze measurement.

`geometry_status` is one of:

- `VALID_OBSERVED`;
- `VALID_REPAIRED_DERIVED_ARTIFACT`;
- `AMBIGUOUS_DUPLICATE_CK_VINTAGE`; or
- `AMBIGUOUS_DUPLICATE_CK_VINTAGE_REPAIRED_DERIVED_ARTIFACT`.

The human-readable matrix accepts only a unique valid observation for a
canonical district/vintage.  Ambiguous or absent observations are blank
(`NULL`), not zero.  The long table preserves the observations and the reason
they were excluded from the matrix.

## Temporal alignment

For every event year `y`, the selected source geometry vintages are global
observations, not dates that are imputed to the event:

- `pre_vintage = max(vintage < y)`;
- `post_vintage = min(vintage > y)`.

The output stores `years_before_event = y - pre_vintage`,
`years_after_event = post_vintage - y`, and
`temporal_gap_years = post_vintage - pre_vintage`.  If one side is unavailable
(for example an event at the observed boundary), the event is `UNMEASURED` for
the relevant area calculation.  A 2025 geometry is never backfilled into a
historical event.

## Event semantics and equations

Let `P0` be a source district area at the pre-vintage, `P1` its area at the
post-vintage when the source persists, and `Tj` the post-vintage area of target
`j`.  Values are in km².

### CLEAN_SPLIT

The parent is expected not to persist.  The territory available for allocation
is `P0`; `parent_area_loss_km2 = P0`, `parent_area_retained_km2 = 0`, and the
external conservation comparison is:

`E = P0 - Σ Tj`.

### CARVE_OUT and MULTI_CARVE_OUT

The parent persists.  `L = P0 - P1` is the observed parent area change and is
the primary allocation constraint.  Positive loss `max(L, 0)` is territory
available for transfer.  `P1` is reported as retained area.  The external
comparison is `E = L - Σ Tj` for the event/source group.  A negative parent
change is preserved as evidence, reported as `PARENT_GAIN_NO_TRANSFER`, and is
not silently converted into a negative territorial transfer.

### MERGE

For `n` predecessors and one successor, the external comparison is
`E = Σ Pi0 - T1`.  The target area is allocated across source relationships in
proportion to valid source-target spatial evidence, falling back to source
pre-vintage area shares only when no intersections exist.  The fallback is
explicitly marked as derived.  No current Gold event is classified as MERGE.

### RENAME

No transfer is generated.  The source pre-vintage and successor post-vintage
areas are compared.  A difference beyond tolerance is
`RENAME_AREA_INCONSISTENCY`, signaling either a non-nominal event or
temporally inconsistent observations.

## Spatial evidence and allocation

For each event × source × target relationship with usable source pre-vintage and
target post-vintage geometries, the pipeline calculates geodesic intersection
area.  It is stored only as `raw_intersection_area_km2`.

Within an event/source group,

`w_j = I_j / Σ I_j`

when `Σ I_j > 0`, and `allocated_transfer_j = available_area × w_j`.  Thus the
allocated transfer sums to the observed available area, not to the raw
intersection sum.  When `Σ I_j = 0`, allocation is null and the relationship is
`UNMEASURED`; the system does not make an automatic non-spatial distribution.

The union of target geometries is also intersected with the source.  The
diagnostic is:

`overlap_excess_km2 = Σ I_j - area(source ∩ union(target_j))`.

It is classified using configurable thresholds: `NO_OVERLAP` (<0.5% of raw
intersection sum), `MINOR_OVERLAP` (<2%), `MATERIAL_OVERLAP` (<5%), and
`SEVERE_OVERLAP` (≥5%).  Overlap never increases territory available for
transfer.

## Tolerances and status

The default scientific tolerances are configurable in the pipeline:

- absolute: 5.0 km²;
- relative: 0.5% of the expected comparison area.

`abs(error) <= max(absolute_tolerance, relative_tolerance × |expected|)` is a
`CONSERVATION_PASS`.  Up to two times that tolerance is
`MINOR_DISCREPANCY`; up to five times is `MATERIAL_DISCREPANCY`; larger
differences are `CONSERVATION_FAILURE`.  This tolerates measurement and
vintage uncertainty without hiding it through rounding.

`measurement_status` is `MEASURED` only when the required unique, valid
observations exist.  It is `UNMEASURED` for missing, ambiguous or unusable
geometry/lineage.  `RECONSTRUCTED` is reserved for an implemented, defensible
reconstruction; this first revision does not create reconstructed areas, so
`is_derived` and `derivation_method` remain null for event allocations.

## Products and data grain

The core event table has one row per event × source × target relationship.  A
placeholder row with no fabricated target is retained only for events lacking a
resolvable relationship, so all 1,550 events remain visible in event-level
diagnostics.  It has the concise researcher-facing schema requested in the
method specification.  No fragment geometry is exported.

The summary has one row per event and contains event/source/target counts,
temporal alignment, observed area quantities, allocated transfer totals,
unaccounted comparison error, overlap diagnostic, status, method and
confidence.  The matrix is the sole wide product and has the nine known
vintages as columns.

## Validation strategy

The pipeline writes conservation, temporal-alignment, overlap, and
district-change diagnostics.  It additionally compares 2025 recomputed areas
to the stored SOI geometries where both have the same canonical observation;
this is a reproducibility comparison, not an independent historical benchmark.

Random validation is deterministic (`seed=20260820`) and stratifies on each
available event semantic plus high-overlap, low-coverage, unmeasured and
reconstructed categories.  Each selected row carries its source/target
identities, temporal evidence, observed areas, allocation, conservation error,
overlap condition and final validation status for review.
