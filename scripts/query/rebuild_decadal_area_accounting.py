"""Rebuild Phase 9 around decadal district-area evidence.

This script reads Gold and Silver artifacts only.  It does not modify Bronze,
Silver, Gold tables, or the historical intersection-based Phase 9 products.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import geopandas as gpd
import numpy as np
import pandas as pd
from pyproj import Geod
from shapely import wkb


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "data" / "gold" / "district_evolution.duckdb"
PRODUCT_DIR = PROJECT_ROOT / "data" / "products"
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase9"
SILVER_GEOMETRY_DIR = PROJECT_ROOT / "data" / "silver" / "geometry"
LEGACY_DIR = PRODUCT_DIR / "legacy_phase9_intersection_method_20260820"

AREA_METHOD = "PYPROJ_GEOD_WGS84_GEODESIC"
AREA_CRS = "EPSG:4326 (WGS 84 ellipsoid)"
ABSOLUTE_TOLERANCE_KM2 = 5.0
RELATIVE_TOLERANCE_PCT = 0.5
OVERLAP_THRESHOLDS_PCT = (0.5, 2.0, 5.0)
SAMPLE_SEED = "20260820"

TIME_SERIES_COLUMNS = [
    "canonical_key", "source_dataset", "source_year", "source_pk",
    "district_name_original", "district_name_standardized", "state_name",
    "geometry_id", "geometry_status", "area_km2", "area_method", "area_crs",
    "is_observed", "is_derived", "derivation_method", "geometry_confidence",
    "source_provenance",
]

ACCOUNTING_COLUMNS = [
    "event_id", "event_type", "event_year", "source_ck", "target_ck",
    "source_district", "target_district", "pre_vintage", "post_vintage",
    "source_area_before_km2", "source_area_after_km2", "target_area_after_km2",
    "parent_area_loss_km2", "parent_area_retained_km2",
    "raw_intersection_area_km2", "raw_spatial_weight",
    "allocated_transfer_area_km2", "conservation_error_km2",
    "conservation_error_pct", "overlap_excess_km2", "overlap_excess_pct",
    "area_method", "measurement_status", "temporal_gap_years",
    "geometry_confidence", "lineage_confidence", "overall_confidence",
    "is_derived", "derivation_method", "validation_status",
]

SUMMARY_COLUMNS = [
    "event_id", "event_type", "event_year", "source_count", "target_count",
    "pre_vintage", "post_vintage", "source_area_before_km2",
    "source_area_after_km2", "total_area_lost_km2",
    "total_area_transferred_km2", "total_area_retained_km2",
    "accounted_area_km2", "unaccounted_area_km2", "conservation_error_km2",
    "conservation_error_pct", "overlap_excess_km2", "measurement_status",
    "area_method", "confidence",
]


def clean_text(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None


def normalized_name(value: object) -> str:
    text = clean_text(value) or ""
    return "".join(ch for ch in text.casefold() if ch.isalnum())


def numeric(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


GEOD = Geod(ellps="WGS84")


def geodesic_area_wkb(value: object) -> float | None:
    """Return WGS84 ellipsoidal area in km², including valid multipart geometry."""
    if value is None or pd.isna(value):
        return None
    try:
        geometry = wkb.loads(bytes(value))
        area_m2, _ = GEOD.geometry_area_perimeter(geometry)
        return abs(float(area_m2)) / 1_000_000.0
    except Exception:
        return None


def unique_non_null(values: pd.Series) -> list[object]:
    return list(pd.unique(values.dropna()))


def combine_confidence(values: list[object]) -> str:
    ranks = {"HIGH": 3, "MEDIUM": 2, "LOW": 1, "UNKNOWN": 0}
    observed = [str(v).upper() for v in values if clean_text(v)]
    if not observed:
        return "UNKNOWN"
    return min(observed, key=lambda item: ranks.get(item, 0))


def overall_confidence(
    geometry_confidence: str,
    lineage_confidence: float | None,
    temporal_gap: int | None,
    overlap_pct: float | None,
    measurement_status: str,
) -> str:
    if measurement_status == "UNMEASURED":
        return "LOW"
    lineage = lineage_confidence or 0.0
    if (
        geometry_confidence == "HIGH"
        and lineage >= 0.9
        and (temporal_gap is not None and temporal_gap <= 10)
        and (overlap_pct is None or overlap_pct < OVERLAP_THRESHOLDS_PCT[0])
    ):
        return "HIGH"
    if geometry_confidence in {"HIGH", "MEDIUM"} and lineage >= 0.6:
        return "MEDIUM"
    return "LOW"


def tolerance_km2(expected: float | None) -> float | None:
    if expected is None or pd.isna(expected):
        return None
    return max(ABSOLUTE_TOLERANCE_KM2, abs(float(expected)) * RELATIVE_TOLERANCE_PCT / 100.0)


def conservation_status(error: float | None, expected: float | None) -> str:
    if error is None or expected is None or pd.isna(error) or pd.isna(expected):
        return "UNMEASURED"
    magnitude = abs(float(error))
    tol = tolerance_km2(float(expected))
    if magnitude <= tol:
        return "CONSERVATION_PASS"
    if magnitude <= 2 * tol:
        return "MINOR_DISCREPANCY"
    if magnitude <= 5 * tol:
        return "MATERIAL_DISCREPANCY"
    return "CONSERVATION_FAILURE"


def overlap_class(excess_pct: float | None) -> str:
    if excess_pct is None or pd.isna(excess_pct):
        return "UNKNOWN"
    if excess_pct < OVERLAP_THRESHOLDS_PCT[0]:
        return "NO_OVERLAP"
    if excess_pct < OVERLAP_THRESHOLDS_PCT[1]:
        return "MINOR_OVERLAP"
    if excess_pct < OVERLAP_THRESHOLDS_PCT[2]:
        return "MATERIAL_OVERLAP"
    return "SEVERE_OVERLAP"


def source_attributes() -> dict[tuple[str, int, int], dict[str, str | None]]:
    """Read Silver names/state metadata without modifying its geometry artifacts."""
    attributes: dict[tuple[str, int, int], dict[str, str | None]] = {}
    for path in sorted(SILVER_GEOMETRY_DIR.glob("*.geoparquet")):
        source_dataset = "soi" if path.name.startswith("soi_") else "stanford"
        source_year = int(path.stem.rsplit("_", 1)[-1])
        frame = gpd.read_parquet(path).reset_index(drop=True)
        for position, row in frame.iterrows():
            attributes[(source_dataset, source_year, int(position))] = {
                "district_name_original": clean_text(row.get("_silver_name_original")),
                "district_name_standardized": clean_text(row.get("_silver_name_std")),
                "state_name": clean_text(row.get("_silver_state_std")),
            }
    return attributes


def observation_row_index(source_observation_id: object) -> int | None:
    try:
        return int(str(source_observation_id).rsplit("_", 1)[-1])
    except (ValueError, AttributeError):
        return None


def archive_legacy_products() -> list[str]:
    """Preserve the exact pre-rebuild products before replacing their names."""
    LEGACY_DIR.mkdir(parents=True, exist_ok=True)
    names = [
        "event_area_accounting.csv", "event_area_accounting.parquet",
        "event_area_summary.csv", "event_area_summary.parquet",
        "district_area_change.csv",
    ]
    preserved = []
    for name in names:
        source = PRODUCT_DIR / name
        destination = LEGACY_DIR / name
        if source.exists() and not destination.exists():
            shutil.copy2(source, destination)
            preserved.append(name)
    manifest = LEGACY_DIR / "README.md"
    if not manifest.exists():
        manifest.write_text(
            "# Superseded Phase 9 intersection-led products\n\n"
            "These files were copied before the decadal evidence rebuild on "
            "2026-08-20. They are retained for audit/provenance only and must "
            "not be interpreted as the revised area-accounting outputs.\n",
            encoding="utf-8",
        )
    return preserved


def build_timeseries(con: duckdb.DuckDBPyConnection) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    registry = con.execute(
        """
        SELECT canonical_key, display_name, state_at_creation, closed_date
        FROM canonical_key_registry
        """
    ).fetchdf()
    observations = con.execute(
        """
        SELECT
            g.geom_obs_id::VARCHAR AS geometry_id,
            g.canonical_key,
            s.source_name AS source_dataset,
            EXTRACT(YEAR FROM g.observed_at)::INTEGER AS source_year,
            g.source_pk,
            g.source_observation_id,
            g.geometry_provenance,
            g.is_valid_geom,
            g.was_repaired,
            g.repair_area_delta_pct,
            g.spatial_confidence AS geometry_confidence,
            ST_AsWKB(g.geom) AS geometry_wkb
        FROM geometry_observation g
        JOIN dim_source s ON g.source_id = s.source_sk
        ORDER BY source_year, canonical_key, geometry_id
        """
    ).fetchdf()
    observations["area_km2"] = observations["geometry_wkb"].map(geodesic_area_wkb)
    if observations["area_km2"].isna().any():
        raise RuntimeError("A valid Gold geometry could not be measured geodesically.")
    attrs = source_attributes()
    observations = observations.merge(registry, on="canonical_key", how="left")
    names = []
    for row in observations.itertuples(index=False):
        position = observation_row_index(row.source_observation_id)
        attrs_row = attrs.get((row.source_dataset, int(row.source_year), position), {})
        names.append({
            "district_name_original": attrs_row.get("district_name_original") or row.display_name,
            "district_name_standardized": attrs_row.get("district_name_standardized") or row.display_name,
            "state_name": attrs_row.get("state_name") or row.state_at_creation,
        })
    observations = pd.concat([observations, pd.DataFrame(names)], axis=1)
    duplicate_counts = observations.groupby(["canonical_key", "source_year"])["geometry_id"].transform("size")
    observations["_duplicate_ck_vintage"] = duplicate_counts.gt(1)
    observations["geometry_status"] = np.where(
        observations["_duplicate_ck_vintage"] & observations["was_repaired"],
        "AMBIGUOUS_DUPLICATE_CK_VINTAGE_REPAIRED_DERIVED_ARTIFACT",
        np.where(
            observations["_duplicate_ck_vintage"],
            "AMBIGUOUS_DUPLICATE_CK_VINTAGE",
            np.where(observations["was_repaired"], "VALID_REPAIRED_DERIVED_ARTIFACT", "VALID_OBSERVED"),
        ),
    )
    observations["area_method"] = AREA_METHOD
    observations["area_crs"] = AREA_CRS
    observations["is_observed"] = True
    observations["is_derived"] = observations["was_repaired"].astype(bool)
    observations["derivation_method"] = np.where(
        observations["was_repaired"], "SILVER_GEOMETRY_REPAIR", None
    )
    observations["source_provenance"] = observations.apply(
        lambda row: (
            f"{row.source_dataset}; geometry_provenance={row.geometry_provenance}; "
            f"source_observation_id={row.source_observation_id}"
        ),
        axis=1,
    )
    timeseries = observations[TIME_SERIES_COLUMNS].copy()
    timeseries = timeseries.sort_values(["canonical_key", "source_year", "geometry_id"], kind="stable")
    usable = observations.loc[
        observations["geometry_status"].isin(["VALID_OBSERVED", "VALID_REPAIRED_DERIVED_ARTIFACT"])
    ].copy()
    # The status condition guarantees at most one row per CK/vintage.
    assert not usable.duplicated(["canonical_key", "source_year"]).any()
    return timeseries, usable, registry


def build_matrix(usable: pd.DataFrame, registry: pd.DataFrame) -> pd.DataFrame:
    vintages = sorted(usable["source_year"].unique().tolist())
    matrix = usable.pivot(index="canonical_key", columns="source_year", values="area_km2")
    matrix = matrix.reindex(columns=vintages)
    matrix.columns = [f"area_{year}_km2" for year in matrix.columns]
    matrix = matrix.reset_index().merge(
        registry[["canonical_key", "display_name", "state_at_creation"]], on="canonical_key", how="left"
    ).rename(columns={"display_name": "district_name", "state_at_creation": "state"})
    return matrix[["canonical_key", "district_name", "state", *[f"area_{year}_km2" for year in vintages]]].sort_values(
        ["state", "district_name", "canonical_key"], kind="stable"
    )


def build_relationships(con: duckdb.DuckDBPyConnection) -> tuple[pd.DataFrame, pd.DataFrame]:
    events = con.execute(
        """
        SELECT event_id::VARCHAR AS event_id, event_type, split_case,
               EXTRACT(YEAR FROM event_date_est)::INTEGER AS event_year,
               CAST(lineage_confidence AS DOUBLE) AS event_lineage_confidence
        FROM boundary_event
        ORDER BY event_id
        """
    ).fetchdf()
    explicit = con.execute(
        """
        SELECT supporting_event_id::VARCHAR AS event_id, from_ck AS source_ck,
               to_ck AS target_ck, relationship_type,
               CAST(lineage_confidence AS DOUBLE) AS lineage_confidence
        FROM district_relationship
        WHERE supporting_event_id IS NOT NULL
        """
    ).fetchdf()
    participants = con.execute(
        """
        SELECT event_id::VARCHAR AS event_id, role, canonical_key
        FROM event_participant
        ORDER BY event_id, role, canonical_key
        """
    ).fetchdf()
    participant_map: dict[str, dict[str, list[str]]] = defaultdict(lambda: {"PREDECESSOR": [], "SUCCESSOR": []})
    for row in participants.itertuples(index=False):
        participant_map[row.event_id][row.role].append(row.canonical_key)
    explicit_ids = set(explicit["event_id"])
    rows = []
    for row in explicit.itertuples(index=False):
        rows.append({
            "event_id": row.event_id,
            "source_ck": row.source_ck,
            "target_ck": row.target_ck,
            "relationship_type": row.relationship_type,
            "lineage_confidence": numeric(row.lineage_confidence),
            "relationship_basis": "EXPLICIT_LINEAGE",
        })
    for event in events.loc[~events["event_id"].isin(explicit_ids)].itertuples(index=False):
        roles = participant_map[event.event_id]
        predecessors = sorted(set(roles["PREDECESSOR"]))
        successors = sorted(set(roles["SUCCESSOR"]))
        pair_is_unique = len(predecessors) == 1 and len(successors) == 1
        rows.append({
            "event_id": event.event_id,
            "source_ck": predecessors[0] if len(predecessors) == 1 else None,
            "target_ck": successors[0] if len(successors) == 1 else None,
            "relationship_type": None,
            "lineage_confidence": numeric(event.event_lineage_confidence),
            "relationship_basis": "EVENT_PARTICIPANT_PAIR" if pair_is_unique else "NO_RESOLVABLE_RELATIONSHIP",
        })
    relationships = pd.DataFrame(rows).merge(events, on="event_id", how="left", validate="many_to_one")
    relationships["relationship_row_id"] = relationships.apply(
        lambda row: hashlib.sha256(
            f"{row.event_id}|{row.source_ck}|{row.target_ck}|{row.relationship_basis}".encode()
        ).hexdigest(),
        axis=1,
    )
    return relationships, events


def event_semantic(event_type: object, split_case: object, relationship_type: object, target_count: int) -> str:
    event_type = clean_text(event_type) or "UNKNOWN"
    split_case = clean_text(split_case)
    relationship_type = clean_text(relationship_type)
    if event_type == "RENAME":
        return "RENAME"
    if event_type == "MERGE" or relationship_type == "MERGED_INTO":
        return "MERGE"
    if event_type == "SPLIT" and split_case == "CLEAN_SPLIT":
        return "CLEAN_SPLIT"
    if event_type == "SPLIT" and split_case == "CARVE_OUT":
        return "MULTI_CARVE_OUT" if target_count > 1 else "CARVE_OUT"
    if event_type == "NEW_DISTRICT" or relationship_type == "FORMED_FROM":
        return "MULTI_CARVE_OUT" if target_count > 1 else "CARVE_OUT"
    return "UNKNOWN"


def add_temporal_and_geometry_inputs(relationships: pd.DataFrame, usable: pd.DataFrame) -> pd.DataFrame:
    vintages = sorted(int(year) for year in usable["source_year"].unique())
    source_target_counts = relationships.groupby(["event_id", "source_ck"], dropna=False)["target_ck"].transform(
        lambda values: len(set(value for value in values if clean_text(value)))
    )
    relationships["_target_count_for_source"] = source_target_counts.astype(int)
    relationships["event_semantic"] = relationships.apply(
        lambda row: event_semantic(
            row.event_type, row.split_case, row.relationship_type, int(row._target_count_for_source)
        ),
        axis=1,
    )
    relationships["pre_vintage"] = relationships["event_year"].map(
        lambda event_year: max((year for year in vintages if year < event_year), default=None)
    )
    relationships["post_vintage"] = relationships["event_year"].map(
        lambda event_year: min((year for year in vintages if year > event_year), default=None)
    )
    relationships["years_before_event"] = relationships.apply(
        lambda row: row.event_year - row.pre_vintage if pd.notna(row.pre_vintage) else None, axis=1
    )
    relationships["years_after_event"] = relationships.apply(
        lambda row: row.post_vintage - row.event_year if pd.notna(row.post_vintage) else None, axis=1
    )
    relationships["temporal_gap_years"] = relationships.apply(
        lambda row: row.post_vintage - row.pre_vintage
        if pd.notna(row.pre_vintage) and pd.notna(row.post_vintage)
        else None,
        axis=1,
    )
    lookup = usable.set_index(["canonical_key", "source_year"])

    def lookup_observation(canonical_key: object, source_year: object, prefix: str) -> dict[str, object]:
        fields = {
            f"{prefix}_area": None,
            f"{prefix}_geometry_id": None,
            f"{prefix}_confidence": None,
        }
        if not clean_text(canonical_key) or source_year is None or pd.isna(source_year):
            return fields
        key = (canonical_key, int(source_year))
        if key not in lookup.index:
            return fields
        item = lookup.loc[key]
        fields[f"{prefix}_area"] = numeric(item.area_km2)
        fields[f"{prefix}_geometry_id"] = item.geometry_id
        fields[f"{prefix}_confidence"] = item.geometry_confidence
        return fields

    inputs = []
    for row in relationships.itertuples(index=False):
        data = {}
        data.update(lookup_observation(row.source_ck, row.pre_vintage, "source_before"))
        data.update(lookup_observation(row.source_ck, row.post_vintage, "source_after"))
        data.update(lookup_observation(row.target_ck, row.post_vintage, "target_after"))
        inputs.append(data)
    return pd.concat([relationships.reset_index(drop=True), pd.DataFrame(inputs)], axis=1)


def add_intersection_metrics(con: duckdb.DuckDBPyConnection, relations: pd.DataFrame) -> pd.DataFrame:
    work = relations[[
        "relationship_row_id", "event_id", "source_ck", "target_ck", "event_semantic",
        "source_before_geometry_id", "target_after_geometry_id",
    ]].copy()
    work["allocation_group_id"] = np.where(
        work["event_semantic"].eq("MERGE"),
        work["event_id"] + "|TARGET|" + work["target_ck"].fillna("<NULL>"),
        work["event_id"] + "|SOURCE|" + work["source_ck"].fillna("<NULL>"),
    )
    work["group_mode"] = np.where(work["event_semantic"].eq("MERGE"), "MERGE", "SOURCE_TO_TARGETS")
    con.register("event_relation_inputs", work)
    metrics = con.execute(
        """
        WITH paired AS (
            SELECT
                r.relationship_row_id, r.allocation_group_id, r.group_mode,
                r.event_id, r.source_ck, r.target_ck,
                r.source_before_geometry_id, r.target_after_geometry_id,
                s.geom AS source_geom, t.geom AS target_geom,
                CASE
                    WHEN s.geom IS NOT NULL AND t.geom IS NOT NULL
                    THEN ST_AsWKB(ST_Intersection(s.geom, t.geom))
                END AS raw_intersection_wkb
            FROM event_relation_inputs r
            LEFT JOIN geometry_observation s
                ON r.source_before_geometry_id = s.geom_obs_id::VARCHAR
            LEFT JOIN geometry_observation t
                ON r.target_after_geometry_id = t.geom_obs_id::VARCHAR
        ),
        grouped AS (
            SELECT
                allocation_group_id,
                group_mode,
                ST_Union_Agg(
                    CASE WHEN group_mode = 'MERGE' THEN source_geom ELSE target_geom END
                ) AS union_geometry
            FROM paired
            WHERE source_geom IS NOT NULL AND target_geom IS NOT NULL
            GROUP BY allocation_group_id, group_mode
        ),
        coverage AS (
            SELECT
                g.allocation_group_id,
                CASE
                    WHEN g.group_mode = 'MERGE' THEN
                        ST_AsWKB(ST_Intersection(t.geom, g.union_geometry))
                    ELSE ST_AsWKB(ST_Intersection(s.geom, g.union_geometry))
                END AS union_intersection_wkb
            FROM grouped g
            JOIN paired p ON p.allocation_group_id = g.allocation_group_id
            LEFT JOIN geometry_observation s
                ON p.source_before_geometry_id = s.geom_obs_id::VARCHAR
            LEFT JOIN geometry_observation t
                ON p.target_after_geometry_id = t.geom_obs_id::VARCHAR
            QUALIFY ROW_NUMBER() OVER (PARTITION BY g.allocation_group_id ORDER BY p.relationship_row_id) = 1
        )
        SELECT
            p.relationship_row_id,
            p.allocation_group_id,
            p.raw_intersection_wkb,
            c.union_intersection_wkb
        FROM paired p
        LEFT JOIN coverage c USING (allocation_group_id)
        """
    ).fetchdf()
    con.unregister("event_relation_inputs")
    metrics["raw_intersection_area_km2"] = metrics["raw_intersection_wkb"].map(geodesic_area_wkb)
    metrics["union_intersection_area_km2"] = metrics["union_intersection_wkb"].map(geodesic_area_wkb)
    metrics["raw_intersection_sum_km2"] = metrics.groupby("allocation_group_id", dropna=False)[
        "raw_intersection_area_km2"
    ].transform("sum", min_count=1)
    metrics = metrics.drop(columns=["raw_intersection_wkb", "union_intersection_wkb"])
    return relations.merge(metrics, on="relationship_row_id", how="left", validate="one_to_one")


def group_values(frame: pd.DataFrame, field: str) -> list[float]:
    return [float(value) for value in unique_non_null(frame[field])]


def classify_relationships(relations: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    relations = relations.copy()
    relations["_group_expected"] = np.nan
    relations["_group_accounted"] = np.nan
    relations["_group_available"] = np.nan
    relations["parent_area_loss_km2"] = np.nan
    relations["parent_area_retained_km2"] = np.nan
    relations["raw_spatial_weight"] = np.nan
    relations["allocated_transfer_area_km2"] = np.nan
    relations["conservation_error_km2"] = np.nan
    relations["conservation_error_pct"] = np.nan
    relations["overlap_excess_km2"] = np.nan
    relations["overlap_excess_pct"] = np.nan
    relations["_overlap_class"] = "UNKNOWN"
    relations["measurement_status"] = "UNMEASURED"
    relations["validation_status"] = "UNMEASURED"
    group_rows = []

    for allocation_group_id, group in relations.groupby("allocation_group_id", dropna=False, sort=False):
        semantic = group["event_semantic"].iloc[0]
        raw_sum = numeric(group["raw_intersection_sum_km2"].iloc[0])
        union_area = numeric(group["union_intersection_area_km2"].iloc[0])
        overlap = raw_sum - union_area if raw_sum is not None and union_area is not None else None
        overlap_pct = (100.0 * overlap / raw_sum) if raw_sum and raw_sum > 0 and overlap is not None else None
        source_before_values = group_values(group, "source_before_area")
        source_after_values = group_values(group, "source_after_area")
        target_after_values = group_values(group, "target_after_area")
        source_before = source_before_values[0] if len(source_before_values) == 1 else None
        source_after = source_after_values[0] if len(source_after_values) == 1 else None
        target_total = sum(target_after_values) if target_after_values else None
        expected = accounted = available = parent_loss = retained = None

        if semantic == "CLEAN_SPLIT":
            expected = source_before
            accounted = target_total
            available = source_before
            parent_loss = source_before
            retained = 0.0 if source_before is not None else None
        elif semantic in {"CARVE_OUT", "MULTI_CARVE_OUT"}:
            if source_before is not None and source_after is not None:
                parent_loss = source_before - source_after
                expected = parent_loss
                available = max(parent_loss, 0.0)
            accounted = target_total
            retained = source_after
        elif semantic == "MERGE":
            expected = sum(source_before_values) if source_before_values else None
            accounted = target_total
            available = target_total
            retained = 0.0 if expected is not None else None
        elif semantic == "RENAME":
            expected = source_before
            accounted = target_total

        error = expected - accounted if expected is not None and accounted is not None else None
        error_pct = 100.0 * abs(error) / abs(expected) if expected not in (None, 0) and error is not None else None
        has_required_areas = expected is not None and accounted is not None
        transfer_semantic = semantic in {"CLEAN_SPLIT", "CARVE_OUT", "MULTI_CARVE_OUT", "MERGE"}
        parent_gain = semantic in {"CARVE_OUT", "MULTI_CARVE_OUT"} and parent_loss is not None and parent_loss < 0
        if not has_required_areas or semantic == "UNKNOWN":
            measurement_status = "UNMEASURED"
            validation = "UNMEASURED"
        elif semantic == "RENAME":
            measurement_status = "MEASURED"
            validation = "RENAME_AREA_STABLE" if conservation_status(error, expected) == "CONSERVATION_PASS" else "RENAME_AREA_INCONSISTENCY"
        elif parent_gain:
            measurement_status = "MEASURED_PARENT_GAIN"
            validation = "PARENT_GAIN_NO_TRANSFER"
        elif raw_sum is None or raw_sum <= 0:
            measurement_status = "MEASURED_NO_SPATIAL_EVIDENCE"
            validation = "NO_SPATIAL_ALLOCATION_EVIDENCE"
        else:
            measurement_status = "MEASURED"
            validation = conservation_status(error, expected)

        raw_weights = group["raw_intersection_area_km2"] / raw_sum if raw_sum and raw_sum > 0 else pd.Series(np.nan, index=group.index)
        allocation_allowed = (
            transfer_semantic
            and available is not None
            and available >= 0
            and raw_sum is not None
            and raw_sum > 0
            and measurement_status == "MEASURED"
        )
        allocated = raw_weights * available if allocation_allowed else pd.Series(np.nan, index=group.index)
        relations.loc[group.index, "_group_expected"] = expected
        relations.loc[group.index, "_group_accounted"] = accounted
        relations.loc[group.index, "_group_available"] = available
        relations.loc[group.index, "parent_area_loss_km2"] = parent_loss
        relations.loc[group.index, "parent_area_retained_km2"] = retained
        relations.loc[group.index, "raw_spatial_weight"] = raw_weights
        relations.loc[group.index, "allocated_transfer_area_km2"] = allocated
        relations.loc[group.index, "conservation_error_km2"] = error
        relations.loc[group.index, "conservation_error_pct"] = error_pct
        relations.loc[group.index, "overlap_excess_km2"] = overlap
        relations.loc[group.index, "overlap_excess_pct"] = overlap_pct
        relations.loc[group.index, "_overlap_class"] = overlap_class(overlap_pct)
        relations.loc[group.index, "measurement_status"] = measurement_status
        relations.loc[group.index, "validation_status"] = validation
        group_rows.append({
            "allocation_group_id": allocation_group_id,
            "event_id": group["event_id"].iloc[0],
            "source_ck": group["source_ck"].iloc[0],
            "event_semantic": semantic,
            "raw_intersection_sum_km2": raw_sum,
            "union_intersection_area_km2": union_area,
            "overlap_excess_km2": overlap,
            "overlap_excess_pct": overlap_pct,
            "overlap_class": overlap_class(overlap_pct),
            "expected_area_km2": expected,
            "accounted_area_km2": accounted,
            "available_area_km2": available,
            "conservation_error_km2": error,
            "conservation_error_pct": error_pct,
            "measurement_status": measurement_status,
            "validation_status": validation,
        })
    return relations, pd.DataFrame(group_rows)


def build_accounting(relations: pd.DataFrame, registry: pd.DataFrame) -> pd.DataFrame:
    names = registry.set_index("canonical_key")
    frame = relations.copy()
    frame["source_district"] = frame["source_ck"].map(names["display_name"])
    frame["target_district"] = frame["target_ck"].map(names["display_name"])
    frame["geometry_confidence"] = frame.apply(
        lambda row: combine_confidence([row.source_before_confidence, row.target_after_confidence]), axis=1
    )
    frame["overall_confidence"] = frame.apply(
        lambda row: overall_confidence(
            row.geometry_confidence,
            numeric(row.lineage_confidence),
            int(row.temporal_gap_years) if pd.notna(row.temporal_gap_years) else None,
            numeric(row.overlap_excess_pct),
            row.measurement_status,
        ),
        axis=1,
    )
    frame["is_derived"] = False
    frame["derivation_method"] = None
    frame["area_method"] = AREA_METHOD
    frame["event_type"] = frame["event_semantic"]
    frame = frame.rename(columns={
        "source_before_area": "source_area_before_km2",
        "source_after_area": "source_area_after_km2",
        "target_after_area": "target_area_after_km2",
    })
    # Source event type is represented by the explicit Phase 9 semantic.
    return frame[ACCOUNTING_COLUMNS].sort_values(["event_year", "event_id", "source_ck", "target_ck"], kind="stable")


def event_status(values: pd.Series) -> str:
    observed = set(values.dropna().astype(str))
    if observed == {"UNMEASURED"} or not observed:
        return "UNMEASURED"
    if "UNMEASURED" in observed:
        return "PARTIALLY_MEASURED"
    if "MEASURED_NO_SPATIAL_EVIDENCE" in observed:
        return "MEASURED_NO_SPATIAL_EVIDENCE"
    if "MEASURED_PARENT_GAIN" in observed:
        return "MEASURED_PARENT_GAIN"
    return "MEASURED"


def build_summary(
    relations: pd.DataFrame, group_metrics: pd.DataFrame, events: pd.DataFrame
) -> pd.DataFrame:
    rows = []
    metric_by_event = group_metrics.groupby("event_id", sort=False)
    rel_by_event = relations.groupby("event_id", sort=False)
    for event in events.itertuples(index=False):
        rel = rel_by_event.get_group(event.event_id) if event.event_id in rel_by_event.groups else pd.DataFrame()
        metrics = metric_by_event.get_group(event.event_id) if event.event_id in metric_by_event.groups else pd.DataFrame()
        source_count = len(set(value for value in rel.get("source_ck", pd.Series(dtype=object)).dropna()))
        target_count = len(set(value for value in rel.get("target_ck", pd.Series(dtype=object)).dropna()))
        pre_values = unique_non_null(rel.get("pre_vintage", pd.Series(dtype=float)))
        post_values = unique_non_null(rel.get("post_vintage", pd.Series(dtype=float)))
        expected_values = group_values(metrics, "expected_area_km2") if not metrics.empty else []
        source_before = sum(expected_values) if expected_values else None
        source_after = None
        if not rel.empty:
            after_values = rel[["source_ck", "source_after_area"]].dropna().drop_duplicates("source_ck")["source_after_area"]
            source_after = float(after_values.sum()) if not after_values.empty else None
        lost_values = [value for value in group_values(metrics, "available_area_km2") if value >= 0]
        transferred = rel["allocated_transfer_area_km2"].sum(min_count=1) if not rel.empty else None
        retained_values = rel[["source_ck", "parent_area_retained_km2"]].dropna().drop_duplicates("source_ck")["parent_area_retained_km2"] if not rel.empty else pd.Series(dtype=float)
        retained = float(retained_values.sum()) if not retained_values.empty else None
        accounted_values = group_values(metrics, "accounted_area_km2") if not metrics.empty else []
        accounted = sum(accounted_values) if accounted_values else None
        error = source_before - accounted if source_before is not None and accounted is not None else None
        error_pct = 100.0 * abs(error) / abs(source_before) if source_before not in (None, 0) and error is not None else None
        overlaps = group_values(metrics, "overlap_excess_km2") if not metrics.empty else []
        confidence_values = [
            overall_confidence(
                combine_confidence([item.source_before_confidence, item.target_after_confidence]),
                numeric(item.lineage_confidence),
                int(item.temporal_gap_years) if pd.notna(item.temporal_gap_years) else None,
                numeric(item.overlap_excess_pct),
                item.measurement_status,
            )
            for item in rel.itertuples(index=False)
        ]
        rows.append({
            "event_id": event.event_id,
            "event_type": rel["event_semantic"].iloc[0] if not rel.empty else "UNKNOWN",
            "event_year": int(event.event_year),
            "source_count": source_count,
            "target_count": target_count,
            "pre_vintage": int(pre_values[0]) if len(pre_values) == 1 else None,
            "post_vintage": int(post_values[0]) if len(post_values) == 1 else None,
            "source_area_before_km2": source_before,
            "source_area_after_km2": source_after,
            "total_area_lost_km2": sum(lost_values) if lost_values else None,
            "total_area_transferred_km2": numeric(transferred),
            "total_area_retained_km2": retained,
            "accounted_area_km2": accounted,
            "unaccounted_area_km2": error,
            "conservation_error_km2": error,
            "conservation_error_pct": error_pct,
            "overlap_excess_km2": sum(overlaps) if overlaps else None,
            "measurement_status": event_status(rel["measurement_status"]) if not rel.empty else "UNMEASURED",
            "area_method": AREA_METHOD,
            "confidence": combine_confidence(confidence_values),
        })
    return pd.DataFrame(rows)[SUMMARY_COLUMNS].sort_values(["event_year", "event_id"], kind="stable")


def build_area_change(usable: pd.DataFrame, registry: pd.DataFrame, relationships: pd.DataFrame) -> pd.DataFrame:
    vintages = sorted(int(year) for year in usable["source_year"].unique())
    closed = registry.set_index("canonical_key")["closed_date"].to_dict()
    rel_events = pd.concat([
        relationships[["event_id", "event_year", "source_ck"]].rename(columns={"source_ck": "canonical_key"}),
        relationships[["event_id", "event_year", "target_ck"]].rename(columns={"target_ck": "canonical_key"}),
    ]).dropna().drop_duplicates()
    event_map: dict[str, list[tuple[int, str]]] = defaultdict(list)
    for row in rel_events.itertuples(index=False):
        event_map[row.canonical_key].append((int(row.event_year), row.event_id))
    labels = registry.set_index("canonical_key")
    rows = []
    for canonical_key, group in usable.groupby("canonical_key", sort=False):
        group = group.sort_values("source_year", kind="stable")
        previous = None
        for item in group.itertuples(index=False):
            current_year = int(item.source_year)
            if previous is None:
                rows.append({
                    "canonical_key": canonical_key,
                    "district_name": labels.at[canonical_key, "display_name"],
                    "state": labels.at[canonical_key, "state_at_creation"],
                    "from_year": None,
                    "to_year": current_year,
                    "area_from_km2": None,
                    "area_to_km2": numeric(item.area_km2),
                    "area_change_km2": None,
                    "area_change_pct": None,
                    "possible_event_link": None,
                    "change_class": "NEW_OBSERVATION",
                })
            else:
                before = numeric(previous.area_km2)
                after = numeric(item.area_km2)
                delta = after - before
                delta_pct = 100.0 * delta / before if before else None
                tolerance = tolerance_km2(before)
                if abs(delta) <= tolerance:
                    change_class = "STABLE"
                else:
                    change_class = "GROWTH" if delta > 0 else "SHRINKAGE"
                links = sorted({event_id for event_year, event_id in event_map[canonical_key] if previous.source_year < event_year < current_year})
                rows.append({
                    "canonical_key": canonical_key,
                    "district_name": labels.at[canonical_key, "display_name"],
                    "state": labels.at[canonical_key, "state_at_creation"],
                    "from_year": int(previous.source_year),
                    "to_year": current_year,
                    "area_from_km2": before,
                    "area_to_km2": after,
                    "area_change_km2": delta,
                    "area_change_pct": delta_pct,
                    "possible_event_link": ";".join(links) if links else None,
                    "change_class": change_class,
                })
            previous = item
        closed_date = closed.get(canonical_key)
        last_year = int(previous.source_year)
        if pd.notna(closed_date):
            closed_year = int(pd.Timestamp(closed_date).year)
            next_vintage = min((year for year in vintages if year > closed_year), default=None)
            if next_vintage is not None and next_vintage > last_year:
                rows.append({
                    "canonical_key": canonical_key,
                    "district_name": labels.at[canonical_key, "display_name"],
                    "state": labels.at[canonical_key, "state_at_creation"],
                    "from_year": last_year,
                    "to_year": next_vintage,
                    "area_from_km2": numeric(previous.area_km2),
                    "area_to_km2": None,
                    "area_change_km2": None,
                    "area_change_pct": None,
                    "possible_event_link": None,
                    "change_class": "DISAPPEARED",
                })
    return pd.DataFrame(rows).sort_values(["canonical_key", "to_year"], kind="stable")


def deterministic_sample(frame: pd.DataFrame, category: str, predicate: pd.Series, count: int = 10) -> pd.DataFrame:
    subset = frame.loc[predicate].copy()
    if subset.empty:
        return subset
    subset["_sample_hash"] = subset.apply(
        lambda row: hashlib.sha256(
            f"{SAMPLE_SEED}|{category}|{row.event_id}|{row.source_ck}|{row.target_ck}".encode()
        ).hexdigest(),
        axis=1,
    )
    subset = subset.sort_values(["_sample_hash", "event_id"], kind="stable").head(count).drop(columns="_sample_hash")
    subset.insert(0, "validation_category", category)
    return subset


def build_random_validation(accounting: pd.DataFrame) -> pd.DataFrame:
    samples = []
    for semantic in ["CLEAN_SPLIT", "CARVE_OUT", "MULTI_CARVE_OUT", "MERGE", "RENAME"]:
        samples.append(deterministic_sample(accounting, semantic, accounting["event_type"].eq(semantic)))
    samples.append(deterministic_sample(
        accounting, "HIGH_OVERLAP", accounting["overlap_excess_pct"].fillna(-1).ge(OVERLAP_THRESHOLDS_PCT[2])
    ))
    samples.append(deterministic_sample(
        accounting, "LOW_COVERAGE", accounting["measurement_status"].eq("MEASURED_NO_SPATIAL_EVIDENCE")
    ))
    samples.append(deterministic_sample(accounting, "UNMEASURED", accounting["measurement_status"].eq("UNMEASURED")))
    samples.append(deterministic_sample(accounting, "RECONSTRUCTED", accounting["is_derived"].eq(True)))
    material = [sample for sample in samples if not sample.empty]
    if not material:
        return pd.DataFrame(columns=["validation_category", *ACCOUNTING_COLUMNS])
    return pd.concat(material, ignore_index=True)[["validation_category", *ACCOUNTING_COLUMNS]]


def official_comparison(timeseries: pd.DataFrame) -> dict[str, float | int | None]:
    benchmark_path = PROJECT_ROOT / "data" / "reference" / "official_district_area_benchmark.csv"
    if not benchmark_path.exists():
        return {"matches": 0, "mean_pct": None, "median_pct": None, "max_pct": None}
    benchmark = pd.read_csv(benchmark_path)
    computed = timeseries.loc[
        (timeseries["source_year"] == 2025)
        & timeseries["geometry_status"].isin(["VALID_OBSERVED", "VALID_REPAIRED_DERIVED_ARTIFACT"])
    ].copy()
    computed["_name"] = computed["district_name_original"].map(normalized_name)
    computed["_state"] = computed["state_name"].map(normalized_name)
    benchmark["_name"] = benchmark["official_district_name"].map(normalized_name)
    benchmark["_state"] = benchmark["official_state_name"].map(normalized_name)
    merged = computed.merge(benchmark, on=["_name", "_state"], how="inner")
    if merged.empty:
        return {"matches": 0, "mean_pct": None, "median_pct": None, "max_pct": None}
    merged["pct"] = 100.0 * (merged["area_km2"] - merged["official_area_km2"]).abs() / merged["official_area_km2"]
    return {
        "matches": int(len(merged)),
        "mean_pct": float(merged["pct"].mean()),
        "median_pct": float(merged["pct"].median()),
        "max_pct": float(merged["pct"].max()),
    }


def write_validation_report(
    timeseries: pd.DataFrame,
    accounting: pd.DataFrame,
    summary: pd.DataFrame,
    group_metrics: pd.DataFrame,
    official: dict[str, float | int | None],
    preserved: list[str],
) -> None:
    observed = timeseries["geometry_status"].value_counts().to_dict()
    measured = accounting["measurement_status"].value_counts().to_dict()
    statuses = accounting["validation_status"].value_counts().to_dict()
    def percentage(value: float | int | None) -> str:
        return f"{float(value):.4f}%" if value is not None else "not available"
    report = f"""# Revised Phase 9 Area Validation

## Method status

This is the decadal-evidence Phase 9 product.  It treats census/source
vintages as observations surrounding an event, uses the observed parent area
change as the transfer constraint, and uses intersections solely as allocation
evidence.  It does not export intersection fragments and does not represent an
observed geometry as the exact event-date boundary.

## Input evidence

- Geometry observations: {len(timeseries):,}
- Vintages: {', '.join(str(year) for year in sorted(timeseries.source_year.unique()))}
- Events in summary: {len(summary):,}
- Accounting relationships/placeholders: {len(accounting):,}
- Explicitly preserved legacy files: {', '.join(preserved) if preserved else 'present in the legacy archive (or unavailable before this build)'}

Geometry status counts: `{json.dumps(observed, sort_keys=True)}`.

## Tolerances

- Absolute tolerance: {ABSOLUTE_TOLERANCE_KM2:.1f} km²
- Relative tolerance: {RELATIVE_TOLERANCE_PCT:.1f}% of the expected comparison area
- Overlap classes: no overlap < {OVERLAP_THRESHOLDS_PCT[0]:.1f}%; minor < {OVERLAP_THRESHOLDS_PCT[1]:.1f}%; material < {OVERLAP_THRESHOLDS_PCT[2]:.1f}%; severe otherwise.

## Results

Measurement status counts: `{json.dumps(measured, sort_keys=True)}`.

Validation status counts: `{json.dumps(statuses, sort_keys=True)}`.

Event/source groups with material or severe overlap: {int(group_metrics.overlap_class.isin(['MATERIAL_OVERLAP', 'SEVERE_OVERLAP']).sum()):,}.

## 2025 Survey of India comparison

The comparison uses the locally available SOI benchmark where district and
state names match. It is a method comparison (geodesic area versus the
benchmark's source geometry calculation), not a historical benchmark.

- Matched 2025 observations: {official['matches']:,}
- Mean absolute percentage difference: {percentage(official['mean_pct'])}
- Median absolute percentage difference: {percentage(official['median_pct'])}
- Maximum percentage difference: {percentage(official['max_pct'])}

## Interpretation

`UNMEASURED` means the project does not possess the lineage or unique observed
geometry needed for the stated measurement. A blank matrix value means no
usable observation, never zero area. `MEASURED_NO_SPATIAL_EVIDENCE` means the
areas exist but no intersection supports an allocation; it is not a fabricated
transfer. `RENAME_AREA_INCONSISTENCY` is a diagnostic, not a reassignment of
territory.
"""
    (OUTPUT_DIR / "revised_area_validation.md").write_text(report, encoding="utf-8")


def export_frame(frame: pd.DataFrame, name: str, parquet: bool = True) -> None:
    frame.to_csv(PRODUCT_DIR / f"{name}.csv", index=False)
    if parquet:
        frame.to_parquet(PRODUCT_DIR / f"{name}.parquet", index=False, compression="zstd")


def main() -> None:
    PRODUCT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    preserved = archive_legacy_products()
    con = duckdb.connect(str(DB_PATH), read_only=True)
    con.execute("LOAD spatial")
    try:
        timeseries, usable, registry = build_timeseries(con)
        matrix = build_matrix(usable, registry)
        relationships, events = build_relationships(con)
        relations = add_temporal_and_geometry_inputs(relationships, usable)
        relations = add_intersection_metrics(con, relations)
        relations, group_metrics = classify_relationships(relations)
        accounting = build_accounting(relations, registry)
        summary = build_summary(relations, group_metrics, events)
        area_change = build_area_change(usable, registry, relationships)
        temporal = events[["event_id", "event_type", "event_year"]].merge(
            relations[["event_id", "pre_vintage", "post_vintage", "years_before_event", "years_after_event", "temporal_gap_years"]].drop_duplicates("event_id"),
            on="event_id", how="left",
        ).sort_values(["event_year", "event_id"], kind="stable")
        random_validation = build_random_validation(accounting)
        official = official_comparison(timeseries)

        export_frame(timeseries, "district_area_timeseries")
        matrix.to_csv(PRODUCT_DIR / "district_area_by_census.csv", index=False)
        area_change.to_csv(PRODUCT_DIR / "district_area_change.csv", index=False)
        export_frame(accounting, "event_area_accounting")
        export_frame(summary, "event_area_summary")

        event_audit = summary.merge(
            accounting.groupby("event_id", dropna=False)["validation_status"].agg(lambda values: ";".join(sorted(set(values)))).rename("validation_status").reset_index(),
            on="event_id", how="left",
        )
        event_audit.to_csv(OUTPUT_DIR / "event_conservation_audit.csv", index=False)
        area_change.to_csv(OUTPUT_DIR / "area_change_diagnostics.csv", index=False)
        group_metrics.sort_values(["event_id", "source_ck"], kind="stable").to_csv(
            OUTPUT_DIR / "overlap_diagnostics.csv", index=False
        )
        temporal.to_csv(OUTPUT_DIR / "temporal_alignment_diagnostics.csv", index=False)
        random_validation.to_csv(OUTPUT_DIR / "random_event_area_validation.csv", index=False)
        write_validation_report(timeseries, accounting, summary, group_metrics, official, preserved)
    finally:
        con.close()
    print("Rebuilt decadal Phase 9 products")
    print(f"  district_area_timeseries: {len(timeseries):,} rows")
    print(f"  event_area_accounting: {len(accounting):,} rows")
    print(f"  event_area_summary: {len(summary):,} rows")


if __name__ == "__main__":
    main()
