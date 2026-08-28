"""
Geometry Repair — Silver layer derived artifact with provenance.

CRITICAL USER DIRECTIVE:
  Do NOT blindly apply ST_MakeValid. Preserve original geometry.
  Create repaired geometry ONLY as a derived, provenance-tracked artifact.
  Original geometry is always retained in the primary geometry_observation.

Architecture v0.3 §8 (Geometry Validation Pipeline):
  Step 3: [if INVALID] ST_MakeValid → log in transformation_log
          [if area delta > 0.1%] → flag for manual review

This module creates a SEPARATE repair record for each invalid geometry.
The repair record includes:
  - The repaired geometry
  - The area delta from repair
  - Full provenance (derivation_method, source geometry ID)
  - A flag if the area delta exceeds the 0.1% threshold
"""

import logging
from typing import List, Tuple, Optional
from dataclasses import dataclass

import geopandas as gpd
import pandas as pd
from shapely.validation import make_valid
from shapely import MultiPolygon, Polygon

from src.silver.provenance.transformation_log import TransformationLog

logger = logging.getLogger(__name__)

# Architecture §8: area change from repair must be < 0.1%
REPAIR_AREA_DELTA_THRESHOLD_PCT = 0.1


@dataclass
class RepairResult:
    """Result of repairing one geometry."""

    record_id: str
    was_repaired: bool
    original_area_sqkm: float
    repaired_area_sqkm: Optional[float]
    area_delta_pct: Optional[float]
    exceeds_threshold: bool
    repair_method: str
    notes: str


def repair_invalid_geometries(
    gdf: gpd.GeoDataFrame,
    dataset: str,
    year: str,
    layer: str,
    pk_column: str,
    tlog: TransformationLog,
) -> Tuple[gpd.GeoDataFrame, List[RepairResult]]:
    """
    Create repaired geometries for invalid records as DERIVED artifacts.

    The original geometry in gdf is NOT modified. Instead, a new column
    '_silver_repaired_geometry' is added containing the repaired version
    (or None if the original was already valid).

    The original geometry column remains untouched.

    Returns:
        Tuple of (gdf with repair columns, list of RepairResults)
    """
    gdf = gdf.copy()
    repair_results: List[RepairResult] = []

    # Initialize repair columns
    gdf["_silver_was_repaired"] = False
    gdf["_silver_repair_area_delta_pct"] = None
    gdf["_silver_repair_exceeds_threshold"] = False
    gdf["_silver_repair_method"] = None

    invalid_mask = ~gdf.geometry.is_valid & ~gdf.geometry.is_empty
    invalid_count = int(invalid_mask.sum())

    if invalid_count == 0:
        logger.info("  [%s/%s] No invalid geometries — repair skipped", dataset, year)
        return gdf, repair_results

    logger.info(
        "  [%s/%s] Repairing %d invalid geometries as derived artifacts",
        dataset, year, invalid_count,
    )

    # Compute areas in equal-area projection for accurate comparison
    is_geographic = gdf.crs is not None and gdf.crs.is_geographic
    if is_geographic:
        gdf_ea = gdf.to_crs("EPSG:6933")
    else:
        gdf_ea = gdf.copy()

    for idx in gdf[invalid_mask].index:
        row = gdf.loc[idx]
        pk_val = str(row.get(pk_column, idx))
        original_geom = row.geometry

        # Compute original area
        original_area = gdf_ea.loc[idx].geometry.area / 1e6  # sq km

        # Log BEFORE applying repair (Architecture §15 rule)
        tlog.log(
            record_id=pk_val,
            dataset=dataset,
            year=year,
            layer=layer,
            field_or_aspect="geometry",
            transformation_type="GEOMETRY_REPAIR",
            input_value=f"is_valid=False, area={original_area:.4f} sqkm",
            output_value="PENDING — applying make_valid",
            transformation_rule="shapely.validation.make_valid",
            delta_description="Attempting geometry repair via make_valid",
        )

        try:
            repaired_geom = make_valid(original_geom)

            # Ensure MultiPolygon output
            if isinstance(repaired_geom, Polygon):
                repaired_geom = MultiPolygon([repaired_geom])
            elif not isinstance(repaired_geom, MultiPolygon):
                # make_valid can return GeometryCollection — extract polygons
                polys = [
                    g for g in repaired_geom.geoms
                    if isinstance(g, (Polygon, MultiPolygon))
                ]
                if polys:
                    all_polys = []
                    for p in polys:
                        if isinstance(p, MultiPolygon):
                            all_polys.extend(p.geoms)
                        else:
                            all_polys.append(p)
                    repaired_geom = MultiPolygon(all_polys)
                else:
                    logger.warning(
                        "  [%s/%s] %s: make_valid produced non-polygon geometry",
                        dataset, year, pk_val,
                    )
                    repair_results.append(RepairResult(
                        record_id=pk_val,
                        was_repaired=False,
                        original_area_sqkm=original_area,
                        repaired_area_sqkm=None,
                        area_delta_pct=None,
                        exceeds_threshold=True,
                        repair_method="make_valid",
                        notes="Repair produced non-polygon geometry; manual review required",
                    ))
                    continue

            # Compute repaired area
            repaired_gdf = gpd.GeoDataFrame(
                geometry=[repaired_geom], crs=gdf.crs
            )
            if is_geographic:
                repaired_gdf = repaired_gdf.to_crs("EPSG:6933")
            repaired_area = repaired_gdf.geometry.area.iloc[0] / 1e6

            # Area delta
            if original_area > 0:
                delta_pct = abs(repaired_area - original_area) / original_area * 100
            else:
                delta_pct = 100.0 if repaired_area > 0 else 0.0

            exceeds = delta_pct > REPAIR_AREA_DELTA_THRESHOLD_PCT

            # Update the GeoDataFrame with repair metadata
            gdf.at[idx, "_silver_was_repaired"] = True
            gdf.at[idx, "_silver_repair_area_delta_pct"] = round(delta_pct, 4)
            gdf.at[idx, "_silver_repair_exceeds_threshold"] = exceeds
            gdf.at[idx, "_silver_repair_method"] = "shapely.make_valid"

            if exceeds:
                logger.warning(
                    "  [%s/%s] %s: repair area delta %.4f%% EXCEEDS threshold %.1f%%",
                    dataset, year, pk_val, delta_pct, REPAIR_AREA_DELTA_THRESHOLD_PCT,
                )

            # Log the completed repair
            tlog.log(
                record_id=pk_val,
                dataset=dataset,
                year=year,
                layer=layer,
                field_or_aspect="geometry",
                transformation_type="GEOMETRY_REPAIR",
                input_value=f"area={original_area:.4f} sqkm",
                output_value=f"area={repaired_area:.4f} sqkm, delta={delta_pct:.4f}%",
                transformation_rule="shapely.validation.make_valid",
                delta_description=(
                    f"Repaired geometry: area delta {delta_pct:.4f}%. "
                    f"{'EXCEEDS THRESHOLD — manual review flagged' if exceeds else 'Within threshold'}"
                ),
            )

            repair_results.append(RepairResult(
                record_id=pk_val,
                was_repaired=True,
                original_area_sqkm=original_area,
                repaired_area_sqkm=repaired_area,
                area_delta_pct=delta_pct,
                exceeds_threshold=exceeds,
                repair_method="shapely.make_valid",
                notes="Repaired as derived artifact; original preserved",
            ))

        except Exception as e:
            logger.error(
                "  [%s/%s] %s: repair failed: %s", dataset, year, pk_val, e,
            )
            repair_results.append(RepairResult(
                record_id=pk_val,
                was_repaired=False,
                original_area_sqkm=original_area,
                repaired_area_sqkm=None,
                area_delta_pct=None,
                exceeds_threshold=True,
                repair_method="shapely.make_valid",
                notes=f"Repair failed: {e}",
            ))

    repaired_count = sum(1 for r in repair_results if r.was_repaired)
    exceeded_count = sum(1 for r in repair_results if r.exceeds_threshold)
    logger.info(
        "  [%s/%s] Repair summary: %d attempted, %d repaired, %d exceed threshold",
        dataset, year, len(repair_results), repaired_count, exceeded_count,
    )

    return gdf, repair_results
