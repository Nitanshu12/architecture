"""
Geometry Validation — Silver layer geometry quality assessment.

Architecture v0.3 §8 (Geometry Validation Pipeline):
  Step 2: ST_IsValid check
  Record validation status WITHOUT modifying the geometry.

CRITICAL RULE (user directive):
  Do NOT blindly apply ST_MakeValid. Preserve original geometry,
  record validation status. Repaired geometry is created ONLY as
  a derived, provenance-tracked artifact (see repair.py).
"""

import logging
from typing import Tuple

import geopandas as gpd
import numpy as np
from shapely.validation import explain_validity

from src.silver.provenance.transformation_log import TransformationLog

logger = logging.getLogger(__name__)


def validate_geometries(
    gdf: gpd.GeoDataFrame,
    dataset: str,
    year: str,
    layer: str,
    pk_column: str,
    tlog: TransformationLog,
) -> gpd.GeoDataFrame:
    """
    Assess geometry validity and record status WITHOUT modifying geometry.

    Adds columns:
      _silver_geom_is_valid     : bool
      _silver_validity_reason   : str (shapely explanation for invalid)
      _silver_geom_type_original: str (original geometry type)
      _silver_geom_is_empty     : bool

    Returns the GeoDataFrame with validation columns added.
    The geometry column is NOT modified.
    """
    gdf = gdf.copy()

    gdf["_silver_geom_is_valid"] = gdf.geometry.is_valid
    gdf["_silver_geom_is_empty"] = gdf.geometry.is_empty
    gdf["_silver_geom_type_original"] = gdf.geometry.geom_type

    # Detailed validity reason for invalid geometries
    reasons = []
    for idx, row in gdf.iterrows():
        if row["_silver_geom_is_valid"]:
            reasons.append(None)
        else:
            try:
                reason = explain_validity(row.geometry)
            except Exception:
                reason = "Could not determine validity reason"
            reasons.append(reason)
    gdf["_silver_validity_reason"] = reasons

    invalid_count = int((~gdf["_silver_geom_is_valid"]).sum())
    empty_count = int(gdf["_silver_geom_is_empty"].sum())

    if invalid_count > 0:
        logger.warning(
            "  [%s/%s] %d invalid geometries detected (NOT repaired)",
            dataset, year, invalid_count,
        )
        # Log each invalid geometry for provenance
        for idx, row in gdf[~gdf["_silver_geom_is_valid"]].iterrows():
            pk_val = str(row.get(pk_column, idx))
            tlog.log(
                record_id=pk_val,
                dataset=dataset,
                year=year,
                layer=layer,
                field_or_aspect="geometry",
                transformation_type="GEOMETRY_VALIDATION",
                input_value=f"is_valid=False: {row['_silver_validity_reason']}",
                output_value="FLAGGED — original preserved, repair pending",
                transformation_rule="shapely.is_valid + explain_validity",
                delta_description=(
                    f"Invalid geometry detected: {row['_silver_validity_reason']}. "
                    "Original geometry preserved. Repair is a separate derived artifact."
                ),
            )

    if empty_count > 0:
        logger.warning(
            "  [%s/%s] %d empty geometries detected", dataset, year, empty_count,
        )

    logger.info(
        "  [%s/%s] Geometry validation: %d total, %d valid, %d invalid, %d empty",
        dataset, year, len(gdf), len(gdf) - invalid_count, invalid_count, empty_count,
    )

    return gdf


def compute_area_sqkm(
    gdf: gpd.GeoDataFrame,
    dataset: str,
    year: str,
    layer: str,
    pk_column: str,
    tlog: TransformationLog,
) -> gpd.GeoDataFrame:
    """
    Compute area in sq km using spheroidal geometry (geography type).

    Architecture v0.3 §2, Principle 9:
      "Area calculations use spheroidal geometry (PostGIS geography type)."

    For data already in EPSG:4326, we use the geodesic area calculation.
    For data in metric projections (e.g. SOI LCC), we compute area in
    the native CRS as well for comparison.

    Adds columns:
      _silver_area_sqkm          : float — geodesic area (EPSG:4326 based)
      _silver_area_native_sqkm   : float — area in native CRS units (if metric)
      _silver_perimeter_km       : float — geodesic perimeter
      _silver_centroid_lat       : float
      _silver_centroid_lon       : float
    """
    gdf = gdf.copy()

    # Determine if CRS is geographic or projected
    is_geographic = gdf.crs is not None and gdf.crs.is_geographic

    if is_geographic:
        # Geodesic area for geographic CRS (EPSG:4326)
        # Use geopandas to_crs to a suitable equal-area projection for area
        gdf_ea = gdf.to_crs("EPSG:6933")  # World Cylindrical Equal Area
        gdf["_silver_area_sqkm"] = gdf_ea.geometry.area / 1e6
        gdf["_silver_perimeter_km"] = gdf_ea.geometry.length / 1e3
        gdf["_silver_area_native_sqkm"] = None  # native IS geographic
    else:
        # Projected CRS — compute in native units first
        native_area = gdf.geometry.area
        native_unit = "metres"  # assume metric for SOI LCC
        gdf["_silver_area_native_sqkm"] = native_area / 1e6

        # Also compute geodesic area via temporary reprojection
        gdf_4326 = gdf.to_crs("EPSG:4326")
        gdf_ea = gdf_4326.to_crs("EPSG:6933")
        gdf["_silver_area_sqkm"] = gdf_ea.geometry.area / 1e6
        gdf["_silver_perimeter_km"] = gdf_ea.geometry.length / 1e3

    # Centroids (always in EPSG:4326 for lat/lon)
    if is_geographic:
        centroids = gdf.geometry.centroid
    else:
        centroids = gdf.to_crs("EPSG:4326").geometry.centroid
    gdf["_silver_centroid_lon"] = centroids.x
    gdf["_silver_centroid_lat"] = centroids.y

    # Log the area computation
    tlog.log(
        record_id="ALL",
        dataset=dataset,
        year=year,
        layer=layer,
        field_or_aspect="area",
        transformation_type="AREA_COMPUTE",
        input_value=f"CRS={gdf.crs}",
        output_value=f"area_sqkm via EPSG:6933 equal-area projection",
        transformation_rule="geopandas.to_crs(EPSG:6933).area / 1e6",
        delta_description=(
            f"Computed geodesic area for {len(gdf)} geometries. "
            f"Range: {gdf['_silver_area_sqkm'].min():.2f} — "
            f"{gdf['_silver_area_sqkm'].max():.2f} sq km"
        ),
    )

    return gdf
