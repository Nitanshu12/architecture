"""
Geometry Transform — CRS reprojection and type casting.

Architecture v0.3 §8 (Geometry Validation Pipeline):
  Step 4: ST_GeometryType check — POLYGON → ST_Multi
  Step 5: ST_Transform to EPSG:4326 (if CRS differs)

USER DIRECTIVE:
  Reproject SOI to standardized CRS as required, but keep metric CRS
  calculations separate from storage CRS.

All transformations are logged in the transformation_log BEFORE they
are applied.
"""

import logging

import geopandas as gpd
from shapely import MultiPolygon, Polygon

from src.silver.provenance.transformation_log import TransformationLog

logger = logging.getLogger(__name__)

STANDARD_CRS = "EPSG:4326"


def reproject_to_standard_crs(
    gdf: gpd.GeoDataFrame,
    dataset: str,
    year: str,
    layer: str,
    tlog: TransformationLog,
) -> gpd.GeoDataFrame:
    """
    Reproject geometry to EPSG:4326 if not already in that CRS.

    The original CRS is recorded. For metric CRS (e.g., SOI LCC),
    native-CRS area has already been computed by validation.py and
    stored in _silver_area_native_sqkm.

    Returns reprojected GeoDataFrame.
    """
    if gdf.crs is None:
        logger.warning(
            "  [%s/%s] No CRS defined — assuming EPSG:4326", dataset, year,
        )
        gdf = gdf.set_crs(STANDARD_CRS)
        tlog.log(
            record_id="ALL",
            dataset=dataset,
            year=year,
            layer=layer,
            field_or_aspect="crs",
            transformation_type="CRS_TRANSFORM",
            input_value="None (undefined)",
            output_value=STANDARD_CRS,
            transformation_rule="assumed EPSG:4326 — no CRS metadata in source",
            delta_description="CRS was undefined; assumed EPSG:4326",
        )
        return gdf

    source_crs = str(gdf.crs)

    if gdf.crs.to_epsg() == 4326:
        logger.info(
            "  [%s/%s] Already in EPSG:4326 — no reprojection needed",
            dataset, year,
        )
        gdf["_silver_reprojected"] = False
        gdf["_silver_source_crs"] = source_crs
        return gdf

    # Log BEFORE reprojecting
    tlog.log(
        record_id="ALL",
        dataset=dataset,
        year=year,
        layer=layer,
        field_or_aspect="crs",
        transformation_type="CRS_TRANSFORM",
        input_value=source_crs[:100],
        output_value=STANDARD_CRS,
        transformation_rule="geopandas.GeoDataFrame.to_crs(EPSG:4326)",
        delta_description=(
            f"Reprojected {len(gdf)} geometries from {source_crs[:50]} to {STANDARD_CRS}. "
            "Native-CRS metrics preserved in _silver_area_native_sqkm."
        ),
    )

    gdf = gdf.to_crs(STANDARD_CRS)
    gdf["_silver_reprojected"] = True
    gdf["_silver_source_crs"] = source_crs

    logger.info(
        "  [%s/%s] Reprojected %d geometries from %s to %s",
        dataset, year, len(gdf), source_crs[:40], STANDARD_CRS,
    )

    return gdf


def cast_to_multipolygon(
    gdf: gpd.GeoDataFrame,
    dataset: str,
    year: str,
    layer: str,
    pk_column: str,
    tlog: TransformationLog,
) -> gpd.GeoDataFrame:
    """
    Cast single Polygon geometries to MultiPolygon.

    Architecture v0.3 §8, Step 4:
      "POLYGON → ST_Multi; already MULTIPOLYGON → keep"

    Each cast is logged individually in the transformation log.
    """
    gdf = gdf.copy()

    single_mask = gdf.geometry.geom_type == "Polygon"
    cast_count = int(single_mask.sum())

    if cast_count == 0:
        logger.info(
            "  [%s/%s] All geometries are MultiPolygon — no casting needed",
            dataset, year,
        )
        gdf["_silver_cast_to_multi"] = False
        return gdf

    # Log BEFORE casting
    tlog.log(
        record_id="BATCH",
        dataset=dataset,
        year=year,
        layer=layer,
        field_or_aspect="geometry_type",
        transformation_type="CAST_TO_MULTI",
        input_value=f"{cast_count} Polygon geometries",
        output_value=f"{cast_count} MultiPolygon geometries",
        transformation_rule="shapely.MultiPolygon([polygon])",
        delta_description=(
            f"Cast {cast_count} single Polygon(s) to MultiPolygon. "
            "No geometry data modified — only type wrapper changed."
        ),
    )

    # Apply cast
    def to_multi(geom):
        if isinstance(geom, Polygon):
            return MultiPolygon([geom])
        return geom

    gdf["geometry"] = gdf.geometry.apply(to_multi)
    gdf["_silver_cast_to_multi"] = single_mask

    logger.info(
        "  [%s/%s] Cast %d Polygon(s) to MultiPolygon", dataset, year, cast_count,
    )

    return gdf
