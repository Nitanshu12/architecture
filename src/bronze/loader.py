"""
Bronze Layer Loader
===================
Ingests source data into the Bronze layer, preserving ALL original fields
and identifiers. Adds ingestion metadata without modifying source data.

Architecture Compliance:
  - Principle 1: Source immutability — original records never modified.
  - Bronze is append-only. Original GPKG/CSV files are NEVER overwritten.
  - ALL original source fields are preserved in Bronze output.
  - Geometry validity is RECORDED but NOT repaired.
    Repair is a Silver-layer operation producing a derived, provenance-tracked
    artifact. Original geometry is always the geometry stored in Bronze.
  - effective_year is preserved as an observed integer year.
    It is NOT converted to a DATE. Any downstream DATE anchor must be
    explicitly labelled as estimated/representational.
  - CRS is preserved as-is. SOI LCC projection is NOT reprojected in Bronze.
  - Quarantine layers are ingested and isolated, not discarded.

NOT Responsible For (these belong to later pipeline stages):
  - CRS reprojection (Silver)
  - Geometry repair / ST_MakeValid (Silver — derived artifact with provenance)
  - Name standardization (Silver)
  - Date parsing or conversion (Silver)
  - CK generation (Identity layer)
  - Event type mapping to Architecture taxonomy (Event layer)
  - Split case classification — CLEAN_SPLIT vs CARVE_OUT (Event layer)
  - Lineage, snapshots, harmonization (Gold layers)
"""

import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Optional

import yaml
import pandas as pd
import geopandas as gpd
import fiona

from src.pipeline.run_context import RunContext
from src.bronze.immutability import compute_sha256
from src.bronze.manifest import (
    IngestManifest,
    IngestRecord,
    SourcePKValidationResult,
    GeometryStatusSummary,
)

logger = logging.getLogger(__name__)


class BronzeLoader:
    """
    Loads source datasets into the Bronze layer.

    Responsibilities:
      1. Read source files (GPKG, CSV) preserving ALL original fields.
      2. Compute and record SHA-256 checksums for immutability.
      3. Validate source primary keys for uniqueness and nullability.
      4. Record geometry validity status WITHOUT modifying geometry.
      5. Add bronze-layer metadata columns (prefixed with _bronze_).
      6. Persist outputs as GeoParquet (spatial) or Parquet (tabular).
      7. Generate ingest manifests with full provenance.
      8. Quarantine suspect data layers with documented reasons.
    """

    # Bronze metadata column prefix — ensures no collision with source fields
    META_PREFIX = "_bronze_"

    def __init__(self, project_root: Path, run_context: RunContext):
        self.project_root = Path(project_root)
        self.run_context = run_context
        self.config = self._load_config()
        self.manifest = IngestManifest(
            run_id=str(run_context.run_id),
            pipeline_version=run_context.pipeline_version,
            started_at=run_context.started_at.isoformat(),
        )
        self.output_base = self.project_root / "data" / "bronze" / "ingested"
        self.manifest_dir = self.project_root / "data" / "bronze" / "manifests"

    def _load_config(self) -> dict:
        """Load source configuration from config/sources.yaml."""
        config_path = self.project_root / "config" / "sources.yaml"
        with open(config_path) as f:
            return yaml.safe_load(f)

    # ------------------------------------------------------------------ #
    #  PUBLIC API                                                          #
    # ------------------------------------------------------------------ #

    def ingest_all(self) -> IngestManifest:
        """
        Run the full Bronze ingestion pipeline.

        Returns the completed IngestManifest with all records and
        validation results.
        """
        logger.info(
            "Starting Bronze ingestion — run_id=%s, version=%s",
            self.run_context.run_id,
            self.run_context.pipeline_version,
        )

        for source_key, source_config in self.config["sources"].items():
            source_type = source_config["type"]
            logger.info("Processing source: %s (type=%s)", source_key, source_type)

            if source_type == "spatial":
                self._ingest_spatial_source(source_key, source_config)
            elif source_type == "tabular":
                self._ingest_tabular_source(source_key, source_config)
            else:
                logger.warning("Unknown source type '%s' for %s", source_type, source_key)

        # Finalize
        self.run_context.complete()
        self.manifest.completed_at = self.run_context.completed_at.isoformat()
        manifest_path = self.manifest.save(self.manifest_dir)
        logger.info("Bronze ingestion complete. Manifest: %s", manifest_path)

        return self.manifest

    # ------------------------------------------------------------------ #
    #  SPATIAL INGESTION                                                   #
    # ------------------------------------------------------------------ #

    def _ingest_spatial_source(self, source_key: str, source_config: dict) -> None:
        """Ingest all datasets for a spatial source (stanford, soi)."""
        for year_key, ds_cfg in source_config["datasets"].items():
            year_str = str(year_key)
            filepath = self.project_root / ds_cfg["path"]

            if not filepath.exists():
                logger.error("Source file not found: %s", filepath)
                continue

            checksum = compute_sha256(filepath)
            file_size = filepath.stat().st_size
            logger.info(
                "  [%s/%s] file=%s size=%d checksum=%s",
                source_key, year_str, filepath.name, file_size, checksum[:16],
            )

            # --- Primary layer ---
            primary_layer = ds_cfg["primary_layer"]
            self._ingest_spatial_layer(
                filepath=filepath,
                source_key=source_key,
                year_str=year_str,
                layer=primary_layer,
                ds_cfg=ds_cfg,
                checksum=checksum,
                file_size=file_size,
                is_quarantine=False,
            )

            # --- Quarantine layers ---
            quarantine_layers = ds_cfg.get("quarantine_layers", {})
            for q_layer_name, q_cfg in quarantine_layers.items():
                self._ingest_spatial_layer(
                    filepath=filepath,
                    source_key=source_key,
                    year_str=year_str,
                    layer=str(q_layer_name),
                    ds_cfg=ds_cfg,
                    checksum=checksum,
                    file_size=file_size,
                    is_quarantine=True,
                    quarantine_reason=q_cfg.get("reason", "Reason not specified"),
                )

    def _ingest_spatial_layer(
        self,
        filepath: Path,
        source_key: str,
        year_str: str,
        layer: str,
        ds_cfg: dict,
        checksum: str,
        file_size: int,
        is_quarantine: bool,
        quarantine_reason: Optional[str] = None,
    ) -> None:
        """Ingest a single GPKG layer."""
        try:
            gdf = gpd.read_file(str(filepath), layer=layer)
        except Exception as e:
            logger.error("Failed to read layer %s from %s: %s", layer, filepath, e)
            self.manifest.add_record(
                IngestRecord(
                    source_file=str(filepath),
                    source_dataset=source_key,
                    layer_name=layer,
                    file_size_bytes=file_size,
                    sha256_checksum=checksum,
                    record_count=0,
                    geometry_count=None,
                    output_path="",
                    ingested_at=datetime.now(timezone.utc).isoformat(),
                    run_id=str(self.run_context.run_id),
                    pipeline_version=self.run_context.pipeline_version,
                    status="ERROR",
                    notes=str(e),
                )
            )
            return

        # --- Validate source PK (only for primary layers) ---
        pk_validation = None
        if not is_quarantine:
            pk_col = ds_cfg.get("source_pk")
            if pk_col and pk_col in gdf.columns:
                pk_validation = self._validate_source_pk(
                    gdf, pk_col, source_key, layer, year_str
                )
                self.manifest.add_pk_validation(pk_validation)
                if not pk_validation.is_valid_pk:
                    logger.warning(
                        "  PK VALIDATION FAILED for %s/%s/%s: %s",
                        source_key, year_str, layer, pk_validation.issues,
                    )

        # --- Record geometry status (WITHOUT modifying geometry) ---
        geom_status = self._record_geometry_status(gdf)
        gdf = self._add_geometry_status_columns(gdf)

        # --- Add bronze metadata columns ---
        gdf = self._add_bronze_metadata_spatial(
            gdf, filepath, source_key, layer, year_str, checksum,
        )

        # --- Persist ---
        if is_quarantine:
            subdir = "quarantine"
            status = "QUARANTINED"
        else:
            subdir = "spatial"
            status = "SUCCESS"

        output_name = f"{source_key}_{layer}_{year_str}.geoparquet"
        output_path = self.output_base / subdir / output_name
        output_path.parent.mkdir(parents=True, exist_ok=True)

        gdf.to_parquet(str(output_path))
        logger.info(
            "    -> %s: %d records -> %s",
            status, len(gdf), output_path.name,
        )

        # --- Manifest record ---
        non_empty = int((~gdf.geometry.is_empty).sum()) if "geometry" in gdf.columns else None
        self.manifest.add_record(
            IngestRecord(
                source_file=str(filepath),
                source_dataset=source_key,
                layer_name=layer,
                file_size_bytes=file_size,
                sha256_checksum=checksum,
                record_count=len(gdf),
                geometry_count=non_empty,
                output_path=str(output_path),
                ingested_at=datetime.now(timezone.utc).isoformat(),
                run_id=str(self.run_context.run_id),
                pipeline_version=self.run_context.pipeline_version,
                status=status,
                pk_validation=pk_validation,
                geometry_status=geom_status,
                quarantine_reason=quarantine_reason,
            )
        )

    # ------------------------------------------------------------------ #
    #  TABULAR INGESTION                                                   #
    # ------------------------------------------------------------------ #

    def _ingest_tabular_source(self, source_key: str, source_config: dict) -> None:
        """Ingest all datasets for a tabular source (events CSV)."""
        for ds_key, ds_cfg in source_config["datasets"].items():
            filepath = self.project_root / ds_cfg["path"]

            if not filepath.exists():
                logger.error("Source file not found: %s", filepath)
                continue

            checksum = compute_sha256(filepath)
            file_size = filepath.stat().st_size

            df = pd.read_csv(filepath)
            logger.info(
                "  [%s/%s] rows=%d cols=%d checksum=%s",
                source_key, ds_key, len(df), len(df.columns), checksum[:16],
            )

            # --- Add bronze metadata ---
            df = self._add_bronze_metadata_tabular(
                df, filepath, source_key, ds_key, checksum,
            )

            # --- Persist ---
            output_name = f"{ds_key}.parquet"
            output_path = self.output_base / "events" / output_name
            output_path.parent.mkdir(parents=True, exist_ok=True)

            df.to_parquet(str(output_path), index=False)
            logger.info("    -> SUCCESS: %d records -> %s", len(df), output_path.name)

            self.manifest.add_record(
                IngestRecord(
                    source_file=str(filepath),
                    source_dataset=source_key,
                    layer_name=None,
                    file_size_bytes=file_size,
                    sha256_checksum=checksum,
                    record_count=len(df),
                    geometry_count=None,
                    output_path=str(output_path),
                    ingested_at=datetime.now(timezone.utc).isoformat(),
                    run_id=str(self.run_context.run_id),
                    pipeline_version=self.run_context.pipeline_version,
                    status="SUCCESS",
                )
            )

    # ------------------------------------------------------------------ #
    #  SOURCE PK VALIDATION                                                #
    # ------------------------------------------------------------------ #

    def _validate_source_pk(
        self,
        df: pd.DataFrame,
        pk_column: str,
        dataset: str,
        layer: str,
        year: str,
    ) -> SourcePKValidationResult:
        """
        Validate a candidate source primary key for uniqueness and nullability.

        Architecture v0.3 §4: Source PKs must be validated before registration
        as source identifiers. A valid PK is both unique and non-null.
        """
        total = len(df)
        null_count = int(df[pk_column].isnull().sum())
        unique_count = int(df[pk_column].nunique())
        is_unique = unique_count == total
        is_complete = null_count == 0
        is_valid = is_unique and is_complete

        issues = []
        if not is_complete:
            issues.append(f"{null_count} NULL values in {pk_column}")
        if not is_unique:
            dup_count = total - unique_count
            issues.append(
                f"{dup_count} duplicate values in {pk_column} "
                f"({unique_count} unique / {total} total)"
            )

        result = SourcePKValidationResult(
            dataset=dataset,
            year=year,
            layer=layer,
            pk_column=pk_column,
            total_rows=total,
            unique_values=unique_count,
            null_count=null_count,
            is_unique=is_unique,
            is_complete=is_complete,
            is_valid_pk=is_valid,
            issues=issues,
        )

        logger.info(
            "    PK validation [%s/%s] %s: unique=%s complete=%s valid=%s",
            dataset, year, pk_column, is_unique, is_complete, is_valid,
        )
        return result

    # ------------------------------------------------------------------ #
    #  GEOMETRY STATUS (RECORD ONLY — NO REPAIR)                           #
    # ------------------------------------------------------------------ #

    def _record_geometry_status(self, gdf: gpd.GeoDataFrame) -> GeometryStatusSummary:
        """
        Compute aggregate geometry status for a GeoDataFrame.

        CRITICAL: This method does NOT modify any geometry. It only reads
        geometry properties. The original geometry from the source file
        is preserved exactly as-is. Repair is a Silver-layer operation
        that creates a separate derived artifact with full provenance.
        """
        total = len(gdf)
        is_valid = gdf.geometry.is_valid
        is_empty = gdf.geometry.is_empty
        geom_types = gdf.geometry.geom_type

        valid_count = int(is_valid.sum())
        invalid_count = total - valid_count
        empty_count = int(is_empty.sum())
        multi_count = int(geom_types.str.startswith("Multi").sum())
        single_count = total - multi_count

        crs_str = str(gdf.crs) if gdf.crs else "UNKNOWN"
        type_list = sorted(geom_types.dropna().unique().tolist())

        return GeometryStatusSummary(
            total_geometries=total,
            valid_count=valid_count,
            invalid_count=invalid_count,
            empty_count=empty_count,
            multipart_count=multi_count,
            singlepart_count=single_count,
            original_crs=crs_str,
            geometry_types=type_list,
        )

    def _add_geometry_status_columns(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Add per-row geometry status columns WITHOUT modifying the geometry.

        Columns added (all prefixed with _bronze_):
          _bronze_geom_is_valid   : bool — shapely is_valid result
          _bronze_geom_type       : str  — original geometry type string
          _bronze_geom_is_empty   : bool — shapely is_empty result
          _bronze_geom_is_multi   : bool — whether geometry is a Multi* type
          _bronze_original_crs    : str  — CRS of the source file
        """
        gdf[f"{self.META_PREFIX}geom_is_valid"] = gdf.geometry.is_valid
        gdf[f"{self.META_PREFIX}geom_type"] = gdf.geometry.geom_type
        gdf[f"{self.META_PREFIX}geom_is_empty"] = gdf.geometry.is_empty
        gdf[f"{self.META_PREFIX}geom_is_multi"] = (
            gdf.geometry.geom_type.str.startswith("Multi")
        )
        crs_str = str(gdf.crs) if gdf.crs else "UNKNOWN"
        gdf[f"{self.META_PREFIX}original_crs"] = crs_str
        return gdf

    # ------------------------------------------------------------------ #
    #  BRONZE METADATA COLUMNS                                             #
    # ------------------------------------------------------------------ #

    def _add_bronze_metadata_spatial(
        self,
        gdf: gpd.GeoDataFrame,
        source_file: Path,
        source_dataset: str,
        layer: str,
        year: str,
        checksum: str,
    ) -> gpd.GeoDataFrame:
        """Add bronze-layer metadata to a spatial GeoDataFrame."""
        gdf[f"{self.META_PREFIX}run_id"] = str(self.run_context.run_id)
        gdf[f"{self.META_PREFIX}pipeline_version"] = self.run_context.pipeline_version
        gdf[f"{self.META_PREFIX}ingested_at"] = datetime.now(timezone.utc).isoformat()
        gdf[f"{self.META_PREFIX}source_file"] = str(source_file.name)
        gdf[f"{self.META_PREFIX}source_dataset"] = source_dataset
        gdf[f"{self.META_PREFIX}source_layer"] = layer
        gdf[f"{self.META_PREFIX}observation_year"] = year
        gdf[f"{self.META_PREFIX}source_checksum"] = checksum
        gdf[f"{self.META_PREFIX}row_id"] = range(1, len(gdf) + 1)
        return gdf

    def _add_bronze_metadata_tabular(
        self,
        df: pd.DataFrame,
        source_file: Path,
        source_dataset: str,
        dataset_key: str,
        checksum: str,
    ) -> pd.DataFrame:
        """
        Add bronze-layer metadata to a tabular DataFrame.

        NOTE on effective_year: The original effective_year column is
        preserved as-is (integer). It represents an OBSERVED YEAR, not
        an exact date. No DATE conversion is performed in Bronze.
        """
        df[f"{self.META_PREFIX}run_id"] = str(self.run_context.run_id)
        df[f"{self.META_PREFIX}pipeline_version"] = self.run_context.pipeline_version
        df[f"{self.META_PREFIX}ingested_at"] = datetime.now(timezone.utc).isoformat()
        df[f"{self.META_PREFIX}source_file"] = str(source_file.name)
        df[f"{self.META_PREFIX}source_dataset"] = source_dataset
        df[f"{self.META_PREFIX}source_key"] = dataset_key
        df[f"{self.META_PREFIX}source_checksum"] = checksum
        df[f"{self.META_PREFIX}row_id"] = range(1, len(df) + 1)
        return df
