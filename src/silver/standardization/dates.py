"""
Date Standardization — Silver layer temporal handling.

USER DIRECTIVE (CRITICAL):
  effective_year is an OBSERVED YEAR, NOT January 1. Preserve the
  original year and precision. Any DATE anchor must be explicitly
  labelled as an estimated/representational date and must NEVER be
  interpreted as an exact event date.

Architecture v0.3 §5 (Temporal Model):
  "Every temporal field that represents a known or estimable date
   must be NOT NULL. Uncertainty is carried by the corresponding
   _precision field."

This module adds precision labels and estimated date anchors where
needed for downstream temporal processing, while ALWAYS preserving
the original source value and marking any date representation as
estimated.
"""

import logging

import pandas as pd

from src.silver.provenance.transformation_log import TransformationLog

logger = logging.getLogger(__name__)

# Valid precision values from Architecture §5
VALID_PRECISIONS = {
    "EXACT", "MONTH", "YEAR", "DECADE", "CENTURY", "UNKNOWN",
}


def add_temporal_precision(
    df: pd.DataFrame,
    year_column: str,
    dataset: str,
    year: str,
    layer: str,
    tlog: TransformationLog,
) -> pd.DataFrame:
    """
    Add temporal precision labels to year-based fields.

    For events data where only effective_year (integer) is available:
      - Preserves the original effective_year as-is
      - Adds _silver_temporal_precision = 'YEAR'
      - Adds _silver_date_est = representational DATE anchor (YYYY-01-01)
      - Adds _silver_date_is_estimated = True (ALWAYS)
      - Adds _silver_date_est_note explaining the representation

    The DATE anchor is NEVER to be interpreted as an exact event date.
    It exists solely for temporal range queries and ordering.
    """
    df = df.copy()

    if year_column not in df.columns:
        logger.warning(
            "  [%s/%s] Column '%s' not found — skipping temporal precision",
            dataset, year, year_column,
        )
        return df

    # Preserve original
    df["_silver_temporal_original"] = df[year_column]
    df["_silver_temporal_precision"] = "YEAR"
    df["_silver_date_is_estimated"] = True
    df["_silver_date_est_note"] = (
        f"Representational date derived from observed {year_column}. "
        "This is NOT an exact event date. The source provides only a year."
    )

    # Create estimated date anchor (for temporal ordering only)
    # Architecture §5: use January 1 of the year as conservative lower bound
    df["_silver_date_est"] = pd.to_datetime(
        df[year_column].astype(str) + "-01-01",
        format="%Y-%m-%d",
        errors="coerce",
    )

    null_dates = int(df["_silver_date_est"].isnull().sum())
    if null_dates > 0:
        logger.warning(
            "  [%s/%s] %d rows could not produce a date anchor from %s",
            dataset, year, null_dates, year_column,
        )

    tlog.log(
        record_id="ALL",
        dataset=dataset,
        year=year,
        layer=layer,
        field_or_aspect="temporal",
        transformation_type="DATE_PARSE",
        input_value=f"{year_column} (integer year)",
        output_value="_silver_date_est (DATE, estimated) + precision=YEAR",
        transformation_rule=(
            "YYYY → YYYY-01-01 as representational anchor; "
            "precision=YEAR; date_is_estimated=True"
        ),
        delta_description=(
            f"Added temporal precision labels to {len(df)} records. "
            f"Original {year_column} preserved. Date anchor is estimated, "
            "never to be interpreted as exact."
        ),
    )

    logger.info(
        "  [%s/%s] Temporal precision added: %d records, precision=YEAR, "
        "all dates marked as estimated",
        dataset, year, len(df),
    )

    return df


def add_observation_year_precision(
    df: pd.DataFrame,
    observation_year: int,
    dataset: str,
    year: str,
    layer: str,
    tlog: TransformationLog,
) -> pd.DataFrame:
    """
    Add observation date metadata for spatial datasets.

    For spatial datasets, the observation year comes from the source
    configuration (e.g., census year). This is the date the geometry
    was observed/recorded, not the date the administrative boundary
    was legally in effect.
    """
    df = df.copy()

    df["_silver_observation_year"] = observation_year
    df["_silver_observation_precision"] = "YEAR"
    df["_silver_observation_date_est"] = pd.Timestamp(f"{observation_year}-01-01")
    df["_silver_observation_date_is_estimated"] = True
    df["_silver_observation_note"] = (
        f"Observation date represents census/source year {observation_year}. "
        "Exact observation date unknown; year-level precision only."
    )

    return df
