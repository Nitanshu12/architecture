"""
Name Standardization — Silver layer name normalization.

Architecture v0.3 §17 (Silver Layer):
  "name_variant — One name form per district per source per period"

This module normalizes district and state names to enable downstream
cross-source matching while preserving the original name as recorded
in the source.

Standardization operations:
  - Strip leading/trailing whitespace
  - Collapse multiple spaces
  - Unicode NFC normalization
  - Consistent title-casing for display
  - Ampersand normalization (& vs and)
  - Common abbreviation expansion

CRITICAL: The original name is ALWAYS preserved. The standardized name
is stored in a separate column. Identity is NOT determined by name alone
(Architecture §2, Principle 2: Identity ≠ Name).
"""

import re
import unicodedata
import logging

import pandas as pd

from src.silver.provenance.transformation_log import TransformationLog

logger = logging.getLogger(__name__)

# Common abbreviations in Indian district names
ABBREVIATIONS = {
    r"\bN\.\s*": "North ",
    r"\bS\.\s*": "South ",
    r"\bE\.\s*": "East ",
    r"\bW\.\s*": "West ",
    r"\bDist\.\s*": "District ",
    r"\bDt\.\s*": "District ",
}


def normalize_name(name: str) -> str:
    """
    Normalize a single name string.

    Steps:
      1. Unicode NFC normalization
      2. Strip whitespace
      3. Collapse multiple spaces
      4. Normalize & to "and"
      5. Remove stray punctuation artifacts
      6. Title case for display consistency

    Returns the normalized name.
    """
    if not isinstance(name, str) or not name.strip():
        return name

    # Unicode NFC
    s = unicodedata.normalize("NFC", name)

    # Strip and collapse spaces
    s = s.strip()
    s = re.sub(r"\s+", " ", s)

    # Normalize ampersand
    s = s.replace(" & ", " and ")
    s = s.replace("&", " and ")

    # Expand common abbreviations
    for pattern, replacement in ABBREVIATIONS.items():
        s = re.sub(pattern, replacement, s, flags=re.IGNORECASE)

    # Remove trailing/leading punctuation artifacts
    s = s.strip(" .,;:")

    # Collapse spaces again after expansions
    s = re.sub(r"\s+", " ", s).strip()

    # Title case (respects "and", "of")
    words = s.split()
    titled = []
    for i, w in enumerate(words):
        if i > 0 and w.lower() in ("and", "of", "the"):
            titled.append(w.lower())
        else:
            titled.append(w.capitalize())
    s = " ".join(titled)

    return s


def standardize_names(
    df: pd.DataFrame,
    name_column: str,
    state_column: str,
    dataset: str,
    year: str,
    layer: str,
    pk_column: str,
    tlog: TransformationLog,
) -> pd.DataFrame:
    """
    Add standardized name columns while preserving originals.

    Adds:
      _silver_name_original    : str — original name as-is from source
      _silver_name_std         : str — standardized name
      _silver_state_original   : str — original state name (if available)
      _silver_state_std        : str — standardized state name
      _silver_name_was_changed : bool — whether standardization altered the name
    """
    df = df.copy()

    # Preserve originals
    df["_silver_name_original"] = df[name_column]

    # Standardize names
    df["_silver_name_std"] = df[name_column].apply(
        lambda x: normalize_name(str(x)) if pd.notna(x) else x
    )

    # Track which names changed
    df["_silver_name_was_changed"] = (
        df["_silver_name_original"] != df["_silver_name_std"]
    )

    changed_count = int(df["_silver_name_was_changed"].sum())

    # State names
    if state_column and state_column in df.columns:
        df["_silver_state_original"] = df[state_column]
        df["_silver_state_std"] = df[state_column].apply(
            lambda x: normalize_name(str(x)) if pd.notna(x) else x
        )
    else:
        df["_silver_state_original"] = None
        df["_silver_state_std"] = None

    if changed_count > 0:
        tlog.log(
            record_id="BATCH",
            dataset=dataset,
            year=year,
            layer=layer,
            field_or_aspect="name",
            transformation_type="NAME_NORMALIZE",
            input_value=f"{changed_count} names required normalization",
            output_value=f"{changed_count} names standardized",
            transformation_rule="Unicode NFC + whitespace + ampersand + title case",
            delta_description=(
                f"Standardized {changed_count}/{len(df)} district names. "
                "Original names preserved in _silver_name_original."
            ),
        )

    logger.info(
        "  [%s/%s] Name standardization: %d/%d names changed",
        dataset, year, changed_count, len(df),
    )

    return df
