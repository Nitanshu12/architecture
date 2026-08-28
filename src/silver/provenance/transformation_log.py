"""
Transformation Log — Silver layer provenance tracking.

Architecture v0.3 §15 (Provenance Model):
  "silver.transformation_log records every transformation applied
   between bronze and silver."

Every cleaning, repair, reprojection, or normalization step is logged
BEFORE it is applied. If the system crashes after logging but before
applying, the log shows what was intended.

Transformation types (from Architecture §15):
  GEOMETRY_REPAIR | CRS_TRANSFORM | CAST_TO_MULTI | NAME_NORMALIZE |
  DATE_PARSE | DEDUP_RESOLUTION | SNAP_TO_GRID | AREA_COMPUTE |
  ATTRIBUTE_NORMALIZE
"""

import json
from datetime import datetime, timezone
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import List, Optional


@dataclass
class TransformationRecord:
    """One transformation applied to one record field."""

    record_id: str          # source PK or synthetic row ID
    dataset: str            # e.g., "stanford", "soi"
    year: str               # observation year
    layer: str              # source layer name
    field_or_aspect: str    # e.g., "geometry", "name", "crs", "area"
    transformation_type: str  # from Architecture enum
    input_value: str        # text representation before transformation
    output_value: str       # text representation after transformation
    transformation_rule: str  # algorithm / rule applied
    delta_description: str  # human-readable change description
    applied_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    run_id: str = ""
    pipeline_version: str = ""
    notes: Optional[str] = None


class TransformationLog:
    """
    Accumulates transformation records for a pipeline run.

    Rule: log BEFORE applying the transformation. This ensures
    recoverability if the process is interrupted.
    """

    def __init__(self, run_id: str, pipeline_version: str):
        self.run_id = run_id
        self.pipeline_version = pipeline_version
        self.records: List[TransformationRecord] = []

    def log(
        self,
        record_id: str,
        dataset: str,
        year: str,
        layer: str,
        field_or_aspect: str,
        transformation_type: str,
        input_value: str,
        output_value: str,
        transformation_rule: str,
        delta_description: str,
        notes: Optional[str] = None,
    ) -> TransformationRecord:
        """Log a transformation BEFORE applying it."""
        rec = TransformationRecord(
            record_id=record_id,
            dataset=dataset,
            year=year,
            layer=layer,
            field_or_aspect=field_or_aspect,
            transformation_type=transformation_type,
            input_value=input_value,
            output_value=output_value,
            transformation_rule=transformation_rule,
            delta_description=delta_description,
            run_id=self.run_id,
            pipeline_version=self.pipeline_version,
            notes=notes,
        )
        self.records.append(rec)
        return rec

    def save(self, output_dir: Path) -> Path:
        """Persist transformation log as JSON."""
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / f"transformation_log_{self.run_id}.json"
        with open(path, "w") as f:
            json.dump(
                [asdict(r) for r in self.records],
                f,
                indent=2,
                default=str,
            )
        return path

    def summary(self) -> dict:
        """Return counts by transformation type."""
        counts: dict = {}
        for r in self.records:
            counts[r.transformation_type] = counts.get(
                r.transformation_type, 0
            ) + 1
        return {
            "total_transformations": len(self.records),
            "by_type": counts,
        }
