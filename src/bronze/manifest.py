"""
Ingest manifest generation and persistence.

Architecture v0.3 §17 (Table Inventory, Bronze Layer):
  "ingest_manifest — One ingested file — File integrity, SHA-256, path"

Every Bronze ingestion run produces a persistent JSON manifest recording
every source file processed, its checksum, record counts, output paths,
and quarantine status.
"""

import json
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import List, Optional


@dataclass
class SourcePKValidationResult:
    """Result of source primary key validation for one dataset layer."""

    dataset: str
    year: str
    layer: str
    pk_column: str
    total_rows: int
    unique_values: int
    null_count: int
    is_unique: bool
    is_complete: bool  # no nulls
    is_valid_pk: bool  # unique AND complete
    issues: List[str] = field(default_factory=list)


@dataclass
class GeometryStatusSummary:
    """Aggregate geometry validity status for one dataset layer."""

    total_geometries: int
    valid_count: int
    invalid_count: int
    empty_count: int
    multipart_count: int
    singlepart_count: int
    original_crs: str
    geometry_types: List[str]


@dataclass
class IngestRecord:
    """One ingested source file or layer."""

    source_file: str
    source_dataset: str
    layer_name: Optional[str]
    file_size_bytes: int
    sha256_checksum: str
    record_count: int
    geometry_count: Optional[int]
    output_path: str
    ingested_at: str
    run_id: str
    pipeline_version: str
    status: str  # SUCCESS | QUARANTINED | ERROR
    pk_validation: Optional[SourcePKValidationResult] = None
    geometry_status: Optional[GeometryStatusSummary] = None
    quarantine_reason: Optional[str] = None
    notes: Optional[str] = None


@dataclass
class IngestManifest:
    """Complete manifest for one Bronze ingestion run."""

    run_id: str
    pipeline_version: str
    started_at: str
    completed_at: Optional[str] = None
    records: List[IngestRecord] = field(default_factory=list)
    pk_validations: List[SourcePKValidationResult] = field(
        default_factory=list
    )

    def add_record(self, record: IngestRecord) -> None:
        self.records.append(record)

    def add_pk_validation(self, result: SourcePKValidationResult) -> None:
        self.pk_validations.append(result)

    def save(self, output_dir: Path) -> Path:
        """Persist manifest as JSON."""
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / f"ingest_manifest_{self.run_id}.json"
        with open(path, "w") as f:
            json.dump(asdict(self), f, indent=2, default=str)
        return path

    def summary(self) -> dict:
        """Return summary statistics."""
        statuses = [r.status for r in self.records]
        return {
            "total_records": len(self.records),
            "success": statuses.count("SUCCESS"),
            "quarantined": statuses.count("QUARANTINED"),
            "errors": statuses.count("ERROR"),
            "total_rows_ingested": sum(
                r.record_count for r in self.records if r.status == "SUCCESS"
            ),
        }
