"""
Source PK → CK Mapping — provenance-tracked identity assignments.

Architecture v0.3 §17 (Silver Layer):
  "source_pk_to_ck_mapping — One Source PK → CK assignment"

MANDATORY SAFETY RULES:
  1. Existing mappings must never be silently overwritten.
  2. Corrections are explicit, auditable, provenance-preserving.
  3. Every mapping has evidence and provenance.
  4. get_or_create guarantees deterministic CK allocation.
"""

import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List, Dict, Tuple
from dataclasses import dataclass, field, asdict

from src.identity.ck_registry import CKRegistry

logger = logging.getLogger(__name__)


# Match statuses
MATCHED = "MATCHED"
AMBIGUOUS = "AMBIGUOUS"
UNMATCHED = "UNMATCHED"
QUARANTINED = "QUARANTINED"
PENDING_OD01 = "PENDING_OD01"

# Match methods
ANCHOR_YEAR = "ANCHOR_YEAR"
NAME_STATE_EXACT = "NAME_STATE_EXACT"
NAME_STATE_RENAME = "NAME_STATE_RENAME"
EVENT_SUPPORTED = "EVENT_SUPPORTED"
MULTI_EVIDENCE = "MULTI_EVIDENCE"


@dataclass
class MappingRecord:
    """One source record → CK assignment."""

    source_dataset: str
    source_year: str
    source_layer: str
    source_pk: str
    canonical_key: Optional[str]
    district_name: str
    state: Optional[str]
    match_method: str
    match_score: float
    match_status: str
    evidence: List[str] = field(default_factory=list)
    quarantine_reason: Optional[str] = None
    pipeline_run_id: str = ""
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    correction_reference: Optional[str] = None


class SourceToCKMapping:
    """
    Manages the source PK → CK mapping with full provenance.

    Storage: JSON at data/gold/core/source_pk_to_ck_mapping.json

    Key: (source_dataset, source_year, source_pk) → MappingRecord
    """

    def __init__(self, mapping_path: Path, registry: CKRegistry):
        self.mapping_path = mapping_path
        self.mapping_path.parent.mkdir(parents=True, exist_ok=True)
        self.registry = registry
        self._mappings: Dict[str, MappingRecord] = {}
        self._quarantine: List[MappingRecord] = []
        self._load()

    def _make_key(self, dataset: str, year: str, source_pk: str) -> str:
        return f"{dataset}|{year}|{source_pk}"

    def _load(self) -> None:
        """Load existing mappings."""
        if self.mapping_path.exists():
            with open(self.mapping_path) as f:
                data = json.load(f)
            for rec_dict in data.get("mappings", []):
                rec = MappingRecord(**rec_dict)
                key = self._make_key(rec.source_dataset, rec.source_year, rec.source_pk)
                self._mappings[key] = rec
            for rec_dict in data.get("quarantine", []):
                self._quarantine.append(MappingRecord(**rec_dict))
            logger.info(
                "Loaded %d mappings, %d quarantined",
                len(self._mappings), len(self._quarantine),
            )

    def save(self) -> None:
        """Persist mappings to disk."""
        data = {
            "mappings": [asdict(r) for r in self._mappings.values()],
            "quarantine": [asdict(r) for r in self._quarantine],
        }
        with open(self.mapping_path, "w") as f:
            json.dump(data, f, indent=2, default=str)

    def get_existing(
        self, dataset: str, year: str, source_pk: str,
    ) -> Optional[MappingRecord]:
        """Look up existing mapping. Returns None if not found."""
        key = self._make_key(dataset, year, source_pk)
        return self._mappings.get(key)

    def get_or_create_ck(
        self,
        source_dataset: str,
        source_year: str,
        source_layer: str,
        source_pk: str,
        district_name: str,
        state: Optional[str],
        match_method: str,
        match_score: float,
        match_status: str,
        evidence: List[str],
        existing_ck: Optional[str] = None,
        quarantine_reason: Optional[str] = None,
        run_id: str = "",
    ) -> MappingRecord:
        """
        Get existing CK or create new one for a source record.

        If a mapping already exists for (dataset, year, source_pk):
          → RETURN existing mapping (never overwrite).

        If match_status is QUARANTINED or AMBIGUOUS or PENDING_OD01:
          → Add to quarantine, do NOT allocate CK.

        If existing_ck is provided (matched to existing identity):
          → Map to that CK.

        Otherwise:
          → Allocate new CK from registry.
        """
        key = self._make_key(source_dataset, source_year, source_pk)

        # Rule: never overwrite existing mappings
        if key in self._mappings:
            existing = self._mappings[key]
            logger.debug(
                "Existing mapping found: %s → %s", key, existing.canonical_key,
            )
            return existing

        # Quarantine cases — no CK allocated
        if match_status in (QUARANTINED, AMBIGUOUS, PENDING_OD01, UNMATCHED):
            rec = MappingRecord(
                source_dataset=source_dataset,
                source_year=source_year,
                source_layer=source_layer,
                source_pk=source_pk,
                canonical_key=None,
                district_name=district_name,
                state=state,
                match_method=match_method,
                match_score=match_score,
                match_status=match_status,
                evidence=evidence,
                quarantine_reason=quarantine_reason,
                pipeline_run_id=run_id,
            )
            self._quarantine.append(rec)
            # Also store in mappings to prevent re-processing
            self._mappings[key] = rec
            return rec

        # Matched to existing CK
        if existing_ck is not None:
            if not self.registry.ck_exists(existing_ck):
                raise ValueError(
                    f"Cannot map to non-existent CK: {existing_ck}"
                )
            rec = MappingRecord(
                source_dataset=source_dataset,
                source_year=source_year,
                source_layer=source_layer,
                source_pk=source_pk,
                canonical_key=existing_ck,
                district_name=district_name,
                state=state,
                match_method=match_method,
                match_score=match_score,
                match_status=MATCHED,
                evidence=evidence,
                pipeline_run_id=run_id,
            )
            self._mappings[key] = rec
            return rec

        # Allocate new CK
        ck = self.registry.allocate_ck(
            established_year=int(source_year) if source_year.isdigit() else None,
            established_precision="YEAR",
            display_name=district_name,
            state=state,
            allocation_reason=f"{match_method}: {', '.join(evidence)}",
            run_id=run_id,
        )
        rec = MappingRecord(
            source_dataset=source_dataset,
            source_year=source_year,
            source_layer=source_layer,
            source_pk=source_pk,
            canonical_key=ck,
            district_name=district_name,
            state=state,
            match_method=match_method,
            match_score=match_score,
            match_status=MATCHED,
            evidence=evidence,
            pipeline_run_id=run_id,
        )
        self._mappings[key] = rec
        return rec

    def get_ck_pool(self) -> Dict[str, dict]:
        """
        Build a lookup of CK → identity info from all MATCHED mappings.

        Returns dict keyed by (standardized_name, standardized_state) → CK
        for use in matching.
        """
        pool: Dict[str, dict] = {}
        for rec in self._mappings.values():
            if rec.match_status == MATCHED and rec.canonical_key:
                pool[rec.canonical_key] = {
                    "ck": rec.canonical_key,
                    "name": rec.district_name,
                    "state": rec.state,
                    "source_dataset": rec.source_dataset,
                    "source_year": rec.source_year,
                }
        return pool

    @property
    def all_mappings(self) -> List[MappingRecord]:
        return list(self._mappings.values())

    @property
    def quarantined(self) -> List[MappingRecord]:
        return [r for r in self._mappings.values()
                if r.match_status in (QUARANTINED, AMBIGUOUS, PENDING_OD01, UNMATCHED)]

    @property
    def matched(self) -> List[MappingRecord]:
        return [r for r in self._mappings.values()
                if r.match_status == MATCHED and r.canonical_key is not None]

    def summary(self) -> dict:
        statuses: Dict[str, int] = {}
        methods: Dict[str, int] = {}
        for r in self._mappings.values():
            statuses[r.match_status] = statuses.get(r.match_status, 0) + 1
            methods[r.match_method] = methods.get(r.match_method, 0) + 1
        return {
            "total_mappings": len(self._mappings),
            "by_status": statuses,
            "by_method": methods,
            "quarantine_count": len(self.quarantined),
        }

    def promote_quarantined(
        self,
        source_dataset: str,
        source_year: str,
        source_pk: str,
        canonical_key: str,
        district_name: str,
        state: Optional[str],
        match_method: str,
        match_score: float,
        evidence: List[str],
        run_id: str = "",
    ) -> Optional[MappingRecord]:
        """
        Promote a quarantined/unmatched record to MATCHED with new evidence.

        This is an EXPLICIT, AUDITABLE correction:
          - The old mapping status is preserved in correction_reference
          - The new mapping has full evidence and provenance
          - Only works if the record is currently UNMATCHED/QUARANTINED

        Returns the updated MappingRecord, or None if not promotable.
        """
        key = self._make_key(source_dataset, source_year, source_pk)

        if key not in self._mappings:
            return None

        existing = self._mappings[key]

        # Only promote quarantined/unmatched records
        if existing.match_status not in (UNMATCHED, QUARANTINED):
            return existing  # already matched — return as-is

        if not self.registry.ck_exists(canonical_key):
            raise ValueError(f"Cannot promote to non-existent CK: {canonical_key}")

        # Create corrected mapping with audit trail
        corrected = MappingRecord(
            source_dataset=source_dataset,
            source_year=source_year,
            source_layer=existing.source_layer,
            source_pk=source_pk,
            canonical_key=canonical_key,
            district_name=district_name,
            state=state,
            match_method=match_method,
            match_score=match_score,
            match_status=MATCHED,
            evidence=evidence,
            pipeline_run_id=run_id,
            correction_reference=(
                f"PROMOTED from {existing.match_status} "
                f"(prev_method={existing.match_method}, "
                f"prev_run={existing.pipeline_run_id})"
            ),
        )

        self._mappings[key] = corrected
        return corrected

    def promote_quarantined_new_ck(
        self,
        source_dataset: str,
        source_year: str,
        source_pk: str,
        district_name: str,
        state: Optional[str],
        match_method: str,
        match_score: float,
        evidence: List[str],
        run_id: str = "",
    ) -> Optional[MappingRecord]:
        """
        Promote a quarantined record by allocating a NEW CK.

        Used in Tier 3 when formation event evidence supports a new identity.
        """
        key = self._make_key(source_dataset, source_year, source_pk)

        if key not in self._mappings:
            return None

        existing = self._mappings[key]

        if existing.match_status not in (UNMATCHED, QUARANTINED):
            return existing

        # Allocate new CK
        ck = self.registry.allocate_ck(
            established_year=int(source_year) if source_year.isdigit() else None,
            established_precision="YEAR",
            display_name=district_name,
            state=state,
            allocation_reason=f"{match_method}: {', '.join(evidence[:3])}",
            run_id=run_id,
        )

        corrected = MappingRecord(
            source_dataset=source_dataset,
            source_year=source_year,
            source_layer=existing.source_layer,
            source_pk=source_pk,
            canonical_key=ck,
            district_name=district_name,
            state=state,
            match_method=match_method,
            match_score=match_score,
            match_status=MATCHED,
            evidence=evidence,
            pipeline_run_id=run_id,
            correction_reference=(
                f"PROMOTED_NEW_CK from {existing.match_status} "
                f"(prev_method={existing.match_method})"
            ),
        )

        self._mappings[key] = corrected
        return corrected

