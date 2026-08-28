"""
Canonical Key Registry — persistent, immutable CK allocation.

Architecture v0.3 §4 (Canonical Key Model):
  "FUNCTION get_or_create_ck(source_pk, source_dataset_id)
   BEGIN SERIALIZABLE TRANSACTION;
   1. existing_ck → IF NOT NULL: RETURN existing_ck
   3. new_seq = NEXTVAL; ck = 'IND-' || LPAD(new_seq, 6, '0')
   5. RETURN new_ck"

MANDATORY SAFETY RULES:
  1. CKs are persistent and immutable after assignment.
  2. CK assignment must never depend on processing order.
  3. Re-running must not generate different CKs.
  4. Duplicate CKs are forbidden.
  5. Silent reassignment is forbidden.
"""

import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict

logger = logging.getLogger(__name__)

CK_PREFIX = "IND-"
CK_WIDTH = 6  # IND-000001


class CKRegistry:
    """
    Persistent Canonical Key registry.

    Storage: JSON file at data/gold/core/ck_registry.json

    The registry tracks:
      - next_sequence: next CK number to allocate
      - entries: dict of CK → metadata
      - allocation_log: ordered list of CK allocations

    Thread safety: single-process pipeline; no concurrent writes.
    """

    def __init__(self, registry_path: Path):
        self.registry_path = registry_path
        self.registry_path.parent.mkdir(parents=True, exist_ok=True)
        self._data = self._load()

    def _load(self) -> dict:
        """Load existing registry or create empty."""
        if self.registry_path.exists():
            with open(self.registry_path) as f:
                data = json.load(f)
            logger.info(
                "Loaded CK registry: %d entries, next_seq=%d",
                len(data.get("entries", {})),
                data.get("next_sequence", 1),
            )
            return data
        return {
            "next_sequence": 1,
            "entries": {},
            "allocation_log": [],
        }

    def save(self) -> None:
        """Persist registry to disk."""
        with open(self.registry_path, "w") as f:
            json.dump(self._data, f, indent=2, default=str)

    @property
    def size(self) -> int:
        return len(self._data["entries"])

    def _format_ck(self, seq: int) -> str:
        return f"{CK_PREFIX}{seq:0{CK_WIDTH}d}"

    def allocate_ck(
        self,
        established_year: Optional[int] = None,
        established_precision: str = "YEAR",
        display_name: Optional[str] = None,
        state: Optional[str] = None,
        allocation_reason: str = "",
        run_id: str = "",
    ) -> str:
        """
        Allocate a new CK. This is an internal method — external callers
        should use get_or_create_ck via the mapping layer.

        Returns the new CK string (e.g., 'IND-000001').
        """
        seq = self._data["next_sequence"]
        ck = self._format_ck(seq)

        if ck in self._data["entries"]:
            raise ValueError(f"CK collision: {ck} already exists")

        entry = {
            "canonical_key": ck,
            "sequence": seq,
            "established_year": established_year,
            "established_precision": established_precision,
            "display_name": display_name,
            "state": state,
            "is_active": True,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "allocation_reason": allocation_reason,
            "run_id": run_id,
        }

        self._data["entries"][ck] = entry
        self._data["next_sequence"] = seq + 1
        self._data["allocation_log"].append({
            "ck": ck,
            "action": "ALLOCATE",
            "reason": allocation_reason,
            "timestamp": entry["created_at"],
            "run_id": run_id,
        })

        return ck

    def get_ck(self, ck: str) -> Optional[dict]:
        """Look up a CK entry. Returns None if not found."""
        return self._data["entries"].get(ck)

    def ck_exists(self, ck: str) -> bool:
        return ck in self._data["entries"]

    def all_entries(self) -> Dict[str, dict]:
        return dict(self._data["entries"])

    def summary(self) -> dict:
        entries = self._data["entries"]
        return {
            "total_cks": len(entries),
            "next_sequence": self._data["next_sequence"],
            "active": sum(1 for e in entries.values() if e.get("is_active")),
        }
