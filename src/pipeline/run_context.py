"""
Pipeline run context — tracks execution metadata for provenance.

Every pipeline execution creates a RunContext that carries:
- A unique run_id (UUID4)
- Pipeline version (tracks ARCHITECTURE.md version alignment)
- Stage identifier
- Start/completion timestamps

This satisfies Architecture v0.3 §15 (Provenance Model):
  "pipeline_run_id UUID NOT NULL" and "pipeline_version TEXT NOT NULL"
  are MANDATORY on all gold-layer records.
"""

import uuid
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class RunContext:
    """Immutable context for a single pipeline execution stage."""

    run_id: uuid.UUID = field(default_factory=uuid.uuid4)
    pipeline_version: str = "0.3.0"
    stage: str = "bronze"
    started_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    completed_at: Optional[datetime] = None

    def complete(self) -> None:
        """Mark this run as completed with current UTC timestamp."""
        self.completed_at = datetime.now(timezone.utc)

    def to_dict(self) -> dict:
        """Serialize to dictionary for manifest/log inclusion."""
        return {
            "run_id": str(self.run_id),
            "pipeline_version": self.pipeline_version,
            "stage": self.stage,
            "started_at": self.started_at.isoformat(),
            "completed_at": (
                self.completed_at.isoformat() if self.completed_at else None
            ),
        }
