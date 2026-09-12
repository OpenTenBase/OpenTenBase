"""Shared identities and record helpers for IVFFlat diagnosis runs."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import uuid4


SCHEMA_VERSION = "1.0"


class Availability(str, Enum):
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    NOT_SAMPLED = "not_sampled"
    UNSUPPORTED = "unsupported"


class RunStatus(str, Enum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    COLLECTOR_INTERRUPTED = "collector_interrupted"
    SKIPPED = "skipped"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_run_id(prefix: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{prefix}-{timestamp}-{uuid4().hex[:8]}"


def short_run_id(run_id: str) -> str:
    """Return the stable, human-friendly suffix used only in directory names."""
    return run_id.rsplit("-", 1)[-1][:8]


@dataclass(frozen=True)
class DiagnosisContext:
    diagnosis_id: str
    output: Path
    schema_version: str = SCHEMA_VERSION

    def record(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "diagnosis_id": self.diagnosis_id,
        }


@dataclass(frozen=True)
class BuildContext:
    diagnosis_id: str
    build_id: str
    output: Path
    dataset: str
    metric: str
    requested_rows: int
    actual_rows: int
    eligible_vectors: int
    dimension: int
    lists: int
    schema_version: str = SCHEMA_VERSION

    def record(self) -> dict[str, Any]:
        values = asdict(self)
        values["output"] = str(self.output)
        # Compatibility alias: rows always means the observed table row count.
        values["rows"] = self.actual_rows
        return values

    @property
    def rows(self) -> int:
        """Compatibility alias. Never expose requested_rows as observed rows."""
        return self.actual_rows


@dataclass(frozen=True)
class QualityContext:
    diagnosis_id: str
    build_id: str
    quality_run_id: str
    output: Path
    lists: int
    probes: int
    schema_version: str = SCHEMA_VERSION

    def record(self) -> dict[str, Any]:
        values = asdict(self)
        values["output"] = str(self.output)
        return values


def base_record(diagnosis_id: str, *, build_id: str | None = None,
                quality_run_id: str | None = None, source: str,
                availability: Availability | str = Availability.AVAILABLE,
                reason: str | None = None, timestamp_utc: str | None = None) -> dict[str, Any]:
    record: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "diagnosis_id": diagnosis_id,
        "timestamp_utc": timestamp_utc or utc_now(),
        "source": source,
        "availability": availability.value if isinstance(availability, Availability) else availability,
        "reason": reason,
    }
    if build_id is not None:
        record["build_id"] = build_id
    if quality_run_id is not None:
        record["quality_run_id"] = quality_run_id
    return record


def metric_value(value: Any, unit: str, source: str, *,
                 availability: Availability | str = Availability.AVAILABLE,
                 reason: str | None = None, aggregation: str = "absolute",
                 attribution_scope: str = "build_run") -> dict[str, Any]:
    status = availability.value if isinstance(availability, Availability) else availability
    if status != Availability.AVAILABLE.value:
        value = None
    return {
        "value": value,
        "unit": unit,
        "source": source,
        "availability": status,
        "reason": reason,
        "aggregation": aggregation,
        "attribution_scope": attribution_scope,
    }


def validate_identity(record: dict[str, Any], *, require_build: bool = False,
                      require_quality: bool = False) -> None:
    required = {"schema_version", "diagnosis_id", "timestamp_utc", "source",
                "availability", "reason"}
    if require_build:
        required.add("build_id")
    if require_quality:
        required.update(("build_id", "quality_run_id"))
    missing = sorted(required.difference(record))
    if missing:
        raise ValueError(f"record is missing required fields: {', '.join(missing)}")
