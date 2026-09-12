"""P1 joint diagnosis orchestration."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from common.schema import SCHEMA_VERSION, utc_now
from .model import build_candidates
from .rules import DEFAULT_THRESHOLDS, RULESET_VERSION, evaluate_rules


def diagnose(
    diagnosis_id: str,
    builds: list[tuple[dict[str, Any], Path]],
    qualities: list[tuple[dict[str, Any], Path]],
    manifest: dict[str, Any],
    thresholds: dict[str, float] | None = None,
) -> dict[str, Any]:
    candidates, evidence = build_candidates(diagnosis_id, builds, qualities, manifest)
    findings = evaluate_rules(candidates, manifest.get("slo"), thresholds)
    return {
        "schema_version": SCHEMA_VERSION,
        "p1_schema_version": "1.0",
        "diagnosis_id": diagnosis_id,
        "timestamp_utc": utc_now(),
        "source": "p1_joint_diagnosis",
        "availability": "available",
        "reason": None,
        "ruleset_version": RULESET_VERSION,
        "thresholds": {**DEFAULT_THRESHOLDS, **(thresholds or {})},
        "candidates": candidates,
        "evidence": evidence,
        "findings": findings,
    }
