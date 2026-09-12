"""P1 before/after comparability and material-change evaluation."""

from __future__ import annotations

from typing import Any

from common.schema import SCHEMA_VERSION, utc_now


RECALL_TOLERANCE = 0.002
RELATIVE_TOLERANCE = 0.05
COMPARABILITY_KEYS = (
    "dataset", "rows", "metric", "opclass", "ground_truth_id", "k",
    "query_count", "repeat", "cache_mode", "client_mode", "planner_mode",
    "postgresql", "pgvector", "machine",
)


def compare_validation(
    recommendation: dict[str, Any], before: dict[str, Any], after: dict[str, Any],
    before_context: dict[str, Any], after_context: dict[str, Any],
    slo: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Compare a validation rerun without claiming statistical significance."""
    mismatches = {
        key: {"before": before_context.get(key), "after": after_context.get(key)}
        for key in COMPARABILITY_KEYS
        if before_context.get(key) != after_context.get(key)
    }
    before_quality, after_quality = before.get("quality_metrics", {}), after.get("quality_metrics", {})
    before_build, after_build = before.get("build_metrics", {}), after.get("build_metrics", {})
    before_resource, after_resource = before.get("resource_metrics", {}), after.get("resource_metrics", {})
    deltas = {
        "recall_absolute": None,
        "p95_relative": None,
        "build_time_relative": None,
        "peak_memory_relative": None,
    }
    pairs = (
        ("recall_absolute", before_quality.get("recall_at_k"), after_quality.get("recall_at_k"), "absolute"),
        ("p95_relative", before_quality.get("p95_ms"), after_quality.get("p95_ms"), "relative"),
        ("build_time_relative", before_build.get("build_seconds"), after_build.get("build_seconds"), "relative"),
        ("peak_memory_relative", before_resource.get("peak_memory_bytes"), after_resource.get("peak_memory_bytes"), "relative"),
    )
    for name, left, right, kind in pairs:
        if left is not None and right is not None:
            deltas[name] = right - left if kind == "absolute" else (right - left) / left if left else None

    reasons = []
    if mismatches:
        status = "not_comparable"
        reasons.append("workload or environment identity changed")
    elif not after.get("validity", {}).get("eligible_for_comparison"):
        status = "validation_failed"
        reasons.extend(after.get("validity", {}).get("reasons") or ["after candidate is invalid"])
    else:
        slo = slo or {}
        recall = after_quality.get("recall_at_k")
        p95 = after_quality.get("p95_ms")
        p99 = after_quality.get("p99_ms")
        slo_pass = (
            (slo.get("min_recall") is None or (recall is not None and recall + RECALL_TOLERANCE >= slo["min_recall"]))
            and (slo.get("max_p95_ms") is None or (p95 is not None and p95 <= slo["max_p95_ms"] * (1 + RELATIVE_TOLERANCE)))
            and (slo.get("max_p99_ms") is None or (p99 is not None and p99 <= slo["max_p99_ms"] * (1 + RELATIVE_TOLERANCE)))
        )
        action = recommendation.get("action")
        if action == "set_ivfflat_probes":
            expected_direction = deltas["recall_absolute"] is not None and deltas["recall_absolute"] >= -RECALL_TOLERANCE
        elif action == "prefer_lower_probes_at_diminishing_return":
            expected_direction = deltas["p95_relative"] is not None and deltas["p95_relative"] <= RELATIVE_TOLERANCE
        else:
            expected_direction = after_build.get("database_status") == "success"
        status = "validated" if slo_pass and expected_direction else "validation_failed"
        if not slo_pass:
            reasons.append("after candidate did not satisfy configured SLO guardrails")
        if not expected_direction:
            reasons.append("expected material-change direction was not observed")
    return {
        "schema_version": SCHEMA_VERSION,
        "p1_schema_version": "1.0",
        "diagnosis_id": recommendation.get("diagnosis_id"),
        "recommendation_id": recommendation.get("recommendation_id"),
        "timestamp_utc": utc_now(),
        "status": status,
        "before_candidate_id": before.get("candidate_id"),
        "after_candidate_id": after.get("candidate_id"),
        "comparability": {"status": "mismatch" if mismatches else "comparable", "mismatches": mismatches},
        "material_change_tolerance": {
            "recall_absolute": RECALL_TOLERANCE,
            "latency_build_memory_relative": RELATIVE_TOLERANCE,
        },
        "deltas": deltas,
        "reasons": reasons,
    }
