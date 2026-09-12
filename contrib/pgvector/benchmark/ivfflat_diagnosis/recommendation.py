"""P1 feasibility filtering, Pareto comparison, and proposed recommendations."""

from __future__ import annotations

from typing import Any

from common.schema import SCHEMA_VERSION, utc_now


PARETO_DIMENSIONS = {
    "recall_at_k": "maximize",
    "p95_ms": "minimize",
    "build_seconds": "minimize",
    "peak_memory_bytes": "minimize",
    "index_bytes": "minimize",
}


def _values(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "recall_at_k": candidate["quality_metrics"].get("recall_at_k"),
        "p95_ms": candidate["quality_metrics"].get("p95_ms"),
        "build_seconds": candidate["build_metrics"].get("build_seconds"),
        "peak_memory_bytes": candidate["resource_metrics"].get("peak_memory_bytes"),
        "index_bytes": candidate["build_metrics"].get("index_bytes"),
    }


def evaluate_feasibility(candidate: dict[str, Any], slo: dict[str, Any] | None) -> dict[str, Any]:
    """Apply hard validity checks followed by only the SLOs the user supplied."""
    reasons = list(candidate["validity"].get("reasons") or [])
    if not candidate["validity"].get("eligible_for_comparison"):
        status = (
            "infeasible"
            if candidate["validity"].get("status") == "infeasible"
            else "invalid"
        )
        return {"status": status, "slo_status": "not_evaluated", "reasons": reasons}
    slo = slo or {}
    checks = {
        "min_recall": (candidate["quality_metrics"].get("recall_at_k"), lambda value, limit: value >= limit),
        "max_p95_ms": (candidate["quality_metrics"].get("p95_ms"), lambda value, limit: value <= limit),
        "max_p99_ms": (candidate["quality_metrics"].get("p99_ms"), lambda value, limit: value <= limit),
        "max_build_seconds": (candidate["build_metrics"].get("build_seconds"), lambda value, limit: value <= limit),
        "max_build_memory_bytes": (candidate["resource_metrics"].get("peak_memory_bytes"), lambda value, limit: value <= limit),
        "max_index_bytes": (candidate["build_metrics"].get("index_bytes"), lambda value, limit: value <= limit),
    }
    supplied = 0
    unknown = []
    failed = []
    results = {}
    for name, (value, predicate) in checks.items():
        limit = slo.get(name)
        if limit is None:
            continue
        supplied += 1
        if value is None:
            results[name] = "unknown"
            unknown.append(f"{name} evidence is unavailable")
        elif predicate(value, limit):
            results[name] = "pass"
        else:
            results[name] = "fail"
            failed.append(f"{name} failed: observed={value}, limit={limit}")
    if failed:
        status = "slo_rejected"
    elif unknown:
        status = "slo_unknown"
    else:
        status = "feasible"
    return {
        "status": status,
        "slo_status": "not_configured" if supplied == 0 else "fail" if failed else "unknown" if unknown else "pass",
        "checks": results,
        "reasons": reasons + failed + unknown,
    }


def _dominates(left: dict[str, Any], right: dict[str, Any], dimensions: list[str]) -> bool:
    left_values, right_values = _values(left), _values(right)
    no_worse = True
    strictly_better = False
    for dimension in dimensions:
        lv, rv = left_values[dimension], right_values[dimension]
        if PARETO_DIMENSIONS[dimension] == "maximize":
            no_worse &= lv >= rv
            strictly_better |= lv > rv
        else:
            no_worse &= lv <= rv
            strictly_better |= lv < rv
    return no_worse and strictly_better


def build_pareto(candidates: list[dict[str, Any]], slo: dict[str, Any] | None) -> dict[str, Any]:
    feasibility = {
        item["candidate_id"]: evaluate_feasibility(item, slo) for item in candidates
    }
    eligible = [
        item for item in candidates
        if feasibility[item["candidate_id"]]["status"] == "feasible"
    ]
    dimensions = [
        name for name in PARETO_DIMENSIONS
        if eligible and all(_values(item).get(name) is not None for item in eligible)
    ]
    memory_sources = {
        item["resource_metrics"].get("peak_memory_source") for item in eligible
        if item["resource_metrics"].get("peak_memory_bytes") is not None
    }
    incomparable = []
    if "peak_memory_bytes" in dimensions and len(memory_sources) != 1:
        dimensions.remove("peak_memory_bytes")
        incomparable.append({
            "dimension": "peak_memory_bytes",
            "reason": "candidate peak-memory sources/semantics differ",
        })
    for name in PARETO_DIMENSIONS:
        if name not in dimensions and name != "peak_memory_bytes":
            incomparable.append({
                "dimension": name,
                "reason": "one or more feasible candidates lack the metric",
            })

    rows = []
    for candidate in candidates:
        candidate_id = candidate["candidate_id"]
        feasible = feasibility[candidate_id]
        dominators = []
        if feasible["status"] == "feasible" and dimensions:
            dominators = [
                other["candidate_id"] for other in eligible
                if other is not candidate and _dominates(other, candidate, dimensions)
            ]
        rows.append({
            "candidate_id": candidate_id,
            "lists": candidate.get("lists"),
            "probes": candidate.get("probes"),
            "feasibility": feasible,
            "pareto_status": (
                "not_comparable" if feasible["status"] != "feasible" or not dimensions
                else "dominated" if dominators else "pareto"
            ),
            "dominated_by": dominators,
            "values": _values(candidate),
            "peak_memory_source": candidate["resource_metrics"].get("peak_memory_source"),
        })
    return {
        "schema_version": SCHEMA_VERSION,
        "p1_schema_version": "1.0",
        "diagnosis_id": candidates[0]["diagnosis_id"] if candidates else None,
        "timestamp_utc": utc_now(),
        "source": "p1_pareto",
        "availability": "available" if candidates else "unavailable",
        "reason": None if candidates else "no candidates",
        "dimensions": {name: PARETO_DIMENSIONS[name] for name in dimensions},
        "incomparable_dimensions": incomparable,
        "slo": slo or {},
        "candidates": rows,
        "pareto_candidate_ids": [row["candidate_id"] for row in rows if row["pareto_status"] == "pareto"],
        "best_candidate_id": None,
        "best_candidate_reason": "Pareto produces a frontier, not a globally best configuration.",
    }


def build_recommendations(
    candidates: list[dict[str, Any]], findings: list[dict[str, Any]],
    pareto: dict[str, Any], manifest: dict[str, Any],
) -> list[dict[str, Any]]:
    """Generate only evidence-backed proposed actions from triggered findings."""
    by_id = {item["candidate_id"]: item for item in candidates}
    recommendations = []
    seen = set()
    for finding in findings:
        if finding.get("status") != "triggered":
            continue
        rule = finding.get("rule_id")
        related = [by_id[item] for item in finding.get("candidate_ids", []) if item in by_id]
        if not related:
            continue
        current = related[0]
        action = None
        recommended: dict[str, Any] = {}
        expected = ""
        cost = ""
        if rule == "memory_hard_failure":
            estimate = current["build_metrics"].get("estimated_checked_memory_bytes")
            limit = current["build_metrics"].get("memory_limit_bytes")
            if estimate is None or limit is None:
                continue
            action = "increase_maintenance_work_mem_or_reduce_lists"
            recommended = {
                "minimum_checked_memory_bytes": estimate,
                "lists": current["lists"],
                "probes": current["probes"],
            }
            expected = "Make the tested IVFFlat build feasible for pgvector checked allocations."
            cost = "Higher session build-memory allowance, or a separate lower-lists experiment; total process RSS is not bounded by this value."
        elif rule == "probes_improve_recall":
            details = finding.get("details") or {}
            target = details.get("recommended_probes")
            target_candidate = next((item for item in related if item.get("probes") == target), None)
            if target_candidate is None:
                continue
            action = "set_ivfflat_probes"
            recommended = {"lists": target_candidate["lists"], "probes": target}
            expected = f"Reach the configured minimum Recall using the smallest tested qualifying probes value ({target})."
            cost = "More lists scanned per query; use measured P95/P99 as the observed latency cost."
        elif rule == "probes_diminishing_returns":
            details = finding.get("details") or {}
            lower = details.get("lower_probes")
            higher = details.get("higher_probes")
            higher_candidate = next(
                (item for item in related if item.get("probes") == higher), None
            )
            if lower is None or higher_candidate is None:
                continue
            current = higher_candidate
            action = "prefer_lower_probes_at_diminishing_return"
            recommended = {"lists": current["lists"], "probes": lower}
            expected = "Avoid a measured P95 increase whose Recall gain is below the P1-v1 material threshold."
            cost = "Recall may decrease by the measured delta; validate against the configured Recall SLO."
        else:
            # Structure-only findings intentionally do not imply a lists change.
            continue
        key = (action, tuple(sorted(recommended.items())))
        if key in seen:
            continue
        seen.add(key)
        recommendations.append({
            "schema_version": SCHEMA_VERSION,
            "p1_schema_version": "1.0",
            "diagnosis_id": current["diagnosis_id"],
            "recommendation_id": f"recommendation-{len(recommendations) + 1:03d}",
            "timestamp_utc": utc_now(),
            "status": "proposed",
            "action": action,
            "current": {"lists": current["lists"], "probes": current["probes"]},
            "recommended": recommended,
            "candidate_ids": finding["candidate_ids"],
            "finding_ids": [finding["finding_id"]],
            "evidence_ids": finding["evidence_ids"],
            "expected_effect": expected,
            "cost": cost,
            "scope": {
                "dataset": manifest.get("dataset"), "metric": manifest.get("metric"),
                "rows": current.get("actual_rows"), "cache": manifest.get("cache"),
                "client": manifest.get("client"),
            },
            "verification_workload": {
                key: manifest.get(key) for key in (
                    "dataset", "metric", "k", "query_count", "repeat",
                    "cache", "client", "pgvector", "postgresql",
                )
            } | {"rows": current.get("actual_rows")},
        })
    return recommendations
