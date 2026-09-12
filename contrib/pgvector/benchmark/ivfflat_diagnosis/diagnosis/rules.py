"""Explainable P1-v1 rules over Candidate facts."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from common.schema import SCHEMA_VERSION, utc_now


RULESET_VERSION = "p1-v1"
DEFAULT_THRESHOLDS = {
    "moderate_cv": 0.5,
    "moderate_gini": 0.30,
    "moderate_p95_p50": 2.0,
    "severe_cv": 1.0,
    "severe_gini": 0.50,
    "empty_ratio": 0.05,
    "recall_material_change": 0.002,
    "latency_relative_change": 0.20,
}


def _finding(
    ordinal: int, rule_id: str, status: str, candidates: list[dict[str, Any]],
    summary: str, likely_cause: str, confidence: str, limitations: list[str],
    evidence_metrics: tuple[str, ...], extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    evidence_ids = []
    for candidate in candidates:
        for metric in evidence_metrics:
            evidence_id = candidate.get("evidence_ids", {}).get(metric)
            if evidence_id and evidence_id not in evidence_ids:
                evidence_ids.append(evidence_id)
    result = {
        "schema_version": SCHEMA_VERSION,
        "p1_schema_version": "1.0",
        "ruleset_version": RULESET_VERSION,
        "diagnosis_id": candidates[0]["diagnosis_id"],
        "finding_id": f"finding-{ordinal:03d}-{rule_id}",
        "timestamp_utc": utc_now(),
        "rule_id": rule_id,
        "scope": "candidate" if len(candidates) == 1 else "candidate_comparison",
        "candidate_ids": [item["candidate_id"] for item in candidates],
        "status": status,
        "evidence_ids": evidence_ids,
        "summary": summary,
        "likely_cause": likely_cause,
        "confidence": confidence,
        "limitations": limitations,
    }
    if extra:
        result["details"] = extra
    return result


def evaluate_rules(
    candidates: list[dict[str, Any]], slo: dict[str, Any] | None = None,
    thresholds: dict[str, float] | None = None,
) -> list[dict[str, Any]]:
    """Evaluate structure, memory, worker, probes, and joint-quality rules."""
    if not candidates:
        return []
    limits = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    slo = slo or {}
    findings: list[dict[str, Any]] = []

    # Build-scoped rules are emitted once even when a Build has many probes.
    by_build: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for candidate in candidates:
        by_build[candidate["build_id"]].append(candidate)
    for group in by_build.values():
        candidate = group[0]
        build = candidate["build_metrics"]
        prediction = build.get("predicted_memory_failure")
        memory_status = (
            "triggered" if prediction is True else
            "clear" if prediction is False else "unknown"
        )
        findings.append(_finding(
            len(findings) + 1, "memory_hard_failure",
            memory_status,
            group,
            "Build is infeasible under the current maintenance_work_mem."
            if prediction is True else
            "Checked build memory fits the current maintenance_work_mem."
            if prediction is False else
            "Build-memory feasibility is unknown because checked-memory evidence is unavailable.",
            "Estimated pgvector checked allocations exceed the configured limit."
            if prediction is True else
            "No checked-memory hard failure is predicted."
            if prediction is False else "No memory cause can be established.",
            "high" if prediction is not None else "low",
            [] if prediction is not None else ["memory estimate is unavailable"],
            ("memory.predicted_hard_failure", "memory.estimated_checked_bytes", "memory.limit_bytes"),
        ))

        planned = candidate["resource_metrics"].get("planned_workers")
        launched = len(candidate["resource_metrics"].get("worker_pids") or [])
        if planned is None:
            worker_status = "unknown"
            worker_summary = "Worker quota could not be evaluated because planned worker count is unavailable."
            worker_limitations = ["planned_workers was not observed"]
        else:
            worker_status = "triggered" if launched < planned else "clear"
            worker_summary = (
                f"Only {launched} of {planned} planned workers were observed."
                if worker_status == "triggered" else
                f"Observed workers met the planned count ({launched}/{planned})."
            )
            worker_limitations = ["worker discovery is sampling-based"]
        findings.append(_finding(
            len(findings) + 1, "worker_quota", worker_status, group,
            worker_summary, "Parallel worker quota or competing worker demand." if worker_status == "triggered" else "No worker quota cause established.",
            "medium" if planned is not None else "low", worker_limitations, (),
            {"planned_workers": planned, "observed_workers": launched},
        ))

        structure = candidate["structure_metrics"]
        cv, gini, ratio = structure.get("cv"), structure.get("gini"), structure.get("p95_over_p50")
        if None in (cv, gini, ratio):
            structure_status = "unknown"
            structure_summary = "List imbalance could not be evaluated because structure evidence is incomplete."
        elif cv >= limits["severe_cv"] and gini >= limits["severe_gini"]:
            structure_status = "triggered"
            structure_summary = "IVFFlat list distribution has severe imbalance under P1-v1 heuristics."
        elif cv >= limits["moderate_cv"] and (gini >= limits["moderate_gini"] or ratio >= limits["moderate_p95_p50"]):
            structure_status = "triggered"
            structure_summary = "IVFFlat list distribution has moderate imbalance under P1-v1 heuristics."
        else:
            structure_status = "clear"
            structure_summary = "No moderate or severe list imbalance was detected by P1-v1 heuristics."
        findings.append(_finding(
            len(findings) + 1, "structure_imbalance", structure_status, group,
            structure_summary, "Non-uniform assignment of vectors to IVFFlat centroids.",
            "high" if structure_status != "unknown" else "low",
            ["heuristic thresholds are Project2 P1-v1, not pgvector guarantees"],
            ("structure.cv", "structure.gini", "structure.p95_over_p50"),
            {"cv": cv, "gini": gini, "p95_over_p50": ratio},
        ))

        eligible = candidate.get("eligible_vectors")
        declared = structure.get("declared_lists")
        empty_ratio = structure.get("empty_ratio")
        if declared is not None and eligible is not None and eligible < declared:
            empty_status = "not_applicable"
            empty_summary = "Empty-list anomaly is not applicable because eligible vectors are fewer than declared lists."
        elif empty_ratio is None:
            empty_status = "unknown"
            empty_summary = "Empty-list anomaly could not be evaluated because exact empty ratio is unavailable."
        else:
            empty_status = "triggered" if empty_ratio >= limits["empty_ratio"] else "clear"
            empty_summary = (
                f"Empty-list ratio {empty_ratio:.3f} exceeds the P1-v1 warning threshold."
                if empty_status == "triggered" else
                f"Empty-list ratio {empty_ratio:.3f} is below the P1-v1 warning threshold."
            )
        findings.append(_finding(
            len(findings) + 1, "empty_list_warning", empty_status, group,
            empty_summary, "Too many centroids relative to eligible vectors or sparse centroid assignment.",
            "high" if empty_status in ("triggered", "clear") else "low", [],
            ("structure.empty_ratio",), {"empty_ratio": empty_ratio},
        ))

    # Quality curve rules compare adjacent probes only within the same Build.
    min_recall = slo.get("min_recall")
    for group in by_build.values():
        valid = sorted(
            (item for item in group if item["validity"]["eligible_for_comparison"]),
            key=lambda item: item.get("probes") if item.get("probes") is not None else -1,
        )
        if min_recall is None:
            findings.append(_finding(
                len(findings) + 1, "probes_improve_recall", "not_applicable", group,
                "Recall-improvement rule requires a minimum Recall SLO.",
                "No Recall target was supplied.", "low", ["min_recall is unset"],
                ("quality.recall_at_k",),
            ))
        else:
            below = [item for item in valid if (item["quality_metrics"].get("recall_at_k") or 0) < min_recall]
            for current in below:
                better = next((
                    item for item in valid
                    if item["probes"] > current["probes"]
                    and (item["quality_metrics"].get("recall_at_k") or 0) >= min_recall
                ), None)
                findings.append(_finding(
                    len(findings) + 1, "probes_improve_recall",
                    "triggered" if better else "clear", [current] + ([better] if better else []),
                    f"Increasing probes from {current['probes']} to {better['probes']} reaches the Recall SLO."
                    if better else "No tested higher-probes candidate reaches the Recall SLO.",
                    "The current probes value scans too few IVFFlat lists." if better else "No proven probes-only remedy in the tested matrix.",
                    "high" if better else "medium", ["only tested probes values are compared"],
                    ("quality.recall_at_k", "quality.p95_ms"),
                    {"current_probes": current["probes"], "recommended_probes": better["probes"] if better else None},
                ))

        for lower, higher in zip(valid, valid[1:]):
            low_recall = lower["quality_metrics"].get("recall_at_k")
            high_recall = higher["quality_metrics"].get("recall_at_k")
            low_p95 = lower["quality_metrics"].get("p95_ms")
            high_p95 = higher["quality_metrics"].get("p95_ms")
            if None in (low_recall, high_recall, low_p95, high_p95) or low_p95 <= 0:
                status, recall_delta, latency_delta = "unknown", None, None
            else:
                recall_delta = high_recall - low_recall
                latency_delta = (high_p95 - low_p95) / low_p95
                status = "triggered" if recall_delta < limits["recall_material_change"] and latency_delta >= limits["latency_relative_change"] else "clear"
            findings.append(_finding(
                len(findings) + 1, "probes_diminishing_returns", status,
                [lower, higher],
                f"probes {lower['probes']}→{higher['probes']}: Recall Δ={recall_delta}, P95 relative Δ={latency_delta}." if recall_delta is not None else "Adjacent probes candidates lack comparable Recall/P95 evidence.",
                "Additional list scans add latency with little Recall benefit." if status == "triggered" else "No diminishing-return cause established.",
                "high" if status != "unknown" else "low",
                ["thresholds are configurable P1-v1 heuristics"],
                ("quality.recall_at_k", "quality.p95_ms"),
                {"lower_probes": lower["probes"], "higher_probes": higher["probes"], "recall_delta": recall_delta, "p95_relative_delta": latency_delta},
            ))

    # Joint structure/quality inference requires a structure trigger, a quality SLO
    # violation, and a different-lists control candidate.
    min_recall = slo.get("min_recall")
    max_p95 = slo.get("max_p95_ms")
    comparable = [item for item in candidates if item["validity"]["eligible_for_comparison"]]
    for candidate in comparable:
        structure = candidate["structure_metrics"]
        imbalance = (
            structure.get("cv") is not None and structure.get("gini") is not None
            and structure["cv"] >= limits["moderate_cv"]
            and (structure["gini"] >= limits["moderate_gini"] or (structure.get("p95_over_p50") or 0) >= limits["moderate_p95_p50"])
        )
        quality_bad = (
            (min_recall is not None and (candidate["quality_metrics"].get("recall_at_k") or 0) < min_recall)
            or (max_p95 is not None and (candidate["quality_metrics"].get("p95_ms") or float("inf")) > max_p95)
        )
        controls = [
            item for item in comparable
            if item["lists"] != candidate["lists"] and item["probes"] == candidate["probes"]
        ]
        control = next((
            item for item in controls
            if item["structure_metrics"].get("cv") is not None
            and item["structure_metrics"]["cv"] < structure["cv"]
            and not (
                min_recall is not None
                and (item["quality_metrics"].get("recall_at_k") or 0) < min_recall
            )
            and not (
                max_p95 is not None
                and item["quality_metrics"].get("p95_ms") is not None
                and item["quality_metrics"]["p95_ms"] > max_p95
            )
        ), None)
        if min_recall is None and max_p95 is None:
            status = "not_applicable"
        elif not imbalance or not quality_bad:
            status = "clear"
        elif control is None:
            status = "unknown"
        else:
            status = "triggered"
        findings.append(_finding(
            len(findings) + 1, "structure_impacts_quality", status,
            [candidate] + ([control] if control else []),
            "Structure imbalance coincides with a Quality SLO violation and has a different-lists control."
            if status == "triggered" else
            "A causal structure/quality relationship is not established for this candidate.",
            "Uneven centroid assignment may increase scanned work or reduce effective candidate coverage.",
            "medium" if status == "triggered" else "low",
            ["correlation is not proof of causation", "requires the same probes and comparable workload"],
            ("structure.cv", "structure.gini", "quality.recall_at_k", "quality.p95_ms"),
        ))
    return findings
