"""Build versioned P1 Candidate and Evidence records from collected facts."""

from __future__ import annotations

from pathlib import Path
import re
from typing import Any

from common.schema import SCHEMA_VERSION, utc_now


P1_SCHEMA_VERSION = "1.0"


def _id_part(value: Any) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "-", str(value)).strip("-")


def _metric(summary: dict[str, Any], name: str) -> Any:
    item = (summary.get("metrics") or {}).get(name) or {}
    return item.get("value") if item.get("availability") == "available" else None


def _evidence(
    *, diagnosis_id: str, candidate_id: str, ordinal: int, metric: str,
    value: Any, unit: str, source: str, availability: str = "available",
    reason: str | None = None, build_id: str, quality_run_id: str | None,
    artifact_ref: str,
) -> dict[str, Any]:
    if availability != "available":
        value = None
    return {
        "schema_version": SCHEMA_VERSION,
        "p1_schema_version": P1_SCHEMA_VERSION,
        "diagnosis_id": diagnosis_id,
        "evidence_id": f"evidence-{_id_part(candidate_id)}-{ordinal:02d}",
        "timestamp_utc": utc_now(),
        "metric": metric,
        "value": value,
        "unit": unit,
        "source": source,
        "build_id": build_id,
        "quality_run_id": quality_run_id,
        "availability": availability,
        "reason": reason,
        "artifact_ref": artifact_ref,
    }


def _peak_memory(build: dict[str, Any]) -> tuple[Any, str | None]:
    if build.get("process_group_peak_pss_bytes") is not None:
        return build["process_group_peak_pss_bytes"], "process_group_pss"
    value = build.get(
        "process_group_peak_summed_rss_bytes",
        build.get("process_group_peak_rss_bytes"),
    )
    return value, "summed_process_group_rss" if value is not None else None


def build_candidates(
    diagnosis_id: str,
    builds: list[tuple[dict[str, Any], Path]],
    qualities: list[tuple[dict[str, Any], Path]],
    manifest: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Join lists-scoped Build/Structure facts to probes-scoped Quality facts."""
    qualities_by_build: dict[str, list[tuple[dict[str, Any], Path]]] = {}
    for quality, path in qualities:
        qualities_by_build.setdefault(quality.get("build_id"), []).append((quality, path))

    candidates: list[dict[str, Any]] = []
    all_evidence: list[dict[str, Any]] = []
    configured_probes = list(dict.fromkeys(manifest.get("probes") or [None]))
    for build, build_path in builds:
        associated = qualities_by_build.get(build.get("build_id")) or [
            (None, None) for _ in configured_probes
        ]
        for position, pair in enumerate(associated):
            quality, quality_path = pair
            probes = quality.get("probes") if quality else configured_probes[position]
            candidate_id = (
                f"candidate-lists-{build.get('lists')}-probes-{probes}-"
                f"{str(build.get('build_id')).rsplit('-', 1)[-1][:8]}"
            )
            structure = build.get("structure_summary") or {}
            peak_memory, memory_source = _peak_memory(build)
            memory = build.get("memory_estimate") or {}
            metadata = build.get("index_metadata") or {}
            quality_metrics = {
                "recall_at_k": quality.get("recall_at_k") if quality else None,
                "p95_ms": quality.get("p95_ms") if quality else None,
                "p99_ms": quality.get("p99_ms") if quality else None,
                "mean_ms": quality.get("mean_ms") if quality else None,
                "single_client_qps": quality.get("single_client_qps") if quality else None,
                "used_ivfflat": quality.get("used_ivfflat") if quality else None,
                "returned_rows_min": quality.get("returned_rows_min") if quality else None,
                "returned_rows_max": quality.get("returned_rows_max") if quality else None,
                "k": quality.get("k") if quality else manifest.get("k"),
                "ground_truth_id": quality.get("ground_truth_id") if quality else None,
                "query_count": quality.get("query_count") if quality else None,
                "repeat": quality.get("repeat") if quality else manifest.get("repeat"),
                "cache_mode": quality.get("cache_mode") if quality else manifest.get("cache"),
                "client_mode": quality.get("client_mode") if quality else manifest.get("client"),
                "planner_mode": quality.get("planner_mode") if quality else "natural",
            }
            reasons: list[str] = []
            predicted_memory_failure = memory.get("predicted_memory_failure")
            infeasible = predicted_memory_failure is True
            if infeasible:
                reasons.append("predicted pgvector maintenance_work_mem hard failure")
            if build.get("database_status") != "success":
                reasons.append(build.get("reason") or "Build did not succeed")
            if quality is None:
                reasons.append("Quality Run was not executed")
            elif quality.get("status") != "success":
                reasons.append(quality.get("invalid_reason") or quality.get("reason") or "Quality Run is invalid")
            elif quality.get("used_ivfflat") is not True:
                reasons.append("natural planner did not use the target IVFFlat index")
            elif quality.get("returned_rows_min") is not None and quality.get("returned_rows_min") < quality.get("k", 0):
                reasons.append("Quality Run returned fewer than K rows")

            comparable = (
                not infeasible
                and build.get("database_status") == "success"
                and quality is not None
                and quality.get("status") == "success"
                and quality.get("used_ivfflat") is True
                and (quality.get("returned_rows_min") or 0) >= quality.get("k", 0)
            )
            validity_status = (
                "infeasible" if infeasible else
                "invalid" if not comparable else
                "partial" if build.get("structure_status") != "success" else
                "valid"
            )
            build_ref = f"{build_path.as_posix()}/raw/build.json"
            structure_ref = (
                f"{build_path.as_posix()}/structure/summary.json"
                if build.get("structure_summary") else build_ref
            )
            quality_ref = (
                f"{quality_path.as_posix()}/raw/quality.json" if quality_path else build_ref
            )
            facts = (
                ("memory.predicted_hard_failure", predicted_memory_failure, "boolean", "memory_estimator", memory.get("availability", "unavailable"), memory.get("reason"), build_ref),
                ("memory.estimated_checked_bytes", memory.get("estimated_pgvector_checked_memory_bytes"), "bytes", "memory_estimator", memory.get("availability", "unavailable"), memory.get("reason"), build_ref),
                ("memory.limit_bytes", memory.get("memory_limit_bytes"), "bytes", "preflight", "available" if memory.get("memory_limit_bytes") is not None else "unavailable", None, build_ref),
                ("build.seconds", build.get("build_seconds"), "seconds", "build_summary", "available" if build.get("build_seconds") is not None else "unavailable", build.get("reason"), build_ref),
                ("resource.peak_memory_bytes", peak_memory, "bytes", memory_source or "runtime_collector", "available" if peak_memory is not None else "unavailable", None, build_ref),
                ("index.bytes", metadata.get("index_bytes"), "bytes", "index_metadata", metadata.get("availability", "unavailable"), metadata.get("reason"), build_ref),
                ("structure.cv", _metric(structure, "cv"), "ratio", "structure_summary", "available" if _metric(structure, "cv") is not None else "unavailable", structure.get("reason"), structure_ref),
                ("structure.gini", _metric(structure, "gini"), "ratio", "structure_summary", "available" if _metric(structure, "gini") is not None else "unavailable", structure.get("reason"), structure_ref),
                ("structure.p95_over_p50", _metric(structure, "p95_over_p50"), "ratio", "structure_summary", "available" if _metric(structure, "p95_over_p50") is not None else "unavailable", structure.get("reason"), structure_ref),
                ("structure.empty_ratio", _metric(structure, "empty_ratio"), "ratio", "structure_summary", "available" if _metric(structure, "empty_ratio") is not None else "unavailable", structure.get("reason"), structure_ref),
                ("quality.recall_at_k", quality_metrics["recall_at_k"], "ratio", "quality_runner", "available" if quality_metrics["recall_at_k"] is not None else "unavailable", quality.get("invalid_reason") if quality else "Quality Run was not executed", quality_ref),
                ("quality.p95_ms", quality_metrics["p95_ms"], "milliseconds", "quality_runner", "available" if quality_metrics["p95_ms"] is not None else "unavailable", quality.get("invalid_reason") if quality else "Quality Run was not executed", quality_ref),
            )
            evidence = [
                _evidence(
                    diagnosis_id=diagnosis_id, candidate_id=candidate_id,
                    ordinal=ordinal, metric=metric, value=value, unit=unit,
                    source=source, availability=availability, reason=reason,
                    build_id=build["build_id"],
                    quality_run_id=quality.get("quality_run_id") if quality else None,
                    artifact_ref=artifact_ref,
                )
                for ordinal, (metric, value, unit, source, availability, reason, artifact_ref)
                in enumerate(facts, 1)
            ]
            candidate = {
                "schema_version": SCHEMA_VERSION,
                "p1_schema_version": P1_SCHEMA_VERSION,
                "diagnosis_id": diagnosis_id,
                "candidate_id": candidate_id,
                "timestamp_utc": utc_now(),
                "build_id": build.get("build_id"),
                "quality_run_id": quality.get("quality_run_id") if quality else None,
                "lists": build.get("lists"),
                "probes": probes,
                "requested_rows": build.get("requested_rows"),
                "actual_rows": build.get("actual_rows"),
                "eligible_vectors": build.get("eligible_vectors"),
                "build_metrics": {
                    "status": build.get("status"),
                    "database_status": build.get("database_status"),
                    "collection_status": build.get("collection_status"),
                    "build_seconds": build.get("build_seconds"),
                    "index_bytes": metadata.get("index_bytes"),
                    "predicted_memory_failure": predicted_memory_failure,
                    "estimated_checked_memory_bytes": memory.get("estimated_pgvector_checked_memory_bytes"),
                    "memory_limit_bytes": memory.get("memory_limit_bytes"),
                },
                "resource_metrics": {
                    "peak_memory_bytes": peak_memory,
                    "peak_memory_source": memory_source,
                    "worker_pids": build.get("discovered_worker_pids") or [],
                    "planned_workers": build.get("planned_workers"),
                },
                "structure_metrics": {
                    "availability": structure.get("availability", "unavailable"),
                    "scan_scope": structure.get("scan_scope"),
                    "declared_lists": structure.get("declared_lists"),
                    "cv": _metric(structure, "cv"),
                    "gini": _metric(structure, "gini"),
                    "p95_over_p50": _metric(structure, "p95_over_p50"),
                    "empty_ratio": _metric(structure, "empty_ratio"),
                    "coverage_ratio": _metric(structure, "coverage_ratio"),
                },
                "quality_metrics": quality_metrics,
                "validity": {
                    "status": validity_status,
                    "eligible_for_comparison": comparable,
                    "reasons": list(dict.fromkeys(reason for reason in reasons if reason)),
                },
                "evidence_ids": {item["metric"]: item["evidence_id"] for item in evidence},
                "artifact_refs": {
                    "build": build_ref,
                    "structure": structure_ref,
                    "quality": quality_ref,
                },
            }
            candidates.append(candidate)
            all_evidence.extend(evidence)
    return candidates, all_evidence
