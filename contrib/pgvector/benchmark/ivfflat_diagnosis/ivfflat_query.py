"""Quality Run orchestration for a successfully built IVFFlat index."""

from __future__ import annotations

from typing import Any

from common.db import INDEX_NAME, connect_dsn
from common.metrics import summarize
from common.query_quality import inspect_query_plan, measure_query
from common.run_store import RunStore
from common.schema import QualityContext, SCHEMA_VERSION, utc_now


def run_quality_evaluation(dsn: str, dataset, truth, context: QualityContext,
                           store: RunStore, *, query_count: int, k: int,
                           repeat: int = 3, warmup: int = 20,
                           ground_truth_id: str) -> dict[str, Any]:
    """Run one probes configuration and keep repeats under one quality_run_id."""
    if repeat < 1:
        raise ValueError("repeat must be positive")
    output = context.output.resolve()
    prefix = output.relative_to(store.root)
    raw_prefix = prefix / "raw"
    if context.probes < 1 or context.probes > context.lists:
        result = {
            "schema_version": SCHEMA_VERSION,
            "diagnosis_id": context.diagnosis_id,
            "build_id": context.build_id,
            "quality_run_id": context.quality_run_id,
            "timestamp_utc": utc_now(),
            "source": "quality_runner",
            "availability": "unavailable",
            "reason": "probes must be between 1 and lists",
            "status": "skipped",
            "invalid_reason": "probes must be between 1 and lists",
            "lists": context.lists,
            "probes": context.probes,
            "repeat": repeat,
            "k": k,
            "query_count": query_count,
            "ground_truth_id": ground_truth_id,
        }
        store.write_json(raw_prefix / "quality.json", result)
        return result
    queries = dataset.queries[:query_count]
    truth = truth[:query_count]
    started_at = utc_now()
    repeats: list[dict[str, Any]] = []
    natural_plan: dict[str, Any] | None = None
    forced_plan: dict[str, Any] | None = None
    status = "success"
    invalid_reason = None
    error = None

    try:
        with connect_dsn(dsn) as conn:
            representative = queries[0]
            natural_plan = inspect_query_plan(
                conn, dataset.metric, context.probes, representative, k,
                index_name=INDEX_NAME, planner_mode="natural",
            )
            forced_plan = inspect_query_plan(
                conn, dataset.metric, context.probes, representative, k,
                index_name=INDEX_NAME, planner_mode="forced_validation",
            )
            store.write_text(raw_prefix / "plan-natural.txt", natural_plan["plan_text"] + "\n")
            store.write_text(raw_prefix / "plan-forced.txt", forced_plan["plan_text"] + "\n")

            for repeat_number in range(1, repeat + 1):
                measured = measure_query(
                    conn, queries, truth, dataset.metric, context.probes, k,
                    planner_mode="natural", warmup=warmup, include_details=True,
                )
                samples = measured.pop("samples")
                for sample in samples:
                    store.append_jsonl(raw_prefix / "queries.jsonl", {
                        "schema_version": SCHEMA_VERSION,
                        "diagnosis_id": context.diagnosis_id,
                        "build_id": context.build_id,
                        "quality_run_id": context.quality_run_id,
                        "timestamp_utc": utc_now(),
                        "source": "quality_runner",
                        "availability": "available",
                        "reason": None,
                        "repeat": repeat_number,
                        **sample,
                    })
                repeats.append({"repeat": repeat_number, **measured, "samples": samples})
    except Exception as exc:
        status = "failed"
        error = str(exc)

    if natural_plan is not None and not natural_plan["used_ivfflat"]:
        status = "invalid-for-index-diagnosis"
        invalid_reason = "natural planner did not use the target IVFFlat index"

    all_samples = [
        sample
        for repeat_result in repeats
        for sample in repeat_result.pop("samples")
    ]
    aggregate = None
    if all_samples:
        aggregate = summarize(
            [sample["latency_ms"] for sample in all_samples],
            [sample["recall_at_k"] for sample in all_samples],
            k,
            [sample["returned_rows"] for sample in all_samples],
        )
        if aggregate["returned_rows_min"] is not None and aggregate["returned_rows_min"] < k:
            status = "invalid-for-index-diagnosis"
            invalid_reason = f"at least one query returned fewer than K={k} rows"

    finished_at = utc_now()
    result = {
        "schema_version": SCHEMA_VERSION,
        "diagnosis_id": context.diagnosis_id,
        "build_id": context.build_id,
        "quality_run_id": context.quality_run_id,
        "timestamp_utc": finished_at,
        "source": "quality_runner",
        "availability": "available" if aggregate else "unavailable",
        "reason": error,
        "status": status,
        "invalid_reason": invalid_reason,
        "lists": context.lists,
        "probes": context.probes,
        "repeat": repeat,
        "k": k,
        "query_count": query_count,
        "cache_mode": "warm",
        "client_mode": "single",
        "ground_truth_id": ground_truth_id,
        "planner_mode": "natural",
        "used_ivfflat": natural_plan["used_ivfflat"] if natural_plan else None,
        "forced_validation_used_ivfflat": forced_plan["used_ivfflat"] if forced_plan else None,
        "started_at": started_at,
        "finished_at": finished_at,
        "repeats": repeats,
        "aggregate": aggregate,
        **(aggregate or {}),
    }
    store.mark_complete(raw_prefix / "queries.jsonl")
    store.write_json(raw_prefix / "quality.json", result)
    return result
