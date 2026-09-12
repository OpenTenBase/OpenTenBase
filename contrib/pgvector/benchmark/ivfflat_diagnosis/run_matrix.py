#!/usr/bin/env python3
"""Run one or all IVFFlat build diagnosis matrices."""

from __future__ import annotations

import argparse
import copy
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from build import build_index
from common.config import PROJECT_DATABASES, metric_config, resolve_metrics
from common.datasets import load_metric
from common.db import (INDEX_NAME, TABLE_NAME, connect_dsn, load_vectors,
                       prepare_table, project_dsn)
from common.ground_truth import truth_for_rows
from common.manifest import build_manifest
from common.run_store import RunStore
from common.schema import (BuildContext, QualityContext, SCHEMA_VERSION,
                           new_run_id, short_run_id, utc_now)
from failure_tests import run_all
from ivfflat_query import run_quality_evaluation
from memory_estimator import estimate_pgvector_checked_memory
from preflight import (PreflightValidationError, collect_preflight_snapshot,
                       probe_capabilities)
from report import write_build_report, write_quality_report, write_root_reports
from diagnosis import diagnose
from recommendation import build_pareto, build_recommendations
from structure import collect_structure_stats
from validation import compare_validation


def diagnosis_status(builds, qualities, structures=None):
    structures = structures or []
    if any(b.get('status') in ('failed', 'cancelled') for b in builds) or any(
        q.get('status') not in ('success', 'skipped') for q in qualities
    ):
        return 'complete_with_failures'
    if any(b.get('collection_status') != 'success' or b.get('status') != 'success'
           for b in builds) or any(q.get('status') != 'success' for q in qualities) or any(
        row.get('status') != 'success' for row in structures
    ):
        return 'complete_with_partial_data'
    return 'complete'


def _diagnosis_id(args, metric: str) -> str:
    return f"diag-{args.run_id}-{metric}" if args.run_id else new_run_id(f"diag-{metric}")


def _deduplicate(values, name: str) -> list:
    resolved = list(dict.fromkeys(values))
    if len(resolved) != len(values):
        print(
            f"warning: duplicate resolved {name} values were removed: "
            f"{list(values)} -> {resolved}",
            flush=True,
        )
    return resolved


def _skipped_build(context: BuildContext, memory_estimate: dict) -> dict:
    timestamp = utc_now()
    return {
        "schema_version": SCHEMA_VERSION,
        "diagnosis_id": context.diagnosis_id,
        "build_id": context.build_id,
        "timestamp_utc": timestamp,
        "source": "project2_build_runner",
        "availability": "available",
        "reason": "predicted pgvector maintenance_work_mem hard failure",
        "status": "skipped",
        "database_status": "not_started",
        "collection_status": "not_started",
        "dataset": context.dataset,
        "metric": context.metric,
        "requested_rows": context.requested_rows,
        "actual_rows": context.actual_rows,
        "eligible_vectors": context.eligible_vectors,
        "rows": context.actual_rows,
        "dimension": context.dimension,
        "lists": context.lists,
        "runner_started_at": timestamp,
        "runner_finished_at": timestamp,
        "build_started_at": None,
        "build_finished_at": None,
        "started_at": None,
        "finished_at": None,
        "total_elapsed_ms": None,
        "build_seconds": None,
        "sqlstate": None,
        "error": None,
        "notices": [],
        "index_oid": None,
        "relfilenode": None,
        "index_bytes": None,
        "index_pages": None,
        "index_metadata": {
            "availability": "unavailable",
            "reason": "CREATE INDEX was skipped",
            "index_oid": None, "relfilenode": None,
            "index_bytes": None, "index_pages": None,
        },
        "memory_estimate": memory_estimate,
        "phase_summaries": {},
        "collector_errors": [],
    }


def _preflight_failed_build(context: BuildContext, reason: str) -> dict:
    timestamp = utc_now()
    return {
        "schema_version": SCHEMA_VERSION,
        "diagnosis_id": context.diagnosis_id,
        "build_id": context.build_id,
        "timestamp_utc": timestamp,
        "source": "project2_build_runner",
        "availability": "available",
        "reason": reason,
        "status": "failed",
        "database_status": "not_started",
        "collection_status": "success",
        "dataset": context.dataset,
        "metric": context.metric,
        "requested_rows": context.requested_rows,
        "actual_rows": context.actual_rows,
        "eligible_vectors": context.eligible_vectors,
        "rows": context.actual_rows,
        "dimension": context.dimension,
        "lists": context.lists,
        "started_at": None,
        "finished_at": None,
        "build_started_at": None,
        "build_finished_at": None,
        "total_elapsed_ms": None,
        "build_seconds": None,
        "failure_phase": "preflight",
        "sqlstate": None,
        "error": reason,
        "notices": [],
        "index_oid": None,
        "relfilenode": None,
        "index_bytes": None,
        "index_pages": None,
        "index_metadata": {
            "availability": "unavailable",
            "reason": "CREATE INDEX was blocked by Preflight",
            "index_oid": None, "relfilenode": None,
            "index_bytes": None, "index_pages": None,
        },
        "memory_estimate": None,
        "phase_summaries": {},
        "collector_errors": [],
    }


def _set_maintenance_work_mem(conn, value: str | None) -> None:
    if value is not None:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT set_config('maintenance_work_mem', %s, false)",
                (value,),
            )


def _validation_context(manifest: dict, candidate: dict, ground_truth_id: str) -> dict:
    quality = candidate.get("quality_metrics") or {}
    return {
        "dataset": manifest.get("dataset"),
        "rows": candidate.get("actual_rows"),
        "metric": manifest.get("metric"),
        "opclass": manifest.get("opclass"),
        "ground_truth_id": quality.get("ground_truth_id") or ground_truth_id,
        "k": quality.get("k") or manifest.get("k"),
        "query_count": manifest.get("query_count"),
        "repeat": quality.get("repeat") or manifest.get("repeat"),
        "cache_mode": quality.get("cache_mode") or manifest.get("cache"),
        "client_mode": quality.get("client_mode") or manifest.get("client"),
        "planner_mode": quality.get("planner_mode") or "natural",
        "postgresql": manifest.get("postgresql"),
        "pgvector": manifest.get("pgvector"),
        "machine": manifest.get("machine"),
    }


def _run_recommendation_validations(
    args, *, diagnosis_id: str, dataset, truth, loaded: int,
    ground_truth_id: str, manifest: dict, capabilities: dict,
    store: RunStore, recommendations: list[dict], candidates: list[dict],
) -> list[dict]:
    """Explicitly rebuild/requery a bounded number of proposed recommendations."""
    if not args.validate_recommendations:
        return []
    validations = []
    current_loaded = loaded
    for recommendation in recommendations[:args.max_validations]:
        current_spec = recommendation.get("current") or {}
        before = next((
            item for item in candidates
            if item.get("lists") == current_spec.get("lists")
            and item.get("probes") == current_spec.get("probes")
        ), None)
        if before is None:
            continue
        rec_id = recommendation["recommendation_id"]
        recommended = recommendation.get("recommended") or {}
        lists = recommended.get("lists")
        probes = recommended.get("probes")
        if lists is None or probes is None:
            continue
        validation_prefix = Path("validations") / rec_id
        target_rows = int(before.get("actual_rows"))
        if current_loaded != target_rows:
            with connect_dsn(args.dsn) as conn:
                prepare_table(conn, dimension=dataset.dimension, recreate=True)
                current_loaded = load_vectors(
                    conn, dataset.base, target_rows, dataset.dimension
                )
        validation_truth = (
            truth if current_loaded == loaded and target_rows == loaded
            else truth_for_rows(
                dataset, manifest["metric"], current_loaded,
                args.query_count, args.k,
            )
        )
        validation_ground_truth_id = (
            f"{dataset.name}:{manifest['metric']}:rows={current_loaded}:"
            f"k={args.k}:queries={args.query_count}"
        )

        maintenance = args.maintenance_work_mem
        if recommendation.get("action") == "increase_maintenance_work_mem_or_reduce_lists":
            minimum = recommended.get("minimum_checked_memory_bytes")
            if minimum is None:
                continue
            maintenance_bytes = max(int(minimum * 1.05), int(minimum) + 1024 * 1024)
            maintenance_kib = (maintenance_bytes + 1023) // 1024
            maintenance = f"{maintenance_kib}kB"
            recommended["maintenance_work_mem_bytes"] = maintenance_kib * 1024

        build_id = new_run_id("validation-build")
        build_prefix = validation_prefix / "builds" / f"lists-{lists}__{short_run_id(build_id)}"
        build_output = store.root / build_prefix
        with connect_dsn(args.dsn, autocommit=True) as conn:
            _set_maintenance_work_mem(conn, maintenance)
            preflight = collect_preflight_snapshot(
                conn, table=TABLE_NAME, column="embedding", index_name=INDEX_NAME,
                metric=manifest["metric"], lists=lists,
                diagnosis_id=diagnosis_id, build_id=build_id,
                requested_rows=current_loaded, enforce_requested_rows=True,
            )
        memory = estimate_pgvector_checked_memory(
            pgvector_version=manifest["pgvector"], lists=lists,
            dimension=preflight["dimension"], item_size=preflight["item_size"],
            heap_blocks=preflight["heap_blocks"],
            eligible_vectors=preflight["eligible_vectors"],
            maintenance_work_mem_bytes=preflight["memory_limit_bytes"],
            max_heap_tuples_per_page=preflight["max_heap_tuples_per_page"],
            diagnosis_id=diagnosis_id, build_id=build_id,
        )
        preflight["memory_estimate"] = memory
        store.write_json(build_prefix / "raw/preflight.json", preflight)
        store.write_json(build_prefix / "raw/memory-estimate.json", memory)
        result = build_index(
            args.dsn, dataset.name, manifest["metric"], dataset.dimension,
            current_loaded, lists, build_output, actual_rows=preflight["actual_rows"],
            eligible_vectors=preflight["eligible_vectors"],
            diagnosis_id=diagnosis_id, build_id=build_id, preflight=preflight,
            capabilities=capabilities, store=store,
            sample_interval=args.sample_interval,
            trust_assign_progress=args.trust_assign_progress,
            maintenance_work_mem=maintenance,
        )
        aggregate_path = build_output / "raw/aggregate.json"
        aggregate = json.loads(aggregate_path.read_text()) if aggregate_path.exists() else None
        if result.get("database_status") == "success" and result.get("index_oid") is not None:
            with connect_dsn(args.dsn, autocommit=True) as conn:
                structure = collect_structure_stats(
                    conn, diagnosis_id=diagnosis_id, build_id=build_id,
                    index_oid=result["index_oid"], relfilenode=result["relfilenode"],
                    sample_rate=manifest.get("structure_sample_rate", 1.0),
                    eligible_vectors=preflight["eligible_vectors"],
                    index_bytes=result.get("index_bytes"),
                    raw_vector_bytes=preflight["eligible_vectors"] * preflight["dimension"] * 4,
                    store=store, output_prefix=build_prefix,
                )
            result["structure_status"] = structure["scan"]["status"]
            result["structure_summary"] = structure["summary"]
            store.write_json(build_prefix / "raw/build.json", result)
        write_build_report(store, build_prefix, result, preflight, memory, aggregate)

        quality_reports = []
        if result.get("database_status") == "success":
            quality_run_id = new_run_id("validation-quality")
            quality_prefix = validation_prefix / "quality" / f"probes-{probes}__{short_run_id(quality_run_id)}"
            quality_context = QualityContext(
                diagnosis_id, build_id, quality_run_id, store.root / quality_prefix,
                lists, probes,
            )
            quality = run_quality_evaluation(
                args.dsn, dataset, validation_truth, quality_context, store,
                query_count=args.query_count, k=args.k, repeat=args.repeat,
                warmup=args.warmup,
                ground_truth_id=validation_ground_truth_id,
            )
            write_quality_report(store, quality_prefix, quality)
            quality_reports.append((quality, quality_prefix))

        after_analysis = diagnose(
            diagnosis_id, [(result, build_prefix)], quality_reports, manifest,
            manifest.get("p1_thresholds"),
        )
        after = after_analysis["candidates"][0]
        validation = compare_validation(
            recommendation, before, after,
            _validation_context(manifest, before, validation_ground_truth_id),
            _validation_context(manifest, after, validation_ground_truth_id),
            manifest.get("slo"),
        )
        store.write_json(validation_prefix / "validation.json", validation)
        recommendation["status"] = validation["status"]
        recommendation["validation_ref"] = f"{validation_prefix.as_posix()}/validation.json"
        validations.append(validation)
    return validations


def run_metric(args, metric: str) -> Path:
    """Run a single-metric Diagnosis Run with multiple Build and Quality Runs."""
    config = metric_config(metric)
    dataset = load_metric(
        metric, args.data_root, args.query_count, args.k,
        validate_expected_size=not args.allow_nonstandard_dataset_size,
    )
    row_counts = _deduplicate(
        [dataset.size if rows == 0 else rows for rows in args.rows], "rows"
    )
    lists_values = _deduplicate(args.lists, "lists")
    probes_values = _deduplicate(args.probes, "probes")
    if any(rows < 1 or rows > dataset.size for rows in row_counts):
        raise ValueError(
            f"rows must be 0 (full) or between 1 and {dataset.size} for {dataset.name}"
        )
    if args.repeat < 1 or args.query_count < 1 or args.k < 1:
        raise ValueError("repeat, query-count, and K must be positive")
    if args.sample_interval <= 0:
        raise ValueError("sample-interval must be positive")
    if args.max_validations < 1:
        raise ValueError("max-validations must be positive")
    structure_sample_rate = getattr(args, "structure_sample_rate", 1.0)
    if not 0.0 < structure_sample_rate <= 1.0:
        raise ValueError("structure-sample-rate must be in (0, 1]")
    p1_thresholds = {
        "moderate_cv": args.moderate_imbalance_cv,
        "moderate_gini": args.moderate_imbalance_gini,
        "moderate_p95_p50": args.moderate_imbalance_p95_p50,
        "severe_cv": args.severe_imbalance_cv,
        "severe_gini": args.severe_imbalance_gini,
        "empty_ratio": args.empty_list_ratio,
        "recall_material_change": args.diminishing_recall_delta,
        "latency_relative_change": args.diminishing_p95_increase,
    }

    diagnosis_id = _diagnosis_id(args, metric)
    output = ROOT / "results" / config["result_name"] / diagnosis_id
    store = RunStore(output)
    with connect_dsn(args.dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
        _set_maintenance_work_mem(conn, args.maintenance_work_mem)
        capabilities = probe_capabilities(conn, host_context=args.host_context)
        if args.trust_assign_progress:
            capabilities = copy.deepcopy(capabilities)
            capabilities["capabilities"]["assign_progress"] = {
                "available": True,
                "availability": "available",
                "reason": "explicitly trusted by command-line option",
            }
        manifest = build_manifest(
            conn,
            "ivfflat-build-diagnosis",
            diagnosis_id=diagnosis_id,
            project="build",
            database=PROJECT_DATABASES["build"],
            dataset=dataset.name,
            dataset_size=dataset.size,
            dimension=dataset.dimension,
            metric=metric,
            operator=config["operator"],
            opclass=config["opclass"],
            ground_truth=config["ground_truth"],
            rows=row_counts,
            lists=lists_values,
            probes=probes_values,
            query_count=args.query_count,
            k=args.k,
            repeat=args.repeat,
            cache="warm",
            client="single",
            throughput="single-client",
            index_type="ivfflat",
            sample_interval_seconds=args.sample_interval,
            on_predicted_memory_failure=args.on_predicted_memory_failure,
            enforce_requested_rows=args.enforce_requested_rows,
            maintenance_work_mem_override=args.maintenance_work_mem,
            structure_sample_rate=structure_sample_rate,
            p1_ruleset="p1-v1",
            p1_thresholds=p1_thresholds,
            validate_recommendations=args.validate_recommendations,
            capabilities=capabilities,
            slo={
                "min_recall": args.min_recall,
                "max_p95_ms": args.max_p95_ms,
                "max_p99_ms": args.max_p99_ms,
                "max_build_memory_bytes": args.max_build_memory_bytes,
                "max_build_seconds": args.max_build_seconds,
                "max_index_bytes": args.max_index_bytes,
            },
        )
    store.write_json("raw/manifest.json", manifest)

    build_rows: list[dict] = []
    quality_rows: list[dict] = []
    structure_rows: list[dict] = []
    build_reports: list[tuple[dict, Path]] = []
    quality_reports: list[tuple[dict, Path]] = []
    for requested_rows in row_counts:
        with connect_dsn(args.dsn) as conn:
            prepare_table(conn, dimension=dataset.dimension, recreate=True)
            loaded = load_vectors(
                conn, dataset.base, requested_rows, dataset.dimension
            )

        truth = truth_for_rows(
            dataset, metric, loaded, args.query_count, args.k
        )
        ground_truth_id = (
            f"{dataset.name}:{metric}:rows={loaded}:k={args.k}:"
            f"queries={args.query_count}"
        )
        for lists in lists_values:
            build_id = new_run_id("build")
            build_prefix = (
                Path("builds") / f"lists-{lists}__{short_run_id(build_id)}"
            )
            build_output = output / build_prefix
            try:
                with connect_dsn(args.dsn, autocommit=True) as conn:
                    _set_maintenance_work_mem(conn, args.maintenance_work_mem)
                    preflight = collect_preflight_snapshot(
                        conn,
                        table=TABLE_NAME,
                        column="embedding",
                        index_name=INDEX_NAME,
                        metric=metric,
                        lists=lists,
                        diagnosis_id=diagnosis_id,
                        build_id=build_id,
                        requested_rows=requested_rows,
                        enforce_requested_rows=args.enforce_requested_rows,
                    )
            except PreflightValidationError as exc:
                preflight = exc.snapshot
                build_context = BuildContext(
                    diagnosis_id, build_id, build_output, dataset.name, metric,
                    requested_rows, preflight["actual_rows"],
                    preflight["eligible_vectors"], dataset.dimension, lists,
                )
                memory = {
                    "schema_version": SCHEMA_VERSION,
                    "diagnosis_id": diagnosis_id,
                    "build_id": build_id,
                    "timestamp_utc": utc_now(),
                    "source": "pgvector_source_estimator",
                    "availability": "not_sampled",
                    "reason": "not evaluated because row-count Preflight failed",
                    "predicted_memory_failure": None,
                }
                result = _preflight_failed_build(build_context, str(exc))
                store.write_json(build_prefix / "raw/preflight.json", preflight)
                store.write_json(build_prefix / "raw/memory-estimate.json", memory)
                store.write_json(build_prefix / "raw/build.json", result)
                write_build_report(store, build_prefix, result, preflight, memory, None)
                build_rows.append(result)
                build_reports.append((result, build_prefix))
                print(result, flush=True)
                continue

            build_context = BuildContext(
                diagnosis_id, build_id, build_output, dataset.name, metric,
                requested_rows, preflight["actual_rows"],
                preflight["eligible_vectors"], dataset.dimension, lists,
            )
            memory = estimate_pgvector_checked_memory(
                pgvector_version=manifest["pgvector"],
                lists=lists,
                dimension=preflight["dimension"],
                item_size=preflight["item_size"],
                heap_blocks=preflight["heap_blocks"],
                eligible_vectors=preflight["eligible_vectors"],
                maintenance_work_mem_bytes=preflight["memory_limit_bytes"],
                max_heap_tuples_per_page=preflight["max_heap_tuples_per_page"],
                diagnosis_id=diagnosis_id,
                build_id=build_id,
            )
            preflight["memory_estimate"] = memory
            store.write_json(build_prefix / "raw/preflight.json", preflight)
            store.write_json(build_prefix / "raw/memory-estimate.json", memory)

            if (
                memory.get("predicted_memory_failure") is True
                and args.on_predicted_memory_failure == "skip"
            ):
                result = _skipped_build(build_context, memory)
                store.write_json(build_prefix / "raw/build.json", result)
                aggregate = None
            else:
                result = build_index(
                    args.dsn,
                    dataset.name,
                    metric,
                    dataset.dimension,
                    requested_rows,
                    lists,
                    build_output,
                    actual_rows=preflight["actual_rows"],
                    eligible_vectors=preflight["eligible_vectors"],
                    diagnosis_id=diagnosis_id,
                    build_id=build_id,
                    preflight=preflight,
                    capabilities=capabilities,
                    store=store,
                    sample_interval=args.sample_interval,
                    trust_assign_progress=args.trust_assign_progress,
                    maintenance_work_mem=args.maintenance_work_mem,
                )
                aggregate = json.loads(
                    (build_output / "raw/aggregate.json").read_text()
                )
            if (
                result["database_status"] == "success"
                and result.get("index_oid") is not None
                and result.get("relfilenode") is not None
            ):
                with connect_dsn(args.dsn, autocommit=True) as conn:
                    structure = collect_structure_stats(
                        conn,
                        diagnosis_id=diagnosis_id,
                        build_id=build_id,
                        index_oid=result["index_oid"],
                        relfilenode=result["relfilenode"],
                        sample_rate=structure_sample_rate,
                        eligible_vectors=preflight["eligible_vectors"],
                        index_bytes=result.get("index_bytes"),
                        raw_vector_bytes=(
                            preflight["eligible_vectors"]
                            * preflight["dimension"] * 4
                        ),
                        store=store,
                        output_prefix=build_prefix,
                    )
                result["structure_status"] = structure["scan"]["status"]
                result["structure_summary"] = structure["summary"]
                structure_rows.append(structure["scan"])
                store.write_json(build_prefix / "raw/build.json", result)
            write_build_report(
                store, build_prefix, result, preflight, memory, aggregate
            )
            build_rows.append(result)
            build_reports.append((result, build_prefix))
            print(result, flush=True)

            if result["database_status"] != "success":
                continue
            for probes in probes_values:
                quality_run_id = new_run_id("quality")
                quality_prefix = (
                    Path("quality") / f"lists-{lists}"
                    / f"probes-{probes}__{short_run_id(quality_run_id)}"
                )
                quality_output = output / quality_prefix
                quality_context = QualityContext(
                    diagnosis_id, build_id, quality_run_id, quality_output,
                    lists, probes,
                )
                quality = run_quality_evaluation(
                    args.dsn,
                    dataset,
                    truth,
                    quality_context,
                    store,
                    query_count=args.query_count,
                    k=args.k,
                    repeat=args.repeat,
                    warmup=args.warmup,
                    ground_truth_id=ground_truth_id,
                )
                write_quality_report(store, quality_prefix, quality)
                quality_rows.append(quality)
                quality_reports.append((quality, quality_prefix))
                print(quality, flush=True)

    failures = [] if args.skip_failure_tests else run_all(
        args.dsn, metric, dataset.dimension
    )
    error_text = "".join(
        json.dumps(row, ensure_ascii=False) + "\n" for row in failures
    )
    store.write_text("raw/errors.log", error_text)
    store.write_json("raw/builds.json", build_rows)
    store.write_json("raw/quality.json", quality_rows)
    store.write_json("raw/structures.json", structure_rows)
    p1 = diagnose(
        diagnosis_id, build_reports, quality_reports, manifest, p1_thresholds
    )
    pareto = build_pareto(p1["candidates"], manifest.get("slo"))
    recommendations = build_recommendations(
        p1["candidates"], p1["findings"], pareto, manifest
    )
    validations = _run_recommendation_validations(
        args, diagnosis_id=diagnosis_id, dataset=dataset, truth=truth,
        loaded=loaded, ground_truth_id=ground_truth_id, manifest=manifest,
        capabilities=capabilities, store=store,
        recommendations=recommendations, candidates=p1["candidates"],
    )
    p1["pareto"] = pareto
    p1["recommendations"] = recommendations
    p1["validations"] = validations
    store.write_json("candidates.json", p1["candidates"])
    store.write_json("evidence.json", p1["evidence"])
    store.write_json("findings.json", p1["findings"])
    store.write_json("pareto.json", pareto)
    store.write_json("recommendations.json", recommendations)
    store.write_json("diagnosis.json", p1)
    store.write_json("validations/index.json", validations)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "diagnosis_id": diagnosis_id,
        "timestamp_utc": utc_now(),
        "source": "project2_p1_orchestrator",
        "availability": "available",
        "reason": None,
        "status": diagnosis_status(build_rows, quality_rows, structure_rows),
        "build_runs": len(build_rows),
        "successful_build_runs": sum(
            row["database_status"] == "success" for row in build_rows
        ),
        "failed_build_runs": sum(row["status"] == "failed" for row in build_rows),
        "preflight_failed_runs": sum(
            row.get("failure_phase") == "preflight" for row in build_rows
        ),
        "skipped_build_runs": sum(row["status"] == "skipped" for row in build_rows),
        "quality_runs": len(quality_rows),
        "valid_quality_runs": sum(
            row["status"] == "success" for row in quality_rows
        ),
        "structure_runs": len(structure_rows),
        "valid_structure_runs": sum(
            row["status"] == "success" for row in structure_rows
        ),
        "p1_candidates": len(p1["candidates"]),
        "triggered_findings": sum(
            row["status"] == "triggered" for row in p1["findings"]
        ),
        "pareto_candidates": len(pareto["pareto_candidate_ids"]),
        "recommendations": len(recommendations),
        "validations": len(validations),
        "validated_recommendations": sum(
            row["status"] == "validated" for row in validations
        ),
        "validation_failed": sum(
            row["status"] == "validation_failed" for row in validations
        ),
        "validation_not_comparable": sum(
            row["status"] == "not_comparable" for row in validations
        ),
        "failure_tests": failures,
    }
    store.write_json("raw/p0-summary.json", summary)
    write_root_reports(
        store, manifest, summary, build_reports, quality_reports,
        p1, pareto, recommendations, validations,
    )
    print(f"results: {output}")
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dsn", default=os.environ.get("PG_DSN"),
        help="existing cluster DSN; database name is replaced with pgvector_benchmark by default",
    )
    parser.add_argument("--metric", choices=("l2", "cosine", "ip", "all"), default="all")
    parser.add_argument("--index-type", choices=("ivfflat",), default="ivfflat")
    parser.add_argument("--data-root", type=Path, default=ROOT / "data")
    parser.add_argument(
        "--allow-nonstandard-dataset-size", action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--rows", nargs="+", type=int, default=(100_000, 0))
    parser.add_argument("--lists", nargs="+", type=int, default=(100, 1000, 3162))
    parser.add_argument(
        "--probes", "--recall-probes", dest="probes",
        nargs="+", type=int, default=(10,),
    )
    parser.add_argument(
        "--query-count", "--recall-queries", dest="query_count",
        type=int, default=100,
    )
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=20)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--sample-interval", type=float, default=0.5)
    parser.add_argument(
        "--structure-sample-rate", type=float, default=1.0,
        help="fraction of IVFFlat lists to inspect; P1 exact metrics require 1.0",
    )
    parser.add_argument(
        "--maintenance-work-mem",
        help="session-level maintenance_work_mem used by manifest, Preflight, and CREATE INDEX",
    )
    parser.add_argument(
        "--enforce-requested-rows",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="fail Preflight on requested_rows != actual_rows; disable only for anomaly tests",
    )
    parser.add_argument("--trust-assign-progress", action="store_true")
    parser.add_argument(
        "--on-predicted-memory-failure",
        choices=("skip", "run"),
        default="skip",
    )
    parser.add_argument(
        "--host-context", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument("--min-recall", type=float)
    parser.add_argument("--max-p95-ms", type=float)
    parser.add_argument("--max-p99-ms", type=float)
    parser.add_argument("--max-build-memory-bytes", type=int)
    parser.add_argument("--max-build-seconds", type=float)
    parser.add_argument("--max-index-bytes", type=int)
    parser.add_argument("--moderate-imbalance-cv", type=float, default=0.5)
    parser.add_argument("--moderate-imbalance-gini", type=float, default=0.30)
    parser.add_argument("--moderate-imbalance-p95-p50", type=float, default=2.0)
    parser.add_argument("--severe-imbalance-cv", type=float, default=1.0)
    parser.add_argument("--severe-imbalance-gini", type=float, default=0.50)
    parser.add_argument("--empty-list-ratio", type=float, default=0.05)
    parser.add_argument("--diminishing-recall-delta", type=float, default=0.002)
    parser.add_argument("--diminishing-p95-increase", type=float, default=0.20)
    parser.add_argument(
        "--validate-recommendations", action="store_true",
        help="rerun evidence-backed recommendations with the same workload",
    )
    parser.add_argument(
        "--max-validations", type=int, default=1,
        help="maximum proposed recommendations to rerun when validation is enabled",
    )
    parser.add_argument("--skip-failure-tests", action="store_true")
    parser.add_argument(
        "--run-id",
        help="optional stable experiment label; metric is appended for uniqueness",
    )
    args = parser.parse_args()
    args.dsn = project_dsn("build", args.dsn)
    outputs = [
        str(run_metric(args, metric))
        for metric in resolve_metrics(args.metric)
    ]
    print(json.dumps({"completed": outputs}, indent=2))


if __name__ == "__main__":
    main()
