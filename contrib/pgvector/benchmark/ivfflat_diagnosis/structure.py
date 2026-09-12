"""Collect and summarize read-only IVFFlat structure facts."""

from __future__ import annotations

import math
from pathlib import Path
import statistics
import time
from typing import Any, Iterable

from common.run_store import RunStore
from common.schema import SCHEMA_VERSION, base_record, metric_value


STRUCTURE_SCHEMA_VERSION = "1.0"
SAMPLING_METHOD = "list_id_multiplicative_hash_v1"
LIST_COLUMNS = (
    "format_version", "dimensions", "declared_lists", "list_id",
    "list_page", "list_offset", "start_page", "insert_page",
    "entry_pages", "physical_tuple_count", "usable_bytes", "used_bytes",
    "free_bytes",
)


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile / 100.0
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return float(ordered[lower])
    weight = position - lower
    return float(ordered[lower] * (1.0 - weight) + ordered[upper] * weight)


def _gini(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    total = sum(ordered)
    if total == 0:
        return 0.0
    weighted = sum((position + 1) * value for position, value in enumerate(ordered))
    return (2.0 * weighted) / (len(ordered) * total) - (len(ordered) + 1) / len(ordered)


def _available(value: Any, unit: str, aggregation: str) -> dict[str, Any]:
    return metric_value(
        value, unit, "ivfflat_list_stats",
        aggregation=aggregation, attribution_scope="index_structure",
    )


def _unavailable(unit: str, reason: str, aggregation: str) -> dict[str, Any]:
    return metric_value(
        None, unit, "ivfflat_list_stats", availability="unavailable",
        reason=reason, aggregation=aggregation,
        attribution_scope="index_structure",
    )


def summarize_structure(
    rows: Iterable[dict[str, Any]], *, diagnosis_id: str, build_id: str,
    index_oid: int, relfilenode: int, sample_rate: float,
    eligible_vectors: int | None, index_bytes: int | None,
    raw_vector_bytes: int | None,
) -> dict[str, Any]:
    """Create versioned structure metrics from per-list physical facts."""
    rows = list(rows)
    full_scan = sample_rate == 1.0
    counts = [int(row["physical_tuple_count"]) for row in rows]
    declared_lists = int(rows[0]["declared_lists"]) if rows else None
    physical_tuples = sum(counts)
    empty_lists = sum(value == 0 for value in counts)
    mean = statistics.fmean(counts) if counts else None
    stddev = statistics.pstdev(counts) if counts else None
    p50 = _percentile(counts, 50)
    p95 = _percentile(counts, 95)
    total_usable = sum(int(row["usable_bytes"]) for row in rows)
    total_used = sum(int(row["used_bytes"]) for row in rows)
    total_free = sum(int(row["free_bytes"]) for row in rows)
    top_n = min(10, len(counts))
    exact_reason = "metric requires sample_rate=1.0 full-list scan"
    eligibility_reason = "eligible_vectors is unavailable or not positive"

    distribution = {
        "min": _available(min(counts), "tuples", "min") if counts else _unavailable("tuples", "no list rows", "min"),
        "mean": _available(mean, "tuples", "mean") if counts else _unavailable("tuples", "no list rows", "mean"),
        "p50": _available(p50, "tuples", "p50") if counts else _unavailable("tuples", "no list rows", "p50"),
        "p90": _available(_percentile(counts, 90), "tuples", "p90") if counts else _unavailable("tuples", "no list rows", "p90"),
        "p95": _available(p95, "tuples", "p95") if counts else _unavailable("tuples", "no list rows", "p95"),
        "p99": _available(_percentile(counts, 99), "tuples", "p99") if counts else _unavailable("tuples", "no list rows", "p99"),
        "max": _available(max(counts), "tuples", "max") if counts else _unavailable("tuples", "no list rows", "max"),
        "stddev": _available(stddev, "tuples", "population_stddev") if counts else _unavailable("tuples", "no list rows", "population_stddev"),
    }
    metrics = {
        "physical_indexed_tuples": (
            _available(physical_tuples, "tuples", "sum") if full_scan
            else _unavailable("tuples", exact_reason, "sum")
        ),
        "empty_lists": (
            _available(empty_lists, "lists", "count") if full_scan
            else _unavailable("lists", exact_reason, "count")
        ),
        "empty_ratio": (
            _available(empty_lists / declared_lists, "ratio", "ratio")
            if full_scan and declared_lists else
            _unavailable("ratio", exact_reason if not full_scan else "declared_lists is zero", "ratio")
        ),
        "cv": (
            _available(stddev / mean, "ratio", "population_stddev/mean")
            if mean not in (None, 0) else
            _unavailable("ratio", "list tuple mean is zero", "population_stddev/mean")
        ),
        "gini": (
            _available(_gini(counts), "ratio", "gini") if counts
            else _unavailable("ratio", "no list rows", "gini")
        ),
        "max_over_avg": (
            _available(max(counts) / mean, "ratio", "max/mean")
            if mean not in (None, 0) else
            _unavailable("ratio", "list tuple mean is zero", "max/mean")
        ),
        "p95_over_p50": (
            _available(p95 / p50, "ratio", "p95/p50")
            if p50 not in (None, 0) else
            _unavailable("ratio", "P50 list tuple count is zero", "p95/p50")
        ),
        "top_n_tuple_share": (
            _available(sum(sorted(counts, reverse=True)[:top_n]) / physical_tuples,
                       "ratio", f"top_{top_n}_share")
            if physical_tuples else
            _unavailable("ratio", "physical tuple count is zero", f"top_{top_n}_share")
        ),
        "fill_ratio": (
            _available(total_used / total_usable, "ratio", "sum(used)/sum(usable)")
            if total_usable else
            _unavailable("ratio", "entry-page usable bytes is zero", "sum(used)/sum(usable)")
        ),
        "coverage_ratio": (
            _available(physical_tuples / eligible_vectors, "ratio", "physical/eligible")
            if full_scan and eligible_vectors is not None and eligible_vectors > 0 else
            _unavailable("ratio", exact_reason if not full_scan else eligibility_reason,
                         "physical/eligible")
        ),
        "amplification": (
            _available(index_bytes / raw_vector_bytes, "ratio", "index/raw_vector")
            if index_bytes is not None and raw_vector_bytes is not None and raw_vector_bytes > 0 else
            _unavailable("ratio", "index_bytes or raw_vector_bytes is unavailable",
                         "index/raw_vector")
        ),
        "entry_pages": _available(sum(int(row["entry_pages"]) for row in rows), "pages", "sum"),
        "usable_bytes": _available(total_usable, "bytes", "sum"),
        "used_bytes": _available(total_used, "bytes", "sum"),
        "free_bytes": _available(total_free, "bytes", "sum"),
    }
    return {
        **base_record(
            diagnosis_id, build_id=build_id, source="structure_summary",
            availability="available" if rows else "unavailable",
            reason=None if rows else "ivfflat_list_stats returned no rows",
        ),
        "structure_schema_version": STRUCTURE_SCHEMA_VERSION,
        "index_oid": index_oid,
        "relfilenode": relfilenode,
        "sample_rate": sample_rate,
        "scan_scope": "full" if full_scan else "sample",
        "sampling_method": "all_lists" if full_scan else SAMPLING_METHOD,
        "scanned_lists": len(rows),
        "declared_lists": declared_lists,
        "distribution": distribution,
        "metrics": metrics,
        "heuristic_version": "p1-v1",
    }


def _collect_structure_stats_impl(
    conn, *, diagnosis_id: str, build_id: str, index_oid: int,
    relfilenode: int, sample_rate: float, eligible_vectors: int | None,
    index_bytes: int | None, raw_vector_bytes: int | None,
    store: RunStore, output_prefix: str | Path,
) -> dict[str, Any]:
    """Validate Build identity, collect list facts, and persist P1 artifacts."""
    if not 0.0 < sample_rate <= 1.0 or not math.isfinite(sample_rate):
        raise ValueError("sample_rate must be finite and in (0, 1]")
    prefix = Path(output_prefix) / "structure"
    lists_path = prefix / "lists.jsonl"
    scan_path = prefix / "scan.json"
    summary_path = prefix / "summary.json"
    started = time.perf_counter()

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.oid, c.relfilenode, am.amname
              FROM pg_class c
              JOIN pg_am am ON am.oid = c.relam
             WHERE c.oid = %s
            """,
            (index_oid,),
        )
        identity = cur.fetchone()
    if identity is None:
        raise ValueError(f"index_oid {index_oid} is not catalog-visible")
    current_oid, current_relfilenode, amname = identity
    if int(current_oid) != int(index_oid) or int(current_relfilenode) != int(relfilenode):
        raise ValueError(
            "index identity changed after Build: "
            f"expected oid/relfilenode={index_oid}/{relfilenode}, "
            f"observed={current_oid}/{current_relfilenode}"
        )
    if amname != "ivfflat":
        raise ValueError(f"index_oid {index_oid} uses {amname}, not ivfflat")

    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM ivfflat_list_stats(%s::regclass, %s::real)",
            (index_oid, sample_rate),
        )
        rows = [dict(zip(LIST_COLUMNS, row)) for row in cur.fetchall()]

    records = []
    for row in rows:
        record = {
            **base_record(
                diagnosis_id, build_id=build_id,
                source="ivfflat_list_stats", availability="available",
            ),
            "structure_schema_version": STRUCTURE_SCHEMA_VERSION,
            "record_type": "ivfflat_list",
            "index_oid": int(index_oid),
            "relfilenode": int(relfilenode),
            "sample_rate": sample_rate,
            "scan_scope": "full" if sample_rate == 1.0 else "sample",
            "sampling_method": (
                "all_lists" if sample_rate == 1.0 else SAMPLING_METHOD
            ),
            **row,
        }
        store.append_jsonl(lists_path, record)
        records.append(record)
    store.mark_complete(lists_path)

    summary = summarize_structure(
        records, diagnosis_id=diagnosis_id, build_id=build_id,
        index_oid=index_oid, relfilenode=relfilenode, sample_rate=sample_rate,
        eligible_vectors=eligible_vectors, index_bytes=index_bytes,
        raw_vector_bytes=raw_vector_bytes,
    )
    scan = {
        **base_record(
            diagnosis_id, build_id=build_id, source="structure_collector",
            availability="available" if records else "unavailable",
            reason=None if records else "ivfflat_list_stats returned no rows",
        ),
        "structure_schema_version": STRUCTURE_SCHEMA_VERSION,
        "status": "success" if records else "partial",
        "index_oid": int(index_oid),
        "relfilenode": int(relfilenode),
        "sample_rate": sample_rate,
        "scan_scope": "full" if sample_rate == 1.0 else "sample",
        "sampling_method": (
            "all_lists" if sample_rate == 1.0 else SAMPLING_METHOD
        ),
        "scanned_lists": len(records),
        "declared_lists": summary["declared_lists"],
        "collector_elapsed_ms": (time.perf_counter() - started) * 1000.0,
        "exact_metrics_available": sample_rate == 1.0 and bool(records),
    }
    store.write_json(scan_path, scan)
    store.write_json(summary_path, summary)
    return {"scan": scan, "summary": summary, "lists": records}


def collect_structure_stats(
    conn, *, diagnosis_id: str, build_id: str, index_oid: int,
    relfilenode: int, sample_rate: float, eligible_vectors: int | None,
    index_bytes: int | None, raw_vector_bytes: int | None,
    store: RunStore, output_prefix: str | Path,
) -> dict[str, Any]:
    """Collect structure facts and persist an explicit failed status on error."""
    if not 0.0 < sample_rate <= 1.0 or not math.isfinite(sample_rate):
        raise ValueError("sample_rate must be finite and in (0, 1]")
    try:
        return _collect_structure_stats_impl(
            conn, diagnosis_id=diagnosis_id, build_id=build_id,
            index_oid=index_oid, relfilenode=relfilenode,
            sample_rate=sample_rate, eligible_vectors=eligible_vectors,
            index_bytes=index_bytes, raw_vector_bytes=raw_vector_bytes,
            store=store, output_prefix=output_prefix,
        )
    except Exception as exc:
        reason = str(exc)
        prefix = Path(output_prefix) / "structure"
        store.mark_incomplete(prefix / "lists.jsonl", reason)
        scan = {
            **base_record(
                diagnosis_id, build_id=build_id, source="structure_collector",
                availability="unavailable", reason=reason,
            ),
            "structure_schema_version": STRUCTURE_SCHEMA_VERSION,
            "status": "failed",
            "index_oid": int(index_oid),
            "relfilenode": int(relfilenode),
            "sample_rate": sample_rate,
            "scan_scope": "full" if sample_rate == 1.0 else "sample",
            "sampling_method": (
                "all_lists" if sample_rate == 1.0 else SAMPLING_METHOD
            ),
            "scanned_lists": 0,
            "declared_lists": None,
            "collector_elapsed_ms": 0.0,
            "exact_metrics_available": False,
        }
        summary = {
            **base_record(
                diagnosis_id, build_id=build_id, source="structure_summary",
                availability="unavailable", reason=reason,
            ),
            "structure_schema_version": STRUCTURE_SCHEMA_VERSION,
            "index_oid": int(index_oid),
            "relfilenode": int(relfilenode),
            "sample_rate": sample_rate,
            "scan_scope": "full" if sample_rate == 1.0 else "sample",
            "sampling_method": (
                "all_lists" if sample_rate == 1.0 else SAMPLING_METHOD
            ),
            "scanned_lists": 0,
            "declared_lists": None,
            "distribution": {},
            "metrics": {},
            "heuristic_version": "p1-v1",
        }
        store.write_json(prefix / "scan.json", scan)
        store.write_json(prefix / "summary.json", summary)
        return {"scan": scan, "summary": summary, "lists": [], "error": reason}
