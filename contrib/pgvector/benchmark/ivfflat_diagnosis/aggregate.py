"""Scope-aware aggregation of IVFFlat build samples."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
import json
from pathlib import Path
from typing import Any, Callable

from common.schema import SCHEMA_VERSION, metric_value, utc_now


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records = []
    with path.open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSONL at {path}:{line_number}: {exc}") from exc
    return records


def _time(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def _counter(record: dict[str, Any], name: str) -> float | None:
    value = record.get(name)
    return float(value) if value is not None else None


def _nested_counter(record: dict[str, Any], group: str, name: str) -> float | None:
    value = record.get(group)
    if not value:
        return None
    if group == "pg_io":
        value = value.get("totals", {})
    raw = value.get(name)
    return float(raw) if raw is not None else None


def _delta(previous: dict[str, Any], current: dict[str, Any],
           getter: Callable[[dict[str, Any], str], float | None],
           name: str) -> float | None:
    left = getter(previous, name)
    right = getter(current, name)
    if left is None or right is None or right < left:
        return None
    return right - left


def _add(summary: dict[str, Any], name: str, value: float | None) -> None:
    if value is not None:
        summary[name] = (summary[name] or 0) + value


def _scoped_delta(rows: list[dict[str, Any]], group: str, name: str,
                  unit: str, scope: str,
                  window_reason: str | None = None) -> dict[str, Any]:
    if len(rows) < 2:
        return metric_value(
            None, unit, group, availability="unavailable",
            reason="fewer than two in-window backend samples",
            aggregation="sampled_counter_delta", attribution_scope=scope,
        )
    value = _delta(
        rows[0], rows[-1],
        lambda row, key: _nested_counter(row, group, key), name,
    )
    if value is None:
        return metric_value(
            None, unit, group, availability="unavailable",
            reason="counter missing, reset, or backend identity changed",
            aggregation="sampled_counter_delta", attribution_scope=scope,
        )
    return metric_value(
        int(value), unit, group, aggregation="sampled_counter_delta",
        attribution_scope=scope, reason=window_reason,
    )


def _counter_window(rows: list[dict[str, Any]], start: float,
                    finish: float) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    ordered = sorted(rows, key=lambda row: row["timestamp_utc"])
    explicit_baselines = [
        row for row in ordered
        if row.get("counter_sample_role") == "baseline"
        and _time(row["timestamp_utc"]) <= start
    ]
    explicit_finals = [
        row for row in ordered
        if row.get("counter_sample_role") == "final"
        and _time(row["timestamp_utc"]) >= finish
    ]
    if explicit_baselines and explicit_finals:
        baseline = explicit_baselines[-1]
        final = explicit_finals[0]
        return [baseline, final], {
            "availability": "available",
            "reason": None,
            "baseline_source": "explicit_pre_build_sample",
            "final_source": "explicit_post_build_sample",
            "baseline_timestamp_utc": baseline["timestamp_utc"],
            "final_timestamp_utc": final["timestamp_utc"],
        }

    before = [row for row in ordered if _time(row["timestamp_utc"]) <= start]
    after = [row for row in ordered if _time(row["timestamp_utc"]) >= finish]
    baseline = before[-1] if before else (ordered[0] if ordered else None)
    final = after[0] if after else (ordered[-1] if ordered else None)
    if baseline is None or final is None or baseline is final:
        return [], {
            "availability": "unavailable",
            "reason": "no distinct baseline and final backend counter samples",
            "baseline_source": None,
            "final_source": None,
            "baseline_timestamp_utc": baseline.get("timestamp_utc") if baseline else None,
            "final_timestamp_utc": final.get("timestamp_utc") if final else None,
        }
    reason = (
        "partial sampled counter window: explicit pre-build baseline or "
        "post-build final sample is missing"
    )
    return [baseline, final], {
        "availability": "partial",
        "reason": reason,
        "baseline_source": (
            "last_sample_at_or_before_build" if before else "first_observed_backend_sample"
        ),
        "final_source": (
            "first_sample_at_or_after_build" if after else "last_observed_backend_sample"
        ),
        "baseline_timestamp_utc": baseline["timestamp_utc"],
        "final_timestamp_utc": final["timestamp_utc"],
    }


def _sum_available(metrics: list[dict[str, Any]], unit: str,
                   source: str) -> dict[str, Any]:
    available = [item["value"] for item in metrics if item["availability"] == "available"]
    if not available:
        return metric_value(
            None, unit, source, availability="unavailable",
            reason="no backend has a reliable sampled counter delta",
            aggregation="sum_backend_deltas", attribution_scope="build_run",
        )
    partial = any(item.get("reason") for item in metrics)
    return metric_value(
        int(sum(available)), unit, source, aggregation="sum_backend_deltas",
        attribution_scope="build_run",
        reason=(
            "partial: one or more backend counter windows lack an explicit boundary"
            if partial else None if len(available) == len(metrics)
            else f"partial: {len(available)} of {len(metrics)} backend deltas available"
        ),
    )


def aggregate_build_samples(progress_path: Path, resources_path: Path, *,
                            diagnosis_id: str, build_id: str,
                            started_at: str, finished_at: str,
                            sample_interval_seconds: float,
                            run_level_stats: dict[str, Any] | None = None,
                            pg_counter_finalization: dict[str, Any] | None = None) -> dict[str, Any]:
    """Aggregate only samples inside the real CREATE INDEX wall-time window.

    /proc counters and waits are assigned to sampled phases. PostgreSQL backend
    IO/WAL counters are deliberately retained at backend/build-run scope.
    """
    start = _time(started_at)
    finish = _time(finished_at)
    if finish < start:
        raise ValueError("finished_at precedes started_at")
    pg_counter_finalization = pg_counter_finalization or {
        "availability": "available",
        "reason": None,
        "attribution_scope": "backend",
        "timing": "caller-certified final counter visibility",
    }

    all_progress = _load_jsonl(progress_path)
    all_resources = _load_jsonl(resources_path)
    progress = sorted(
        (row for row in all_progress if start <= _time(row["timestamp_utc"]) <= finish),
        key=lambda row: row["timestamp_utc"],
    )
    resources = sorted(
        (row for row in all_resources if start <= _time(row["timestamp_utc"]) <= finish),
        key=lambda row: (int(row["pid"]), row["timestamp_utc"]),
    )

    transitions: list[tuple[str, float]] = []
    for row in progress:
        phase = row.get("native_phase")
        if not phase:
            continue
        timestamp = min(finish, max(start, _time(row["timestamp_utc"])))
        if not transitions or transitions[-1][0] != phase:
            transitions.append((phase, timestamp))

    segments: list[tuple[str, float, float]] = []
    for index, (phase, phase_start) in enumerate(transitions):
        phase_end = transitions[index + 1][1] if index + 1 < len(transitions) else finish
        phase_start = min(finish, max(start, phase_start))
        phase_end = min(finish, max(phase_start, phase_end))
        segments.append((phase, phase_start, phase_end))

    phase_summaries: dict[str, dict[str, Any]] = {}
    for phase, phase_start, phase_end in segments:
        summary = phase_summaries.setdefault(phase, {
            "wall_time_ms": 0.0,
            "boundary_source": "sampled_progress_transition_clipped_to_create_index",
            "maximum_boundary_error_ms": sample_interval_seconds * 1000,
            "timing_accuracy": "sampling_approximation",
            "sample_interval_seconds": sample_interval_seconds,
            "cpu_seconds": None,
            "proc_read_bytes": None,
            "proc_write_bytes": None,
            "peak_rss_bytes": None,
            "peak_pss_bytes": None,
            "wait_samples": {},
            "worker_pids": [],
            "counter_gaps": 0,
            "attribution_scope": "phase",
        })
        summary["wall_time_ms"] += (phase_end - phase_start) * 1000

    by_process: dict[tuple[int, int | None], list[dict[str, Any]]] = defaultdict(list)
    for row in resources:
        by_process[(int(row["pid"]), row.get("process_start_ticks"))].append(row)

    all_by_process: dict[tuple[int, int | None], list[dict[str, Any]]] = defaultdict(list)
    for row in all_resources:
        all_by_process[(int(row["pid"]), row.get("process_start_ticks"))].append(row)

    discovered_workers: set[int] = {
        int(row["pid"]) for row in all_resources if row.get("role") == "worker"
    }
    backend_deltas: list[dict[str, Any]] = []
    for (pid, process_start_ticks), rows in all_by_process.items():
        rows.sort(key=lambda row: row["timestamp_utc"])
        role = "worker" if any(row.get("role") == "worker" for row in rows) else "leader"
        counter_rows, counter_window = _counter_window(rows, start, finish)
        backend_deltas.append({
            "pid": pid,
            "process_start_ticks": process_start_ticks,
            "role": role,
            "attribution_scope": "backend",
            "sample_count": len(rows),
            "counter_window": counter_window,
            "pg_read_bytes": _scoped_delta(
                counter_rows, "pg_io", "read_bytes", "bytes", "backend",
                counter_window.get("reason"),
            ),
            "pg_write_bytes": _scoped_delta(
                counter_rows, "pg_io", "write_bytes", "bytes", "backend",
                counter_window.get("reason"),
            ),
            "wal_bytes": _scoped_delta(
                counter_rows, "pg_wal", "wal_bytes", "bytes", "backend",
                counter_window.get("reason"),
            ),
        })

    for (pid, _), rows in by_process.items():
        rows.sort(key=lambda row: row["timestamp_utc"])
        for previous, current in zip(rows, rows[1:]):
            phase = current.get("native_phase")
            if not phase or phase not in phase_summaries or previous.get("native_phase") != phase:
                continue
            summary = phase_summaries[phase]
            cpu_user = _delta(previous, current, _counter, "cpu_user_seconds")
            cpu_system = _delta(previous, current, _counter, "cpu_system_seconds")
            read_bytes = _delta(previous, current, _counter, "proc_read_bytes")
            write_bytes = _delta(previous, current, _counter, "proc_write_bytes")
            if cpu_user is None or cpu_system is None:
                summary["counter_gaps"] += 1
            else:
                _add(summary, "cpu_seconds", cpu_user + cpu_system)
            if read_bytes is None:
                summary["counter_gaps"] += 1
            else:
                _add(summary, "proc_read_bytes", int(read_bytes))
            if write_bytes is None:
                summary["counter_gaps"] += 1
            else:
                _add(summary, "proc_write_bytes", int(write_bytes))
            if current.get("role") == "worker":
                summary["worker_pids"].append(pid)

    by_timestamp: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in resources:
        by_timestamp[row["timestamp_utc"]].append(row)
        phase = row.get("native_phase")
        if phase in phase_summaries and row.get("wait_event_type"):
            key = f"{row['wait_event_type']}:{row.get('wait_event') or 'unknown'}"
            waits = Counter(phase_summaries[phase]["wait_samples"])
            waits[key] += 1
            phase_summaries[phase]["wait_samples"] = dict(waits)

    group_peak_rss = None
    group_peak_pss = None
    for rows in by_timestamp.values():
        rss_values = [row["rss_bytes"] for row in rows if row.get("rss_bytes") is not None]
        pss_values = [row["pss_bytes"] for row in rows if row.get("pss_bytes") is not None]
        if rss_values:
            group_peak_rss = max(group_peak_rss or 0, sum(rss_values))
        if pss_values:
            group_peak_pss = max(group_peak_pss or 0, sum(pss_values))
        for phase in {row.get("native_phase") for row in rows}:
            if phase not in phase_summaries:
                continue
            phase_rows = [row for row in rows if row.get("native_phase") == phase]
            phase_rss = [row["rss_bytes"] for row in phase_rows if row.get("rss_bytes") is not None]
            phase_pss = [row["pss_bytes"] for row in phase_rows if row.get("pss_bytes") is not None]
            summary = phase_summaries[phase]
            if phase_rss:
                summary["peak_rss_bytes"] = max(summary["peak_rss_bytes"] or 0, sum(phase_rss))
            if phase_pss:
                summary["peak_pss_bytes"] = max(summary["peak_pss_bytes"] or 0, sum(phase_pss))

    for summary in phase_summaries.values():
        wall_seconds = summary["wall_time_ms"] / 1000
        summary["effective_cpu_cores"] = (
            summary["cpu_seconds"] / wall_seconds
            if wall_seconds > 0 and summary["cpu_seconds"] is not None else None
        )
        summary["worker_pids"] = sorted(set(summary["worker_pids"]))
        summary["field_availability"] = {
            name: {
                "availability": "available" if summary[name] is not None else "unavailable",
                "reason": (
                    None if summary[name] is not None
                    else "no reliable in-window samples or counter deltas"
                ),
                "attribution_scope": "phase",
            }
            for name in (
                "cpu_seconds", "proc_read_bytes", "proc_write_bytes",
                "peak_rss_bytes", "peak_pss_bytes",
            )
        }

    pg_read_metrics = [row["pg_read_bytes"] for row in backend_deltas]
    pg_write_metrics = [row["pg_write_bytes"] for row in backend_deltas]
    wal_metrics = [row["wal_bytes"] for row in backend_deltas]
    build_run_pg_delta = {
        "attribution_scope": "build_run",
        "counter_window": "pre-build baseline to first reliable post-build final sample",
        "counter_finalization": pg_counter_finalization,
        "sampling_note": (
            "phase metrics use the strict CREATE INDEX window; PG backend IO/WAL "
            "uses separately marked baseline/final counter samples"
        ),
        "pg_read_bytes": _sum_available(pg_read_metrics, "bytes", "pg_stat_get_backend_io"),
        "pg_write_bytes": _sum_available(pg_write_metrics, "bytes", "pg_stat_get_backend_io"),
        "wal_bytes": _sum_available(wal_metrics, "bytes", "pg_stat_get_backend_wal"),
    }
    if pg_counter_finalization.get("availability") != "available":
        reason = (
            pg_counter_finalization.get("reason")
            or "final backend counter visibility was not established"
        )
        for name, source in (
            ("pg_read_bytes", "pg_stat_get_backend_io"),
            ("pg_write_bytes", "pg_stat_get_backend_io"),
            ("wal_bytes", "pg_stat_get_backend_wal"),
        ):
            build_run_pg_delta[name] = metric_value(
                None, "bytes", source, availability="unavailable",
                reason=reason, aggregation="baseline_to_post_build_final",
                attribution_scope="build_run",
            )

    observed_duration_ms = max(0.0, (finish - start) * 1000)
    phase_duration_ms = min(
        observed_duration_ms,
        sum(item["wall_time_ms"] for item in phase_summaries.values()),
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "diagnosis_id": diagnosis_id,
        "build_id": build_id,
        "timestamp_utc": utc_now(),
        "source": "project2_phase_aggregator",
        "availability": "available" if progress or resources else "not_sampled",
        "reason": None if progress or resources else "no in-window progress or resource samples",
        "build_started_at": started_at,
        "build_finished_at": finished_at,
        "sample_interval_seconds": sample_interval_seconds,
        "timing_accuracy": {
            "kind": "sampling_approximation",
            "maximum_boundary_error_ms": sample_interval_seconds * 1000,
            "timeline_clipped_to_create_index": True,
        },
        "input_progress_samples": len(all_progress),
        "input_resource_samples": len(all_resources),
        "progress_samples": len(progress),
        "resource_samples": len(resources),
        "observed_duration_ms": observed_duration_ms,
        "phase_duration_ms": phase_duration_ms,
        "unassigned_duration_ms": max(0.0, observed_duration_ms - phase_duration_ms),
        "phase_summaries": phase_summaries,
        "progress_observation": {
            "runtime_observed": bool(transitions),
            "observed_phases": sorted(phase_summaries),
            "load_progress_runtime_observed": any(
                row.get('native_phase') == 'loading tuples'
                and row.get('progress_percent') is not None for row in progress
            ),
            "availability": "available" if transitions else "unavailable",
            "reason": None if transitions else "no native phase sampled within CREATE INDEX",
        },
        "discovered_worker_pids": sorted(discovered_workers),
        "process_group_peak_summed_rss_bytes": group_peak_rss,
        # Compatibility alias for schema 1.0 readers.
        "process_group_peak_rss_bytes": group_peak_rss,
        "process_group_peak_pss_bytes": group_peak_pss,
        "rss_aggregation_semantics": (
            "sum of per-process RSS at each sample, then maximum across samples; "
            "shared pages may be counted more than once"
        ),
        "process_group_peak_availability": {
            "rss": {
                "availability": "available" if group_peak_rss is not None else "unavailable",
                "reason": None if group_peak_rss is not None else "no readable in-window RSS samples",
                "attribution_scope": "build_run",
            },
            "pss": {
                "availability": "available" if group_peak_pss is not None else "unavailable",
                "reason": None if group_peak_pss is not None else "no readable in-window PSS samples",
                "attribution_scope": "build_run",
            },
        },
        "pg_backend_deltas": backend_deltas,
        "build_run_pg_delta": build_run_pg_delta,
        "run_level_stats": run_level_stats or {},
    }
