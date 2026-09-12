#!/usr/bin/env python3
"""Build one IVFFlat index while preserving P0 evidence."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import re
import sys
import threading
import time
from typing import Any

from psycopg import sql

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from aggregate import aggregate_build_samples
from common.config import metric_config, metric_names
from common.db import INDEX_NAME, TABLE_NAME, connect_dsn
from common.run_store import RunStore
from common.schema import BuildContext, SCHEMA_VERSION, new_run_id, utc_now
from monitor import monitor_build


_TEMP_FILE = re.compile(r'temporary file: path ".+?" size (\d+)')


def _statement_stats_snapshot(conn, index_name: str,
                              enabled: bool) -> dict[str, Any]:
    if not enabled:
        return {"availability": "unavailable", "reason": "statement stats capability unavailable"}
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT coalesce(sum(calls), 0),
                       coalesce(sum(total_exec_time), 0),
                       coalesce(sum(shared_blks_hit), 0),
                       coalesce(sum(shared_blks_read), 0),
                       coalesce(sum(shared_blks_dirtied), 0),
                       coalesce(sum(shared_blks_written), 0),
                       coalesce(sum(local_blks_hit), 0),
                       coalesce(sum(local_blks_read), 0),
                       coalesce(sum(local_blks_dirtied), 0),
                       coalesce(sum(local_blks_written), 0),
                       coalesce(sum(temp_blks_read), 0),
                       coalesce(sum(temp_blks_written), 0),
                       coalesce(sum(wal_records), 0),
                       coalesce(sum(wal_fpi), 0),
                       coalesce(sum(wal_bytes), 0)
                  FROM pg_stat_statements
                 WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                   AND query ILIKE 'CREATE INDEX%%'
                   AND query ILIKE %s
                """,
                (f"%{index_name}%",),
            )
            row = cur.fetchone()
        names = (
            "calls", "total_exec_time_ms",
            "shared_blks_hit", "shared_blks_read", "shared_blks_dirtied",
            "shared_blks_written", "local_blks_hit", "local_blks_read",
            "local_blks_dirtied", "local_blks_written",
            "temp_blks_read", "temp_blks_written",
            "wal_records", "wal_fpi", "wal_bytes",
        )
        return {
            "availability": "available",
            "reason": None,
            **{name: float(value) for name, value in zip(names, row)},
        }
    except Exception as exc:
        return {"availability": "unavailable", "reason": str(exc)}


def _stats_delta(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    if before.get("availability") != "available" or after.get("availability") != "available":
        return {
            "availability": "unavailable",
            "reason": after.get("reason") or before.get("reason"),
            "attribution_scope": "statement",
        }
    result: dict[str, Any] = {
        "availability": "available",
        "reason": None,
        "attribution_scope": "statement",
        "limitation": "matched by normalized CREATE INDEX text; concurrent identical builds may overlap",
    }
    for key, value in after.items():
        if key in ("availability", "reason"):
            continue
        previous = before.get(key)
        result[key] = max(0, value - previous) if previous is not None else None
    return result


def _log_position(capabilities: dict[str, Any]) -> tuple[Path | None, int | None]:
    log = capabilities.get("capabilities", {}).get("log", {})
    if not log.get("available"):
        return None, None
    path = Path(log["path"])
    try:
        return path, path.stat().st_size
    except OSError:
        return None, None


def _log_delta(path: Path | None, offset: int | None) -> dict[str, Any]:
    if path is None or offset is None:
        return {
            "availability": "unavailable",
            "reason": "log capability unavailable",
            "attribution_scope": "instance_delta",
        }
    try:
        with path.open("rb") as stream:
            if path.stat().st_size < offset:
                return {
                    "availability": "unavailable",
                    "reason": "log rotated during build",
                    "attribution_scope": "instance_delta",
                }
            stream.seek(offset)
            text = stream.read().decode("utf-8", errors="replace")
        temp_sizes = [int(value) for value in _TEMP_FILE.findall(text)]
        interesting = [
            line for line in text.splitlines()
            if "temporary file:" in line or "sort" in line.lower()
        ]
        return {
            "availability": "available",
            "reason": None,
            "attribution_scope": "instance_delta",
            "limitation": "log delta can include other sessions",
            "temp_files": len(temp_sizes),
            "temp_bytes": sum(temp_sizes),
            "events": interesting,
        }
    except OSError as exc:
        return {
            "availability": "unavailable",
            "reason": str(exc),
            "attribution_scope": "instance_delta",
        }


def _relative(store: RunStore, path: Path) -> Path:
    return path.resolve().relative_to(store.root)


def _force_backend_stats_flush(conn, enabled: bool) -> dict[str, Any]:
    if not enabled:
        return {
            "availability": "unavailable",
            "reason": "pg_stat_force_next_flush() capability unavailable",
            "attribution_scope": "backend",
        }
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_stat_force_next_flush()")
            cur.fetchone()
        return {
            "availability": "available",
            "reason": None,
            "attribution_scope": "backend",
            "timing": "after CREATE INDEX returned, before final counter sample",
        }
    except Exception as exc:
        return {
            "availability": "unavailable",
            "reason": str(exc),
            "attribution_scope": "backend",
        }


def _collect_index_metadata(conn, index_name: str) -> dict[str, Any]:
    """Collect catalog-visible index identity and size after CREATE commits."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.oid, c.relfilenode, pg_relation_size(c.oid),
                       c.relpages, current_setting('block_size')::bigint
                  FROM pg_class c
                 WHERE c.oid = to_regclass(%s)
                """,
                (index_name,),
            )
            row = cur.fetchone()
        if row is None:
            return {
                "availability": "unavailable",
                "reason": "index is not catalog-visible after successful CREATE INDEX",
                "index_oid": None,
                "relfilenode": None,
                "index_bytes": None,
                "index_pages": None,
                "catalog_relpages": None,
            }
        index_oid, relfilenode, index_bytes, catalog_relpages, block_size = row
        core = (index_oid, relfilenode, index_bytes)
        if any(value is None for value in core):
            return {
                "availability": "unavailable",
                "reason": "catalog query returned incomplete core index metadata",
                "index_oid": index_oid,
                "relfilenode": relfilenode,
                "index_bytes": index_bytes,
                "index_pages": None,
                "catalog_relpages": catalog_relpages,
            }
        return {
            "availability": "available",
            "reason": None,
            "collected_after_commit": True,
            "index_oid": int(index_oid),
            "relfilenode": int(relfilenode),
            "index_bytes": int(index_bytes),
            "index_pages": math.ceil(int(index_bytes) / int(block_size)),
            "catalog_relpages": int(catalog_relpages),
            "block_size": int(block_size),
        }
    except Exception as exc:
        return {
            "availability": "unavailable",
            "reason": str(exc),
            "index_oid": None,
            "relfilenode": None,
            "index_bytes": None,
            "index_pages": None,
            "catalog_relpages": None,
        }


def build_index(dsn: str, dataset: str, metric: str, dimension: int,
                requested_rows: int, lists: int, output: Path, *,
                actual_rows: int | None = None,
                eligible_vectors: int | None = None,
                diagnosis_id: str | None = None, build_id: str | None = None,
                preflight: dict[str, Any] | None = None,
                capabilities: dict[str, Any] | None = None,
                store: RunStore | None = None,
                sample_interval: float = 0.5,
                trust_assign_progress: bool = False,
                maintenance_work_mem: str | None = None) -> dict[str, Any]:
    """Execute one Build Run and keep database and collector outcomes separate."""
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    diagnosis_id = diagnosis_id or new_run_id("diag")
    build_id = build_id or new_run_id("build")
    actual_rows = (
        int(actual_rows) if actual_rows is not None
        else int(preflight["actual_rows"]) if preflight else int(requested_rows)
    )
    eligible_vectors = (
        int(eligible_vectors) if eligible_vectors is not None
        else int(preflight["eligible_vectors"]) if preflight else int(actual_rows)
    )
    context = BuildContext(
        diagnosis_id, build_id, output, dataset, metric,
        requested_rows, actual_rows, eligible_vectors, dimension, lists,
    )
    local_store = store is None
    store = store or RunStore(output)
    prefix = Path() if local_store else _relative(store, output)
    progress_relative = prefix / "raw" / "progress.jsonl"
    resources_relative = prefix / "raw" / "resources.jsonl"
    aggregate_relative = prefix / "raw" / "aggregate.json"
    build_relative = prefix / "raw" / "build.json"
    capabilities = capabilities or {"capabilities": {}}

    notices: list[str] = []
    stop = threading.Event()
    baseline_ready = threading.Event()
    monitor_state: dict[str, Any] = {}
    database_status = "success"
    error = None
    sqlstate = None
    index_metadata = {
        "availability": "unavailable",
        "reason": "CREATE INDEX did not complete successfully",
        "index_oid": None,
        "relfilenode": None,
        "index_bytes": None,
        "index_pages": None,
        "catalog_relpages": None,
    }
    leader_pid = None
    runner_started_at = utc_now()
    build_started_at = None
    build_finished_at = None
    elapsed = 0.0
    statement_before: dict[str, Any] = {}
    statement_after: dict[str, Any] = {}
    backend_stats_flush = {
        "availability": "unavailable",
        "reason": "build connection did not reach counter finalization",
        "attribution_scope": "backend",
    }
    log_path, log_offset = _log_position(capabilities)

    with connect_dsn(dsn, autocommit=True) as conn:
        conn.add_notice_handler(
            lambda diagnostic: notices.append(str(diagnostic.message_primary))
        )
        with conn.cursor() as cur:
            cur.execute("SELECT pg_backend_pid()")
            leader_pid = int(cur.fetchone()[0])
            cur.execute(
                "SELECT set_config('application_name', %s, false)",
                (f"project2:{build_id}",),
            )
            if maintenance_work_mem is not None:
                cur.execute(
                    "SELECT set_config('maintenance_work_mem', %s, false)",
                    (maintenance_work_mem,),
                )
            cur.execute(
                sql.SQL("DROP INDEX IF EXISTS {}").format(sql.Identifier(INDEX_NAME))
            )

        statement_enabled = bool(
            capabilities.get("capabilities", {}).get("statement_stats", {}).get("available")
        )
        statement_before = _statement_stats_snapshot(conn, INDEX_NAME, statement_enabled)
        monitor = threading.Thread(
            target=monitor_build,
            kwargs={
                "dsn": dsn,
                "backend_pid": leader_pid,
                "stop": stop,
                "diagnosis_id": diagnosis_id,
                "build_id": build_id,
                "store": store,
                "progress_path": progress_relative.as_posix(),
                "resources_path": resources_relative.as_posix(),
                "capability_map": capabilities,
                "interval": sample_interval,
                "trust_assign_progress": trust_assign_progress,
                "state": monitor_state,
                "baseline_ready": baseline_ready,
            },
            daemon=True,
        )
        monitor.start()
        if not baseline_ready.wait(timeout=max(5.0, sample_interval * 4)):
            monitor_state.setdefault("collector_errors", []).append({
                "source": "collector_baseline",
                "error": "timed out waiting for the pre-build baseline sample",
            })
        started = time.perf_counter()
        build_started_at = utc_now()
        try:
            opclass = sql.SQL(metric_config(metric)["opclass"])
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "CREATE INDEX {} ON {} USING ivfflat "
                        "(embedding {}) WITH (lists = {})"
                    ).format(
                        sql.Identifier(INDEX_NAME),
                        sql.Identifier(TABLE_NAME),
                        opclass,
                        sql.Literal(lists),
                    )
                )
        except KeyboardInterrupt:
            database_status = "cancelled"
            error = "cancelled by user"
        except Exception as exc:
            database_status = "failed"
            error = str(exc)
            sqlstate = getattr(exc, "sqlstate", None)
        finally:
            elapsed = time.perf_counter() - started
            build_finished_at = utc_now()
            backend_stats_flush = _force_backend_stats_flush(
                conn,
                bool(
                    capabilities.get("capabilities", {})
                    .get("backend_stats_flush", {}).get("available")
                ),
            )
            stop.set()
            monitor.join()

        statement_after = _statement_stats_snapshot(conn, INDEX_NAME, statement_enabled)
        if database_status == "success":
            # autocommit=True means CREATE INDEX is committed and visible here.
            index_metadata = _collect_index_metadata(conn, INDEX_NAME)

    runner_finished_at = utc_now()
    run_level_stats = {
        "statement_delta": _stats_delta(statement_before, statement_after),
        "log_delta": _log_delta(log_path, log_offset),
    }
    aggregate = aggregate_build_samples(
        store.root / progress_relative,
        store.root / resources_relative,
        diagnosis_id=diagnosis_id,
        build_id=build_id,
        started_at=build_started_at,
        finished_at=build_finished_at,
        sample_interval_seconds=sample_interval,
        run_level_stats=run_level_stats,
        pg_counter_finalization=backend_stats_flush,
    )
    store.write_json(aggregate_relative, aggregate)

    result = {
        "schema_version": SCHEMA_VERSION,
        "diagnosis_id": diagnosis_id,
        "build_id": build_id,
        "timestamp_utc": runner_finished_at,
        "source": "project2_build_runner",
        "availability": "available",
        "reason": None,
        "status": database_status,
        "database_status": database_status,
        "collection_status": (
            "collector_interrupted"
            if monitor_state.get("collector_interrupted") else "partial"
            if monitor_state.get("partial_resources") or monitor_state.get("collector_errors")
            else "success"
        ),
        "dataset": dataset,
        "metric": metric,
        "requested_rows": requested_rows,
        "actual_rows": actual_rows,
        "eligible_vectors": eligible_vectors,
        "rows": actual_rows,
        "dimension": dimension,
        "lists": lists,
        "leader_pid": leader_pid,
        "discovered_worker_pids": [pid for pid in monitor_state.get("discovered_pids", []) if pid != leader_pid],
        "runner_started_at": runner_started_at,
        "runner_finished_at": runner_finished_at,
        "build_started_at": build_started_at,
        "build_finished_at": build_finished_at,
        # Compatibility aliases now point to the CREATE INDEX interval.
        "started_at": build_started_at,
        "finished_at": build_finished_at,
        "total_elapsed_ms": elapsed * 1000,
        "build_seconds": elapsed,
        "failure_phase": monitor_state.get("last_phase") if database_status != "success" else None,
        "sqlstate": sqlstate,
        "error": error,
        "notices": notices,
        "index_metadata": index_metadata,
        "index_oid": index_metadata["index_oid"],
        "relfilenode": index_metadata["relfilenode"],
        "index_bytes": index_metadata["index_bytes"],
        "index_pages": index_metadata["index_pages"],
        "memory_estimate": preflight.get("memory_estimate") if preflight else None,
        "phase_summaries": aggregate["phase_summaries"],
        "progress_observation": aggregate.get("progress_observation", {}),
        "collector_errors": monitor_state.get("collector_errors", []),
        "process_group_peak_summed_rss_bytes": aggregate[
            "process_group_peak_summed_rss_bytes"
        ],
        # Compatibility aliases for existing schema 1.0 consumers.
        "process_group_peak_rss_bytes": aggregate[
            "process_group_peak_summed_rss_bytes"
        ],
        "process_group_peak_pss_bytes": aggregate["process_group_peak_pss_bytes"],
        "peak_postgres_rss_bytes": aggregate[
            "process_group_peak_summed_rss_bytes"
        ],
        "rss_aggregation_semantics": aggregate["rss_aggregation_semantics"],
        "process_group_peak_availability": aggregate["process_group_peak_availability"],
        "build_run_pg_delta": aggregate["build_run_pg_delta"],
        "backend_stats_flush": backend_stats_flush,
        "phase_timing_accuracy": aggregate["timing_accuracy"],
        "run_level_stats": run_level_stats,
    }
    if database_status == "success" and index_metadata["availability"] != "available":
        result["collection_status"] = "partial"
        result["reason"] = f"index metadata collection failed: {index_metadata['reason']}"
    store.write_json(build_relative, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--metric", choices=metric_names(), required=True)
    parser.add_argument("--dimension", type=int, required=True)
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--lists", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sample-interval", type=float, default=0.5)
    parser.add_argument("--maintenance-work-mem")
    parser.add_argument("--trust-assign-progress", action="store_true")
    args = parser.parse_args()
    print(json.dumps(build_index(
        args.dsn, args.dataset, args.metric, args.dimension,
        args.rows, args.lists, args.output,
        sample_interval=args.sample_interval,
        trust_assign_progress=args.trust_assign_progress,
        maintenance_work_mem=args.maintenance_work_mem,
    ), indent=2))


if __name__ == "__main__":
    main()
