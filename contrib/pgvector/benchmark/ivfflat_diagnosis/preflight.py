"""Capability and build-input checks for the P0 diagnosis flow."""

from __future__ import annotations

import math
import os
from pathlib import Path
import shutil
from typing import Any

from psycopg import sql

from common.config import PGLOG
from common.db import checked_name, fetch_settings, function_exists, relation_exists
from common.schema import SCHEMA_VERSION, utc_now


SETTINGS = (
    "block_size", "data_directory", "log_directory", "log_temp_files",
    "maintenance_io_concurrency", "maintenance_work_mem",
    "max_parallel_maintenance_workers", "max_parallel_workers",
    "max_worker_processes", "shared_buffers", "track_io_timing", "work_mem",
)


class PreflightValidationError(RuntimeError):
    """A build-blocking validation failure with its evidence snapshot."""

    def __init__(self, message: str, snapshot: dict[str, Any]):
        super().__init__(message)
        self.snapshot = snapshot


def validate_row_count(requested_rows: int, actual_rows: int, *,
                       enforce: bool = True) -> dict[str, Any]:
    matches = requested_rows == actual_rows
    return {
        "enforced": enforce,
        "status": "passed" if matches or not enforce else "failed",
        "requested_rows": requested_rows,
        "actual_rows": actual_rows,
        "matches": matches,
        "reason": None if matches else (
            f"requested_rows={requested_rows} does not match actual_rows={actual_rows}"
        ),
    }


def excludes_zero_norm(metric: str) -> bool:
    """Match pgvector indexing semantics: only cosine rejects zero norm."""
    if metric not in ("l2", "cosine", "ip"):
        raise ValueError(f"unsupported metric: {metric}")
    return metric == "cosine"


def _capability(available: bool, reason: str | None = None, **details) -> dict[str, Any]:
    return {
        "available": available,
        "availability": "available" if available else "unavailable",
        "reason": reason,
        **details,
    }


def _try_capability(check, unavailable_reason: str) -> dict[str, Any]:
    try:
        available = bool(check())
        return _capability(available, None if available else unavailable_reason)
    except Exception as exc:
        return _capability(False, f"{unavailable_reason}: {exc}")


def _readable(path: Path) -> bool:
    try:
        with path.open("rb"):
            return True
    except (FileNotFoundError, PermissionError, OSError):
        return False


def probe_capabilities(conn, host_context: bool = True) -> dict[str, Any]:
    """Probe each optional metric source without failing the whole run."""
    with conn.cursor() as cur:
        cur.execute("SELECT pg_backend_pid(), current_setting('track_io_timing')")
        backend_pid, track_io_timing = cur.fetchone()
        cur.execute("SELECT current_setting('server_version_num')")
        server_version_num = int(cur.fetchone()[0])
        cur.execute("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
        vector_row = cur.fetchone()
        pgvector_version = vector_row[0] if vector_row else None
        cur.execute(
            """
            SELECT array_agg(a.attname ORDER BY a.attnum)
              FROM pg_attribute a
             WHERE a.attrelid = to_regclass(
                       'pg_catalog.pg_stat_progress_create_index'
                   )
               AND a.attnum > 0
               AND NOT a.attisdropped
            """
        )
        progress_columns = set(cur.fetchone()[0] or ())

    progress_available = relation_exists(
        conn, "pg_catalog.pg_stat_progress_create_index"
    )
    native_progress_verified = (
        progress_available and pgvector_version in ("0.8.6", "0.8.7")
    )
    load_progress_verified = (
        native_progress_verified
        and {"phase", "tuples_total", "tuples_done"}.issubset(progress_columns)
    )

    proc_root = Path(f"/proc/{backend_pid}")
    capabilities = {
        "progress": _try_capability(
            lambda: progress_available,
            "pg_stat_progress_create_index is unavailable",
        ),
        "native_progress_phases": _capability(
            native_progress_verified,
            None if native_progress_verified else (
                "native IVFFlat phase semantics are only source-validated for "
                "pgvector 0.8.6/0.8.7 with pg_stat_progress_create_index"
            ),
            phases=[
                "initializing", "performing k-means",
                "assigning tuples", "loading tuples",
            ],
            evidence="pgvector 0.8.6/0.8.7 ivfflatbuildphasename()",
        ),
        "load_progress": _capability(
            load_progress_verified,
            None if load_progress_verified else (
                "loading tuples progress requires pgvector 0.8.6/0.8.7 and "
                "tuples_total/tuples_done columns"
            ),
            phase="loading tuples",
            numerator="tuples_done",
            denominator="tuples_total",
            evidence="pgvector 0.8.6/0.8.7 ivfbuild.c progress updates",
        ),
        "activity": _try_capability(
            lambda: relation_exists(conn, "pg_catalog.pg_stat_activity"),
            "pg_stat_activity is unavailable",
        ),
        "backend_io": _try_capability(
            lambda: function_exists(conn, "pg_catalog.pg_stat_get_backend_io(integer)"),
            "pg_stat_get_backend_io(integer) is unavailable",
        ),
        "backend_wal": _try_capability(
            lambda: function_exists(conn, "pg_catalog.pg_stat_get_backend_wal(integer)"),
            "pg_stat_get_backend_wal(integer) is unavailable",
        ),
        "backend_stats_flush": _try_capability(
            lambda: function_exists(conn, "pg_catalog.pg_stat_force_next_flush()"),
            "pg_stat_force_next_flush() is unavailable; final backend counters "
            "cannot be made reliably visible",
        ),
        "proc": _capability(
            host_context and _readable(proc_root / "stat"),
            None if host_context and _readable(proc_root / "stat")
            else "database PID namespace is not visible or /proc is unreadable",
        ),
        "pss": _capability(
            host_context and _readable(proc_root / "smaps_rollup"),
            None if host_context and _readable(proc_root / "smaps_rollup")
            else "smaps_rollup is unavailable",
        ),
        "io_timing": _capability(
            track_io_timing == "on",
            None if track_io_timing == "on" else "track_io_timing is off",
        ),
        "statement_stats": _try_capability(
            lambda: relation_exists(conn, "public.pg_stat_statements")
            or relation_exists(conn, "pg_catalog.pg_stat_statements"),
            "pg_stat_statements is not installed in this database",
        ),
        "log": _capability(
            PGLOG.is_file() and os.access(PGLOG, os.R_OK),
            None if PGLOG.is_file() and os.access(PGLOG, os.R_OK)
            else f"configured benchmark log is unreadable: {PGLOG}",
            path=str(PGLOG),
        ),
        "assign_progress": _capability(
            False,
            "blocks_done/blocks_total semantics require target-version validation",
        ),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "timestamp_utc": utc_now(),
        "source": "capability_probe",
        "availability": "available",
        "reason": None,
        "server_version_num": server_version_num,
        "pgvector_version": pgvector_version,
        "probe_backend_pid": backend_pid,
        "progress_validation": {
            "source_validated": native_progress_verified,
            "load_source_validated": load_progress_verified,
            "runtime_observed": False,
            "reason": "Preflight validates support; runtime observations are recorded per Build Run",
        },
        "capabilities": capabilities,
    }


def _parse_size(value: str) -> int:
    units = {
        "b": 1,
        "kb": 1024,
        "mb": 1024 ** 2,
        "gb": 1024 ** 3,
        "tb": 1024 ** 4,
    }
    text = value.strip().lower()
    for unit in sorted(units, key=len, reverse=True):
        if text.endswith(unit):
            return int(float(text[:-len(unit)]) * units[unit])
    return int(text)


def _cgroup_memory_limit() -> int | None:
    candidates = (
        Path("/sys/fs/cgroup/memory.max"),
        Path("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
    )
    for path in candidates:
        try:
            value = path.read_text().strip()
        except (FileNotFoundError, PermissionError, OSError):
            continue
        if value != "max":
            limit = int(value)
            if limit < (1 << 60):
                return limit
    return None


def collect_preflight_snapshot(conn, *, table: str, column: str,
                               index_name: str, metric: str, lists: int,
                               diagnosis_id: str, build_id: str,
                               requested_rows: int,
                               enforce_requested_rows: bool = True) -> dict[str, Any]:
    """Collect database and host facts needed before CREATE INDEX."""
    checked_name(table)
    checked_name(column)
    checked_name(index_name)
    if metric not in ("l2", "cosine", "ip"):
        raise ValueError(f"unsupported metric: {metric}")
    if lists < 1 or lists > 32768:
        raise ValueError("lists must be between 1 and 32768")

    settings = fetch_settings(conn, SETTINGS)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.oid, a.atttypid::regtype::text, a.atttypmod,
                   pg_relation_size(c.oid), c.relpages, c.reltuples, c.reloptions
              FROM pg_class c
              JOIN pg_attribute a ON a.attrelid = c.oid
             WHERE c.oid = to_regclass(%s)
               AND a.attname = %s
               AND a.attnum > 0
               AND NOT a.attisdropped
            """,
            (table, column),
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"table/column does not exist: {table}.{column}")
        table_oid, type_name, dimension, table_bytes, relpages, reltuples, reloptions = row
        if type_name != "vector":
            raise ValueError(f"P0 supports vector columns, got {type_name}")
        if dimension < 1:
            raise ValueError("vector column must declare a positive dimension")

        table_identifier = sql.Identifier(table)
        column_identifier = sql.Identifier(column)
        eligible_filter = sql.SQL("{} IS NOT NULL").format(column_identifier)
        if excludes_zero_norm(metric):
            eligible_filter = sql.SQL("{} AND vector_norm({}) > 0").format(
                eligible_filter, column_identifier
            )
        cur.execute(
            sql.SQL("SELECT count(*), count(*) FILTER (WHERE {}) FROM {}").format(
                eligible_filter, table_identifier
            )
        )
        actual_rows, eligible_vectors = map(int, cur.fetchone())

    block_size = int(settings.get("block_size", "8192").removesuffix("B"))
    heap_blocks = math.ceil(table_bytes / block_size) if table_bytes else 0
    max_heap_tuples_per_page = max(1, (block_size - 24) // 28)
    item_size = 8 + 4 * int(dimension)
    data_directory = Path(settings["data_directory"])
    disk_free = shutil.disk_usage(data_directory).free
    table_parallel_workers = None
    for option in reloptions or ():
        if option.startswith("parallel_workers="):
            table_parallel_workers = int(option.split("=", 1)[1])

    row_validation = validate_row_count(
        requested_rows, actual_rows, enforce=enforce_requested_rows
    )
    snapshot = {
        "schema_version": SCHEMA_VERSION,
        "diagnosis_id": diagnosis_id,
        "build_id": build_id,
        "timestamp_utc": utc_now(),
        "source": "preflight",
        "availability": "available",
        "reason": None,
        "table": table,
        "table_oid": table_oid,
        "column": column,
        "index_name": index_name,
        "metric": metric,
        "requested_rows": requested_rows,
        "actual_rows": actual_rows,
        # Compatibility alias. Unlike the old implementation, this is observed.
        "rows": actual_rows,
        "eligible_vectors": eligible_vectors,
        "row_count_validation": row_validation,
        "heap_blocks": heap_blocks,
        "catalog_relpages": relpages,
        "catalog_reltuples": reltuples,
        "dimension": int(dimension),
        "vector_type": type_name,
        "item_size": item_size,
        "lists": lists,
        "block_size": block_size,
        "max_heap_tuples_per_page": max_heap_tuples_per_page,
        "settings": settings,
        "memory_limit_bytes": _parse_size(settings["maintenance_work_mem"]),
        "table_parallel_workers": table_parallel_workers,
        "planned_workers": None,
        "host": {
            "cpu_count": os.cpu_count(),
            "cgroup_memory_limit_bytes": _cgroup_memory_limit(),
            "data_directory_free_bytes": disk_free,
        },
    }
    if row_validation["status"] == "failed":
        raise PreflightValidationError(row_validation["reason"], snapshot)
    return snapshot
