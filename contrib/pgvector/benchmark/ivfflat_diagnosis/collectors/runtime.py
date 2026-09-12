"""Collect one aligned PostgreSQL and operating-system build snapshot."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import os
from pathlib import Path
import re
from typing import Any

from common.schema import SCHEMA_VERSION, utc_now


_STATUS_VALUE = re.compile(r"^(VmRSS):\s+(\d+)\s+kB$", re.MULTILINE)
_PSS_VALUE = re.compile(r"^Pss:\s+(\d+)\s+kB$", re.MULTILINE)


def _enabled(capability_map: dict[str, Any], name: str) -> bool:
    values = capability_map.get("capabilities", capability_map)
    value = values.get(name, {})
    return bool(value.get("available")) if isinstance(value, dict) else bool(value)


def _plain(value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _rows(cur) -> list[dict[str, Any]]:
    names = [column.name for column in cur.description]
    return [
        {name: _plain(value) for name, value in zip(names, row)}
        for row in cur.fetchall()
    ]


def _read_key_values(path: Path) -> dict[str, int]:
    values: dict[str, int] = {}
    text = path.read_text()
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, raw = line.split(":", 1)
        token = raw.strip().split()[0]
        if token.isdigit():
            values[key] = int(token)
    return values


def _field_status(available: bool, reason: str | None = None) -> dict[str, Any]:
    return {
        "availability": "available" if available else "unavailable",
        "reason": reason,
        "attribution_scope": "backend",
    }


def _sample_proc(pid: int, include_pss: bool) -> tuple[
        dict[str, Any], dict[str, dict[str, Any]], list[str]]:
    root = Path(f"/proc/{pid}")
    values = {
        "process_start_ticks": None, "cpu_user_seconds": None,
        "cpu_system_seconds": None, "rss_bytes": None, "pss_bytes": None,
        "proc_read_bytes": None, "proc_write_bytes": None,
        "process_lifecycle_event": None,
    }
    availability: dict[str, dict[str, Any]] = {}
    reasons: list[str] = []

    def record_unavailable(source: str, exc: Exception) -> str:
        reason = f"/proc {source} unavailable: {exc}"
        reasons.append(reason)
        process_disappeared = (
            isinstance(exc, ProcessLookupError)
            or isinstance(exc, FileNotFoundError)
            and (source in ("stat", "status") or not root.exists())
        )
        if process_disappeared:
            values["process_lifecycle_event"] = "process_exited_during_sample"
        return reason

    try:
        stat = (root / "stat").read_text()
        close = stat.rfind(")")
        if close < 0:
            raise ValueError(f"cannot parse /proc/{pid}/stat")
        fields = stat[close + 2:].split()
        ticks = os.sysconf("SC_CLK_TCK")
        values.update({
            "process_start_ticks": int(fields[19]),
            "cpu_user_seconds": int(fields[11]) / ticks,
            "cpu_system_seconds": int(fields[12]) / ticks,
        })
        availability["cpu"] = _field_status(True)
    except (FileNotFoundError, ProcessLookupError, PermissionError, OSError, ValueError) as exc:
        reason = record_unavailable("stat", exc)
        availability["cpu"] = _field_status(False, reason)
    try:
        status = (root / "status").read_text()
        rss_match = _STATUS_VALUE.search(status)
        if not rss_match:
            raise ValueError("VmRSS is absent")
        values["rss_bytes"] = int(rss_match.group(2)) * 1024
        availability["rss"] = _field_status(True)
    except (FileNotFoundError, ProcessLookupError, PermissionError, OSError, ValueError) as exc:
        reason = record_unavailable("status", exc)
        availability["rss"] = _field_status(False, reason)
    try:
        io = _read_key_values(root / "io")
        values["proc_read_bytes"] = io.get("read_bytes")
        values["proc_write_bytes"] = io.get("write_bytes")
        if values["proc_read_bytes"] is None or values["proc_write_bytes"] is None:
            raise ValueError("read_bytes/write_bytes are absent")
        availability["proc_io"] = _field_status(True)
    except (FileNotFoundError, ProcessLookupError, PermissionError, OSError, ValueError) as exc:
        reason = record_unavailable("io", exc)
        availability["proc_io"] = _field_status(False, reason)
    if include_pss:
        try:
            pss_match = _PSS_VALUE.search((root / "smaps_rollup").read_text())
            if not pss_match:
                raise ValueError("Pss is absent")
            values["pss_bytes"] = int(pss_match.group(1)) * 1024
            availability["pss"] = _field_status(True)
        except (FileNotFoundError, ProcessLookupError, PermissionError, OSError, ValueError) as exc:
            reason = record_unavailable("smaps_rollup", exc)
            availability["pss"] = _field_status(False, reason)
    else:
        availability["pss"] = _field_status(
            False, "smaps_rollup capability unavailable"
        )
    return values, availability, reasons


def _normalize_phase(phase: str | None) -> str | None:
    prefix = "building index: "
    if phase and phase.startswith(prefix):
        return phase[len(prefix):]
    return phase


def _internal_stage(native_phase: str | None) -> tuple[str | None, str]:
    mapping = {
        "initializing": ("initializing", "native"),
        "assigning tuples": ("assign/scan", "derived_from_native"),
        "loading tuples": ("load", "derived_from_native"),
    }
    if native_phase == "performing k-means":
        return None, "unavailable_sampling_and_kmeans_not_separable"
    return mapping.get(native_phase, (None, "unavailable"))


def _activity(conn, leader_pid: int) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT pid, leader_pid, backend_start, state,
                   wait_event_type, wait_event, query_start
              FROM pg_stat_activity
             WHERE pid = %s OR leader_pid = %s
             ORDER BY pid
            """,
            (leader_pid, leader_pid),
        )
        return _rows(cur)


def _progress(conn, leader_pid: int) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT pid, datid, relid AS table_oid, index_relid AS index_oid,
                   phase, blocks_total, blocks_done, tuples_total, tuples_done
              FROM pg_stat_progress_create_index
             WHERE pid = %s
            """,
            (leader_pid,),
        )
        names = [column.name for column in cur.description]
        row = cur.fetchone()
    return None if row is None else {
        name: _plain(value) for name, value in zip(names, row)
    }


def _backend_io(conn, pid: int) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM pg_stat_get_backend_io(%s)", (pid,))
        rows = _rows(cur)
    totals = {
        "reads": 0, "read_bytes": 0, "read_time_ms": 0.0,
        "writes": 0, "write_bytes": 0, "write_time_ms": 0.0,
        "hits": 0, "evictions": 0, "fsyncs": 0, "fsync_time_ms": 0.0,
    }
    mapping = {
        "reads": "reads", "read_bytes": "read_bytes", "read_time": "read_time_ms",
        "writes": "writes", "write_bytes": "write_bytes", "write_time": "write_time_ms",
        "hits": "hits", "evictions": "evictions", "fsyncs": "fsyncs",
        "fsync_time": "fsync_time_ms",
    }
    for row in rows:
        for source, target in mapping.items():
            totals[target] += row.get(source) or 0
    return {"totals": totals, "breakdown": rows}


def _backend_wal(conn, pid: int) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM pg_stat_get_backend_wal(%s)", (pid,))
        rows = _rows(cur)
    return rows[0] if rows else None


def collect_runtime_snapshot(stats_conn, leader_pid: int,
                             process_registry: dict[int, int],
                             capability_map: dict[str, Any], *,
                             diagnosis_id: str, build_id: str,
                             trust_assign_progress: bool = False) -> dict[str, Any]:
    """Collect one timestamp worth of progress and per-process resource facts."""
    timestamp = utc_now()
    errors: list[dict[str, Any]] = []
    activities: list[dict[str, Any]] = []
    if _enabled(capability_map, "activity"):
        try:
            activities = _activity(stats_conn, leader_pid)
        except Exception as exc:
            errors.append({"source": "pg_stat_activity", "error": str(exc)})
    if not activities:
        activities = [{
            "pid": leader_pid, "leader_pid": None, "backend_start": None,
            "state": None, "wait_event_type": None, "wait_event": None,
            "query_start": None,
        }]

    progress_row = None
    if _enabled(capability_map, "progress"):
        try:
            progress_row = _progress(stats_conn, leader_pid)
        except Exception as exc:
            errors.append({"source": "pg_stat_progress_create_index", "error": str(exc)})

    native_phase = (
        _normalize_phase(progress_row.get("phase"))
        if progress_row and _enabled(capability_map, "native_progress_phases")
        else None
    )
    internal_stage, stage_source = _internal_stage(native_phase)
    progress = {
        "schema_version": SCHEMA_VERSION,
        "diagnosis_id": diagnosis_id,
        "build_id": build_id,
        "timestamp_utc": timestamp,
        "source": "pg_stat_progress_create_index",
        "availability": "available" if progress_row else "not_sampled",
        "reason": None if progress_row else "no progress row at this sample",
        "leader_pid": leader_pid,
        "table_oid": progress_row.get("table_oid") if progress_row else None,
        "index_oid": progress_row.get("index_oid") if progress_row else None,
        "native_phase": native_phase,
        "native_phase_availability": (
            "available" if _enabled(capability_map, "native_progress_phases")
            else "unavailable"
        ),
        "internal_stage": internal_stage,
        "stage_source": stage_source,
        "blocks_total": progress_row.get("blocks_total") if progress_row else None,
        "blocks_done": progress_row.get("blocks_done") if progress_row else None,
        "tuples_total": progress_row.get("tuples_total") if progress_row else None,
        "tuples_done": progress_row.get("tuples_done") if progress_row else None,
        "progress_percent": None,
        "progress_unit": None,
        "load_progress_availability": (
            "available" if _enabled(capability_map, "load_progress")
            else "unavailable"
        ),
    }
    if (
        progress_row and native_phase == "loading tuples"
        and _enabled(capability_map, "load_progress")
        and (progress["tuples_total"] or 0) > 0
    ):
        progress["progress_percent"] = 100.0 * progress["tuples_done"] / progress["tuples_total"]
        progress["progress_unit"] = "tuples"
    elif (
        progress_row and native_phase == "assigning tuples" and trust_assign_progress
        and (progress["blocks_total"] or 0) > 0
    ):
        progress["progress_percent"] = 100.0 * progress["blocks_done"] / progress["blocks_total"]
        progress["progress_unit"] = "blocks"

    resources: list[dict[str, Any]] = []
    for activity in activities:
        pid = int(activity["pid"])
        reasons: list[str] = []
        proc_values = {
            "process_start_ticks": None, "cpu_user_seconds": None,
            "cpu_system_seconds": None, "rss_bytes": None, "pss_bytes": None,
            "proc_read_bytes": None, "proc_write_bytes": None,
            "process_lifecycle_event": None,
        }
        field_availability: dict[str, Any] = {}
        if _enabled(capability_map, "proc"):
            proc_values, proc_availability, proc_reasons = _sample_proc(
                pid, _enabled(capability_map, "pss")
            )
            field_availability.update(proc_availability)
            reasons.extend(proc_reasons)
        else:
            reasons.append("proc capability unavailable")
            for field in ("cpu", "rss", "proc_io", "pss"):
                field_availability[field] = _field_status(
                    False, "proc capability unavailable"
                )

        start_ticks = proc_values["process_start_ticks"]
        pid_reused = (
            start_ticks is not None and pid in process_registry
            and process_registry[pid] != start_ticks
        )
        if start_ticks is not None:
            process_registry[pid] = start_ticks

        pg_io = None
        if _enabled(capability_map, "backend_io"):
            try:
                pg_io = _backend_io(stats_conn, pid)
            except Exception as exc:
                reasons.append(f"backend IO unavailable: {exc}")
        field_availability["pg_io"] = _field_status(
            pg_io is not None,
            None if pg_io is not None else "backend IO capability or sample unavailable",
        )
        pg_wal = None
        if _enabled(capability_map, "backend_wal"):
            try:
                pg_wal = _backend_wal(stats_conn, pid)
            except Exception as exc:
                reasons.append(f"backend WAL unavailable: {exc}")
        field_availability["pg_wal"] = _field_status(
            pg_wal is not None,
            None if pg_wal is not None else "backend WAL capability or sample unavailable",
        )

        role = "leader" if pid == leader_pid else "worker"
        worker_exited = (
            role == "worker"
            and proc_values.get("process_lifecycle_event")
            == "process_exited_during_sample"
        )
        sample_availability = (
            "not_sampled" if worker_exited else
            "available" if not reasons else "partial" if any(
                item["availability"] == "available"
                for item in field_availability.values()
            ) else "unavailable"
        )
        resources.append({
            "schema_version": SCHEMA_VERSION,
            "diagnosis_id": diagnosis_id,
            "build_id": build_id,
            "timestamp_utc": timestamp,
            "source": "postgresql+/proc",
            "availability": sample_availability,
            "reason": (
                "worker exited between discovery and /proc sampling"
                if worker_exited else "; ".join(reasons) if reasons else None
            ),
            "collection_impact": (
                "none" if sample_availability == "available" or worker_exited
                else "partial"
            ),
            "process_lifecycle": ({
                "event": "worker_exited",
                "normal": True,
                "observation": "disappeared_between_discovery_and_proc_sample",
            } if worker_exited else None),
            "field_availability": field_availability,
            "pid": pid,
            "process_start_ticks": start_ticks,
            "pid_reused": pid_reused,
            "role": role,
            "leader_pid": activity.get("leader_pid"),
            "backend_start": activity.get("backend_start"),
            "state": activity.get("state"),
            "native_phase": native_phase,
            "wait_event_type": activity.get("wait_event_type"),
            "wait_event": activity.get("wait_event"),
            **proc_values,
            "pg_io": pg_io,
            "pg_wal": pg_wal,
        })

    return {
        "timestamp_utc": timestamp,
        "progress": progress,
        "resources": resources,
        "process_registry": process_registry,
        "errors": errors,
    }
