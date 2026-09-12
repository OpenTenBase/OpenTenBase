"""Sample a CREATE INDEX leader and its parallel workers."""

from __future__ import annotations

import threading
import time
from typing import Any

from common.db import connect_dsn
from common.run_store import RunStore
from common.schema import SCHEMA_VERSION, utc_now

from collectors.runtime import collect_runtime_snapshot


def resource_degrades_collection(resource: dict[str, Any]) -> bool:
    """Return whether an unavailable sample represents lost collection data."""
    return (
        resource.get("availability") != "available"
        and resource.get("collection_impact") != "none"
    )


def monitor_build(dsn: str, backend_pid: int, stop: threading.Event, *,
                  diagnosis_id: str, build_id: str, store: RunStore,
                  progress_path: str, resources_path: str,
                  capability_map: dict[str, Any],
                  interval: float = 0.5,
                  trust_assign_progress: bool = False,
                  state: dict[str, Any] | None = None,
                  baseline_ready: threading.Event | None = None) -> dict[str, Any]:
    """Run the collector loop; database and collector status remain separate."""
    if interval <= 0:
        raise ValueError("sample interval must be positive")
    state = state if state is not None else {}
    state.update({
        "collector_interrupted": False,
        "collector_errors": [],
        "discovered_pids": [backend_pid],
        "sample_count": 0,
        "last_phase": None,
        "baseline_collected": False,
        "final_collected": False,
        "partial_resources": False,
    })
    process_registry: dict[int, int] = {}
    consecutive_failures = 0
    baseline_collected = False

    try:
        with connect_dsn(dsn, autocommit=True) as conn:
            while True:
                sample_started = time.perf_counter()
                sample_role = (
                    "baseline" if not baseline_collected
                    else "final" if stop.is_set() else "periodic"
                )
                sample_succeeded = False
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT pg_stat_clear_snapshot()")
                    snapshot = collect_runtime_snapshot(
                        conn,
                        backend_pid,
                        process_registry,
                        capability_map,
                        diagnosis_id=diagnosis_id,
                        build_id=build_id,
                        trust_assign_progress=trust_assign_progress,
                    )
                    snapshot["progress"]["counter_sample_role"] = sample_role
                    store.append_jsonl(progress_path, snapshot["progress"])
                    for resource in snapshot["resources"]:
                        if resource_degrades_collection(resource):
                            state["partial_resources"] = True
                        resource["counter_sample_role"] = sample_role
                        store.append_jsonl(resources_path, resource)
                    state["collector_errors"].extend(snapshot["errors"])
                    state["sample_count"] += 1
                    state["last_phase"] = (
                        snapshot["progress"].get("native_phase") or state["last_phase"]
                    )
                    state["discovered_pids"] = sorted({
                        *state["discovered_pids"],
                        *(row["pid"] for row in snapshot["resources"]),
                    })
                    consecutive_failures = 0
                    sample_succeeded = True
                    if sample_role == "baseline":
                        baseline_collected = True
                        state["baseline_collected"] = True
                        state["baseline_timestamp_utc"] = snapshot["timestamp_utc"]
                        if baseline_ready is not None:
                            baseline_ready.set()
                    elif sample_role == "final":
                        state["final_collected"] = True
                        state["final_timestamp_utc"] = snapshot["timestamp_utc"]
                except Exception as exc:
                    consecutive_failures += 1
                    state["collector_errors"].append({
                        "source": "collector_loop",
                        "error": str(exc),
                        "consecutive_failures": consecutive_failures,
                    })
                    if consecutive_failures >= 3:
                        state["collector_interrupted"] = True
                        break

                if sample_role == "final" and sample_succeeded:
                    break
                if stop.is_set():
                    # Always make a separate post-build final counter sample.
                    continue
                remaining = max(0.0, interval - (time.perf_counter() - sample_started))
                stop.wait(remaining)
    except Exception as exc:
        state["collector_interrupted"] = True
        state["collector_errors"].append({
            "source": "collector_connection",
            "error": str(exc),
        })
    finally:
        if baseline_ready is not None:
            baseline_ready.set()
        if state.get("sample_count", 0) == 0:
            timestamp = utc_now()
            reason = "collector stopped before producing a runtime sample"
            store.append_jsonl(progress_path, {
                "schema_version": SCHEMA_VERSION,
                "diagnosis_id": diagnosis_id,
                "build_id": build_id,
                "timestamp_utc": timestamp,
                "source": "collector_loop",
                "availability": "unavailable",
                "reason": reason,
                "leader_pid": backend_pid,
                "counter_sample_role": "unavailable",
            })
            store.append_jsonl(resources_path, {
                "schema_version": SCHEMA_VERSION,
                "diagnosis_id": diagnosis_id,
                "build_id": build_id,
                "timestamp_utc": timestamp,
                "source": "collector_loop",
                "availability": "unavailable",
                "reason": reason,
                "collection_impact": "partial",
                "pid": backend_pid,
                "role": "leader",
                "counter_sample_role": "unavailable",
            })
        store.mark_complete(progress_path)
        store.mark_complete(resources_path)
    return state
