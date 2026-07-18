#!/usr/bin/env python3
"""Collect and analyze distributed OpenTenBase operational snapshots.

Copyright (c) 2026 OpenTenBase Contributors

This file is licensed under the same terms as OpenTenBase. See LICENSE.txt
in the repository root for details.

The collector uses psql and read-only catalog queries.  It does not require a
server extension, a daemon, or third-party Python packages.  Snapshots can be
rendered as Prometheus text, analyzed offline, and compared over time.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import datetime as dt
import json
import math
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple


SNAPSHOT_FORMAT = "opentenbase-observer-snapshot"
SNAPSHOT_VERSION = 1
CONFIG_VERSION = 1
VALID_ROLES = {"coordinator", "datanode", "gtm", "standalone"}
VALID_SSL_MODES = {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}
SEVERITY_ORDER = {"info": 0, "warning": 1, "critical": 2}
METRIC_NAME_RE = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")
LABEL_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
COUNTER_METRICS = {
    "opentenbase_xact_commit_total",
    "opentenbase_xact_rollback_total",
    "opentenbase_blocks_read_total",
    "opentenbase_blocks_hit_total",
    "opentenbase_temp_files_total",
    "opentenbase_temp_bytes_total",
    "opentenbase_deadlocks_total",
    "opentenbase_tuples_returned_total",
    "opentenbase_tuples_fetched_total",
    "opentenbase_tuples_inserted_total",
    "opentenbase_tuples_updated_total",
    "opentenbase_tuples_deleted_total",
    "opentenbase_checkpoints_timed_total",
    "opentenbase_checkpoints_requested_total",
    "opentenbase_checkpoint_write_milliseconds_total",
    "opentenbase_checkpoint_sync_milliseconds_total",
    "opentenbase_buffers_checkpoint_total",
    "opentenbase_buffers_clean_total",
    "opentenbase_maxwritten_clean_total",
    "opentenbase_buffers_backend_total",
    "opentenbase_buffers_backend_fsync_total",
    "opentenbase_buffers_alloc_total",
}


COLLECTION_SQL = r"""
WITH
settings AS (
    SELECT current_setting('max_connections')::numeric AS max_connections,
           current_setting('server_version_num')::numeric AS server_version_num
),
activity AS (
    SELECT count(*)::numeric AS connections_total,
           count(*) FILTER (WHERE state = 'active')::numeric AS connections_active,
           count(*) FILTER (WHERE state = 'idle in transaction')::numeric AS idle_in_transaction,
           COALESCE(max(EXTRACT(epoch FROM (clock_timestamp() - xact_start)))
                    FILTER (WHERE xact_start IS NOT NULL), 0)::numeric AS oldest_transaction_seconds,
           COALESCE(max(EXTRACT(epoch FROM (clock_timestamp() - query_start)))
                    FILTER (WHERE state = 'active' AND query_start IS NOT NULL), 0)::numeric AS longest_query_seconds
    FROM pg_catalog.pg_stat_activity
),
locks AS (
    SELECT count(*) FILTER (WHERE NOT granted)::numeric AS waiting_locks,
           count(*)::numeric AS locks_total
    FROM pg_catalog.pg_locks
),
database_stats AS (
    SELECT COALESCE(sum(xact_commit), 0)::numeric AS xact_commit,
           COALESCE(sum(xact_rollback), 0)::numeric AS xact_rollback,
           COALESCE(sum(blks_read), 0)::numeric AS blocks_read,
           COALESCE(sum(blks_hit), 0)::numeric AS blocks_hit,
           COALESCE(sum(temp_files), 0)::numeric AS temp_files,
           COALESCE(sum(temp_bytes), 0)::numeric AS temp_bytes,
           COALESCE(sum(deadlocks), 0)::numeric AS deadlocks,
           COALESCE(sum(tup_returned), 0)::numeric AS tuples_returned,
           COALESCE(sum(tup_fetched), 0)::numeric AS tuples_fetched,
           COALESCE(sum(tup_inserted), 0)::numeric AS tuples_inserted,
           COALESCE(sum(tup_updated), 0)::numeric AS tuples_updated,
           COALESCE(sum(tup_deleted), 0)::numeric AS tuples_deleted
    FROM pg_catalog.pg_stat_database
),
database_size AS (
    SELECT COALESCE(sum(pg_catalog.pg_database_size(datname)), 0)::numeric AS bytes
    FROM pg_catalog.pg_database
    WHERE datallowconn
),
bgwriter AS (
    SELECT checkpoints_timed::numeric,
           checkpoints_req::numeric,
           checkpoint_write_time::numeric,
           checkpoint_sync_time::numeric,
           buffers_checkpoint::numeric,
           buffers_clean::numeric,
           maxwritten_clean::numeric,
           buffers_backend::numeric,
           buffers_backend_fsync::numeric,
           buffers_alloc::numeric
    FROM pg_catalog.pg_stat_bgwriter
),
metrics(name, value, labels) AS (
    SELECT 'opentenbase_max_connections', max_connections, '{}'::json FROM settings
    UNION ALL SELECT 'opentenbase_server_version_num', server_version_num, '{}'::json FROM settings
    UNION ALL SELECT 'opentenbase_connections_total', connections_total, '{}'::json FROM activity
    UNION ALL SELECT 'opentenbase_connections_active', connections_active, '{}'::json FROM activity
    UNION ALL SELECT 'opentenbase_idle_in_transaction', idle_in_transaction, '{}'::json FROM activity
    UNION ALL SELECT 'opentenbase_oldest_transaction_seconds', oldest_transaction_seconds, '{}'::json FROM activity
    UNION ALL SELECT 'opentenbase_longest_query_seconds', longest_query_seconds, '{}'::json FROM activity
    UNION ALL SELECT 'opentenbase_locks_total', locks_total, '{}'::json FROM locks
    UNION ALL SELECT 'opentenbase_waiting_locks', waiting_locks, '{}'::json FROM locks
    UNION ALL SELECT 'opentenbase_xact_commit_total', xact_commit, '{}'::json FROM database_stats
    UNION ALL SELECT 'opentenbase_xact_rollback_total', xact_rollback, '{}'::json FROM database_stats
    UNION ALL SELECT 'opentenbase_blocks_read_total', blocks_read, '{}'::json FROM database_stats
    UNION ALL SELECT 'opentenbase_blocks_hit_total', blocks_hit, '{}'::json FROM database_stats
    UNION ALL SELECT 'opentenbase_temp_files_total', temp_files, '{}'::json FROM database_stats
    UNION ALL SELECT 'opentenbase_temp_bytes_total', temp_bytes, '{}'::json FROM database_stats
    UNION ALL SELECT 'opentenbase_deadlocks_total', deadlocks, '{}'::json FROM database_stats
    UNION ALL SELECT 'opentenbase_tuples_returned_total', tuples_returned, '{}'::json FROM database_stats
    UNION ALL SELECT 'opentenbase_tuples_fetched_total', tuples_fetched, '{}'::json FROM database_stats
    UNION ALL SELECT 'opentenbase_tuples_inserted_total', tuples_inserted, '{}'::json FROM database_stats
    UNION ALL SELECT 'opentenbase_tuples_updated_total', tuples_updated, '{}'::json FROM database_stats
    UNION ALL SELECT 'opentenbase_tuples_deleted_total', tuples_deleted, '{}'::json FROM database_stats
    UNION ALL SELECT 'opentenbase_database_bytes', bytes, '{}'::json FROM database_size
    UNION ALL SELECT 'opentenbase_checkpoints_timed_total', checkpoints_timed, '{}'::json FROM bgwriter
    UNION ALL SELECT 'opentenbase_checkpoints_requested_total', checkpoints_req, '{}'::json FROM bgwriter
    UNION ALL SELECT 'opentenbase_checkpoint_write_milliseconds_total', checkpoint_write_time, '{}'::json FROM bgwriter
    UNION ALL SELECT 'opentenbase_checkpoint_sync_milliseconds_total', checkpoint_sync_time, '{}'::json FROM bgwriter
    UNION ALL SELECT 'opentenbase_buffers_checkpoint_total', buffers_checkpoint, '{}'::json FROM bgwriter
    UNION ALL SELECT 'opentenbase_buffers_clean_total', buffers_clean, '{}'::json FROM bgwriter
    UNION ALL SELECT 'opentenbase_maxwritten_clean_total', maxwritten_clean, '{}'::json FROM bgwriter
    UNION ALL SELECT 'opentenbase_buffers_backend_total', buffers_backend, '{}'::json FROM bgwriter
    UNION ALL SELECT 'opentenbase_buffers_backend_fsync_total', buffers_backend_fsync, '{}'::json FROM bgwriter
    UNION ALL SELECT 'opentenbase_buffers_alloc_total', buffers_alloc, '{}'::json FROM bgwriter
),
long_queries AS (
    SELECT json_build_object(
        'type', 'event',
        'event', 'long_query',
        'pid', pid,
        'database', datname,
        'user', usename,
        'duration_seconds', EXTRACT(epoch FROM (clock_timestamp() - query_start)),
        'wait_event_type', wait_event_type,
        'wait_event', wait_event,
        'query', left(regexp_replace(query, E'[\\n\\r\\t]+', ' ', 'g'), 2000)
    ) AS value
    FROM pg_catalog.pg_stat_activity
    WHERE state = 'active'
      AND pid <> pg_backend_pid()
      AND query_start IS NOT NULL
      AND clock_timestamp() - query_start >= interval '5 seconds'
),
waiting_lock_events AS (
    SELECT json_build_object(
        'type', 'event',
        'event', 'waiting_lock',
        'pid', l.pid,
        'database_oid', l.database,
        'relation_oid', l.relation,
        'lock_type', l.locktype,
        'mode', l.mode,
        'virtual_transaction', l.virtualtransaction
    ) AS value
    FROM pg_catalog.pg_locks AS l
    WHERE NOT l.granted
),
rows AS (
    SELECT json_build_object(
        'type', 'metric',
        'name', name,
        'value', value,
        'labels', labels
    ) AS value
    FROM metrics
    UNION ALL
    SELECT value FROM long_queries
    UNION ALL
    SELECT value FROM waiting_lock_events
)
SELECT value::text
FROM rows
ORDER BY value->>'type', value->>'name', value->>'event', value->>'pid';
""".strip()


class ObserverError(Exception):
    """Base class for concise user-facing failures."""


class ConfigError(ObserverError):
    """Raised when collector configuration is invalid."""


class SnapshotError(ObserverError):
    """Raised when a stored snapshot is malformed."""


@dataclasses.dataclass(frozen=True)
class NodeConfig:
    """Connection metadata for one OpenTenBase node."""

    name: str
    host: str
    port: int
    database: str
    user: str
    role: str
    sslmode: str = "prefer"
    connect_timeout: int = 5

    def labels(self) -> Dict[str, str]:
        return {"node": self.name, "role": self.role}


@dataclasses.dataclass(frozen=True)
class Thresholds:
    """Analysis thresholds with conservative operational defaults."""

    connection_warning: float = 0.80
    connection_critical: float = 0.95
    idle_in_transaction_warning: int = 1
    waiting_locks_warning: int = 1
    longest_query_warning_seconds: float = 30.0
    longest_query_critical_seconds: float = 300.0
    oldest_transaction_warning_seconds: float = 60.0
    oldest_transaction_critical_seconds: float = 600.0
    cache_hit_warning: float = 0.95
    rollback_ratio_warning: float = 0.10
    rollback_ratio_minimum_transactions: int = 100


@dataclasses.dataclass(frozen=True)
class Finding:
    """A deterministic diagnostic result."""

    severity: str
    code: str
    node: str
    summary: str
    evidence: Mapping[str, Any]
    recommendation: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "severity": self.severity,
            "code": self.code,
            "node": self.node,
            "summary": self.summary,
            "evidence": dict(self.evidence),
            "recommendation": self.recommendation,
        }


class CommandRunner:
    """Subprocess adapter that tests can replace without a database."""

    def run(
        self,
        command: Sequence[str],
        environment: Mapping[str, str],
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            list(command),
            env=dict(environment),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
        )


def utc_now() -> str:
    """Return a stable UTC timestamp."""

    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def parse_timestamp(value: str) -> dt.datetime:
    """Parse an RFC 3339 timestamp and require timezone information."""

    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise SnapshotError(f"invalid timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        raise SnapshotError(f"snapshot timestamp has no timezone: {value!r}")
    return parsed


def require_string(value: Any, label: str, allow_empty: bool = False) -> str:
    """Validate a string used in config, labels, or event fields."""

    if not isinstance(value, str):
        raise ConfigError(f"{label} must be a string")
    if not allow_empty and not value:
        raise ConfigError(f"{label} must not be empty")
    if "\x00" in value or "\n" in value or "\r" in value:
        raise ConfigError(f"{label} contains a forbidden control character")
    return value


def require_int(value: Any, label: str, minimum: int, maximum: int) -> int:
    """Validate a non-boolean integer range."""

    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{label} must be an integer")
    if not minimum <= value <= maximum:
        raise ConfigError(f"{label} must be between {minimum} and {maximum}")
    return value


def parse_node(value: Mapping[str, Any], index: int) -> NodeConfig:
    """Parse one node configuration object."""

    label = f"nodes[{index}]"
    if not isinstance(value, Mapping):
        raise ConfigError(f"{label} must be an object")
    role = require_string(value.get("role"), f"{label}.role")
    if role not in VALID_ROLES:
        raise ConfigError(f"{label}.role must be one of: {', '.join(sorted(VALID_ROLES))}")
    sslmode = require_string(value.get("sslmode", "prefer"), f"{label}.sslmode")
    if sslmode not in VALID_SSL_MODES:
        raise ConfigError(f"{label}.sslmode must be one of: {', '.join(sorted(VALID_SSL_MODES))}")
    return NodeConfig(
        name=require_string(value.get("name"), f"{label}.name"),
        host=require_string(value.get("host"), f"{label}.host"),
        port=require_int(value.get("port", 5432), f"{label}.port", 1, 65535),
        database=require_string(value.get("database", "postgres"), f"{label}.database"),
        user=require_string(value.get("user"), f"{label}.user"),
        role=role,
        sslmode=sslmode,
        connect_timeout=require_int(
            value.get("connect_timeout", 5),
            f"{label}.connect_timeout",
            1,
            300,
        ),
    )


def parse_thresholds(value: Any) -> Thresholds:
    """Parse optional threshold overrides and reject unknown keys."""

    if value is None:
        return Thresholds()
    if not isinstance(value, Mapping):
        raise ConfigError("thresholds must be an object")
    defaults = dataclasses.asdict(Thresholds())
    unknown = sorted(set(value) - set(defaults))
    if unknown:
        raise ConfigError("unknown threshold(s): " + ", ".join(unknown))
    merged = {**defaults, **value}
    for key, item in merged.items():
        if isinstance(defaults[key], int):
            if isinstance(item, bool) or not isinstance(item, int) or item < 0:
                raise ConfigError(f"thresholds.{key} must be a non-negative integer")
        else:
            if isinstance(item, bool) or not isinstance(item, (int, float)) or item < 0:
                raise ConfigError(f"thresholds.{key} must be a non-negative number")
            merged[key] = float(item)
    if merged["connection_warning"] >= merged["connection_critical"]:
        raise ConfigError("connection_warning must be below connection_critical")
    if merged["longest_query_warning_seconds"] >= merged["longest_query_critical_seconds"]:
        raise ConfigError("longest query warning must be below critical")
    if merged["oldest_transaction_warning_seconds"] >= merged["oldest_transaction_critical_seconds"]:
        raise ConfigError("oldest transaction warning must be below critical")
    return Thresholds(**merged)


def load_config(path: Path) -> Tuple[List[NodeConfig], Thresholds]:
    """Load and validate a collector JSON configuration."""

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ConfigError(f"cannot read configuration {path}: {exc}") from exc
    if not isinstance(value, Mapping):
        raise ConfigError("configuration root must be an object")
    if value.get("version") != CONFIG_VERSION:
        raise ConfigError(f"configuration version must be {CONFIG_VERSION}")
    node_values = value.get("nodes")
    if not isinstance(node_values, list) or not node_values:
        raise ConfigError("nodes must be a non-empty array")
    nodes = [parse_node(item, index) for index, item in enumerate(node_values)]
    names = [node.name for node in nodes]
    if len(names) != len(set(names)):
        raise ConfigError("node names must be unique")
    return nodes, parse_thresholds(value.get("thresholds"))


def psql_command(node: NodeConfig, psql: str, statement_timeout_ms: int) -> List[str]:
    """Build a password-free psql command with stable output settings."""

    return [
        psql,
        "-X",
        "-A",
        "-t",
        "-v",
        "ON_ERROR_STOP=1",
        "-h",
        node.host,
        "-p",
        str(node.port),
        "-U",
        node.user,
        "-d",
        node.database,
        "-c",
        f"SET statement_timeout = {statement_timeout_ms}; " + COLLECTION_SQL,
    ]


def parse_json_lines(output: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Parse JSON objects emitted one per psql output line."""

    metrics: List[Dict[str, Any]] = []
    events: List[Dict[str, Any]] = []
    for line_number, line in enumerate(output.splitlines(), 1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ObserverError(f"psql returned invalid JSON on line {line_number}: {exc}") from exc
        if not isinstance(value, dict):
            raise ObserverError(f"psql JSON line {line_number} is not an object")
        record_type = value.get("type")
        if record_type == "metric":
            name = value.get("name")
            metric_value = value.get("value")
            labels = value.get("labels", {})
            if not isinstance(name, str) or not METRIC_NAME_RE.fullmatch(name):
                raise ObserverError(f"invalid metric name on line {line_number}")
            if isinstance(metric_value, bool) or not isinstance(metric_value, (int, float)):
                raise ObserverError(f"metric {name} has a non-numeric value")
            if not isinstance(labels, dict) or not all(
                isinstance(key, str) and isinstance(item, str) for key, item in labels.items()
            ):
                raise ObserverError(f"metric {name} has invalid labels")
            metrics.append({"name": name, "value": float(metric_value), "labels": labels})
        elif record_type == "event":
            if not isinstance(value.get("event"), str):
                raise ObserverError(f"event on line {line_number} has no name")
            events.append(value)
        else:
            raise ObserverError(f"unknown record type on line {line_number}: {record_type!r}")
    metrics.sort(key=lambda metric: (metric["name"], sorted(metric["labels"].items())))
    events.sort(key=lambda event: (event.get("event", ""), str(event.get("pid", ""))))
    return metrics, events


def collect_node(
    node: NodeConfig,
    psql: str,
    runner: CommandRunner,
    timeout: float,
    statement_timeout_ms: int,
    base_environment: Optional[Mapping[str, str]] = None,
) -> Dict[str, Any]:
    """Collect one node and always return a structured result."""

    started = dt.datetime.now(dt.timezone.utc)
    environment = dict(os.environ if base_environment is None else base_environment)
    environment.update(
        {
            "PGCONNECT_TIMEOUT": str(node.connect_timeout),
            "PGSSLMODE": node.sslmode,
            "PGAPPNAME": "opentenbase_observer",
        }
    )
    command = psql_command(node, psql, statement_timeout_ms)
    try:
        completed = runner.run(command, environment, timeout)
        duration = (dt.datetime.now(dt.timezone.utc) - started).total_seconds()
        if completed.returncode != 0:
            error = completed.stderr.strip() or f"psql exited with status {completed.returncode}"
            return {
                "name": node.name,
                "role": node.role,
                "endpoint": f"{node.host}:{node.port}",
                "ok": False,
                "duration_seconds": duration,
                "error": error[:2000],
                "metrics": [],
                "events": [],
            }
        metrics, events = parse_json_lines(completed.stdout)
        metrics.append({"name": "opentenbase_up", "value": 1.0, "labels": {}})
        metrics.sort(key=lambda metric: (metric["name"], sorted(metric["labels"].items())))
        return {
            "name": node.name,
            "role": node.role,
            "endpoint": f"{node.host}:{node.port}",
            "ok": True,
            "duration_seconds": duration,
            "metrics": metrics,
            "events": events,
        }
    except subprocess.TimeoutExpired:
        duration = (dt.datetime.now(dt.timezone.utc) - started).total_seconds()
        return {
            "name": node.name,
            "role": node.role,
            "endpoint": f"{node.host}:{node.port}",
            "ok": False,
            "duration_seconds": duration,
            "error": f"collection exceeded {timeout:g} seconds",
            "metrics": [],
            "events": [],
        }
    except (OSError, ObserverError) as exc:
        duration = (dt.datetime.now(dt.timezone.utc) - started).total_seconds()
        return {
            "name": node.name,
            "role": node.role,
            "endpoint": f"{node.host}:{node.port}",
            "ok": False,
            "duration_seconds": duration,
            "error": str(exc)[:2000],
            "metrics": [],
            "events": [],
        }


def collect_snapshot(
    nodes: Sequence[NodeConfig],
    psql: str = "psql",
    jobs: int = 4,
    timeout: float = 15.0,
    statement_timeout_ms: int = 10000,
    runner: Optional[CommandRunner] = None,
    environment: Optional[Mapping[str, str]] = None,
) -> Dict[str, Any]:
    """Collect nodes concurrently and return a deterministic snapshot."""

    if jobs < 1:
        raise ObserverError("jobs must be at least 1")
    if timeout <= 0:
        raise ObserverError("timeout must be positive")
    if statement_timeout_ms < 1:
        raise ObserverError("statement timeout must be positive")
    adapter = runner or CommandRunner()

    def collect(item: NodeConfig) -> Dict[str, Any]:
        return collect_node(item, psql, adapter, timeout, statement_timeout_ms, environment)

    with concurrent.futures.ThreadPoolExecutor(max_workers=min(jobs, len(nodes))) as executor:
        results = list(executor.map(collect, nodes))
    results.sort(key=lambda result: result["name"])
    return {
        "format": SNAPSHOT_FORMAT,
        "version": SNAPSHOT_VERSION,
        "collected_at": utc_now(),
        "nodes": results,
        "summary": {
            "nodes_total": len(results),
            "nodes_up": sum(1 for result in results if result["ok"]),
            "nodes_down": sum(1 for result in results if not result["ok"]),
            "metrics": sum(len(result["metrics"]) for result in results),
            "events": sum(len(result["events"]) for result in results),
        },
    }


def validate_metric(metric: Any, node_name: str) -> Dict[str, Any]:
    """Validate a stored metric record."""

    if not isinstance(metric, dict):
        raise SnapshotError(f"metric on {node_name} must be an object")
    name = metric.get("name")
    value = metric.get("value")
    labels = metric.get("labels", {})
    if not isinstance(name, str) or not METRIC_NAME_RE.fullmatch(name):
        raise SnapshotError(f"invalid metric name on {node_name}")
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise SnapshotError(f"metric {name} on {node_name} must be finite and numeric")
    if not isinstance(labels, dict):
        raise SnapshotError(f"metric {name} labels on {node_name} must be an object")
    for key, item in labels.items():
        if not isinstance(key, str) or not LABEL_NAME_RE.fullmatch(key) or not isinstance(item, str):
            raise SnapshotError(f"metric {name} has invalid labels on {node_name}")
    return {"name": name, "value": float(value), "labels": labels}


def validate_snapshot(value: Any) -> Dict[str, Any]:
    """Validate a snapshot loaded from disk and normalize metric values."""

    if not isinstance(value, dict):
        raise SnapshotError("snapshot root must be an object")
    if value.get("format") != SNAPSHOT_FORMAT or value.get("version") != SNAPSHOT_VERSION:
        raise SnapshotError("unsupported snapshot format or version")
    parse_timestamp(value.get("collected_at"))
    nodes = value.get("nodes")
    if not isinstance(nodes, list):
        raise SnapshotError("snapshot nodes must be an array")
    names: Set[str] = set()
    normalized_nodes: List[Dict[str, Any]] = []
    for index, node in enumerate(nodes):
        if not isinstance(node, dict):
            raise SnapshotError(f"nodes[{index}] must be an object")
        name = node.get("name")
        role = node.get("role")
        ok = node.get("ok")
        if not isinstance(name, str) or not name:
            raise SnapshotError(f"nodes[{index}].name is invalid")
        if name in names:
            raise SnapshotError(f"duplicate node name: {name}")
        names.add(name)
        if role not in VALID_ROLES:
            raise SnapshotError(f"invalid role for node {name}")
        if not isinstance(ok, bool):
            raise SnapshotError(f"ok flag for node {name} must be boolean")
        metrics = node.get("metrics", [])
        events = node.get("events", [])
        if not isinstance(metrics, list) or not isinstance(events, list):
            raise SnapshotError(f"metrics and events on {name} must be arrays")
        normalized = dict(node)
        normalized["metrics"] = [validate_metric(metric, name) for metric in metrics]
        normalized_nodes.append(normalized)
    result = dict(value)
    result["nodes"] = normalized_nodes
    return result


def load_snapshot(path: Path) -> Dict[str, Any]:
    """Load and validate a JSON snapshot."""

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SnapshotError(f"cannot read snapshot {path}: {exc}") from exc
    return validate_snapshot(value)


def atomic_write_json(path: Path, value: Mapping[str, Any]) -> None:
    """Atomically persist a snapshot or report."""

    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=path.name + ".tmp-", dir=str(path.parent))
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(value, stream, sort_keys=True, indent=2, ensure_ascii=False, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def metric_map(node: Mapping[str, Any]) -> Dict[str, float]:
    """Index unlabeled node metrics by name."""

    return {
        metric["name"]: float(metric["value"])
        for metric in node.get("metrics", [])
        if not metric.get("labels")
    }


def add_finding(
    findings: List[Finding],
    severity: str,
    code: str,
    node: str,
    summary: str,
    evidence: Mapping[str, Any],
    recommendation: str,
) -> None:
    """Append a finding while enforcing known severities."""

    if severity not in SEVERITY_ORDER:
        raise ObserverError(f"unknown finding severity: {severity}")
    findings.append(Finding(severity, code, node, summary, evidence, recommendation))


def analyze_node(node: Mapping[str, Any], thresholds: Thresholds) -> List[Finding]:
    """Apply explainable operational rules to one node result."""

    findings: List[Finding] = []
    name = str(node["name"])
    if not node.get("ok"):
        add_finding(
            findings,
            "critical",
            "node_unreachable",
            name,
            "Node collection failed",
            {"error": node.get("error", "unknown error"), "endpoint": node.get("endpoint")},
            "Check node process state, routing, authentication, and the recorded psql error.",
        )
        return findings
    metrics = metric_map(node)
    connections = metrics.get("opentenbase_connections_total", 0)
    maximum = metrics.get("opentenbase_max_connections", 0)
    if maximum > 0:
        utilization = connections / maximum
        if utilization >= thresholds.connection_critical:
            severity = "critical"
        elif utilization >= thresholds.connection_warning:
            severity = "warning"
        else:
            severity = "info"
        if severity != "info":
            add_finding(
                findings,
                severity,
                "connection_pressure",
                name,
                f"Connection utilization is {utilization:.1%}",
                {"connections": connections, "max_connections": maximum, "ratio": utilization},
                "Inspect connection pools and long-lived sessions before raising max_connections.",
            )
    idle = metrics.get("opentenbase_idle_in_transaction", 0)
    if idle >= thresholds.idle_in_transaction_warning:
        add_finding(
            findings,
            "warning",
            "idle_in_transaction",
            name,
            f"{int(idle)} session(s) are idle in transaction",
            {"sessions": idle},
            "End abandoned transactions and set idle_in_transaction_session_timeout where appropriate.",
        )
    waiting = metrics.get("opentenbase_waiting_locks", 0)
    if waiting >= thresholds.waiting_locks_warning:
        add_finding(
            findings,
            "warning",
            "lock_waiters",
            name,
            f"{int(waiting)} lock request(s) are waiting",
            {"waiting_locks": waiting},
            "Inspect waiting_lock events, blocking transactions, and lock acquisition order.",
        )
    longest_query = metrics.get("opentenbase_longest_query_seconds", 0)
    if longest_query >= thresholds.longest_query_critical_seconds:
        query_severity = "critical"
    elif longest_query >= thresholds.longest_query_warning_seconds:
        query_severity = "warning"
    else:
        query_severity = "info"
    if query_severity != "info":
        add_finding(
            findings,
            query_severity,
            "long_running_query",
            name,
            f"Longest active query has run for {longest_query:.1f} seconds",
            {"seconds": longest_query},
            "Review the matching long_query event, its plan, wait event, and data-node distribution.",
        )
    oldest_xact = metrics.get("opentenbase_oldest_transaction_seconds", 0)
    if oldest_xact >= thresholds.oldest_transaction_critical_seconds:
        xact_severity = "critical"
    elif oldest_xact >= thresholds.oldest_transaction_warning_seconds:
        xact_severity = "warning"
    else:
        xact_severity = "info"
    if xact_severity != "info":
        add_finding(
            findings,
            xact_severity,
            "old_transaction",
            name,
            f"Oldest transaction is {oldest_xact:.1f} seconds old",
            {"seconds": oldest_xact},
            "Identify the owning session; old transactions retain snapshots and can delay vacuum cleanup.",
        )
    blocks_read = metrics.get("opentenbase_blocks_read_total", 0)
    blocks_hit = metrics.get("opentenbase_blocks_hit_total", 0)
    block_total = blocks_read + blocks_hit
    if block_total > 0:
        hit_ratio = blocks_hit / block_total
        if hit_ratio < thresholds.cache_hit_warning:
            add_finding(
                findings,
                "warning",
                "low_cache_hit_ratio",
                name,
                f"Cumulative buffer cache hit ratio is {hit_ratio:.1%}",
                {"blocks_hit": blocks_hit, "blocks_read": blocks_read, "ratio": hit_ratio},
                "Compare interval deltas, working-set size, query plans, and shared_buffers before tuning.",
            )
    commits = metrics.get("opentenbase_xact_commit_total", 0)
    rollbacks = metrics.get("opentenbase_xact_rollback_total", 0)
    transactions = commits + rollbacks
    if transactions >= thresholds.rollback_ratio_minimum_transactions:
        rollback_ratio = rollbacks / transactions
        if rollback_ratio >= thresholds.rollback_ratio_warning:
            add_finding(
                findings,
                "warning",
                "high_rollback_ratio",
                name,
                f"Cumulative rollback ratio is {rollback_ratio:.1%}",
                {"commits": commits, "rollbacks": rollbacks, "ratio": rollback_ratio},
                "Use snapshot diff rates and application errors to identify recurring failed transactions.",
            )
    return findings


def analyze_snapshot(snapshot: Mapping[str, Any], thresholds: Optional[Thresholds] = None) -> Dict[str, Any]:
    """Analyze every node and return sorted findings and severity counts."""

    normalized = validate_snapshot(snapshot)
    active_thresholds = thresholds or Thresholds()
    findings: List[Finding] = []
    for node in normalized["nodes"]:
        findings.extend(analyze_node(node, active_thresholds))
    findings.sort(key=lambda item: (-SEVERITY_ORDER[item.severity], item.node, item.code))
    counts = {severity: 0 for severity in SEVERITY_ORDER}
    for finding in findings:
        counts[finding.severity] += 1
    return {
        "snapshot_collected_at": normalized["collected_at"],
        "finding_count": len(findings),
        "severities": counts,
        "findings": [finding.to_dict() for finding in findings],
    }


def prometheus_escape(value: str) -> str:
    """Escape a Prometheus label value."""

    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def format_number(value: float) -> str:
    """Format finite metric values without unnecessary decimal noise."""

    if not math.isfinite(value):
        raise SnapshotError("Prometheus metric value is not finite")
    if value.is_integer():
        return str(int(value))
    return format(value, ".17g")


def render_prometheus(snapshot: Mapping[str, Any], include_findings: bool = True) -> str:
    """Render a validated snapshot in Prometheus text exposition format."""

    normalized = validate_snapshot(snapshot)
    lines = [
        "# HELP opentenbase_observer_collection_duration_seconds Collector duration per node.",
        "# TYPE opentenbase_observer_collection_duration_seconds gauge",
    ]
    for node in normalized["nodes"]:
        base_labels = {"node": node["name"], "role": node["role"]}
        for metric in node["metrics"]:
            labels = {**base_labels, **metric.get("labels", {})}
            label_text = ",".join(
                f'{key}="{prometheus_escape(str(value))}"' for key, value in sorted(labels.items())
            )
            lines.append(f'{metric["name"]}{{{label_text}}} {format_number(float(metric["value"]))}')
        duration = float(node.get("duration_seconds", 0))
        label_text = ",".join(
            f'{key}="{prometheus_escape(str(value))}"' for key, value in sorted(base_labels.items())
        )
        lines.append(
            f"opentenbase_observer_collection_duration_seconds{{{label_text}}} {format_number(duration)}"
        )
        if not node["ok"]:
            lines.append(f"opentenbase_up{{{label_text}}} 0")
    if include_findings:
        report = analyze_snapshot(normalized)
        lines.extend(
            [
                "# HELP opentenbase_observer_findings Diagnostic findings by severity.",
                "# TYPE opentenbase_observer_findings gauge",
            ]
        )
        for severity, count in sorted(report["severities"].items()):
            lines.append(f'opentenbase_observer_findings{{severity="{severity}"}} {count}')
    return "\n".join(lines) + "\n"


def metric_identity(node_name: str, metric: Mapping[str, Any]) -> Tuple[str, str, Tuple[Tuple[str, str], ...]]:
    """Return a stable key for cross-snapshot metric comparison."""

    return node_name, str(metric["name"]), tuple(sorted(metric.get("labels", {}).items()))


def flatten_metrics(snapshot: Mapping[str, Any]) -> Dict[Tuple[str, str, Tuple[Tuple[str, str], ...]], float]:
    """Flatten all node metrics into identity/value pairs."""

    result: Dict[Tuple[str, str, Tuple[Tuple[str, str], ...]], float] = {}
    for node in snapshot["nodes"]:
        for metric in node["metrics"]:
            result[metric_identity(node["name"], metric)] = float(metric["value"])
    return result


def diff_snapshots(before: Mapping[str, Any], after: Mapping[str, Any]) -> Dict[str, Any]:
    """Calculate metric deltas and rates between validated snapshots."""

    first = validate_snapshot(before)
    second = validate_snapshot(after)
    first_time = parse_timestamp(first["collected_at"])
    second_time = parse_timestamp(second["collected_at"])
    elapsed = (second_time - first_time).total_seconds()
    if elapsed <= 0:
        raise SnapshotError("after snapshot must be newer than before snapshot")
    left = flatten_metrics(first)
    right = flatten_metrics(second)
    entries: List[Dict[str, Any]] = []
    for identity in sorted(set(left) & set(right)):
        node, name, labels = identity
        before_value = left[identity]
        after_value = right[identity]
        delta = after_value - before_value
        counter = name in COUNTER_METRICS
        reset = counter and delta < 0
        entry = {
            "node": node,
            "name": name,
            "labels": dict(labels),
            "before": before_value,
            "after": after_value,
            "delta": delta,
            "counter_reset": reset,
        }
        if counter and not reset:
            entry["per_second"] = delta / elapsed
        entries.append(entry)
    first_nodes = {node["name"] for node in first["nodes"]}
    second_nodes = {node["name"] for node in second["nodes"]}
    return {
        "before": first["collected_at"],
        "after": second["collected_at"],
        "elapsed_seconds": elapsed,
        "added_nodes": sorted(second_nodes - first_nodes),
        "removed_nodes": sorted(first_nodes - second_nodes),
        "metrics": entries,
    }


def emit_json(value: Any, stream: Any = None) -> None:
    """Write stable JSON to the current stdout by default."""

    if stream is None:
        stream = sys.stdout
    json.dump(value, stream, sort_keys=True, indent=2, ensure_ascii=False, allow_nan=False)
    stream.write("\n")


def positive_int(value: str) -> int:
    try:
        result = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if result < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return result


def positive_float(value: str) -> float:
    try:
        result = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc
    if result <= 0 or not math.isfinite(result):
        raise argparse.ArgumentTypeError("must be a finite positive number")
    return result


def build_parser() -> argparse.ArgumentParser:
    """Create the command-line interface."""

    parser = argparse.ArgumentParser(
        prog="opentenbase_observer",
        description="Collect, analyze, and export OpenTenBase node diagnostics.",
    )
    parser.add_argument("--version", action="version", version="%(prog)s 1.0")
    commands = parser.add_subparsers(dest="command", required=True)

    check = commands.add_parser("check-config", help="validate collector configuration")
    check.add_argument("config", type=Path)

    collect = commands.add_parser("collect", help="collect all configured nodes")
    collect.add_argument("config", type=Path)
    collect.add_argument("--output", type=Path, required=True)
    collect.add_argument("--psql", default="psql")
    collect.add_argument("--jobs", type=positive_int, default=4)
    collect.add_argument("--timeout", type=positive_float, default=15.0)
    collect.add_argument("--statement-timeout-ms", type=positive_int, default=10000)

    analyze = commands.add_parser("analyze", help="analyze a stored snapshot")
    analyze.add_argument("snapshot", type=Path)
    analyze.add_argument("--config", type=Path, help="use threshold overrides from config")
    analyze.add_argument("--fail-on", choices=["never", "warning", "critical"], default="never")

    prometheus = commands.add_parser("prometheus", help="render Prometheus text")
    prometheus.add_argument("snapshot", type=Path)
    prometheus.add_argument("--no-findings", action="store_true")

    difference = commands.add_parser("diff", help="compare two snapshots")
    difference.add_argument("before", type=Path)
    difference.add_argument("after", type=Path)
    return parser


def run(args: argparse.Namespace, runner: Optional[CommandRunner] = None) -> int:
    """Execute one parsed command."""

    if args.command == "check-config":
        nodes, thresholds = load_config(args.config)
        emit_json(
            {
                "ok": True,
                "nodes": [dataclasses.asdict(node) for node in nodes],
                "thresholds": dataclasses.asdict(thresholds),
            }
        )
        return 0
    if args.command == "collect":
        nodes, _ = load_config(args.config)
        snapshot = collect_snapshot(
            nodes,
            psql=args.psql,
            jobs=args.jobs,
            timeout=args.timeout,
            statement_timeout_ms=args.statement_timeout_ms,
            runner=runner,
        )
        atomic_write_json(args.output, snapshot)
        emit_json(snapshot["summary"])
        return 0 if snapshot["summary"]["nodes_down"] == 0 else 2
    if args.command == "analyze":
        snapshot = load_snapshot(args.snapshot)
        thresholds = load_config(args.config)[1] if args.config else Thresholds()
        report = analyze_snapshot(snapshot, thresholds)
        emit_json(report)
        if args.fail_on == "critical" and report["severities"]["critical"]:
            return 2
        if args.fail_on == "warning" and (
            report["severities"]["critical"] or report["severities"]["warning"]
        ):
            return 2
        return 0
    if args.command == "prometheus":
        sys.stdout.write(render_prometheus(load_snapshot(args.snapshot), not args.no_findings))
        return 0
    if args.command == "diff":
        emit_json(diff_snapshots(load_snapshot(args.before), load_snapshot(args.after)))
        return 0
    raise ObserverError(f"unsupported command: {args.command}")


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI entry point."""

    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return run(args)
    except (ObserverError, OSError) as exc:
        print(f"opentenbase_observer: error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
