"""Shared IVFFlat plan inspection and single-client quality measurement."""

from __future__ import annotations

import json
import time
from typing import Any

from .config import metric_config
from .db import INDEX_NAME, TABLE_NAME, vector_text
from .metrics import recall_at_k, summarize


class QueryMeasurementError(RuntimeError):
    def __init__(self, ordinal: int, cause: Exception):
        super().__init__(f"query {ordinal} failed: {cause}")
        self.ordinal = ordinal
        self.cause = cause


def _ensure_vector_loaded(cur) -> None:
    """Load pgvector's shared library so its _PG_init registers IVFFlat GUCs.

    PostgreSQL can know the SQL type from the catalog without loading the
    extension library.  A vector input operation is therefore required on
    every fresh backend before SHOW/set_config of ivfflat.probes.
    """
    cur.execute("SELECT '[0]'::vector")
    cur.fetchone()


def _settings(cur) -> tuple[str, str]:
    _ensure_vector_loaded(cur)
    cur.execute("SHOW enable_seqscan")
    enable_seqscan = cur.fetchone()[0]
    cur.execute("SHOW ivfflat.probes")
    probes = cur.fetchone()[0]
    return enable_seqscan, probes


def _restore(cur, enable_seqscan: str, probes: str) -> None:
    cur.execute("SELECT set_config('enable_seqscan', %s, false)", (enable_seqscan,))
    cur.execute("SELECT set_config('ivfflat.probes', %s, false)", (probes,))


def _uses_index(node: dict[str, Any], index_name: str) -> bool:
    if node.get("Index Name") == index_name:
        return True
    return any(_uses_index(child, index_name) for child in node.get("Plans", []))


def inspect_query_plan(conn, metric: str, probes: int, query, k: int,
                       index_name: str = INDEX_NAME,
                       planner_mode: str = "natural") -> dict[str, Any]:
    """Return a JSON plan and whether it uses the requested IVFFlat index."""
    if planner_mode not in ("natural", "forced_validation"):
        raise ValueError("planner_mode must be natural or forced_validation")
    operator = metric_config(metric)["operator"]
    statement = (
        f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "
        f"SELECT id FROM {TABLE_NAME} ORDER BY embedding {operator} %s::vector LIMIT %s"
    )
    query_text = query if isinstance(query, str) else vector_text(query)
    with conn.cursor() as cur:
        original_seqscan, original_probes = _settings(cur)
        try:
            cur.execute("SELECT set_config('ivfflat.probes', %s, false)", (str(probes),))
            if planner_mode == "forced_validation":
                cur.execute("SELECT set_config('enable_seqscan', 'off', false)")
            cur.execute(statement, (query_text, k))
            raw = cur.fetchone()[0]
            document = raw if isinstance(raw, list) else json.loads(raw)
            plan = document[0]["Plan"]
            used = _uses_index(plan, index_name)
            return {
                "planner_mode": planner_mode,
                "used_ivfflat": used,
                "index_name": index_name if used else None,
                "plan": document,
                "plan_text": json.dumps(document, indent=2, ensure_ascii=False),
            }
        finally:
            _restore(cur, original_seqscan, original_probes)


def measure_query(conn, queries, truth, metric: str, probes: int, k: int,
                  *, planner_mode: str = "natural", warmup: int = 20,
                  include_details: bool = True) -> dict[str, Any]:
    """Measure one repeat and return both raw samples and their summary."""
    if planner_mode not in ("natural", "forced_validation"):
        raise ValueError("planner_mode must be natural or forced_validation")
    operator = metric_config(metric)["operator"]
    statement = f"SELECT id FROM {TABLE_NAME} ORDER BY embedding {operator} %s::vector LIMIT %s"
    texts = [vector_text(query) for query in queries]
    latencies: list[float] = []
    recalls: list[float] = []
    returned: list[int] = []
    details: list[dict[str, Any]] = []
    with conn.cursor() as cur:
        original_seqscan, original_probes = _settings(cur)
        try:
            cur.execute("SELECT set_config('ivfflat.probes', %s, false)", (str(probes),))
            if planner_mode == "forced_validation":
                cur.execute("SELECT set_config('enable_seqscan', 'off', false)")
            for query in texts[:warmup]:
                cur.execute(statement, (query, k))
                cur.fetchall()
            for ordinal, (query, expected) in enumerate(zip(texts, truth)):
                try:
                    started = time.perf_counter_ns()
                    cur.execute(statement, (query, k))
                    actual = [row[0] for row in cur.fetchall()]
                    latency_ms = (time.perf_counter_ns() - started) / 1_000_000
                except Exception as exc:
                    raise QueryMeasurementError(ordinal, exc) from exc
                recall = recall_at_k(actual, expected, k)
                latencies.append(latency_ms)
                recalls.append(recall)
                returned.append(len(actual))
                if include_details:
                    details.append({
                        "query_ordinal": ordinal,
                        "latency_ms": latency_ms,
                        "recall_at_k": recall,
                        "returned_rows": len(actual),
                    })
        finally:
            _restore(cur, original_seqscan, original_probes)
    return {
        **summarize(latencies, recalls, k, returned),
        "probes": probes,
        "planner_mode": planner_mode,
        "samples": details,
    }
