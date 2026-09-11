"""Recall and latency aggregation."""

import numpy as np


def recall_at_k(actual: list[int], expected: np.ndarray, k: int) -> float:
    return len(set(actual[:k]).intersection(map(int, expected[:k]))) / k


def summarize(latencies_ms: list[float], recalls: list[float], k: int,
              returned_rows: list[int] | None = None) -> dict:
    latency = np.asarray(latencies_ms, dtype=np.float64)
    recall = np.asarray(recalls, dtype=np.float64)
    if latency.size == 0 or recall.size == 0:
        raise ValueError("latencies and recalls must not be empty")
    if latency.size != recall.size:
        raise ValueError("latencies and recalls must have the same length")
    total_seconds = float(latency.sum() / 1000.0)
    mean_recall = float(recall.mean())
    rows = returned_rows or []
    return {
        "k": k,
        "recall": mean_recall,
        "recall_at_k": mean_recall,
        "query_count": int(latency.size),
        "mean_ms": float(latency.mean()),
        "p50_ms": float(np.percentile(latency, 50)),
        "p95_ms": float(np.percentile(latency, 95)),
        "p99_ms": float(np.percentile(latency, 99)),
        "stddev_ms": float(latency.std()),
        "total_seconds": total_seconds,
        "qps": float(latency.size / total_seconds) if total_seconds > 0 else None,
        "single_client_qps": float(latency.size / total_seconds) if total_seconds > 0 else None,
        "returned_rows_min": min(rows) if rows else None,
        "returned_rows_max": max(rows) if rows else None,
    }
