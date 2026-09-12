"""Source-aligned pgvector 0.8.6/0.8.7 IVFFlat checked-memory estimator."""

from __future__ import annotations

from typing import Any

from common.schema import SCHEMA_VERSION, utc_now


ESTIMATOR_VERSION = "pgvector-0.8.6-0.8.7-vector-64bit-v1"
SUPPORTED_PGVECTOR = ("0.8.6", "0.8.7")
SIZE_MAX = (1 << 63) - 1
VECTOR_ARRAY_HEADER_SIZE = 24
FLOAT_SIZE = 4
INT_SIZE = 4
MAXALIGN = 8


def _safe_add(*values: int) -> int:
    total = 0
    for value in values:
        if value < 0 or total > SIZE_MAX - value:
            raise OverflowError("Size addition overflow")
        total += value
    return total


def _safe_mul(left: int, right: int) -> int:
    if left < 0 or right < 0 or (right and left > SIZE_MAX // right):
        raise OverflowError("Size multiplication overflow")
    return left * right


def _align(value: int) -> int:
    return (value + MAXALIGN - 1) & ~(MAXALIGN - 1)


def _vector_array_size(length: int, item_size: int) -> int:
    return _safe_add(VECTOR_ARRAY_HEADER_SIZE, _safe_mul(length, _align(item_size)))


def _unsupported(pgvector_version: str, reason: str, *,
                 diagnosis_id: str | None, build_id: str | None) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "diagnosis_id": diagnosis_id,
        "build_id": build_id,
        "timestamp_utc": utc_now(),
        "source": "pgvector_source_estimator",
        "availability": "unsupported",
        "reason": reason,
        "pgvector_version": pgvector_version,
        "estimator_version": ESTIMATOR_VERSION,
        "predicted_memory_failure": None,
    }


def estimate_pgvector_checked_memory(*, pgvector_version: str, lists: int,
                                     dimension: int, item_size: int,
                                     heap_blocks: int, eligible_vectors: int,
                                     maintenance_work_mem_bytes: int,
                                     max_heap_tuples_per_page: int = 291,
                                     diagnosis_id: str | None = None,
                                     build_id: str | None = None) -> dict[str, Any]:
    """Estimate the exact allocations included in IvfflatCheckMemoryUsage."""
    if pgvector_version not in SUPPORTED_PGVECTOR:
        return _unsupported(
            pgvector_version,
            f"estimator only supports pgvector {', '.join(SUPPORTED_PGVECTOR)}",
            diagnosis_id=diagnosis_id,
            build_id=build_id,
        )
    if lists < 1 or dimension < 1 or item_size < 1:
        raise ValueError("lists, dimension, and item_size must be positive")
    if heap_blocks < 0 or eligible_vectors < 0 or maintenance_work_mem_bytes < 0:
        raise ValueError("heap, eligible vector, and memory values cannot be negative")

    try:
        max_tuples = _safe_mul(heap_blocks, max_heap_tuples_per_page)
        requested_samples = max(lists * 50, 10000)
        target_samples = max(min(requested_samples, max_tuples), 1)
        actual_samples = min(target_samples, eligible_vectors)

        centers = _vector_array_size(lists, item_size)
        samples = _vector_array_size(target_samples, item_size)
        base_memory = _safe_add(centers, samples)
        parts: dict[str, int] = {
            "centers_bytes": centers,
            "samples_bytes": samples,
        }

        if actual_samples > 0:
            parts.update({
                "new_centers_bytes": _vector_array_size(lists, item_size),
                "aggregate_centers_bytes": _safe_mul(FLOAT_SIZE, _safe_mul(lists, dimension)),
                "center_counts_bytes": _safe_mul(INT_SIZE, lists),
                "closest_centers_bytes": _safe_mul(INT_SIZE, actual_samples),
                "lower_bounds_bytes": _safe_mul(
                    FLOAT_SIZE, _safe_mul(actual_samples, lists)
                ),
                "upper_bounds_bytes": _safe_mul(FLOAT_SIZE, actual_samples),
                "center_half_distances_bytes": _safe_mul(
                    FLOAT_SIZE, _safe_mul(lists, lists)
                ),
                "center_min_distances_bytes": _safe_mul(FLOAT_SIZE, lists),
                "new_center_distances_bytes": _safe_mul(FLOAT_SIZE, lists),
            })
            checked_memory = _safe_add(base_memory, *[
                value for key, value in parts.items()
                if key not in ("centers_bytes", "samples_bytes")
            ])
        else:
            checked_memory = base_memory
    except OverflowError as exc:
        return _unsupported(
            pgvector_version, str(exc), diagnosis_id=diagnosis_id, build_id=build_id
        )

    # IvfflatCheckMemoryUsage compares whole kB values.
    predicted = checked_memory // 1024 > maintenance_work_mem_bytes // 1024
    headroom = maintenance_work_mem_bytes - checked_memory
    return {
        "schema_version": SCHEMA_VERSION,
        "diagnosis_id": diagnosis_id,
        "build_id": build_id,
        "timestamp_utc": utc_now(),
        "source": "pgvector_source_estimator",
        "availability": "available",
        "reason": None,
        "pgvector_version": pgvector_version,
        "estimator_version": ESTIMATOR_VERSION,
        "assumptions": {
            "pointer_bits": 64,
            "maxalign": MAXALIGN,
            "vector_array_header_bytes": VECTOR_ARRAY_HEADER_SIZE,
            "eligible_vectors_must_match_the_selected_opclass_predicate": True,
            "zero_norm_vectors_are_excluded_for_cosine_only": True,
        },
        "lists": lists,
        "dimension": dimension,
        "item_size": item_size,
        "requested_sample_count": requested_samples,
        "target_sample_count": target_samples,
        "actual_sample_count_estimate": actual_samples,
        "components": parts,
        "estimated_pgvector_checked_memory_bytes": checked_memory,
        "memory_limit_bytes": maintenance_work_mem_bytes,
        "memory_headroom_bytes": headroom,
        "memory_headroom_ratio": (
            headroom / maintenance_work_mem_bytes
            if maintenance_work_mem_bytes > 0 else None
        ),
        "predicted_memory_failure": predicted,
    }
