"""Configuration for the pgvector IVFFlat diagnosis benchmark."""

import os
from pathlib import Path


BENCHMARK_ROOT = Path(__file__).resolve().parents[1]

METRIC_CONFIG = {
    "l2": {
        "dataset": "sift1m", "result_name": "sift1m-l2", "dimension": 128,
        "expected_size": 1_000_000, "operator": "<->", "opclass": "vector_l2_ops",
        "ground_truth": "official SIFT1M L2",
    },
    "cosine": {
        "dataset": "glove100", "result_name": "glove100-cosine", "dimension": 100,
        "expected_size": 1_183_514, "operator": "<=>", "opclass": "vector_cosine_ops",
        "ground_truth": "ANN-Benchmarks angular",
    },
    "ip": {
        "dataset": "music100", "result_name": "music100-ip", "dimension": 100,
        "expected_size": 1_000_000, "operator": "<#>", "opclass": "vector_ip_ops",
        "ground_truth": "Music100 MIPS Top-100",
    },
}

BENCHMARK_DATABASE = os.environ.get("PGDATABASE", "pgvector_benchmark")
PROJECT_DATABASES = {"build": BENCHMARK_DATABASE}

PGDATA = Path(os.environ.get("PGDATA", BENCHMARK_ROOT / "pgdata"))
PGHOST = os.environ.get("PGHOST", "/tmp")
PGPORT = int(os.environ.get("PGPORT", "5432"))
PGLOG = Path(os.environ.get("PGLOG", PGDATA / "postgresql.log"))
PGBIN = Path(os.environ["PGBIN"]) if os.environ.get("PGBIN") else None
PGUSER = os.environ.get("PGUSER")
MAINTENANCE_WORK_MEM = os.environ.get("MAINTENANCE_WORK_MEM", "1GB")


def metric_names() -> tuple[str, ...]:
    return tuple(METRIC_CONFIG)


def metric_config(metric: str) -> dict:
    try:
        return METRIC_CONFIG[metric]
    except KeyError as exc:
        raise ValueError(f"unsupported metric {metric!r}; choose l2, cosine, ip, or all") from exc


def resolve_metrics(metric: str) -> list[str]:
    if metric == "all":
        return list(METRIC_CONFIG)
    metric_config(metric)
    return [metric]
