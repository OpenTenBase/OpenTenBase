#!/usr/bin/env python3
"""Run a fixed post-build Recall@10 health check for any registered dataset."""

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from common.config import metric_names
from common.datasets import load_metric
from common.db import connect_dsn
from common.ground_truth import truth_for_rows
from common.query_quality import measure_query


def validate(dsn: str, dataset, rows: int, probes: int,
             query_count: int = 100, k: int = 10) -> dict:
    truth = truth_for_rows(dataset, dataset.metric, rows, query_count, k)
    with connect_dsn(dsn) as conn:
        result = measure_query(
            conn, dataset.queries[:query_count], truth,
            dataset.metric, probes, k,
            planner_mode="forced_validation", include_details=False,
        )
        result.pop("samples", None)
        return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--metric", choices=metric_names(), required=True)
    parser.add_argument("--data-root", type=Path, default=ROOT / "data")
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--probes", type=int, default=10)
    parser.add_argument("--query-count", type=int, default=100)
    parser.add_argument("--k", type=int, default=10)
    args = parser.parse_args()
    dataset = load_metric(args.metric, args.data_root, args.query_count, args.k)
    print(json.dumps(validate(args.dsn, dataset, args.rows, args.probes,
                              args.query_count, args.k), indent=2))


if __name__ == "__main__":
    main()
