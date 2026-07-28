#!/usr/bin/env python3
# Copyright (c) 2026 OpenTenBase Authors
# Licensed under the BSD 3-Clause License.
"""Aggregate comparable OpenTenBase benchmark runs."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence, Set, Tuple

from analyze_results import read_summary


FIELDS = (
    "workload",
    "clients",
    "runs",
    "tps_median",
    "tps_cv_pct",
    "latency_avg_median_ms",
    "latency_p95_median_ms",
    "latency_p99_median_ms",
    "transactions",
    "failed",
    "failure_rate_pct",
)


def comparison_signature(environment: Mapping[str, object]) -> Dict[str, object]:
    """Return fields that must match before results can be aggregated."""

    return {
        key: environment.get(key)
        for key in (
            "cluster",
            "connection",
            "data",
            "run",
            "source_commit",
            "tools",
            "workloads",
        )
    }


def read_run(path: Path) -> Tuple[Dict[str, object], List[Dict[str, object]]]:
    environment_path = path / "environment.json"
    summary_path = path / "summary.csv"
    if not environment_path.exists():
        raise FileNotFoundError(f"missing environment file: {environment_path}")
    if not summary_path.exists():
        raise FileNotFoundError(f"missing summary file: {summary_path}")
    environment = json.loads(environment_path.read_text(encoding="utf-8"))
    return comparison_signature(environment), read_summary(summary_path)


def aggregate_runs(input_dirs: Sequence[Path]) -> List[Dict[str, object]]:
    if len(input_dirs) < 2:
        raise ValueError("at least two input directories are required")

    signatures: List[Dict[str, object]] = []
    by_key: Dict[Tuple[str, int], List[Mapping[str, object]]] = defaultdict(list)
    expected_keys: Optional[Set[Tuple[str, int]]] = None
    for path in input_dirs:
        signature, rows = read_run(path)
        signatures.append(signature)
        keys = {(str(row["workload"]), int(row["clients"])) for row in rows}
        if expected_keys is None:
            expected_keys = keys
        elif keys != expected_keys:
            raise ValueError(f"workload/client matrix differs in {path}")
        for row in rows:
            by_key[(str(row["workload"]), int(row["clients"]))].append(row)

    baseline = signatures[0]
    for index, signature in enumerate(signatures[1:], start=2):
        if signature != baseline:
            raise ValueError(f"run {index} environment is not comparable to run 1")

    aggregated: List[Dict[str, object]] = []
    for workload, clients in sorted(by_key):
        rows = by_key[(workload, clients)]
        tps = [float(row["tps"]) for row in rows]
        mean_tps = statistics.fmean(tps)
        cv = statistics.stdev(tps) / mean_tps * 100 if len(tps) > 1 and mean_tps else 0
        transactions = sum(int(row["transactions"]) for row in rows)
        failed = sum(int(row["failed"]) for row in rows)
        aggregated.append(
            {
                "workload": workload,
                "clients": clients,
                "runs": len(rows),
                "tps_median": statistics.median(tps),
                "tps_cv_pct": cv,
                "latency_avg_median_ms": statistics.median(
                    float(row["latency_avg_ms"]) for row in rows
                ),
                "latency_p95_median_ms": statistics.median(
                    float(row["latency_p95_ms"]) for row in rows
                ),
                "latency_p99_median_ms": statistics.median(
                    float(row["latency_p99_ms"]) for row in rows
                ),
                "transactions": transactions,
                "failed": failed,
                "failure_rate_pct": failed / max(transactions + failed, 1) * 100,
            }
        )
    return aggregated


def write_csv(rows: Sequence[Mapping[str, object]], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", action="append", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    write_csv(aggregate_runs(args.input_dir), args.output)
    print(f"aggregate written to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
