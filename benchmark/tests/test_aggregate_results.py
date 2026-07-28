# Copyright (c) 2026 OpenTenBase Authors
# Licensed under the BSD 3-Clause License.

import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path


BENCHMARK_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCHMARK_DIR))

from aggregate_results import aggregate_runs  # noqa: E402
from analyze_results import SUMMARY_FIELDS  # noqa: E402


class AggregateTests(unittest.TestCase):
    def make_run(self, root: Path, name: str, tps: float, clients: int = 1) -> Path:
        path = root / name
        path.mkdir()
        environment = {
            "cluster": {"node_group": "default_group"},
            "connection": {"host": "cn", "port": 5432, "database": "bench"},
            "data": {"account_count": 10000},
            "run": {"clients": [1]},
            "source_commit": "abc123",
            "tools": {"pgbench": "10"},
            "workloads": ["point_read"],
        }
        (path / "environment.json").write_text(
            json.dumps(environment), encoding="utf-8"
        )
        row = dict.fromkeys(SUMMARY_FIELDS, 0)
        row.update(
            {
                "workload": "point_read",
                "clients": clients,
                "jobs": clients,
                "duration_s": 5,
                "transactions": 100,
                "latency_avg_ms": 2,
                "latency_p95_ms": 3,
                "latency_p99_ms": 4,
                "tps": tps,
                "raw_log": "raw.txt",
            }
        )
        with (path / "summary.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=SUMMARY_FIELDS)
            writer.writeheader()
            writer.writerow(row)
        return path

    def test_aggregates_median_and_sample_cv(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = [
                self.make_run(root, "one", 90),
                self.make_run(root, "two", 100),
                self.make_run(root, "three", 110),
            ]
            rows = aggregate_runs(paths)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["runs"], 3)
        self.assertEqual(rows[0]["tps_median"], 100)
        self.assertAlmostEqual(rows[0]["tps_cv_pct"], 10)
        self.assertEqual(rows[0]["transactions"], 300)

    def test_rejects_different_matrix(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = self.make_run(root, "one", 100)
            second = self.make_run(root, "two", 100, clients=4)
            with self.assertRaisesRegex(ValueError, "matrix differs"):
                aggregate_runs([first, second])

    def test_rejects_different_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = self.make_run(root, "one", 100)
            second = self.make_run(root, "two", 100)
            environment_path = second / "environment.json"
            environment = json.loads(environment_path.read_text(encoding="utf-8"))
            environment["source_commit"] = "different"
            environment_path.write_text(json.dumps(environment), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "not comparable"):
                aggregate_runs([first, second])


if __name__ == "__main__":
    unittest.main()
