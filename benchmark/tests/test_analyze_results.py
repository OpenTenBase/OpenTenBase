# Copyright (c) 2026 OpenTenBase Authors
# Licensed under the BSD 3-Clause License.

import sys
import tempfile
import unittest
from pathlib import Path


BENCHMARK_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCHMARK_DIR))

from analyze_results import (  # noqa: E402
    analyze,
    add_scaling_metrics,
    build_hypotheses,
    parse_pgbench_output,
    read_distribution,
)


class PgbenchParserTests(unittest.TestCase):
    def test_prefers_tps_excluding_connection_setup(self):
        parsed = parse_pgbench_output(
            """
number of transactions actually processed: 12,345/12345
latency average = 1.250 ms
tps = 799.000000 (including connections establishing)
tps = 800.000000 (excluding connections establishing)
"""
        )
        self.assertEqual(parsed["transactions"], 12345)
        self.assertEqual(parsed["failed"], 0)
        self.assertEqual(parsed["latency_avg_ms"], 1.25)
        self.assertEqual(parsed["tps"], 800.0)

    def test_parses_recent_failure_and_stddev_fields(self):
        parsed = parse_pgbench_output(
            """
number of transactions actually processed: 100
number of failed transactions: 2 (1.960%)
latency average = 20.4 ms
latency stddev = 4.2 ms
initial connection time = 31.8 ms
tps = 49.01 (without initial connection time)
"""
        )
        self.assertEqual(parsed["failed"], 2)
        self.assertEqual(parsed["latency_stddev_ms"], 4.2)
        self.assertEqual(parsed["initial_connection_ms"], 31.8)


class AnalysisTests(unittest.TestCase):
    def test_marks_throughput_plateau_as_saturation(self):
        rows = [
            {
                "workload": "point_read",
                "clients": 1,
                "tps": 100.0,
                "latency_avg_ms": 10.0,
            },
            {
                "workload": "point_read",
                "clients": 4,
                "tps": 350.0,
                "latency_avg_ms": 11.0,
            },
            {
                "workload": "point_read",
                "clients": 16,
                "tps": 355.0,
                "latency_avg_ms": 45.0,
            },
        ]
        result = add_scaling_metrics(rows)
        self.assertFalse(result[1]["saturated"])
        self.assertTrue(result[2]["saturated"])
        self.assertAlmostEqual(result[0]["scaling_efficiency"], 1.0)

    def test_reads_distribution_skew(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "distribution.csv"
            path.write_text(
                "table_name,node_id,row_count\n"
                "account,1,50\n"
                "account,2,150\n",
                encoding="utf-8",
            )
            self.assertEqual(read_distribution(path)["account"], 1.5)

    def test_cn_hypothesis_requires_saturation_and_cpu_contrast(self):
        rows = [
            {
                "transactions": 100,
                "failed": 0,
                "saturated": True,
            }
        ]
        hosts = {
            "cn": {"avg_cpu_pct": 91.0, "max_iowait_pct": 1.0},
            "dn": {"avg_cpu_pct": 40.0, "max_iowait_pct": 2.0},
        }
        hypotheses = build_hypotheses(rows, hosts, {}, 10000)
        self.assertTrue(any("CN 可能先饱和" in item for item in hypotheses))

    def test_end_to_end_report_contains_evidence_sections(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "summary.csv").write_text(
                "workload,clients,jobs,duration_s,transactions,failed,"
                "latency_avg_ms,latency_stddev_ms,latency_p50_ms,"
                "latency_p95_ms,latency_p99_ms,latency_sample_count,"
                "tps,exit_code,raw_log\n"
                "point_read,1,1,60,6000,0,10,1,9,18,25,600,100,0,raw/a.txt\n"
                "point_read,4,4,60,12000,0,20,2,18,35,50,1200,200,0,raw/b.txt\n",
                encoding="utf-8",
            )
            (root / "environment.json").write_text(
                '{"cluster": {"network_mbps": 10000}}', encoding="utf-8"
            )
            (root / "distribution.csv").write_text(
                "table_name,node_id,row_count\naccount,1,100\naccount,2,100\n",
                encoding="utf-8",
            )
            output = root / "report.md"
            analyze(root, output)
            report = output.read_text(encoding="utf-8")
            self.assertIn("## 性能结果", report)
            self.assertIn("## 瓶颈假设与下一步", report)
            self.assertIn("`account`", report)


if __name__ == "__main__":
    unittest.main()
