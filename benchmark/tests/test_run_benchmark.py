# Copyright (c) 2026 OpenTenBase Authors
# Licensed under the BSD 3-Clause License.

import json
import sys
import tempfile
import unittest
from pathlib import Path


BENCHMARK_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCHMARK_DIR))

from run_benchmark import (  # noqa: E402
    latency_percentiles,
    load_config,
    parse_shard_count,
    pgbench_command,
)


def valid_config():
    return {
        "connection": {
            "host": "cn.test",
            "port": 15432,
            "user": "tester",
            "database": "benchmark",
        },
        "cluster": {
            "node_group": "default_group",
            "network_mbps": 10000,
        },
        "data": {
            "account_count": 2000,
            "order_count": 10000,
            "event_count": 10000,
        },
        "run": {
            "clients": [1, 4],
            "duration_seconds": 60,
            "warmup_seconds": 5,
            "jobs": 8,
            "mode": "prepared",
            "latency_sample_rate": 0.1,
        },
        "workloads": ["point_read"],
    }


class ConfigurationTests(unittest.TestCase):
    def test_reads_machine_parseable_shard_count(self):
        self.assertEqual(parse_shard_count(" benchmark_shard_count=4096\n"), 4096)
        self.assertEqual(parse_shard_count("benchmark_shard_count=0"), 0)
        self.assertIsNone(parse_shard_count("shard count unavailable"))

    def _load(self, config):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(json.dumps(config), encoding="utf-8")
            return load_config(path)

    def test_rejects_password_in_config(self):
        config = valid_config()
        config["connection"]["password"] = "secret"
        with self.assertRaisesRegex(ValueError, "PGPASSWORD"):
            self._load(config)

    def test_rejects_unsafe_group_identifier(self):
        config = valid_config()
        config["cluster"]["node_group"] = "group; DROP SCHEMA public"
        with self.assertRaisesRegex(ValueError, "unquoted SQL identifier"):
            self._load(config)

    def test_builds_pgbench_command_with_bounded_jobs(self):
        command = pgbench_command(
            "/usr/bin/pgbench", valid_config(), "point_read", 4, 60
        )
        self.assertIn("prepared", command)
        self.assertEqual(command[command.index("-j") + 1], "4")
        self.assertIn("account_count=2000", command)
        self.assertTrue(command[command.index("-f") + 1].endswith("point_read.sql"))
        self.assertNotIn("-d", command)
        self.assertEqual(command[-1], "benchmark")

    def test_reads_sampled_latency_percentiles(self):
        with tempfile.TemporaryDirectory() as directory:
            prefix = Path(directory) / "txlog"
            (Path(directory) / "txlog.1").write_text(
                "0 1 1000 0 0\n"
                "0 2 2000 0 0\n"
                "0 3 3000 0 0\n"
                "0 4 4000 0 0\n",
                encoding="utf-8",
            )
            result = latency_percentiles(prefix)
            self.assertEqual(result["latency_sample_count"], 4)
            self.assertEqual(result["latency_p50_ms"], 2)
            self.assertEqual(result["latency_p99_ms"], 4)


if __name__ == "__main__":
    unittest.main()
