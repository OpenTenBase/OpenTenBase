# Copyright (c) 2026 OpenTenBase Authors
# Licensed under the BSD 3-Clause License.

import sys
import unittest
from pathlib import Path


BENCHMARK_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCHMARK_DIR))

from collect_host_metrics import (  # noqa: E402
    calculate_rates,
    parse_cpu,
    parse_diskstats,
    parse_netdev,
)


class ProcParserTests(unittest.TestCase):
    def test_parse_cpu(self):
        self.assertEqual(
            parse_cpu("cpu  10 2 3 80 5 0 0 0\ncpu0 1 1 1 1 1 1 1 1\n"),
            (100, 80, 5),
        )

    def test_parse_selected_disk(self):
        text = (
            "8 0 sda 1 0 10 0 1 0 20 0 0 0 0\n"
            "8 16 sdb 1 0 30 0 1 0 40 0 0 0 0\n"
        )
        self.assertEqual(parse_diskstats(text, ["sdb"]), (30 * 512, 40 * 512))

    def test_parse_network_without_loopback(self):
        text = (
            "Inter-| Receive | Transmit\n"
            " lo: 100 0 0 0 0 0 0 0 200 0 0 0 0 0 0 0\n"
            "eth0: 300 0 0 0 0 0 0 0 400 0 0 0 0 0 0 0\n"
        )
        self.assertEqual(parse_netdev(text), (300, 400))

    def test_calculate_rates(self):
        before = {
            "cpu_total": 100,
            "cpu_idle": 50,
            "cpu_iowait": 5,
            "disk_read": 1000,
            "disk_write": 2000,
            "net_rx": 3000,
            "net_tx": 4000,
        }
        after = {
            "cpu_total": 200,
            "cpu_idle": 70,
            "cpu_iowait": 15,
            "disk_read": 3000,
            "disk_write": 6000,
            "net_rx": 9000,
            "net_tx": 12000,
        }
        result = calculate_rates(before, after, 2)
        self.assertEqual(result["cpu_pct"], 70)
        self.assertEqual(result["iowait_pct"], 10)
        self.assertEqual(result["disk_read_bps"], 1000)
        self.assertEqual(result["net_tx_bps"], 4000)


if __name__ == "__main__":
    unittest.main()
