"""Tests for the OpenTenBase distributed observer.

Copyright (c) 2026 OpenTenBase Contributors

This file is licensed under the same terms as OpenTenBase. See LICENSE.txt
in the repository root for details.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import importlib.util
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple


MODULE_PATH = Path(__file__).resolve().parents[1] / "opentenbase_observer.py"
SPEC = importlib.util.spec_from_file_location("opentenbase_observer", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
observer = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = observer
SPEC.loader.exec_module(observer)


def metric(name: str, value: float, labels: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    return {"name": name, "value": value, "labels": labels or {}}


def node_result(
    name: str = "cn1",
    role: str = "coordinator",
    ok: bool = True,
    metrics: Optional[List[Dict[str, Any]]] = None,
    events: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "name": name,
        "role": role,
        "endpoint": "127.0.0.1:15432",
        "ok": ok,
        "duration_seconds": 0.25,
        "metrics": metrics or [],
        "events": events or [],
    }
    if not ok:
        result["error"] = "connection refused"
    return result


def snapshot(
    nodes: List[Dict[str, Any]],
    collected_at: str = "2026-07-18T03:00:00Z",
) -> Dict[str, Any]:
    return {
        "format": observer.SNAPSHOT_FORMAT,
        "version": observer.SNAPSHOT_VERSION,
        "collected_at": collected_at,
        "nodes": nodes,
        "summary": {
            "nodes_total": len(nodes),
            "nodes_up": sum(1 for node in nodes if node["ok"]),
            "nodes_down": sum(1 for node in nodes if not node["ok"]),
            "metrics": sum(len(node["metrics"]) for node in nodes),
            "events": sum(len(node["events"]) for node in nodes),
        },
    }


class FakeRunner(observer.CommandRunner):
    """Return configured subprocess results and record invocation details."""

    def __init__(self, results: Sequence[Any]) -> None:
        self.results = list(results)
        self.calls: List[Tuple[List[str], Dict[str, str], float]] = []

    def run(
        self,
        command: Sequence[str],
        environment: Mapping[str, str],
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append((list(command), dict(environment), timeout))
        result = self.results.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result


class WorkspaceMixin:
    temporary: tempfile.TemporaryDirectory[str]
    workspace: Path

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.workspace = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def write_json(self, name: str, value: Any) -> Path:
        path = self.workspace / name
        path.write_text(json.dumps(value), encoding="utf-8")
        return path

    @contextlib.contextmanager
    def captured_streams(self) -> Iterator[Tuple[io.StringIO, io.StringIO]]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            yield stdout, stderr


class ConfigTests(WorkspaceMixin, unittest.TestCase):
    def valid_config(self) -> Dict[str, Any]:
        return {
            "version": 1,
            "nodes": [
                {
                    "name": "cn1",
                    "host": "10.0.0.1",
                    "port": 15432,
                    "database": "postgres",
                    "user": "observer",
                    "role": "coordinator",
                },
                {
                    "name": "dn1",
                    "host": "10.0.0.2",
                    "user": "observer",
                    "role": "datanode",
                    "sslmode": "require",
                    "connect_timeout": 9,
                },
            ],
        }

    def test_loads_valid_config(self) -> None:
        path = self.write_json("config.json", self.valid_config())
        nodes, thresholds = observer.load_config(path)
        self.assertEqual([node.name for node in nodes], ["cn1", "dn1"])
        self.assertEqual(nodes[1].port, 5432)
        self.assertEqual(nodes[1].sslmode, "require")
        self.assertEqual(thresholds.connection_warning, 0.8)

    def test_rejects_unknown_config_version(self) -> None:
        value = self.valid_config()
        value["version"] = 2
        with self.assertRaises(observer.ConfigError):
            observer.load_config(self.write_json("config.json", value))

    def test_rejects_empty_nodes(self) -> None:
        value = self.valid_config()
        value["nodes"] = []
        with self.assertRaises(observer.ConfigError):
            observer.load_config(self.write_json("config.json", value))

    def test_rejects_duplicate_node_names(self) -> None:
        value = self.valid_config()
        value["nodes"][1]["name"] = "cn1"
        with self.assertRaises(observer.ConfigError):
            observer.load_config(self.write_json("config.json", value))

    def test_rejects_invalid_role(self) -> None:
        value = self.valid_config()
        value["nodes"][0]["role"] = "router"
        with self.assertRaises(observer.ConfigError):
            observer.load_config(self.write_json("config.json", value))

    def test_rejects_invalid_port(self) -> None:
        value = self.valid_config()
        value["nodes"][0]["port"] = 70000
        with self.assertRaises(observer.ConfigError):
            observer.load_config(self.write_json("config.json", value))

    def test_rejects_boolean_port(self) -> None:
        value = self.valid_config()
        value["nodes"][0]["port"] = True
        with self.assertRaises(observer.ConfigError):
            observer.load_config(self.write_json("config.json", value))

    def test_rejects_control_character_in_host(self) -> None:
        value = self.valid_config()
        value["nodes"][0]["host"] = "host\n-H"
        with self.assertRaises(observer.ConfigError):
            observer.load_config(self.write_json("config.json", value))

    def test_rejects_unknown_sslmode(self) -> None:
        value = self.valid_config()
        value["nodes"][0]["sslmode"] = "sometimes"
        with self.assertRaises(observer.ConfigError):
            observer.load_config(self.write_json("config.json", value))

    def test_applies_threshold_overrides(self) -> None:
        value = self.valid_config()
        value["thresholds"] = {
            "connection_warning": 0.7,
            "connection_critical": 0.9,
            "waiting_locks_warning": 2,
        }
        _, thresholds = observer.load_config(self.write_json("config.json", value))
        self.assertEqual(thresholds.connection_warning, 0.7)
        self.assertEqual(thresholds.waiting_locks_warning, 2)

    def test_rejects_unknown_threshold(self) -> None:
        value = self.valid_config()
        value["thresholds"] = {"magic": 1}
        with self.assertRaises(observer.ConfigError):
            observer.load_config(self.write_json("config.json", value))

    def test_rejects_reversed_connection_thresholds(self) -> None:
        value = self.valid_config()
        value["thresholds"] = {"connection_warning": 0.99, "connection_critical": 0.9}
        with self.assertRaises(observer.ConfigError):
            observer.load_config(self.write_json("config.json", value))

    def test_rejects_malformed_json(self) -> None:
        path = self.workspace / "config.json"
        path.write_text("{", encoding="utf-8")
        with self.assertRaises(observer.ConfigError):
            observer.load_config(path)


class ParsingTests(unittest.TestCase):
    def test_parses_metric_and_event_lines(self) -> None:
        output = "\n".join(
            [
                json.dumps({"type": "metric", "name": "opentenbase_locks_total", "value": 4, "labels": {}}),
                json.dumps({"type": "event", "event": "waiting_lock", "pid": 10}),
            ]
        )
        metrics, events = observer.parse_json_lines(output)
        self.assertEqual(metrics[0]["value"], 4.0)
        self.assertEqual(events[0]["event"], "waiting_lock")

    def test_ignores_blank_lines(self) -> None:
        value = json.dumps({"type": "metric", "name": "opentenbase_up", "value": 1, "labels": {}})
        metrics, events = observer.parse_json_lines("\n" + value + "\n\n")
        self.assertEqual(len(metrics), 1)
        self.assertEqual(events, [])

    def test_sorts_metrics_by_name_and_labels(self) -> None:
        lines = [
            json.dumps({"type": "metric", "name": "z_metric", "value": 1, "labels": {}}),
            json.dumps({"type": "metric", "name": "a_metric", "value": 1, "labels": {"db": "b"}}),
            json.dumps({"type": "metric", "name": "a_metric", "value": 1, "labels": {"db": "a"}}),
        ]
        metrics, _ = observer.parse_json_lines("\n".join(lines))
        self.assertEqual([item["name"] for item in metrics], ["a_metric", "a_metric", "z_metric"])
        self.assertEqual(metrics[0]["labels"], {"db": "a"})

    def test_rejects_invalid_json(self) -> None:
        with self.assertRaises(observer.ObserverError):
            observer.parse_json_lines("not-json")

    def test_rejects_unknown_record_type(self) -> None:
        with self.assertRaises(observer.ObserverError):
            observer.parse_json_lines(json.dumps({"type": "unknown"}))

    def test_rejects_invalid_metric_name(self) -> None:
        value = {"type": "metric", "name": "bad-name", "value": 1, "labels": {}}
        with self.assertRaises(observer.ObserverError):
            observer.parse_json_lines(json.dumps(value))

    def test_rejects_boolean_metric_value(self) -> None:
        value = {"type": "metric", "name": "metric_name", "value": True, "labels": {}}
        with self.assertRaises(observer.ObserverError):
            observer.parse_json_lines(json.dumps(value))

    def test_rejects_non_string_label(self) -> None:
        value = {"type": "metric", "name": "metric_name", "value": 1, "labels": {"db": 3}}
        with self.assertRaises(observer.ObserverError):
            observer.parse_json_lines(json.dumps(value))

    def test_rejects_event_without_name(self) -> None:
        with self.assertRaises(observer.ObserverError):
            observer.parse_json_lines(json.dumps({"type": "event", "pid": 1}))


class CollectionTests(unittest.TestCase):
    def node(self, name: str = "cn1") -> Any:
        return observer.NodeConfig(name, "127.0.0.1", 15432, "postgres", "observer", "coordinator")

    def success_output(self) -> str:
        return "\n".join(
            [
                json.dumps(
                    {"type": "metric", "name": "opentenbase_connections_total", "value": 5, "labels": {}}
                ),
                json.dumps(
                    {"type": "event", "event": "long_query", "pid": 44, "duration_seconds": 8}
                ),
            ]
        )

    def test_psql_command_has_safe_connection_arguments(self) -> None:
        command = observer.psql_command(self.node(), "/usr/bin/psql", 9000)
        self.assertEqual(command[0], "/usr/bin/psql")
        self.assertIn("-X", command)
        self.assertIn("ON_ERROR_STOP=1", command)
        self.assertIn(observer.COLLECTION_SQL, command[-1])

    def test_collect_node_success(self) -> None:
        runner = FakeRunner([subprocess.CompletedProcess([], 0, self.success_output(), "")])
        result = observer.collect_node(self.node(), "psql", runner, 10, 9000, {})
        self.assertTrue(result["ok"])
        self.assertEqual(len(result["events"]), 1)
        self.assertIn("opentenbase_up", [item["name"] for item in result["metrics"]])

    def test_collect_node_sets_connection_environment(self) -> None:
        runner = FakeRunner([subprocess.CompletedProcess([], 0, self.success_output(), "")])
        observer.collect_node(self.node(), "psql", runner, 10, 9000, {"PATH": "x"})
        environment = runner.calls[0][1]
        self.assertEqual(environment["PGCONNECT_TIMEOUT"], "5")
        self.assertEqual(environment["PGSSLMODE"], "prefer")
        self.assertEqual(environment["PGAPPNAME"], "opentenbase_observer")
        self.assertEqual(environment["PATH"], "x")

    def test_collect_node_returns_structured_psql_failure(self) -> None:
        runner = FakeRunner([subprocess.CompletedProcess([], 2, "", "authentication failed")])
        result = observer.collect_node(self.node(), "psql", runner, 10, 9000, {})
        self.assertFalse(result["ok"])
        self.assertIn("authentication failed", result["error"])

    def test_collect_node_handles_timeout(self) -> None:
        runner = FakeRunner([subprocess.TimeoutExpired(["psql"], 2)])
        result = observer.collect_node(self.node(), "psql", runner, 2, 9000, {})
        self.assertFalse(result["ok"])
        self.assertIn("exceeded", result["error"])

    def test_collect_node_handles_invalid_output(self) -> None:
        runner = FakeRunner([subprocess.CompletedProcess([], 0, "bad-json", "")])
        result = observer.collect_node(self.node(), "psql", runner, 10, 9000, {})
        self.assertFalse(result["ok"])
        self.assertIn("invalid JSON", result["error"])

    def test_collect_snapshot_sorts_nodes(self) -> None:
        runners = [
            subprocess.CompletedProcess([], 0, self.success_output(), ""),
            subprocess.CompletedProcess([], 0, self.success_output(), ""),
        ]
        result = observer.collect_snapshot(
            [self.node("z-node"), self.node("a-node")],
            jobs=1,
            runner=FakeRunner(runners),
            environment={},
        )
        self.assertEqual([node["name"] for node in result["nodes"]], ["a-node", "z-node"])

    def test_collect_snapshot_summarizes_down_nodes(self) -> None:
        runner = FakeRunner(
            [
                subprocess.CompletedProcess([], 0, self.success_output(), ""),
                subprocess.CompletedProcess([], 1, "", "offline"),
            ]
        )
        result = observer.collect_snapshot(
            [self.node("cn1"), self.node("cn2")], jobs=1, runner=runner, environment={}
        )
        self.assertEqual(result["summary"]["nodes_up"], 1)
        self.assertEqual(result["summary"]["nodes_down"], 1)

    def test_collect_snapshot_rejects_invalid_jobs(self) -> None:
        with self.assertRaises(observer.ObserverError):
            observer.collect_snapshot([self.node()], jobs=0)

    def test_collect_snapshot_rejects_invalid_timeout(self) -> None:
        with self.assertRaises(observer.ObserverError):
            observer.collect_snapshot([self.node()], timeout=0)


class SnapshotValidationTests(WorkspaceMixin, unittest.TestCase):
    def valid(self) -> Dict[str, Any]:
        return snapshot([node_result(metrics=[metric("opentenbase_up", 1)])])

    def test_valid_snapshot_round_trip(self) -> None:
        path = self.write_json("snapshot.json", self.valid())
        loaded = observer.load_snapshot(path)
        self.assertEqual(loaded["nodes"][0]["metrics"][0]["value"], 1.0)

    def test_rejects_wrong_format(self) -> None:
        value = self.valid()
        value["format"] = "other"
        with self.assertRaises(observer.SnapshotError):
            observer.validate_snapshot(value)

    def test_rejects_naive_timestamp(self) -> None:
        value = self.valid()
        value["collected_at"] = "2026-07-18T03:00:00"
        with self.assertRaises(observer.SnapshotError):
            observer.validate_snapshot(value)

    def test_rejects_duplicate_nodes(self) -> None:
        value = snapshot([node_result("cn1"), node_result("cn1")])
        with self.assertRaises(observer.SnapshotError):
            observer.validate_snapshot(value)

    def test_rejects_nonfinite_metric(self) -> None:
        value = snapshot([node_result(metrics=[metric("opentenbase_up", float("nan"))])])
        with self.assertRaises(observer.SnapshotError):
            observer.validate_snapshot(value)

    def test_rejects_invalid_label_name(self) -> None:
        value = snapshot([node_result(metrics=[metric("opentenbase_up", 1, {"bad-label": "x"})])])
        with self.assertRaises(observer.SnapshotError):
            observer.validate_snapshot(value)

    def test_atomic_write_leaves_no_temp_file(self) -> None:
        path = self.workspace / "snapshot.json"
        observer.atomic_write_json(path, self.valid())
        self.assertTrue(path.is_file())
        self.assertEqual(list(self.workspace.glob("snapshot.json.tmp-*")), [])


class AnalysisTests(unittest.TestCase):
    def report_for(self, metrics: List[Dict[str, Any]], ok: bool = True) -> Dict[str, Any]:
        return observer.analyze_snapshot(snapshot([node_result(ok=ok, metrics=metrics)]))

    def finding_codes(self, report: Mapping[str, Any]) -> List[str]:
        return [finding["code"] for finding in report["findings"]]

    def test_unreachable_node_is_critical(self) -> None:
        report = self.report_for([], ok=False)
        self.assertEqual(report["severities"]["critical"], 1)
        self.assertEqual(self.finding_codes(report), ["node_unreachable"])

    def test_connection_warning(self) -> None:
        report = self.report_for(
            [metric("opentenbase_connections_total", 85), metric("opentenbase_max_connections", 100)]
        )
        self.assertIn("connection_pressure", self.finding_codes(report))
        self.assertEqual(report["findings"][0]["severity"], "warning")

    def test_connection_critical(self) -> None:
        report = self.report_for(
            [metric("opentenbase_connections_total", 98), metric("opentenbase_max_connections", 100)]
        )
        self.assertEqual(report["findings"][0]["severity"], "critical")

    def test_connection_below_threshold_has_no_finding(self) -> None:
        report = self.report_for(
            [metric("opentenbase_connections_total", 20), metric("opentenbase_max_connections", 100)]
        )
        self.assertNotIn("connection_pressure", self.finding_codes(report))

    def test_idle_in_transaction_finding(self) -> None:
        report = self.report_for([metric("opentenbase_idle_in_transaction", 2)])
        self.assertIn("idle_in_transaction", self.finding_codes(report))

    def test_waiting_locks_finding(self) -> None:
        report = self.report_for([metric("opentenbase_waiting_locks", 3)])
        self.assertIn("lock_waiters", self.finding_codes(report))

    def test_long_query_warning(self) -> None:
        report = self.report_for([metric("opentenbase_longest_query_seconds", 40)])
        self.assertEqual(report["findings"][0]["code"], "long_running_query")
        self.assertEqual(report["findings"][0]["severity"], "warning")

    def test_long_query_critical(self) -> None:
        report = self.report_for([metric("opentenbase_longest_query_seconds", 400)])
        self.assertEqual(report["findings"][0]["severity"], "critical")

    def test_old_transaction_warning(self) -> None:
        report = self.report_for([metric("opentenbase_oldest_transaction_seconds", 90)])
        self.assertIn("old_transaction", self.finding_codes(report))

    def test_old_transaction_critical(self) -> None:
        report = self.report_for([metric("opentenbase_oldest_transaction_seconds", 700)])
        self.assertEqual(report["findings"][0]["severity"], "critical")

    def test_low_cache_hit_ratio(self) -> None:
        report = self.report_for(
            [metric("opentenbase_blocks_hit_total", 80), metric("opentenbase_blocks_read_total", 20)]
        )
        self.assertIn("low_cache_hit_ratio", self.finding_codes(report))

    def test_good_cache_hit_ratio(self) -> None:
        report = self.report_for(
            [metric("opentenbase_blocks_hit_total", 99), metric("opentenbase_blocks_read_total", 1)]
        )
        self.assertNotIn("low_cache_hit_ratio", self.finding_codes(report))

    def test_high_rollback_ratio(self) -> None:
        report = self.report_for(
            [metric("opentenbase_xact_commit_total", 800), metric("opentenbase_xact_rollback_total", 200)]
        )
        self.assertIn("high_rollback_ratio", self.finding_codes(report))

    def test_small_transaction_sample_suppresses_rollback_finding(self) -> None:
        report = self.report_for(
            [metric("opentenbase_xact_commit_total", 8), metric("opentenbase_xact_rollback_total", 2)]
        )
        self.assertNotIn("high_rollback_ratio", self.finding_codes(report))

    def test_findings_sort_critical_before_warning(self) -> None:
        report = self.report_for(
            [
                metric("opentenbase_connections_total", 99),
                metric("opentenbase_max_connections", 100),
                metric("opentenbase_waiting_locks", 1),
            ]
        )
        self.assertEqual([item["severity"] for item in report["findings"]], ["critical", "warning"])

    def test_custom_thresholds_change_result(self) -> None:
        thresholds = observer.Thresholds(waiting_locks_warning=5)
        report = observer.analyze_snapshot(
            snapshot([node_result(metrics=[metric("opentenbase_waiting_locks", 3)])]),
            thresholds,
        )
        self.assertEqual(report["finding_count"], 0)


class PrometheusTests(unittest.TestCase):
    def test_renders_node_and_role_labels(self) -> None:
        value = snapshot([node_result(metrics=[metric("opentenbase_connections_total", 5)])])
        text = observer.render_prometheus(value, include_findings=False)
        self.assertIn('opentenbase_connections_total{node="cn1",role="coordinator"} 5', text)

    def test_renders_down_node_up_metric(self) -> None:
        value = snapshot([node_result(ok=False)])
        text = observer.render_prometheus(value, include_findings=False)
        self.assertIn('opentenbase_up{node="cn1",role="coordinator"} 0', text)

    def test_renders_additional_metric_labels(self) -> None:
        value = snapshot(
            [node_result(metrics=[metric("custom_metric", 2, {"database": "app"})])]
        )
        text = observer.render_prometheus(value, include_findings=False)
        self.assertIn('custom_metric{database="app",node="cn1",role="coordinator"} 2', text)

    def test_escapes_label_values(self) -> None:
        self.assertEqual(observer.prometheus_escape('a"b\\c\nd'), 'a\\"b\\\\c\\nd')

    def test_formats_integer_without_decimal(self) -> None:
        self.assertEqual(observer.format_number(5.0), "5")

    def test_rejects_nonfinite_number(self) -> None:
        with self.assertRaises(observer.SnapshotError):
            observer.format_number(float("inf"))

    def test_includes_finding_counts(self) -> None:
        value = snapshot([node_result(ok=False)])
        text = observer.render_prometheus(value, include_findings=True)
        self.assertIn('opentenbase_observer_findings{severity="critical"} 1', text)


class DiffTests(unittest.TestCase):
    def test_counter_delta_and_rate(self) -> None:
        before = snapshot(
            [node_result(metrics=[metric("opentenbase_xact_commit_total", 100)])],
            "2026-07-18T03:00:00Z",
        )
        after = snapshot(
            [node_result(metrics=[metric("opentenbase_xact_commit_total", 160)])],
            "2026-07-18T03:01:00Z",
        )
        result = observer.diff_snapshots(before, after)
        self.assertEqual(result["metrics"][0]["delta"], 60)
        self.assertEqual(result["metrics"][0]["per_second"], 1)

    def test_gauge_delta_has_no_rate(self) -> None:
        before = snapshot([node_result(metrics=[metric("opentenbase_connections_total", 10)])])
        after = snapshot(
            [node_result(metrics=[metric("opentenbase_connections_total", 15)])],
            "2026-07-18T03:01:00Z",
        )
        entry = observer.diff_snapshots(before, after)["metrics"][0]
        self.assertEqual(entry["delta"], 5)
        self.assertNotIn("per_second", entry)

    def test_counter_reset_is_marked(self) -> None:
        before = snapshot([node_result(metrics=[metric("opentenbase_deadlocks_total", 8)])])
        after = snapshot(
            [node_result(metrics=[metric("opentenbase_deadlocks_total", 1)])],
            "2026-07-18T03:01:00Z",
        )
        entry = observer.diff_snapshots(before, after)["metrics"][0]
        self.assertTrue(entry["counter_reset"])
        self.assertNotIn("per_second", entry)

    def test_added_and_removed_nodes(self) -> None:
        before = snapshot([node_result("old")])
        after = snapshot([node_result("new")], "2026-07-18T03:01:00Z")
        result = observer.diff_snapshots(before, after)
        self.assertEqual(result["added_nodes"], ["new"])
        self.assertEqual(result["removed_nodes"], ["old"])

    def test_rejects_nonincreasing_time(self) -> None:
        before = snapshot([node_result()])
        after = snapshot([node_result()])
        with self.assertRaises(observer.SnapshotError):
            observer.diff_snapshots(before, after)


class CommandLineTests(WorkspaceMixin, unittest.TestCase):
    def config(self) -> Path:
        return self.write_json(
            "config.json",
            {
                "version": 1,
                "nodes": [
                    {
                        "name": "cn1",
                        "host": "127.0.0.1",
                        "user": "observer",
                        "role": "coordinator",
                    }
                ],
            },
        )

    def test_check_config_outputs_normalized_values(self) -> None:
        with self.captured_streams() as (stdout, stderr):
            code = observer.main(["check-config", str(self.config())])
        self.assertEqual(code, 0, stderr.getvalue())
        value = json.loads(stdout.getvalue())
        self.assertTrue(value["ok"])
        self.assertEqual(value["nodes"][0]["port"], 5432)

    def test_collect_writes_snapshot(self) -> None:
        line = json.dumps(
            {"type": "metric", "name": "opentenbase_connections_total", "value": 3, "labels": {}}
        )
        runner = FakeRunner([subprocess.CompletedProcess([], 0, line, "")])
        output = self.workspace / "snapshot.json"
        parser = observer.build_parser()
        args = parser.parse_args(["collect", str(self.config()), "--output", str(output)])
        with self.captured_streams() as (stdout, stderr):
            code = observer.run(args, runner=runner)
        self.assertEqual(code, 0, stderr.getvalue())
        self.assertTrue(output.is_file())
        self.assertEqual(json.loads(stdout.getvalue())["nodes_up"], 1)

    def test_collect_returns_two_when_node_is_down(self) -> None:
        runner = FakeRunner([subprocess.CompletedProcess([], 1, "", "offline")])
        output = self.workspace / "snapshot.json"
        parser = observer.build_parser()
        args = parser.parse_args(["collect", str(self.config()), "--output", str(output)])
        with self.captured_streams() as (stdout, stderr):
            code = observer.run(args, runner=runner)
        self.assertEqual(code, 2, stderr.getvalue())

    def test_analyze_fail_on_critical(self) -> None:
        path = self.write_json("snapshot.json", snapshot([node_result(ok=False)]))
        with self.captured_streams() as (stdout, stderr):
            code = observer.main(["analyze", str(path), "--fail-on", "critical"])
        self.assertEqual(code, 2, stderr.getvalue())
        self.assertEqual(json.loads(stdout.getvalue())["severities"]["critical"], 1)

    def test_analyze_never_returns_zero(self) -> None:
        path = self.write_json("snapshot.json", snapshot([node_result(ok=False)]))
        with self.captured_streams() as (stdout, stderr):
            code = observer.main(["analyze", str(path), "--fail-on", "never"])
        self.assertEqual(code, 0, stderr.getvalue())

    def test_prometheus_command_outputs_text(self) -> None:
        path = self.write_json(
            "snapshot.json",
            snapshot([node_result(metrics=[metric("opentenbase_up", 1)])]),
        )
        with self.captured_streams() as (stdout, stderr):
            code = observer.main(["prometheus", str(path), "--no-findings"])
        self.assertEqual(code, 0, stderr.getvalue())
        self.assertIn("opentenbase_up", stdout.getvalue())

    def test_diff_command_outputs_delta(self) -> None:
        before = self.write_json(
            "before.json",
            snapshot([node_result(metrics=[metric("opentenbase_xact_commit_total", 1)])]),
        )
        after = self.write_json(
            "after.json",
            snapshot(
                [node_result(metrics=[metric("opentenbase_xact_commit_total", 3)])],
                "2026-07-18T03:00:02Z",
            ),
        )
        with self.captured_streams() as (stdout, stderr):
            code = observer.main(["diff", str(before), str(after)])
        self.assertEqual(code, 0, stderr.getvalue())
        self.assertEqual(json.loads(stdout.getvalue())["metrics"][0]["delta"], 2)

    def test_missing_snapshot_returns_one_without_traceback(self) -> None:
        with self.captured_streams() as (stdout, stderr):
            code = observer.main(["analyze", str(self.workspace / "missing.json")])
        self.assertEqual(code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("opentenbase_observer: error:", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
