"""Unit tests for the IVFFlat P0/P1 diagnosis layers."""

from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

BENCHMARKS = Path(__file__).resolve().parents[1]
BUILD_BENCHMARK = BENCHMARKS / "ivfflat_diagnosis"
sys.path.insert(0, str(BENCHMARKS))
sys.path.insert(0, str(BUILD_BENCHMARK))

from aggregate import aggregate_build_samples
import build as build_module
import collectors.runtime as runtime_module
from collectors.runtime import _sample_proc, collect_runtime_snapshot
from common.metrics import summarize
from common.query_quality import _settings
from common.run_store import RunStore
from common.schema import BuildContext, base_record, new_run_id, validate_identity
from memory_estimator import estimate_pgvector_checked_memory
import monitor as monitor_module
from preflight import excludes_zero_norm, validate_row_count
from report import write_build_report, write_quality_report, write_root_reports
from diagnosis.model import build_candidates
from diagnosis.rules import evaluate_rules
from recommendation import build_pareto, build_recommendations, evaluate_feasibility
from run_matrix import _deduplicate, diagnosis_status
from structure import collect_structure_stats, summarize_structure
from validation import compare_validation


class SchemaAndStoreTests(unittest.TestCase):
    def test_three_level_identity_validation(self):
        record = base_record(
            "diag-1", build_id="build-1", quality_run_id="quality-1",
            source="test",
        )
        validate_identity(record, require_quality=True)
        self.assertEqual(record["diagnosis_id"], "diag-1")
        self.assertTrue(new_run_id("build").startswith("build-"))

    def test_store_writes_index_and_rejects_escape(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = RunStore(Path(temporary))
            store.write_json("builds/build-1/build.json", {"value": 1})
            store.append_jsonl("builds/build-1/resources.jsonl", {"sample": 1})
            store.mark_complete("builds/build-1/resources.jsonl")
            index = json.loads(
                (Path(temporary) / "raw/artifact-index.json").read_text()
            )
            self.assertEqual(
                index["artifacts"]["builds/build-1/resources.jsonl"]["status"],
                "complete",
            )
            with self.assertRaises(ValueError):
                store.write_json("../outside.json", {"value": 1})
            with self.assertRaises(ValueError):
                store.write_json("bad.json", {"value": float("nan")})

    def test_build_context_rows_are_observed_not_requested(self):
        context = BuildContext(
            "diag", "build", Path("/tmp/out"), "sift1m", "l2",
            1_000_000, 999_999, 999_998, 128, 100,
        )
        self.assertEqual(context.rows, 999_999)
        self.assertEqual(context.record()["rows"], 999_999)

    def test_row_validation_is_strict_unless_explicitly_disabled(self):
        strict = validate_row_count(10, 9)
        anomaly = validate_row_count(10, 9, enforce=False)
        self.assertEqual(strict["status"], "failed")
        self.assertEqual(anomaly["status"], "passed")
        self.assertFalse(anomaly["matches"])

    def test_only_cosine_excludes_zero_norm_vectors(self):
        self.assertFalse(excludes_zero_norm("l2"))
        self.assertTrue(excludes_zero_norm("cosine"))
        self.assertFalse(excludes_zero_norm("ip"))

    def test_resolved_matrix_parameters_are_deduplicated(self):
        self.assertEqual(_deduplicate([1_000_000, 1_000_000], "rows"), [1_000_000])
        self.assertEqual(_deduplicate([100, 1000, 100], "lists"), [100, 1000])


class MetricsAndMemoryTests(unittest.TestCase):
    def test_quality_settings_load_vector_before_ivfflat_guc(self):
        class Cursor:
            def __init__(self):
                self.statements = []

            def execute(self, statement):
                self.statements.append(statement)

            def fetchone(self):
                statement = self.statements[-1]
                if statement == "SHOW enable_seqscan":
                    return ("on",)
                if statement == "SHOW ivfflat.probes":
                    return ("1",)
                return ("[0]",)

        cursor = Cursor()
        self.assertEqual(_settings(cursor), ("on", "1"))
        self.assertEqual(cursor.statements[0], "SELECT '[0]'::vector")
        self.assertEqual(cursor.statements[-1], "SHOW ivfflat.probes")

    def test_latency_summary(self):
        result = summarize([1.0, 2.0], [0.5, 1.0], 10, [10, 9])
        self.assertEqual(result["mean_ms"], 1.5)
        self.assertEqual(result["p99_ms"], 1.99)
        self.assertEqual(result["stddev_ms"], 0.5)
        self.assertEqual(result["returned_rows_min"], 9)
        self.assertAlmostEqual(result["single_client_qps"], 2 / 0.003)

    def test_checked_memory_matches_source_layout(self):
        result = estimate_pgvector_checked_memory(
            pgvector_version="0.8.6",
            lists=2,
            dimension=3,
            item_size=20,
            heap_blocks=1,
            eligible_vectors=5,
            maintenance_work_mem_bytes=8 * 1024,
        )
        self.assertEqual(result["target_sample_count"], 291)
        self.assertEqual(result["actual_sample_count_estimate"], 5)
        self.assertEqual(result["estimated_pgvector_checked_memory_bytes"], 7296)
        self.assertFalse(result["predicted_memory_failure"])

    def test_empty_index_skips_elkan_allocations(self):
        result = estimate_pgvector_checked_memory(
            pgvector_version="0.8.6",
            lists=2,
            dimension=3,
            item_size=20,
            heap_blocks=1,
            eligible_vectors=0,
            maintenance_work_mem_bytes=1024,
        )
        self.assertEqual(result["estimated_pgvector_checked_memory_bytes"], 7080)
        self.assertTrue(result["predicted_memory_failure"])

    def test_unknown_pgvector_version_is_unsupported(self):
        result = estimate_pgvector_checked_memory(
            pgvector_version="0.9.0",
            lists=2,
            dimension=3,
            item_size=20,
            heap_blocks=1,
            eligible_vectors=5,
            maintenance_work_mem_bytes=1024,
        )
        self.assertEqual(result["availability"], "unsupported")
        self.assertIsNone(result["predicted_memory_failure"])

    def test_pgvector_087_keeps_086_build_memory_semantics(self):
        result = estimate_pgvector_checked_memory(
            pgvector_version="0.8.7",
            lists=2,
            dimension=3,
            item_size=20,
            heap_blocks=1,
            eligible_vectors=5,
            maintenance_work_mem_bytes=8 * 1024,
        )
        self.assertEqual(result["availability"], "available")
        self.assertEqual(result["estimated_pgvector_checked_memory_bytes"], 7296)


class AggregationTests(unittest.TestCase):
    def test_pg_counters_use_pre_build_baseline_and_post_build_final(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            progress = root / "progress.jsonl"
            resources = root / "resources.jsonl"
            progress.write_text(
                json.dumps({
                    "timestamp_utc": "2026-01-01T00:00:01+00:00",
                    "native_phase": "loading tuples",
                }) + "\n"
            )
            rows = [
                {
                    "timestamp_utc": "2026-01-01T00:00:00+00:00",
                    "counter_sample_role": "baseline", "pid": 10,
                    "process_start_ticks": 1, "role": "leader",
                    "native_phase": None, "cpu_user_seconds": 0,
                    "cpu_system_seconds": 0, "rss_bytes": 100, "pss_bytes": 80,
                    "proc_read_bytes": 0, "proc_write_bytes": 0,
                    "pg_io": {"totals": {"read_bytes": 0, "write_bytes": 0}},
                    "pg_wal": {"wal_bytes": 0},
                },
                {
                    "timestamp_utc": "2026-01-01T00:00:01+00:00",
                    "counter_sample_role": "periodic", "pid": 10,
                    "process_start_ticks": 1, "role": "leader",
                    "native_phase": "loading tuples", "cpu_user_seconds": 1,
                    "cpu_system_seconds": 0, "rss_bytes": 120, "pss_bytes": 90,
                    "proc_read_bytes": 0, "proc_write_bytes": 0,
                    "pg_io": {"totals": {"read_bytes": 0, "write_bytes": 0}},
                    "pg_wal": {"wal_bytes": 0},
                },
                {
                    "timestamp_utc": "2026-01-01T00:00:02.010000+00:00",
                    "counter_sample_role": "final", "pid": 10,
                    "process_start_ticks": 1, "role": "leader",
                    "native_phase": None, "cpu_user_seconds": 2,
                    "cpu_system_seconds": 0, "rss_bytes": 130, "pss_bytes": 100,
                    "proc_read_bytes": 20, "proc_write_bytes": 30,
                    "pg_io": {"totals": {"read_bytes": 40, "write_bytes": 50}},
                    "pg_wal": {"wal_bytes": 60},
                },
            ]
            resources.write_text("".join(json.dumps(row) + "\n" for row in rows))
            result = aggregate_build_samples(
                progress, resources, diagnosis_id="diag", build_id="build",
                started_at="2026-01-01T00:00:00.500000+00:00",
                finished_at="2026-01-01T00:00:02+00:00",
                sample_interval_seconds=.5,
            )
            self.assertEqual(result["resource_samples"], 1)
            self.assertEqual(
                result["build_run_pg_delta"]["pg_read_bytes"]["value"], 40
            )
            self.assertEqual(
                result["build_run_pg_delta"]["pg_write_bytes"]["value"], 50
            )
            self.assertEqual(result["build_run_pg_delta"]["wal_bytes"]["value"], 60)
            window = result["pg_backend_deltas"][0]["counter_window"]
            self.assertEqual(window["baseline_source"], "explicit_pre_build_sample")
            self.assertEqual(window["final_source"], "explicit_post_build_sample")

            unavailable = aggregate_build_samples(
                progress, resources, diagnosis_id="diag", build_id="build",
                started_at="2026-01-01T00:00:00.500000+00:00",
                finished_at="2026-01-01T00:00:02+00:00",
                sample_interval_seconds=.5,
                pg_counter_finalization={
                    "availability": "unavailable", "reason": "flush unavailable"
                },
            )
            self.assertEqual(
                unavailable["build_run_pg_delta"]["wal_bytes"]["availability"],
                "unavailable",
            )
            self.assertIsNone(
                unavailable["build_run_pg_delta"]["wal_bytes"]["value"]
            )

    def test_phase_and_process_deltas(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            progress = root / "progress.jsonl"
            resources = root / "resources.jsonl"
            progress_rows = [
                {"timestamp_utc": "2026-01-01T00:00:00+00:00", "native_phase": "assigning tuples"},
                {"timestamp_utc": "2026-01-01T00:00:02+00:00", "native_phase": "loading tuples"},
            ]
            resource_rows = [
                {
                    "timestamp_utc": "2026-01-01T00:00:00+00:00", "pid": 10,
                    "process_start_ticks": 1, "role": "leader",
                    "native_phase": "assigning tuples", "cpu_user_seconds": 1.0,
                    "cpu_system_seconds": 0.0, "rss_bytes": 100, "pss_bytes": 80,
                    "proc_read_bytes": 0, "proc_write_bytes": 0,
                    "pg_io": {"totals": {"read_bytes": 0, "write_bytes": 0}},
                    "pg_wal": {"wal_bytes": 0},
                },
                {
                    "timestamp_utc": "2026-01-01T00:00:01+00:00", "pid": 10,
                    "process_start_ticks": 1, "role": "leader",
                    "native_phase": "assigning tuples", "cpu_user_seconds": 2.0,
                    "cpu_system_seconds": 0.0, "rss_bytes": 120, "pss_bytes": 90,
                    "proc_read_bytes": 10, "proc_write_bytes": 20,
                    "pg_io": {"totals": {"read_bytes": 30, "write_bytes": 40}},
                    "pg_wal": {"wal_bytes": 50},
                },
                {
                    "timestamp_utc": "2026-01-01T00:00:02+00:00", "pid": 10,
                    "process_start_ticks": 1, "role": "leader",
                    "native_phase": "loading tuples", "cpu_user_seconds": 3.0,
                    "cpu_system_seconds": 0.0, "rss_bytes": 130, "pss_bytes": 100,
                    "proc_read_bytes": 20, "proc_write_bytes": 30,
                    "pg_io": {"totals": {"read_bytes": 50, "write_bytes": 60}},
                    "pg_wal": {"wal_bytes": 70},
                },
                {
                    "timestamp_utc": "2026-01-01T00:00:03+00:00", "pid": 10,
                    "process_start_ticks": 1, "role": "leader",
                    "native_phase": "loading tuples", "cpu_user_seconds": 4.0,
                    "cpu_system_seconds": 0.0, "rss_bytes": 140, "pss_bytes": 110,
                    "proc_read_bytes": 30, "proc_write_bytes": 40,
                    "pg_io": {"totals": {"read_bytes": 70, "write_bytes": 80}},
                    "pg_wal": {"wal_bytes": 90},
                },
            ]
            progress.write_text("".join(json.dumps(row) + "\n" for row in progress_rows))
            resources.write_text("".join(json.dumps(row) + "\n" for row in resource_rows))
            result = aggregate_build_samples(
                progress,
                resources,
                diagnosis_id="diag-1",
                build_id="build-1",
                started_at="2026-01-01T00:00:00+00:00",
                finished_at="2026-01-01T00:00:04+00:00",
                sample_interval_seconds=1.0,
            )
            assigning = result["phase_summaries"]["assigning tuples"]
            loading = result["phase_summaries"]["loading tuples"]
            self.assertEqual(assigning["wall_time_ms"], 2000)
            self.assertEqual(loading["wall_time_ms"], 2000)
            self.assertEqual(assigning["cpu_seconds"], 1.0)
            self.assertEqual(loading["cpu_seconds"], 1.0)
            self.assertEqual(result["process_group_peak_pss_bytes"], 110)
            self.assertLessEqual(
                result["phase_duration_ms"], result["observed_duration_ms"]
            )
            self.assertNotIn("pg_read_bytes", assigning)
            self.assertEqual(
                result["pg_backend_deltas"][0]["pg_read_bytes"]["attribution_scope"],
                "backend",
            )
            self.assertEqual(
                result["build_run_pg_delta"]["wal_bytes"]["attribution_scope"],
                "build_run",
            )

    def test_samples_and_phases_are_clipped_to_create_index_window(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            progress = root / "progress.jsonl"
            resources = root / "resources.jsonl"
            progress.write_text("".join(json.dumps(row) + "\n" for row in [
                {"timestamp_utc": "2026-01-01T00:00:00+00:00", "native_phase": "before"},
                {"timestamp_utc": "2026-01-01T00:00:02+00:00", "native_phase": "loading tuples"},
                {"timestamp_utc": "2026-01-01T00:00:05+00:00", "native_phase": "after"},
            ]))
            resources.write_text("")
            result = aggregate_build_samples(
                progress, resources, diagnosis_id="diag", build_id="build",
                started_at="2026-01-01T00:00:01+00:00",
                finished_at="2026-01-01T00:00:04+00:00",
                sample_interval_seconds=1,
            )
            self.assertEqual(set(result["phase_summaries"]), {"loading tuples"})
            self.assertEqual(result["phase_duration_ms"], 2000)
            self.assertEqual(result["unassigned_duration_ms"], 1000)
            self.assertTrue(
                result["timing_accuracy"]["timeline_clipped_to_create_index"]
            )


class FailurePathTests(unittest.TestCase):
    def test_diagnosis_partial_and_cancel_are_persisted(self):
        for status, collection, expected in (
            ('success', 'success', 'complete'),
            ('success', 'collector_interrupted', 'complete_with_partial_data'),
            ('success', 'partial', 'complete_with_partial_data'),
            ('cancelled', 'success', 'complete_with_failures'),
        ):
            with self.subTest(status=status, collection=collection), tempfile.TemporaryDirectory() as tmp:
                build = {'status': status, 'collection_status': collection}
                store = RunStore(Path(tmp))
                store.write_json('raw/build.json', build)
                store.write_json('raw/p0-summary.json', {'status': diagnosis_status([build], [])})
                self.assertEqual(json.loads((Path(tmp)/'raw/p0-summary.json').read_text())['status'], expected)
        self.assertEqual(
            diagnosis_status(
                [{"status": "success", "collection_status": "success"}],
                [{"status": "success"}],
                [{"status": "failed"}],
            ),
            "complete_with_partial_data",
        )

    def test_proc_io_permission_failure_keeps_cpu_and_rss(self):
        def fake_read_text(path, *args, **kwargs):
            if path.name == "stat":
                return "10 (postgres) S " + " ".join(["0"] * 30)
            if path.name == "status":
                return "Name:\tpostgres\nVmRSS:\t1234 kB\n"
            if path.name == "io":
                raise PermissionError("denied")
            raise AssertionError(path)

        with patch.object(Path, "read_text", fake_read_text):
            values, availability, reasons = _sample_proc(10, include_pss=False)
        self.assertEqual(values["rss_bytes"], 1234 * 1024)
        self.assertIsNotNone(values["cpu_user_seconds"])
        self.assertEqual(availability["cpu"]["availability"], "available")
        self.assertEqual(availability["rss"]["availability"], "available")
        self.assertEqual(availability["proc_io"]["availability"], "unavailable")
        self.assertTrue(any("denied" in reason for reason in reasons))

    def test_backend_io_wal_unavailable_does_not_abort_snapshot(self):
        capabilities = {
            "capabilities": {
                name: {"available": False}
                for name in (
                    "activity", "progress", "native_progress_phases",
                    "load_progress", "proc", "pss", "backend_io", "backend_wal",
                )
            }
        }
        capabilities["capabilities"]["proc"] = {"available": True}
        snapshot = collect_runtime_snapshot(
            object(), 42, {}, capabilities,
            diagnosis_id="diag", build_id="build",
        )
        resource = snapshot["resources"][0]
        self.assertIsNone(resource["pg_io"])
        self.assertIsNone(resource["pg_wal"])
        self.assertEqual(
            resource["field_availability"]["pg_io"]["availability"],
            "unavailable",
        )
        self.assertEqual(
            resource["field_availability"]["pg_wal"]["availability"],
            "unavailable",
        )

    def test_proc_unavailable_is_persisted_as_partial_collection_not_db_failure(self):
        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def execute(self, statement):
                return None

        class Connection:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def cursor(self):
                return Cursor()

        capabilities = {
            "capabilities": {
                name: {"available": False}
                for name in (
                    "activity", "progress", "native_progress_phases",
                    "load_progress", "proc", "pss", "backend_io", "backend_wal",
                )
            }
        }
        stop = threading.Event()
        stop.set()
        with tempfile.TemporaryDirectory() as temporary:
            state = {}
            with (
                patch.object(monitor_module, "connect_dsn", return_value=Connection()),
                patch.object(Path, "read_text", side_effect=FileNotFoundError("/proc gone")),
            ):
                monitor_module.monitor_build(
                    "dsn", 42, stop,
                    diagnosis_id="diag", build_id="build",
                    store=RunStore(Path(temporary)),
                    progress_path="raw/progress.jsonl",
                    resources_path="raw/resources.jsonl",
                    capability_map=capabilities, interval=.001, state=state,
                )
            resources = [
                json.loads(line) for line in
                (Path(temporary) / "raw/resources.jsonl").read_text().splitlines()
            ]
            self.assertTrue(state["partial_resources"])
            self.assertTrue(resources)
            self.assertTrue(all(row["availability"] == "unavailable" for row in resources))
            self.assertTrue(all(row["collection_impact"] == "partial" for row in resources))
            build = {"status": "success", "database_status": "success",
                     "collection_status": "partial"}
            RunStore(Path(temporary)).write_json("raw/build.json", build)
            persisted = json.loads((Path(temporary) / "raw/build.json").read_text())
            self.assertEqual(persisted["database_status"], "success")
            self.assertEqual(diagnosis_status([persisted], []), "complete_with_partial_data")

    def test_worker_exit_during_proc_sample_is_lifecycle_event_not_partial(self):
        activity = [{
            "pid": 43, "leader_pid": 42, "backend_start": None,
            "state": "active", "wait_event_type": None,
            "wait_event": None, "query_start": None,
        }]
        proc_values = {
            "process_start_ticks": None, "cpu_user_seconds": None,
            "cpu_system_seconds": None, "rss_bytes": None, "pss_bytes": None,
            "proc_read_bytes": None, "proc_write_bytes": None,
            "process_lifecycle_event": "process_exited_during_sample",
        }
        unavailable = {
            name: {"availability": "unavailable", "reason": "process gone",
                   "attribution_scope": "backend"}
            for name in ("cpu", "rss", "proc_io", "pss")
        }
        capabilities = {"capabilities": {
            "activity": {"available": True}, "proc": {"available": True},
            **{name: {"available": False} for name in (
                "progress", "native_progress_phases", "load_progress",
                "pss", "backend_io", "backend_wal",
            )},
        }}
        with (
            patch.object(runtime_module, "_activity", return_value=activity),
            patch.object(
                runtime_module, "_sample_proc",
                return_value=(proc_values, unavailable, ["process gone"]),
            ),
        ):
            snapshot = collect_runtime_snapshot(
                object(), 42, {}, capabilities,
                diagnosis_id="diag", build_id="build",
            )
        resource = snapshot["resources"][0]
        self.assertEqual(resource["role"], "worker")
        self.assertEqual(resource["availability"], "not_sampled")
        self.assertEqual(resource["collection_impact"], "none")
        self.assertEqual(resource["process_lifecycle"]["event"], "worker_exited")
        self.assertFalse(monitor_module.resource_degrades_collection(resource))

    def test_collector_interrupted_is_persisted_without_db_failure(self):
        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def execute(self, statement):
                return None

        class Connection:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def cursor(self):
                return Cursor()

        with tempfile.TemporaryDirectory() as temporary:
            state = {}
            ready = threading.Event()
            with (
                patch.object(monitor_module, "connect_dsn", return_value=Connection()),
                patch.object(
                    monitor_module, "collect_runtime_snapshot",
                    side_effect=RuntimeError("collector failed"),
                ),
            ):
                monitor_module.monitor_build(
                    "dsn", 42, threading.Event(),
                    diagnosis_id="diag", build_id="build",
                    store=RunStore(Path(temporary)),
                    progress_path="raw/progress.jsonl",
                    resources_path="raw/resources.jsonl",
                    capability_map={}, interval=.001, state=state,
                    baseline_ready=ready,
                )
            self.assertTrue(ready.is_set())
            self.assertTrue(state["collector_interrupted"])
            self.assertEqual(len(state["collector_errors"]), 3)
            index = json.loads(
                (Path(temporary) / "raw/artifact-index.json").read_text()
            )
            self.assertEqual(index["artifacts"]["raw/progress.jsonl"]["status"], "complete")
            self.assertEqual(index["artifacts"]["raw/resources.jsonl"]["status"], "complete")

        class BuildCursor(Cursor):
            def execute(self, statement, params=None):
                return None

            def fetchone(self):
                return (321,)

        class BuildConnection(Connection):
            def add_notice_handler(self, handler):
                return None

            def cursor(self):
                return BuildCursor()

        def interrupted_monitor(*args, stop, state, baseline_ready, **kwargs):
            state.update({
                "collector_interrupted": True,
                "collector_errors": [{"source": "collector_loop", "error": "failed"}],
                "discovered_pids": [321], "last_phase": None,
                "partial_resources": False,
            })
            baseline_ready.set()
            stop.wait(1)

        aggregate = {
            "phase_summaries": {}, "progress_observation": {},
            "process_group_peak_summed_rss_bytes": None,
            "process_group_peak_pss_bytes": None,
            "process_group_peak_availability": {},
            "rss_aggregation_semantics": "summed RSS",
            "build_run_pg_delta": {}, "timing_accuracy": {},
        }
        metadata = {
            "availability": "available", "reason": None,
            "index_oid": 99, "relfilenode": 100,
            "index_bytes": 8192, "index_pages": 1,
            "catalog_relpages": 1,
        }
        with tempfile.TemporaryDirectory() as temporary:
            with (
                patch.object(build_module, "connect_dsn", return_value=BuildConnection()),
                patch.object(build_module, "monitor_build", side_effect=interrupted_monitor),
                patch.object(build_module, "aggregate_build_samples", return_value=aggregate),
                patch.object(build_module, "_collect_index_metadata", return_value=metadata),
            ):
                build = build_module.build_index(
                    "dsn", "dataset", "l2", 3, 10, 2, Path(temporary),
                )
            persisted = json.loads((Path(temporary) / "raw/build.json").read_text())
            self.assertEqual(build["database_status"], "success")
            self.assertEqual(persisted["database_status"], "success")
            self.assertEqual(persisted["collection_status"], "collector_interrupted")
            self.assertEqual(diagnosis_status([persisted], []), "complete_with_partial_data")

    def test_cancel_build_is_persisted_as_database_cancelled(self):
        class Cursor:
            def __init__(self):
                self.statement = None

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def execute(self, statement, params=None):
                self.statement = str(statement)
                if "CREATE INDEX" in self.statement:
                    raise KeyboardInterrupt()

            def fetchone(self):
                return (321,)

        class Connection:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def add_notice_handler(self, handler):
                return None

            def cursor(self):
                return Cursor()

        def fake_monitor(*args, stop, state, baseline_ready, **kwargs):
            state.update({
                "collector_interrupted": False, "collector_errors": [],
                "discovered_pids": [321], "last_phase": None,
            })
            baseline_ready.set()
            stop.wait(1)

        aggregate = {
            "phase_summaries": {},
            "process_group_peak_summed_rss_bytes": None,
            "process_group_peak_pss_bytes": None,
            "process_group_peak_availability": {},
            "rss_aggregation_semantics": "summed RSS",
            "build_run_pg_delta": {},
            "timing_accuracy": {},
        }
        with tempfile.TemporaryDirectory() as temporary:
            with (
                patch.object(build_module, "connect_dsn", return_value=Connection()),
                patch.object(build_module, "monitor_build", side_effect=fake_monitor),
                patch.object(
                    build_module, "aggregate_build_samples",
                    return_value=aggregate,
                ),
            ):
                result = build_module.build_index(
                    "dsn", "dataset", "l2", 3, 10, 2, Path(temporary),
                )
            self.assertEqual(result["database_status"], "cancelled")
            persisted = json.loads(
                (Path(temporary) / "raw/build.json").read_text()
            )
            self.assertEqual(persisted["status"], "cancelled")
            self.assertEqual(persisted["database_status"], "cancelled")
            self.assertEqual(diagnosis_status([persisted], []), "complete_with_failures")


class StructureP1Tests(unittest.TestCase):
    def _rows(self):
        return [
            {
                "format_version": 1, "dimensions": 3, "declared_lists": 2,
                "list_id": 0, "list_page": 1, "list_offset": 1,
                "start_page": 2, "insert_page": 2, "entry_pages": 1,
                "physical_tuple_count": 10, "usable_bytes": 1000,
                "used_bytes": 600, "free_bytes": 400,
            },
            {
                "format_version": 1, "dimensions": 3, "declared_lists": 2,
                "list_id": 1, "list_page": 1, "list_offset": 2,
                "start_page": 3, "insert_page": 3, "entry_pages": 1,
                "physical_tuple_count": 30, "usable_bytes": 1000,
                "used_bytes": 800, "free_bytes": 200,
            },
        ]

    def test_full_structure_summary_exact_metrics(self):
        summary = summarize_structure(
            self._rows(), diagnosis_id="diag", build_id="build",
            index_oid=10, relfilenode=20, sample_rate=1.0,
            eligible_vectors=40, index_bytes=600, raw_vector_bytes=400,
        )
        self.assertEqual(summary["scan_scope"], "full")
        self.assertEqual(summary["metrics"]["physical_indexed_tuples"]["value"], 40)
        self.assertEqual(summary["metrics"]["coverage_ratio"]["value"], 1.0)
        self.assertEqual(summary["metrics"]["cv"]["value"], 0.5)
        self.assertEqual(summary["metrics"]["gini"]["value"], 0.25)
        self.assertEqual(summary["metrics"]["fill_ratio"]["value"], 0.7)

    def test_sample_structure_summary_disables_exact_metrics(self):
        summary = summarize_structure(
            self._rows()[:1], diagnosis_id="diag", build_id="build",
            index_oid=10, relfilenode=20, sample_rate=0.5,
            eligible_vectors=40, index_bytes=600, raw_vector_bytes=400,
        )
        self.assertEqual(summary["scan_scope"], "sample")
        self.assertEqual(
            summary["metrics"]["physical_indexed_tuples"]["availability"],
            "unavailable",
        )
        self.assertEqual(
            summary["metrics"]["coverage_ratio"]["availability"],
            "unavailable",
        )

    def test_structure_collector_validates_identity_and_persists_artifacts(self):
        rows = self._rows()

        class Cursor:
            def __init__(self, connection):
                self.connection = connection

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def execute(self, statement, params=None):
                self.connection.statement_count += 1

            def fetchone(self):
                return (10, 20, "ivfflat")

            def fetchall(self):
                return [tuple(row[column] for column in (
                    "format_version", "dimensions", "declared_lists", "list_id",
                    "list_page", "list_offset", "start_page", "insert_page",
                    "entry_pages", "physical_tuple_count", "usable_bytes",
                    "used_bytes", "free_bytes",
                )) for row in rows]

        class Connection:
            statement_count = 0

            def cursor(self):
                return Cursor(self)

        with tempfile.TemporaryDirectory() as temporary:
            result = collect_structure_stats(
                Connection(), diagnosis_id="diag", build_id="build",
                index_oid=10, relfilenode=20, sample_rate=1.0,
                eligible_vectors=40, index_bytes=600, raw_vector_bytes=400,
                store=RunStore(Path(temporary)), output_prefix="builds/build",
            )
            root = Path(temporary) / "builds/build/structure"
            self.assertEqual(result["scan"]["status"], "success")
            self.assertEqual(len((root / "lists.jsonl").read_text().splitlines()), 2)
            self.assertTrue((root / "scan.json").is_file())
            self.assertTrue((root / "summary.json").is_file())

    def test_structure_identity_mismatch_is_persisted_as_failed(self):
        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def execute(self, statement, params=None):
                return None

            def fetchone(self):
                return (10, 999, "ivfflat")

        class Connection:
            def cursor(self):
                return Cursor()

        with tempfile.TemporaryDirectory() as temporary:
            result = collect_structure_stats(
                Connection(), diagnosis_id="diag", build_id="build",
                index_oid=10, relfilenode=20, sample_rate=1.0,
                eligible_vectors=40, index_bytes=600, raw_vector_bytes=400,
                store=RunStore(Path(temporary)), output_prefix="builds/build",
            )
            self.assertEqual(result["scan"]["status"], "failed")
            self.assertEqual(result["scan"]["availability"], "unavailable")
            self.assertIn("identity changed", result["error"])
            self.assertTrue(
                (Path(temporary) / "builds/build/structure/scan.json").is_file()
            )

    def test_structure_schemas_are_valid_json_documents(self):
        schema_root = BUILD_BENCHMARK / "schemas"
        for name in (
            "structure-list.schema.json", "structure-scan.schema.json",
            "structure-summary.schema.json",
        ):
            with self.subTest(name=name):
                schema = json.loads((schema_root / name).read_text())
                self.assertEqual(schema["$schema"], "https://json-schema.org/draft/2020-12/schema")
                self.assertEqual(schema["type"], "object")


class JointDiagnosisP1Tests(unittest.TestCase):
    def _candidate(self, candidate_id, *, lists=100, probes=10, recall=.9,
                   p95=10.0, build_seconds=10.0, memory=100, index_bytes=100,
                   memory_source="process_group_pss", cv=.2, gini=.1,
                   p95_p50=1.2, eligible=True):
        metrics = (
            "memory.predicted_hard_failure", "memory.estimated_checked_bytes",
            "memory.limit_bytes", "quality.recall_at_k", "quality.p95_ms",
            "structure.cv", "structure.gini", "structure.p95_over_p50",
            "structure.empty_ratio",
        )
        return {
            "diagnosis_id": "diag", "candidate_id": candidate_id,
            "build_id": f"build-{lists}", "quality_run_id": f"quality-{candidate_id}",
            "lists": lists, "probes": probes, "actual_rows": 1000,
            "build_metrics": {
                "status": "success", "database_status": "success",
                "collection_status": "success", "build_seconds": build_seconds,
                "index_bytes": index_bytes, "predicted_memory_failure": False,
                "estimated_checked_memory_bytes": 50, "memory_limit_bytes": 200,
            },
            "resource_metrics": {
                "peak_memory_bytes": memory, "peak_memory_source": memory_source,
                "worker_pids": [1], "planned_workers": None,
            },
            "structure_metrics": {
                "availability": "available", "declared_lists": lists,
                "cv": cv, "gini": gini, "p95_over_p50": p95_p50,
                "empty_ratio": 0.0, "coverage_ratio": 1.0,
            },
            "quality_metrics": {
                "recall_at_k": recall, "p95_ms": p95, "p99_ms": p95 * 1.2,
                "used_ivfflat": True, "returned_rows_min": 10,
                "returned_rows_max": 10, "k": 10,
            },
            "validity": {
                "status": "valid" if eligible else "invalid",
                "eligible_for_comparison": eligible,
                "reasons": [] if eligible else ["invalid quality"],
            },
            "evidence_ids": {name: f"evidence-{candidate_id}-{position}" for position, name in enumerate(metrics)},
            "artifact_refs": {},
        }

    def test_candidate_model_preserves_skipped_memory_failure(self):
        build = {
            "build_id": "build-skip", "lists": 1000,
            "requested_rows": 1000, "actual_rows": 1000,
            "status": "skipped", "database_status": "not_started",
            "collection_status": "not_started",
            "reason": "predicted pgvector maintenance_work_mem hard failure",
            "memory_estimate": {
                "availability": "available", "predicted_memory_failure": True,
                "estimated_pgvector_checked_memory_bytes": 300,
                "memory_limit_bytes": 100,
            },
            "index_metadata": {"availability": "unavailable"},
        }
        manifest = {"probes": [10], "k": 10}
        candidates, evidence = build_candidates(
            "diag", [(build, Path("builds/skip"))], [], manifest
        )
        self.assertEqual(candidates[0]["validity"]["status"], "infeasible")
        self.assertFalse(candidates[0]["validity"]["eligible_for_comparison"])
        self.assertTrue(candidates[0]["build_metrics"]["predicted_memory_failure"])
        self.assertEqual(len(evidence), 12)

    def test_rules_detect_probe_improvement_and_diminishing_return(self):
        low = self._candidate("low", probes=10, recall=.90, p95=10)
        middle = self._candidate("middle", probes=20, recall=.96, p95=14)
        high = self._candidate("high", probes=40, recall=.961, p95=20)
        findings = evaluate_rules(
            [low, middle, high], {"min_recall": .95},
        )
        probe = [item for item in findings if item["rule_id"] == "probes_improve_recall" and item["status"] == "triggered"]
        diminishing = [item for item in findings if item["rule_id"] == "probes_diminishing_returns" and item["status"] == "triggered"]
        self.assertEqual(probe[0]["details"]["recommended_probes"], 20)
        self.assertEqual(diminishing[0]["details"]["lower_probes"], 20)

    def test_structure_rule_does_not_create_lists_recommendation(self):
        candidate = self._candidate(
            "imbalanced", cv=.8, gini=.4, p95_p50=2.5
        )
        findings = evaluate_rules([candidate], {})
        structure = next(item for item in findings if item["rule_id"] == "structure_imbalance")
        self.assertEqual(structure["status"], "triggered")
        pareto = build_pareto([candidate], {})
        recommendations = build_recommendations(
            [candidate], findings, pareto, {"dataset": "x", "metric": "l2"}
        )
        self.assertEqual(recommendations, [])

    def test_slo_filter_and_pareto_dominance(self):
        strong = self._candidate("strong", recall=.96, p95=10, build_seconds=5,
                                 memory=80, index_bytes=90)
        weak = self._candidate("weak", probes=20, recall=.95, p95=12,
                               build_seconds=6, memory=90, index_bytes=100)
        rejected = self._candidate("rejected", probes=30, recall=.80, p95=8)
        self.assertEqual(
            evaluate_feasibility(rejected, {"min_recall": .9})["status"],
            "slo_rejected",
        )
        pareto = build_pareto([strong, weak, rejected], {"min_recall": .9})
        rows = {item["candidate_id"]: item for item in pareto["candidates"]}
        self.assertEqual(rows["strong"]["pareto_status"], "pareto")
        self.assertEqual(rows["weak"]["pareto_status"], "dominated")
        self.assertEqual(rows["rejected"]["pareto_status"], "not_comparable")
        self.assertIsNone(pareto["best_candidate_id"])

    def test_validation_detects_comparable_success_and_context_mismatch(self):
        before = self._candidate("before", recall=.90, p95=10)
        after = self._candidate("after", probes=20, recall=.96, p95=12)
        recommendation = {
            "diagnosis_id": "diag", "recommendation_id": "rec",
            "action": "set_ivfflat_probes",
        }
        context = {
            key: "same" for key in (
                "dataset", "rows", "metric", "opclass", "ground_truth_id",
                "k", "query_count", "repeat", "cache_mode", "client_mode",
                "planner_mode", "postgresql", "pgvector", "machine",
            )
        }
        result = compare_validation(
            recommendation, before, after, context, dict(context),
            {"min_recall": .95},
        )
        self.assertEqual(result["status"], "validated")
        changed = dict(context, machine="different")
        mismatch = compare_validation(
            recommendation, before, after, context, changed, {"min_recall": .95}
        )
        self.assertEqual(mismatch["status"], "not_comparable")

    def test_all_p1_schemas_are_valid_json_documents(self):
        schema_root = BUILD_BENCHMARK / "schemas"
        for name in (
            "candidate.schema.json", "evidence.schema.json",
            "finding.schema.json", "pareto.schema.json",
            "recommendation.schema.json", "validation.schema.json",
        ):
            with self.subTest(name=name):
                schema = json.loads((schema_root / name).read_text())
                self.assertEqual(schema["$schema"], "https://json-schema.org/draft/2020-12/schema")
                self.assertEqual(schema["type"], "object")


class ReportTests(unittest.TestCase):
    def test_human_reports_and_raw_layout(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            store = RunStore(root)
            build_prefix = Path("builds/lists-100__abcd1234")
            quality_prefix = Path("quality/lists-100/probes-32__efgh5678")
            build = {
                "build_id": "build-full-id", "lists": 100,
                "requested_rows": 1000, "actual_rows": 1000,
                "eligible_vectors": 1000, "status": "success",
                "database_status": "success", "collection_status": "success",
                "build_seconds": 1.2, "total_elapsed_ms": 1200,
                "process_group_peak_rss_bytes": 1024,
                "process_group_peak_pss_bytes": None,
                "leader_pid": 1, "discovered_worker_pids": [],
                "index_metadata": {
                    "availability": "available", "reason": None,
                    "index_oid": 2, "relfilenode": 3,
                    "index_bytes": 8192, "index_pages": 1,
                },
                "structure_status": "success",
                "structure_summary": {
                    "availability": "available", "reason": None,
                    "scan_scope": "full", "sample_rate": 1.0,
                    "scanned_lists": 100, "declared_lists": 100,
                    "distribution": {
                        name: {"availability": "available", "value": value}
                        for name, value in {
                            "min": 5, "mean": 10.0, "p50": 9.0,
                            "p95": 18.0, "max": 22,
                        }.items()
                    },
                    "metrics": {
                        name: {"availability": "available", "value": value}
                        for name, value in {
                            "physical_indexed_tuples": 1000,
                            "empty_lists": 0, "empty_ratio": 0.0,
                            "cv": 0.3, "gini": 0.2,
                            "max_over_avg": 2.2, "p95_over_p50": 2.0,
                            "top_n_tuple_share": 0.2, "fill_ratio": 0.97,
                            "coverage_ratio": 1.0, "amplification": 1.1,
                        }.items()
                    },
                },
            }
            quality = {
                "quality_run_id": "quality-full-id", "build_id": "build-full-id",
                "lists": 100, "probes": 32, "k": 10, "status": "success",
                "recall_at_k": .9, "mean_ms": 1, "p50_ms": 1,
                "p95_ms": 2, "p99_ms": 3, "stddev_ms": .2,
                "single_client_qps": 1000, "used_ivfflat": True,
            }
            memory = {
                "estimated_pgvector_checked_memory_bytes": 1024,
                "memory_limit_bytes": 2048,
            }
            write_build_report(store, build_prefix, build, {}, memory, None)
            write_quality_report(store, quality_prefix, quality)
            manifest = {
                "dataset": "sift1m", "metric": "l2", "rows": [1000],
                "dimension": 128, "postgresql": "PostgreSQL test",
                "pgvector": "0.8.6",
                "postgresql_settings": {"maintenance_work_mem": "64MB"},
                "capabilities": {"capabilities": {}},
            }
            summary = {
                "diagnosis_id": "diag", "status": "complete",
                "build_runs": 1, "successful_build_runs": 1,
                "failed_build_runs": 0, "skipped_build_runs": 0,
                "quality_runs": 1, "valid_quality_runs": 1,
            }
            write_root_reports(
                store, manifest, summary,
                [(build, build_prefix)], [(quality, quality_prefix)],
            )
            self.assertTrue((root / "README.md").is_file())
            self.assertTrue((root / build_prefix / "report.md").is_file())
            self.assertTrue((root / quality_prefix / "report.md").is_file())
            self.assertTrue((root / "build-summary.csv").is_file())
            readme = (root / "README.md").read_text()
            self.assertIn("## Result", readme)
            self.assertIn("Status: COMPLETE", readme)
            self.assertIn("## Main Findings", readme)
            self.assertIn("## Build Result", readme)
            self.assertIn("## Quality Result", readme)
            self.assertIn("## Structure Summary", readme)
            self.assertIn("physical indexed tuples   1,000", readme)
            self.assertIn("coverage ratio            1.000", readme)
            self.assertIn("## Unavailable Metrics", readme)
            self.assertIn("## Scope", readme)
            self.assertIn("## Details", readme)
            self.assertIn("No parameter ranking", readme)
            build_report = (root / build_prefix / "report.md").read_text()
            self.assertIn("## Structure Summary", build_report)
            self.assertIn("scanned / declared lists  100 / 100", build_report)

    def test_skipped_build_reports_preflight_reason_and_memory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            store = RunStore(root)
            build_prefix = Path("builds/lists-1000__abcd1234")
            memory = {
                "availability": "available",
                "estimated_pgvector_checked_memory_bytes": 232 * 1024 * 1024,
                "memory_limit_bytes": 64 * 1024 * 1024,
                "predicted_memory_failure": True,
            }
            build = {
                "build_id": "build-skipped", "lists": 1000,
                "requested_rows": 1_000_000, "actual_rows": 1_000_000,
                "eligible_vectors": 1_000_000, "status": "skipped",
                "database_status": "not_started",
                "collection_status": "not_started",
                "reason": "predicted pgvector maintenance_work_mem hard failure",
                "memory_estimate": memory,
                "index_metadata": {
                    "availability": "unavailable",
                    "reason": "CREATE INDEX was skipped",
                },
            }
            write_build_report(store, build_prefix, build, {}, memory, None)
            manifest = {
                "dataset": "sift1m", "metric": "l2", "dimension": 128,
                "postgresql_settings": {"maintenance_work_mem": "64MB"},
                "capabilities": {"capabilities": {}},
                "probes": [10],
            }
            summary = {
                "diagnosis_id": "diag", "status": "complete_with_partial_data",
                "build_runs": 1, "successful_build_runs": 0,
                "failed_build_runs": 0, "skipped_build_runs": 1,
                "quality_runs": 0, "valid_quality_runs": 0,
            }
            write_root_reports(store, manifest, summary, [(build, build_prefix)], [])

            for report_path in (root / "README.md", root / build_prefix / "report.md"):
                report = report_path.read_text()
                self.assertIn(
                    "predicted pgvector maintenance_work_mem hard failure", report
                )
                self.assertIn("232 MiB", report)
                self.assertIn("64 MiB", report)
                self.assertIn("Status: NOT RUN", report)


if __name__ == "__main__":
    unittest.main()
