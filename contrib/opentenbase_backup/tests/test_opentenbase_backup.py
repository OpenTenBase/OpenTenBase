"""Tests for the OpenTenBase backup manifest utility.

Copyright (c) 2026 OpenTenBase Contributors

This file is licensed under the same terms as OpenTenBase. See LICENSE.txt
in the repository root for details.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Dict, Iterator, Tuple, Union


MODULE_PATH = Path(__file__).resolve().parents[1] / "opentenbase_backup.py"
SPEC = importlib.util.spec_from_file_location("opentenbase_backup", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
backup = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = backup
SPEC.loader.exec_module(backup)


class WorkspaceMixin:
    """Create an isolated filesystem tree for each test."""

    temporary: tempfile.TemporaryDirectory[str]
    workspace: Path

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.workspace = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def make_tree(self, name: str, files: Dict[str, Union[bytes, str]]) -> Path:
        root = self.workspace / name
        root.mkdir(parents=True)
        for relative, content in files.items():
            target = root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            if isinstance(content, bytes):
                target.write_bytes(content)
            else:
                target.write_text(content, encoding="utf-8")
        return root

    def load_json(self, path: Path) -> dict:
        return json.loads(path.read_text(encoding="utf-8"))

    @contextlib.contextmanager
    def captured_streams(self) -> Iterator[Tuple[io.StringIO, io.StringIO]]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            yield stdout, stderr


class PathSafetyTests(WorkspaceMixin, unittest.TestCase):
    def test_normalize_converts_backslashes(self) -> None:
        self.assertEqual(backup.normalize_manifest_path("base\\123\\456"), "base/123/456")

    def test_normalize_preserves_safe_posix_path(self) -> None:
        self.assertEqual(backup.normalize_manifest_path("global/pg_control"), "global/pg_control")

    def test_normalize_rejects_empty_path(self) -> None:
        with self.assertRaises(backup.ManifestError):
            backup.normalize_manifest_path("")

    def test_normalize_rejects_absolute_posix_path(self) -> None:
        with self.assertRaises(backup.ManifestError):
            backup.normalize_manifest_path("/etc/passwd")

    def test_normalize_rejects_windows_drive_path(self) -> None:
        with self.assertRaises(backup.ManifestError):
            backup.normalize_manifest_path("C:/backup/file")

    def test_normalize_rejects_parent_traversal(self) -> None:
        with self.assertRaises(backup.ManifestError):
            backup.normalize_manifest_path("base/../../outside")

    def test_normalize_rejects_dot_component(self) -> None:
        with self.assertRaises(backup.ManifestError):
            backup.normalize_manifest_path("base/./file")

    def test_resolve_beneath_returns_descendant(self) -> None:
        root = self.make_tree("root", {})
        expected = (root / "base" / "1").resolve()
        self.assertEqual(backup.resolve_beneath(root, "base/1"), expected)

    def test_resolve_beneath_rejects_escape(self) -> None:
        root = self.make_tree("root", {})
        with self.assertRaises(backup.ManifestError):
            backup.resolve_beneath(root, "../outside")

    def test_classifies_wal_segment(self) -> None:
        self.assertEqual(
            backup.classify_file("pg_wal/00000001000000000000000A"),
            backup.KIND_WAL,
        )

    def test_classifies_legacy_xlog_segment(self) -> None:
        self.assertEqual(
            backup.classify_file("pg_xlog/00000001000000000000000B.partial"),
            backup.KIND_WAL,
        )

    def test_does_not_classify_arbitrary_hex_file_as_wal(self) -> None:
        self.assertEqual(
            backup.classify_file("base/00000001000000000000000A"),
            backup.KIND_DATA,
        )

    def test_classifies_known_configuration(self) -> None:
        self.assertEqual(backup.classify_file("postgresql.conf"), backup.KIND_CONFIG)

    def test_classifies_ini_configuration(self) -> None:
        self.assertEqual(backup.classify_file("ctl/cluster.ini"), backup.KIND_CONFIG)

    def test_glob_exclusion_is_case_sensitive(self) -> None:
        self.assertTrue(backup.should_exclude("logs/server.log", ["logs/*.log"]))
        self.assertFalse(backup.should_exclude("logs/server.LOG", ["logs/*.log"]))


class ManifestPrimitiveTests(WorkspaceMixin, unittest.TestCase):
    def test_canonical_json_is_key_order_independent(self) -> None:
        left = backup.canonical_json({"b": 2, "a": 1})
        right = backup.canonical_json({"a": 1, "b": 2})
        self.assertEqual(left, right)

    def test_file_sha256_matches_known_digest(self) -> None:
        root = self.make_tree("root", {"file": "abc"})
        self.assertEqual(
            backup.file_sha256(root / "file"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        )

    def test_require_nonnegative_int_rejects_bool(self) -> None:
        with self.assertRaises(backup.ManifestError):
            backup.require_nonnegative_int(True, "value")

    def test_require_nonnegative_int_rejects_negative(self) -> None:
        with self.assertRaises(backup.ManifestError):
            backup.require_nonnegative_int(-1, "value")

    def test_file_record_round_trip(self) -> None:
        record = backup.FileRecord("base/1", 3, 4, 0o600, "a" * 64, "data", "added")
        self.assertEqual(backup.FileRecord.from_dict(record.to_dict()), record)

    def test_deleted_record_rejects_digest(self) -> None:
        record = {
            "path": "base/1",
            "size": 0,
            "mtime_ns": 0,
            "mode": 0,
            "sha256": "a" * 64,
            "kind": "data",
            "state": "deleted",
        }
        with self.assertRaises(backup.ManifestError):
            backup.FileRecord.from_dict(record)

    def test_non_deleted_record_requires_digest(self) -> None:
        record = {
            "path": "base/1",
            "size": 0,
            "mtime_ns": 0,
            "mode": 0,
            "kind": "data",
            "state": "added",
        }
        with self.assertRaises(backup.ManifestError):
            backup.FileRecord.from_dict(record)

    def test_invalid_record_state_is_rejected(self) -> None:
        record = {
            "path": "base/1",
            "size": 0,
            "mtime_ns": 0,
            "mode": 0,
            "sha256": "a" * 64,
            "kind": "data",
            "state": "mystery",
        }
        with self.assertRaises(backup.ManifestError):
            backup.FileRecord.from_dict(record)

    def test_sign_and_verify(self) -> None:
        unsigned = {"format": backup.FORMAT_NAME, "version": 1, "files": []}
        signed = backup.sign_manifest(unsigned, b"secret")
        backup.verify_signature(signed, b"secret")

    def test_wrong_signature_key_is_rejected(self) -> None:
        signed = backup.sign_manifest({"value": 1}, b"correct")
        with self.assertRaises(backup.VerificationError):
            backup.verify_signature(signed, b"wrong")

    def test_signed_manifest_requires_key(self) -> None:
        signed = backup.sign_manifest({"value": 1}, b"correct")
        with self.assertRaises(backup.VerificationError):
            backup.verify_signature(signed, None)

    def test_unsigned_manifest_rejects_requested_key(self) -> None:
        with self.assertRaises(backup.VerificationError):
            backup.verify_signature({"value": 1}, b"key")

    def test_read_key_rejects_empty_file(self) -> None:
        key = self.workspace / "empty.key"
        key.write_bytes(b"")
        with self.assertRaises(backup.BackupError):
            backup.read_key(key)


class ScanTests(WorkspaceMixin, unittest.TestCase):
    def test_scan_returns_sorted_records(self) -> None:
        root = self.make_tree("root", {"z": "z", "a/2": "2", "a/1": "1"})
        records = backup.scan_backup(root, (), jobs=2, follow_symlinks=False)
        self.assertEqual([record.path for record in records], ["a/1", "a/2", "z"])

    def test_scan_calculates_size_and_digest(self) -> None:
        root = self.make_tree("root", {"base/1": b"payload"})
        record = backup.scan_backup(root, (), jobs=1, follow_symlinks=False)[0]
        self.assertEqual(record.size, 7)
        self.assertEqual(record.sha256, backup.file_sha256(root / "base/1"))

    def test_scan_applies_exclusions(self) -> None:
        root = self.make_tree("root", {"base/1": "data", "logs/a.log": "noise"})
        records = backup.scan_backup(root, ["logs/*"], jobs=1, follow_symlinks=False)
        self.assertEqual([record.path for record in records], ["base/1"])

    def test_scan_rejects_zero_jobs(self) -> None:
        root = self.make_tree("root", {})
        with self.assertRaises(backup.BackupError):
            backup.scan_backup(root, (), jobs=0, follow_symlinks=False)

    def test_scan_rejects_non_directory(self) -> None:
        path = self.workspace / "file"
        path.write_text("x", encoding="utf-8")
        with self.assertRaises(backup.BackupError):
            backup.scan_backup(path, (), jobs=1, follow_symlinks=False)

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks are unavailable")
    def test_scan_rejects_symlink_by_default(self) -> None:
        root = self.make_tree("root", {"real": "value"})
        link = root / "link"
        try:
            link.symlink_to(root / "real")
        except OSError as exc:
            self.skipTest(f"cannot create symlink: {exc}")
        with self.assertRaises(backup.BackupError):
            backup.scan_backup(root, (), jobs=1, follow_symlinks=False)

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks are unavailable")
    def test_scan_can_follow_internal_file_symlink(self) -> None:
        root = self.make_tree("root", {"real": "value"})
        link = root / "link"
        try:
            link.symlink_to(root / "real")
        except OSError as exc:
            self.skipTest(f"cannot create symlink: {exc}")
        records = backup.scan_backup(root, (), jobs=1, follow_symlinks=True)
        self.assertEqual([record.path for record in records], ["link", "real"])


class FullManifestTests(WorkspaceMixin, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.root = self.make_tree(
            "base-backup",
            {
                "PG_VERSION": "10\n",
                "base/1/100": b"table-data",
                "global/pg_control": b"control",
                "pg_wal/000000010000000000000001": b"wal",
                "postgresql.conf": "shared_buffers=1GB\n",
            },
        )
        self.manifest = self.workspace / "base.manifest.json"

    def test_create_manifest_writes_expected_shape(self) -> None:
        result = backup.create_manifest(self.root, self.manifest, jobs=2)
        self.assertEqual(result["format"], backup.FORMAT_NAME)
        self.assertEqual(result["version"], 1)
        self.assertEqual(result["algorithm"], "sha256")
        self.assertEqual(result["summary"]["file_count"], 5)
        self.assertEqual(result["summary"]["states"]["added"], 5)
        self.assertTrue(self.manifest.is_file())

    def test_manifest_entries_are_sorted(self) -> None:
        result = backup.create_manifest(self.root, self.manifest)
        paths = [entry["path"] for entry in result["files"]]
        self.assertEqual(paths, sorted(paths))

    def test_manifest_classifies_wal_and_config(self) -> None:
        result = backup.create_manifest(self.root, self.manifest)
        records = {entry["path"]: entry for entry in result["files"]}
        self.assertEqual(records["pg_wal/000000010000000000000001"]["kind"], "wal")
        self.assertEqual(records["postgresql.conf"]["kind"], "config")

    def test_output_inside_root_is_not_self_included(self) -> None:
        output = self.root / backup.DEFAULT_MANIFEST
        backup.create_manifest(self.root, output)
        second = backup.create_manifest(self.root, output)
        self.assertNotIn(backup.DEFAULT_MANIFEST, [entry["path"] for entry in second["files"]])

    def test_exclusion_is_recorded_by_absence(self) -> None:
        result = backup.create_manifest(self.root, self.manifest, patterns=["pg_wal/*"])
        paths = [entry["path"] for entry in result["files"]]
        self.assertNotIn("pg_wal/000000010000000000000001", paths)

    def test_atomic_write_leaves_no_temporary_file(self) -> None:
        backup.create_manifest(self.root, self.manifest)
        leftovers = list(self.workspace.glob(self.manifest.name + ".tmp-*"))
        self.assertEqual(leftovers, [])

    def test_load_manifest_round_trip(self) -> None:
        created = backup.create_manifest(self.root, self.manifest)
        loaded = backup.load_manifest(self.manifest)
        self.assertEqual(created, loaded)

    def test_load_manifest_rejects_wrong_format(self) -> None:
        self.manifest.write_text('{"format":"other"}', encoding="utf-8")
        with self.assertRaises(backup.ManifestError):
            backup.load_manifest(self.manifest)

    def test_load_manifest_rejects_unsorted_files(self) -> None:
        created = backup.create_manifest(self.root, self.manifest)
        created["files"].reverse()
        backup.atomic_write_json(self.manifest, created)
        with self.assertRaises(backup.ManifestError):
            backup.load_manifest(self.manifest)

    def test_signed_manifest_loads_with_key(self) -> None:
        key = b"manifest authentication key"
        backup.create_manifest(self.root, self.manifest, key=key)
        loaded = backup.load_manifest(self.manifest, key)
        self.assertEqual(loaded["signature"]["algorithm"], "hmac-sha256")

    def test_tampered_signed_manifest_is_rejected(self) -> None:
        key = b"manifest authentication key"
        backup.create_manifest(self.root, self.manifest, key=key)
        value = self.load_json(self.manifest)
        value["summary"]["file_count"] = 999
        backup.atomic_write_json(self.manifest, value)
        with self.assertRaises(backup.VerificationError):
            backup.load_manifest(self.manifest, key)


class VerificationTests(FullManifestTests):
    def test_clean_backup_verifies(self) -> None:
        backup.create_manifest(self.root, self.manifest)
        self.assertEqual(backup.verify_backup(self.manifest, jobs=2), [])

    def test_content_tamper_is_detected(self) -> None:
        backup.create_manifest(self.root, self.manifest)
        (self.root / "base/1/100").write_bytes(b"table-xxxx")
        issues = backup.verify_backup(self.manifest)
        self.assertEqual([(issue.path, issue.code) for issue in issues], [("base/1/100", "sha256")])

    def test_size_change_is_detected_without_hashing_result(self) -> None:
        backup.create_manifest(self.root, self.manifest)
        (self.root / "base/1/100").write_bytes(b"longer table data")
        issues = backup.verify_backup(self.manifest)
        self.assertEqual([(issue.path, issue.code) for issue in issues], [("base/1/100", "size")])

    def test_missing_file_is_detected(self) -> None:
        backup.create_manifest(self.root, self.manifest)
        (self.root / "global/pg_control").unlink()
        issues = backup.verify_backup(self.manifest)
        self.assertEqual([(issue.path, issue.code) for issue in issues], [("global/pg_control", "missing")])

    def test_extra_file_is_ignored_without_strict(self) -> None:
        backup.create_manifest(self.root, self.manifest)
        (self.root / "unexpected").write_text("x", encoding="utf-8")
        self.assertEqual(backup.verify_backup(self.manifest, strict=False), [])

    def test_extra_file_is_reported_with_strict(self) -> None:
        backup.create_manifest(self.root, self.manifest)
        (self.root / "unexpected").write_text("x", encoding="utf-8")
        issues = backup.verify_backup(self.manifest, strict=True)
        self.assertEqual([(issue.path, issue.code) for issue in issues], [("unexpected", "extra")])

    def test_verify_rejects_invalid_job_count(self) -> None:
        backup.create_manifest(self.root, self.manifest)
        with self.assertRaises(backup.BackupError):
            backup.verify_backup(self.manifest, jobs=0)


class IncrementalTests(WorkspaceMixin, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.base_root = self.make_tree(
            "base",
            {
                "unchanged": "same",
                "changed": "before",
                "deleted": "remove me",
                "nested/data": "nested",
            },
        )
        self.next_root = self.make_tree(
            "next",
            {
                "unchanged": "same",
                "changed": "after",
                "added": "new file",
                "nested/data": "nested",
            },
        )
        self.base_manifest = self.workspace / "base.json"
        backup.create_manifest(self.base_root, self.base_manifest)
        self.delta_directory = self.workspace / "delta"

    def test_incremental_manifest_assigns_all_states(self) -> None:
        output = self.workspace / "next.json"
        result = backup.create_manifest(self.next_root, output, parent_path=self.base_manifest)
        states = {entry["path"]: entry["state"] for entry in result["files"]}
        self.assertEqual(states["unchanged"], "unchanged")
        self.assertEqual(states["nested/data"], "unchanged")
        self.assertEqual(states["changed"], "changed")
        self.assertEqual(states["added"], "added")
        self.assertEqual(states["deleted"], "deleted")

    def test_incremental_summary_counts_states(self) -> None:
        output = self.workspace / "next.json"
        result = backup.create_manifest(self.next_root, output, parent_path=self.base_manifest)
        self.assertEqual(
            result["summary"]["states"],
            {"added": 1, "changed": 1, "deleted": 1, "unchanged": 2},
        )

    def test_parent_reference_has_digest(self) -> None:
        output = self.workspace / "next.json"
        result = backup.create_manifest(self.next_root, output, parent_path=self.base_manifest)
        self.assertEqual(result["parent"]["sha256"], backup.manifest_sha256(self.base_manifest))

    def test_delta_copies_only_added_and_changed_files(self) -> None:
        backup.materialize_delta(self.next_root, self.base_manifest, self.delta_directory)
        payload = self.delta_directory / "payload"
        actual = {path.relative_to(payload).as_posix() for path in payload.rglob("*") if path.is_file()}
        self.assertEqual(actual, {"added", "changed"})

    def test_delta_manifest_uses_payload_data_root(self) -> None:
        result = backup.materialize_delta(self.next_root, self.base_manifest, self.delta_directory)
        self.assertEqual(result["data_root"], "payload")
        self.assertTrue((self.delta_directory / backup.DEFAULT_MANIFEST).is_file())

    def test_delta_rejects_nonempty_output_directory(self) -> None:
        self.delta_directory.mkdir()
        (self.delta_directory / "existing").write_text("x", encoding="utf-8")
        with self.assertRaises(backup.BackupError):
            backup.materialize_delta(self.next_root, self.base_manifest, self.delta_directory)

    def test_delta_chain_verifies(self) -> None:
        backup.materialize_delta(self.next_root, self.base_manifest, self.delta_directory, jobs=2)
        manifest = self.delta_directory / backup.DEFAULT_MANIFEST
        self.assertEqual(backup.verify_backup(manifest, jobs=2), [])

    def test_parent_manifest_tamper_breaks_chain(self) -> None:
        backup.materialize_delta(self.next_root, self.base_manifest, self.delta_directory)
        self.base_manifest.write_text(self.base_manifest.read_text(encoding="utf-8") + " ", encoding="utf-8")
        manifest = self.delta_directory / backup.DEFAULT_MANIFEST
        with self.assertRaises(backup.VerificationError):
            backup.load_chain(manifest)

    def test_effective_files_apply_change_add_and_delete(self) -> None:
        backup.materialize_delta(self.next_root, self.base_manifest, self.delta_directory)
        layers = backup.load_chain(self.delta_directory / backup.DEFAULT_MANIFEST)
        effective = backup.effective_files(layers)
        self.assertEqual(set(effective), {"unchanged", "changed", "added", "nested/data"})
        self.assertEqual(effective["changed"][0].data_root, self.delta_directory / "payload")

    def test_two_delta_layers_restore_latest_snapshot(self) -> None:
        backup.materialize_delta(self.next_root, self.base_manifest, self.delta_directory)
        first_delta_manifest = self.delta_directory / backup.DEFAULT_MANIFEST
        third_root = self.make_tree(
            "third",
            {
                "unchanged": "same",
                "changed": "latest",
                "added": "new file",
                "third-only": "third",
            },
        )
        second_delta = self.workspace / "delta-2"
        backup.materialize_delta(third_root, first_delta_manifest, second_delta)
        destination = self.workspace / "restored"
        result = backup.restore_backup(second_delta / backup.DEFAULT_MANIFEST, destination, jobs=2)
        self.assertEqual(result["layers"], 3)
        self.assertEqual((destination / "changed").read_text(encoding="utf-8"), "latest")
        self.assertEqual((destination / "third-only").read_text(encoding="utf-8"), "third")
        self.assertFalse((destination / "nested/data").exists())


class RestoreTests(IncrementalTests):
    def test_restore_materializes_effective_snapshot(self) -> None:
        backup.materialize_delta(self.next_root, self.base_manifest, self.delta_directory)
        destination = self.workspace / "restore"
        result = backup.restore_backup(
            self.delta_directory / backup.DEFAULT_MANIFEST,
            destination,
            jobs=2,
        )
        self.assertEqual(result["files"], 4)
        self.assertEqual((destination / "unchanged").read_text(encoding="utf-8"), "same")
        self.assertEqual((destination / "changed").read_text(encoding="utf-8"), "after")
        self.assertEqual((destination / "added").read_text(encoding="utf-8"), "new file")
        self.assertFalse((destination / "deleted").exists())

    def test_restore_rejects_existing_file_without_overwrite(self) -> None:
        backup.materialize_delta(self.next_root, self.base_manifest, self.delta_directory)
        destination = self.make_tree("restore", {"unchanged": "existing"})
        with self.assertRaises(backup.BackupError):
            backup.restore_backup(self.delta_directory / backup.DEFAULT_MANIFEST, destination)

    def test_restore_can_overwrite_existing_file(self) -> None:
        backup.materialize_delta(self.next_root, self.base_manifest, self.delta_directory)
        destination = self.make_tree("restore", {"unchanged": "existing"})
        backup.restore_backup(
            self.delta_directory / backup.DEFAULT_MANIFEST,
            destination,
            overwrite=True,
        )
        self.assertEqual((destination / "unchanged").read_text(encoding="utf-8"), "same")

    def test_restore_verifies_source_before_copy(self) -> None:
        backup.materialize_delta(self.next_root, self.base_manifest, self.delta_directory)
        (self.delta_directory / "payload/changed").write_text("tampered", encoding="utf-8")
        with self.assertRaises(backup.VerificationError):
            backup.restore_backup(
                self.delta_directory / backup.DEFAULT_MANIFEST,
                self.workspace / "restore",
            )

    def test_inspection_reports_chain_totals(self) -> None:
        backup.materialize_delta(self.next_root, self.base_manifest, self.delta_directory)
        report = backup.inspection(self.delta_directory / backup.DEFAULT_MANIFEST)
        self.assertEqual(len(report["layers"]), 2)
        self.assertEqual(report["effective_files"], 4)
        self.assertGreater(report["effective_bytes"], 0)


class CommandLineTests(WorkspaceMixin, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.root = self.make_tree("root", {"PG_VERSION": "10", "base/1": "data"})
        self.manifest = self.workspace / "manifest.json"

    def test_manifest_command_returns_summary_json(self) -> None:
        with self.captured_streams() as (stdout, stderr):
            code = backup.main(
                ["manifest", str(self.root), "--output", str(self.manifest), "--jobs", "2"]
            )
        self.assertEqual(code, 0, stderr.getvalue())
        result = json.loads(stdout.getvalue())
        self.assertEqual(result["file_count"], 2)

    def test_verify_command_returns_zero_for_clean_backup(self) -> None:
        backup.create_manifest(self.root, self.manifest)
        with self.captured_streams() as (stdout, stderr):
            code = backup.main(["verify", str(self.manifest)])
        self.assertEqual(code, 0, stderr.getvalue())
        self.assertTrue(json.loads(stdout.getvalue())["ok"])

    def test_verify_command_returns_two_for_mismatch(self) -> None:
        backup.create_manifest(self.root, self.manifest)
        (self.root / "base/1").write_text("tampered", encoding="utf-8")
        with self.captured_streams() as (stdout, stderr):
            code = backup.main(["verify", str(self.manifest)])
        self.assertEqual(code, 2, stderr.getvalue())
        self.assertFalse(json.loads(stdout.getvalue())["ok"])

    def test_inspect_command_emits_chain_description(self) -> None:
        backup.create_manifest(self.root, self.manifest)
        with self.captured_streams() as (stdout, stderr):
            code = backup.main(["inspect", str(self.manifest)])
        self.assertEqual(code, 0, stderr.getvalue())
        self.assertEqual(len(json.loads(stdout.getvalue())["layers"]), 1)

    def test_restore_command_copies_files(self) -> None:
        backup.create_manifest(self.root, self.manifest)
        destination = self.workspace / "destination"
        with self.captured_streams() as (stdout, stderr):
            code = backup.main(["restore", str(self.manifest), str(destination)])
        self.assertEqual(code, 0, stderr.getvalue())
        self.assertEqual((destination / "base/1").read_text(encoding="utf-8"), "data")
        self.assertEqual(json.loads(stdout.getvalue())["files"], 2)

    def test_hmac_key_file_is_used(self) -> None:
        key = self.workspace / "key"
        key.write_bytes(b"command line key")
        with self.captured_streams() as (stdout, stderr):
            code = backup.main(
                [
                    "manifest",
                    str(self.root),
                    "--output",
                    str(self.manifest),
                    "--hmac-key-file",
                    str(key),
                ]
            )
        self.assertEqual(code, 0, stderr.getvalue())
        backup.load_manifest(self.manifest, b"command line key")

    def test_user_error_returns_one_without_traceback(self) -> None:
        with self.captured_streams() as (stdout, stderr):
            code = backup.main(["verify", str(self.workspace / "missing.json")])
        self.assertEqual(code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("opentenbase_backup: error:", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())

    def test_positive_integer_rejects_zero(self) -> None:
        with self.assertRaises(Exception):
            backup.positive_integer("0")


if __name__ == "__main__":
    unittest.main()
