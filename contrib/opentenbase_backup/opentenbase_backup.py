#!/usr/bin/env python3
"""Create and verify portable OpenTenBase backup manifests.

Copyright (c) 2026 OpenTenBase Contributors

This file is licensed under the same terms as OpenTenBase. See LICENSE.txt
in the repository root for details.

The tool intentionally operates on an already quiesced backup directory.  It
does not replace pg_basebackup, pg_dump, or cluster orchestration.  Instead it
adds deterministic manifests, incremental delta materialization, integrity
verification, safe restore planning, and optional HMAC authentication to the
files produced by those tools.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import datetime as dt
import fnmatch
import hashlib
import hmac
import json
import os
import shutil
import stat
import sys
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Set, Tuple


FORMAT_NAME = "opentenbase-backup-manifest"
FORMAT_VERSION = 1
DEFAULT_MANIFEST = "opentenbase-backup-manifest.json"
DEFAULT_CHUNK_SIZE = 1024 * 1024
WAL_SEGMENT_LENGTHS = {16, 24, 32, 40}
KIND_DATA = "data"
KIND_WAL = "wal"
KIND_CONFIG = "config"
STATE_ADDED = "added"
STATE_CHANGED = "changed"
STATE_UNCHANGED = "unchanged"
STATE_DELETED = "deleted"
VALID_STATES = {STATE_ADDED, STATE_CHANGED, STATE_UNCHANGED, STATE_DELETED}


class BackupError(Exception):
    """Base class for user-facing backup errors."""


class ManifestError(BackupError):
    """Raised when a manifest is malformed or unsafe."""


class VerificationError(BackupError):
    """Raised when backup content does not match its manifest."""


@dataclasses.dataclass(frozen=True)
class FileRecord:
    """A normalized file entry stored in a manifest."""

    path: str
    size: int
    mtime_ns: int
    mode: int
    sha256: Optional[str]
    kind: str
    state: str

    def to_dict(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "path": self.path,
            "size": self.size,
            "mtime_ns": self.mtime_ns,
            "mode": self.mode,
            "kind": self.kind,
            "state": self.state,
        }
        if self.sha256 is not None:
            result["sha256"] = self.sha256
        return result

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "FileRecord":
        required = {"path", "size", "mtime_ns", "mode", "kind", "state"}
        missing = sorted(required - set(value))
        if missing:
            raise ManifestError("file entry is missing: " + ", ".join(missing))
        path = normalize_manifest_path(str(value["path"]))
        state_value = str(value["state"])
        if state_value not in VALID_STATES:
            raise ManifestError(f"invalid state for {path}: {state_value}")
        digest = value.get("sha256")
        if state_value != STATE_DELETED:
            if not isinstance(digest, str) or len(digest) != 64:
                raise ManifestError(f"invalid SHA-256 digest for {path}")
            try:
                int(digest, 16)
            except ValueError as exc:
                raise ManifestError(f"non-hex SHA-256 digest for {path}") from exc
        elif digest is not None:
            raise ManifestError(f"deleted entry unexpectedly has a digest: {path}")
        size = require_nonnegative_int(value["size"], f"size for {path}")
        mtime_ns = require_nonnegative_int(value["mtime_ns"], f"mtime_ns for {path}")
        mode = require_nonnegative_int(value["mode"], f"mode for {path}")
        kind = str(value["kind"])
        if kind not in {KIND_DATA, KIND_WAL, KIND_CONFIG}:
            raise ManifestError(f"invalid file kind for {path}: {kind}")
        return cls(path, size, mtime_ns, mode, digest, kind, state_value)


@dataclasses.dataclass(frozen=True)
class VerificationIssue:
    """One mismatch found during verification."""

    path: str
    code: str
    detail: str

    def to_dict(self) -> Dict[str, str]:
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True)
class Layer:
    """A loaded manifest and the directory that supplies its file payload."""

    manifest_path: Path
    manifest: Mapping[str, Any]
    data_root: Path
    records: Tuple[FileRecord, ...]


def require_nonnegative_int(value: Any, label: str) -> int:
    """Return *value* as an integer while rejecting bools and negatives."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ManifestError(f"{label} must be a non-negative integer")
    return value


def utc_now() -> str:
    """Return a second-resolution RFC 3339 UTC timestamp."""

    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_manifest_path(value: str) -> str:
    """Validate and normalize a relative manifest path.

    Manifest paths always use POSIX separators so the same manifest can be
    verified on Linux and Windows.  Absolute paths, drive prefixes, empty
    components, dot components, and parent traversal are rejected.
    """

    if not value or "\x00" in value:
        raise ManifestError("manifest path is empty or contains NUL")
    value = value.replace("\\", "/")
    candidate = PurePosixPath(value)
    if candidate.is_absolute():
        raise ManifestError(f"absolute manifest path is not allowed: {value}")
    if len(value) >= 2 and value[1] == ":":
        raise ManifestError(f"drive-qualified manifest path is not allowed: {value}")
    if any(part in {"", ".", ".."} for part in value.split("/")):
        raise ManifestError(f"unsafe manifest path: {value}")
    return candidate.as_posix()


def resolve_beneath(root: Path, relative: str) -> Path:
    """Resolve *relative* below *root* and reject path escape."""

    normalized = normalize_manifest_path(relative)
    root_resolved = root.resolve()
    result = (root_resolved / Path(*PurePosixPath(normalized).parts)).resolve()
    try:
        result.relative_to(root_resolved)
    except ValueError as exc:
        raise ManifestError(f"path escapes backup root: {relative}") from exc
    return result


def canonical_json(value: Mapping[str, Any]) -> bytes:
    """Serialize a mapping deterministically for hashing and HMAC."""

    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def manifest_payload(manifest: Mapping[str, Any]) -> Dict[str, Any]:
    """Return the authenticated portion of a manifest."""

    return {key: value for key, value in manifest.items() if key != "signature"}


def file_sha256(path: Path, chunk_size: int = DEFAULT_CHUNK_SIZE) -> str:
    """Hash a regular file without loading it entirely into memory."""

    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while True:
            chunk = stream.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def manifest_sha256(path: Path) -> str:
    """Hash a manifest file exactly as stored."""

    return file_sha256(path)


def read_key(path: Optional[Path]) -> Optional[bytes]:
    """Load a non-empty HMAC key without exposing it in output."""

    if path is None:
        return None
    try:
        value = path.read_bytes()
    except OSError as exc:
        raise BackupError(f"cannot read HMAC key {path}: {exc}") from exc
    if not value:
        raise BackupError(f"HMAC key is empty: {path}")
    return value


def sign_manifest(manifest: Mapping[str, Any], key: bytes) -> Dict[str, Any]:
    """Return a copy of *manifest* with an HMAC-SHA256 signature."""

    result = dict(manifest_payload(manifest))
    signature = hmac.new(key, canonical_json(result), hashlib.sha256).hexdigest()
    result["signature"] = {"algorithm": "hmac-sha256", "value": signature}
    return result


def verify_signature(manifest: Mapping[str, Any], key: Optional[bytes]) -> None:
    """Validate the manifest HMAC when one is present or requested."""

    signature = manifest.get("signature")
    if signature is None:
        if key is not None:
            raise VerificationError("manifest is not signed")
        return
    if not isinstance(signature, Mapping):
        raise ManifestError("signature must be an object")
    if signature.get("algorithm") != "hmac-sha256":
        raise ManifestError("unsupported manifest signature algorithm")
    value = signature.get("value")
    if not isinstance(value, str) or len(value) != 64:
        raise ManifestError("invalid manifest signature value")
    if key is None:
        raise VerificationError("manifest is signed but no HMAC key was supplied")
    expected = hmac.new(key, canonical_json(manifest_payload(manifest)), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(value, expected):
        raise VerificationError("manifest signature does not match")


def should_exclude(relative: str, patterns: Sequence[str]) -> bool:
    """Return true when any shell-style exclusion matches a path."""

    return any(fnmatch.fnmatchcase(relative, pattern) for pattern in patterns)


def classify_file(relative: str) -> str:
    """Classify common OpenTenBase backup files for reporting."""

    posix = PurePosixPath(relative)
    parts = {part.lower() for part in posix.parts}
    name = posix.name
    if "pg_wal" in parts or "pg_xlog" in parts:
        compact = name.split(".", 1)[0]
        if len(compact) in WAL_SEGMENT_LENGTHS and all(c in "0123456789ABCDEFabcdef" for c in compact):
            return KIND_WAL
    if name in {"postgresql.conf", "pg_hba.conf", "pg_ident.conf", "recovery.conf"}:
        return KIND_CONFIG
    if name.endswith((".conf", ".ini", ".cfg")):
        return KIND_CONFIG
    return KIND_DATA


def iter_regular_files(root: Path, patterns: Sequence[str], follow_symlinks: bool) -> Iterator[Tuple[str, Path]]:
    """Yield normalized relative paths and regular files in lexical order."""

    if not root.is_dir():
        raise BackupError(f"backup root is not a directory: {root}")
    root_resolved = root.resolve()
    pending: List[Path] = [root_resolved]
    discovered: List[Tuple[str, Path]] = []
    visited_dirs: Set[Tuple[int, int]] = set()

    while pending:
        directory = pending.pop()
        try:
            directory_stat = directory.stat()
        except OSError as exc:
            raise BackupError(f"cannot stat directory {directory}: {exc}") from exc
        identity = (directory_stat.st_dev, directory_stat.st_ino)
        if identity in visited_dirs:
            continue
        visited_dirs.add(identity)
        try:
            entries = sorted(directory.iterdir(), key=lambda item: item.name)
        except OSError as exc:
            raise BackupError(f"cannot list directory {directory}: {exc}") from exc

        for path in entries:
            relative = path.relative_to(root_resolved).as_posix()
            if should_exclude(relative, patterns):
                continue
            try:
                is_link = path.is_symlink()
                if is_link and not follow_symlinks:
                    raise BackupError(f"symbolic link is not allowed: {relative}")
                if path.is_dir():
                    if is_link:
                        resolved = path.resolve()
                        try:
                            resolved.relative_to(root_resolved)
                        except ValueError as exc:
                            raise BackupError(f"symbolic link escapes backup root: {relative}") from exc
                    pending.append(path)
                elif path.is_file():
                    discovered.append((normalize_manifest_path(relative), path))
                else:
                    raise BackupError(f"non-regular backup entry is not supported: {relative}")
            except OSError as exc:
                raise BackupError(f"cannot inspect backup entry {relative}: {exc}") from exc

    yield from sorted(discovered, key=lambda item: item[0])


def hash_file_record(item: Tuple[str, Path]) -> FileRecord:
    """Build a base record for one filesystem entry."""

    relative, path = item
    before = path.stat()
    if not stat.S_ISREG(before.st_mode):
        raise BackupError(f"file changed type while scanning: {relative}")
    digest = file_sha256(path)
    after = path.stat()
    if before.st_size != after.st_size or before.st_mtime_ns != after.st_mtime_ns:
        raise BackupError(f"file changed while hashing: {relative}")
    return FileRecord(
        path=relative,
        size=after.st_size,
        mtime_ns=after.st_mtime_ns,
        mode=stat.S_IMODE(after.st_mode),
        sha256=digest,
        kind=classify_file(relative),
        state=STATE_ADDED,
    )


def scan_backup(root: Path, patterns: Sequence[str], jobs: int, follow_symlinks: bool) -> List[FileRecord]:
    """Scan and hash a backup directory with bounded parallelism."""

    if jobs < 1:
        raise BackupError("jobs must be at least 1")
    files = list(iter_regular_files(root, patterns, follow_symlinks))
    if jobs == 1:
        return [hash_file_record(item) for item in files]
    with concurrent.futures.ThreadPoolExecutor(max_workers=jobs) as executor:
        records = list(executor.map(hash_file_record, files))
    return sorted(records, key=lambda record: record.path)


def load_manifest(path: Path, key: Optional[bytes] = None) -> Dict[str, Any]:
    """Load, structurally validate, and optionally authenticate a manifest."""

    try:
        raw = path.read_text(encoding="utf-8")
        value = json.loads(raw)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ManifestError(f"cannot read manifest {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ManifestError("manifest root must be an object")
    if value.get("format") != FORMAT_NAME:
        raise ManifestError("unrecognized manifest format")
    if value.get("version") != FORMAT_VERSION:
        raise ManifestError(f"unsupported manifest version: {value.get('version')}")
    if value.get("algorithm") != "sha256":
        raise ManifestError("unsupported content hash algorithm")
    data_root = value.get("data_root")
    if not isinstance(data_root, str) or not data_root:
        raise ManifestError("data_root must be a non-empty string")
    entries = value.get("files")
    if not isinstance(entries, list):
        raise ManifestError("files must be an array")
    records = [FileRecord.from_dict(entry) for entry in entries]
    paths = [record.path for record in records]
    if paths != sorted(paths):
        raise ManifestError("file entries must be sorted by path")
    if len(paths) != len(set(paths)):
        raise ManifestError("manifest contains duplicate file paths")
    parent = value.get("parent")
    if parent is not None:
        if not isinstance(parent, Mapping):
            raise ManifestError("parent must be an object")
        if not isinstance(parent.get("path"), str) or not parent.get("path"):
            raise ManifestError("parent.path must be a non-empty string")
        digest = parent.get("sha256")
        if not isinstance(digest, str) or len(digest) != 64:
            raise ManifestError("parent.sha256 must be a SHA-256 digest")
    verify_signature(value, key)
    return value


def records_by_path(manifest: Mapping[str, Any]) -> Dict[str, FileRecord]:
    """Return validated manifest records indexed by path."""

    return {record.path: record for record in (FileRecord.from_dict(item) for item in manifest["files"])}


def compare_with_parent(current: Sequence[FileRecord], parent: Optional[Mapping[str, Any]]) -> List[FileRecord]:
    """Assign incremental states and add deletion tombstones."""

    if parent is None:
        return [dataclasses.replace(record, state=STATE_ADDED) for record in current]
    parent_records = records_by_path(parent)
    current_paths: Set[str] = set()
    result: List[FileRecord] = []
    for record in current:
        current_paths.add(record.path)
        previous = parent_records.get(record.path)
        if previous is None or previous.state == STATE_DELETED:
            state_value = STATE_ADDED
        elif previous.sha256 == record.sha256 and previous.size == record.size:
            state_value = STATE_UNCHANGED
        else:
            state_value = STATE_CHANGED
        result.append(dataclasses.replace(record, state=state_value))
    for path, previous in parent_records.items():
        if path not in current_paths and previous.state != STATE_DELETED:
            result.append(
                FileRecord(
                    path=path,
                    size=0,
                    mtime_ns=0,
                    mode=0,
                    sha256=None,
                    kind=previous.kind,
                    state=STATE_DELETED,
                )
            )
    return sorted(result, key=lambda record: record.path)


def relative_reference(target: Path, source_directory: Path) -> str:
    """Create a portable relative filesystem reference when possible."""

    try:
        return os.path.relpath(target.resolve(), source_directory.resolve()).replace("\\", "/")
    except ValueError:
        return str(target.resolve())


def summarize(records: Sequence[FileRecord]) -> Dict[str, Any]:
    """Calculate deterministic manifest totals."""

    states = {state_value: 0 for state_value in sorted(VALID_STATES)}
    kinds = {KIND_CONFIG: 0, KIND_DATA: 0, KIND_WAL: 0}
    bytes_by_state = {state_value: 0 for state_value in sorted(VALID_STATES)}
    for record in records:
        states[record.state] += 1
        kinds[record.kind] += 1
        bytes_by_state[record.state] += record.size
    return {
        "file_count": sum(1 for record in records if record.state != STATE_DELETED),
        "logical_bytes": sum(record.size for record in records if record.state != STATE_DELETED),
        "states": states,
        "bytes_by_state": bytes_by_state,
        "kinds": kinds,
    }


def build_manifest(
    root: Path,
    output: Path,
    parent_path: Optional[Path],
    patterns: Sequence[str],
    jobs: int,
    follow_symlinks: bool,
    key: Optional[bytes],
    data_root: Optional[str] = None,
) -> Dict[str, Any]:
    """Scan *root* and create an in-memory manifest."""

    output_parent = output.resolve().parent
    root_resolved = root.resolve()
    output_resolved = output.resolve()
    automatic_patterns = list(patterns)
    try:
        output_relative = output_resolved.relative_to(root_resolved).as_posix()
        automatic_patterns.append(output_relative)
        automatic_patterns.append(output_relative + ".tmp-*")
    except ValueError:
        pass
    parent: Optional[Dict[str, Any]] = None
    parent_info: Optional[Dict[str, str]] = None
    if parent_path is not None:
        parent_path = parent_path.resolve()
        parent = load_manifest(parent_path, key)
        parent_info = {
            "path": relative_reference(parent_path, output_parent),
            "sha256": manifest_sha256(parent_path),
        }
    scanned = scan_backup(root_resolved, automatic_patterns, jobs, follow_symlinks)
    records = compare_with_parent(scanned, parent)
    if data_root is None:
        data_root = relative_reference(root_resolved, output_parent)
    manifest: Dict[str, Any] = {
        "format": FORMAT_NAME,
        "version": FORMAT_VERSION,
        "created_at": utc_now(),
        "algorithm": "sha256",
        "data_root": data_root,
        "files": [record.to_dict() for record in records],
        "summary": summarize(records),
    }
    if parent_info is not None:
        manifest["parent"] = parent_info
    if key is not None:
        manifest = sign_manifest(manifest, key)
    return manifest


def atomic_write_json(path: Path, value: Mapping[str, Any]) -> None:
    """Write JSON durably and replace the destination atomically."""

    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=path.name + ".tmp-", dir=str(path.parent))
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(value, stream, sort_keys=True, indent=2, ensure_ascii=False, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def create_manifest(
    root: Path,
    output: Path,
    parent_path: Optional[Path] = None,
    patterns: Sequence[str] = (),
    jobs: int = 1,
    follow_symlinks: bool = False,
    key: Optional[bytes] = None,
) -> Dict[str, Any]:
    """Create and atomically store a backup manifest."""

    manifest = build_manifest(root, output, parent_path, patterns, jobs, follow_symlinks, key)
    atomic_write_json(output, manifest)
    return manifest


def copy_file_atomic(source: Path, destination: Path, mode: int, overwrite: bool) -> None:
    """Copy a file into place without exposing partial content."""

    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and not overwrite:
        raise BackupError(f"destination exists: {destination}")
    descriptor, temporary_name = tempfile.mkstemp(prefix=destination.name + ".tmp-", dir=str(destination.parent))
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        shutil.copyfile(source, temporary)
        try:
            os.chmod(temporary, mode)
        except OSError:
            pass
        os.replace(temporary, destination)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def materialize_delta(
    root: Path,
    parent_path: Path,
    output_directory: Path,
    patterns: Sequence[str] = (),
    jobs: int = 1,
    follow_symlinks: bool = False,
    key: Optional[bytes] = None,
) -> Dict[str, Any]:
    """Create a changed-file-only layer with deletion tombstones."""

    output_directory = output_directory.resolve()
    if output_directory.exists() and any(output_directory.iterdir()):
        raise BackupError(f"delta output directory is not empty: {output_directory}")
    output_directory.mkdir(parents=True, exist_ok=True)
    payload = output_directory / "payload"
    manifest_path = output_directory / DEFAULT_MANIFEST
    try:
        manifest = build_manifest(
            root,
            manifest_path,
            parent_path,
            patterns,
            jobs,
            follow_symlinks,
            key,
            data_root="payload",
        )
        records = [FileRecord.from_dict(entry) for entry in manifest["files"]]
        for record in records:
            if record.state not in {STATE_ADDED, STATE_CHANGED}:
                continue
            source = resolve_beneath(root, record.path)
            destination = resolve_beneath(payload, record.path)
            copy_file_atomic(source, destination, record.mode, overwrite=False)
        atomic_write_json(manifest_path, manifest)
        return manifest
    except BaseException:
        if output_directory.exists():
            shutil.rmtree(output_directory, ignore_errors=True)
        raise


def resolve_reference(manifest_path: Path, reference: str) -> Path:
    """Resolve an absolute or manifest-relative filesystem reference."""

    candidate = Path(reference)
    if candidate.is_absolute():
        return candidate.resolve()
    return (manifest_path.resolve().parent / candidate).resolve()


def load_chain(manifest_path: Path, key: Optional[bytes] = None, max_depth: int = 128) -> List[Layer]:
    """Load and authenticate a parent chain from base to newest layer."""

    layers_reversed: List[Layer] = []
    visited: Set[Path] = set()
    current = manifest_path.resolve()
    while True:
        if current in visited:
            raise ManifestError(f"manifest parent cycle detected at {current}")
        if len(layers_reversed) >= max_depth:
            raise ManifestError(f"manifest chain exceeds {max_depth} layers")
        visited.add(current)
        manifest = load_manifest(current, key)
        data_root = resolve_reference(current, str(manifest["data_root"]))
        records = tuple(FileRecord.from_dict(entry) for entry in manifest["files"])
        layers_reversed.append(Layer(current, manifest, data_root, records))
        parent = manifest.get("parent")
        if parent is None:
            break
        parent_path = resolve_reference(current, str(parent["path"]))
        actual_digest = manifest_sha256(parent_path)
        if actual_digest != parent["sha256"]:
            raise VerificationError(f"parent manifest digest mismatch: {parent_path}")
        current = parent_path
    return list(reversed(layers_reversed))


def effective_files(layers: Sequence[Layer]) -> Dict[str, Tuple[Layer, FileRecord]]:
    """Merge manifest layers into the final restore view."""

    result: Dict[str, Tuple[Layer, FileRecord]] = {}
    for layer in layers:
        for record in layer.records:
            if record.state == STATE_DELETED:
                result.pop(record.path, None)
            elif record.state in {STATE_ADDED, STATE_CHANGED}:
                result[record.path] = (layer, record)
            elif record.state == STATE_UNCHANGED and record.path not in result:
                raise ManifestError(
                    f"unchanged file has no earlier payload in chain: {record.path}"
                )
    return result


def verify_layer(layer: Layer, jobs: int = 1, include_unchanged: bool = False) -> List[VerificationIssue]:
    """Verify files physically supplied by one layer."""

    records = [
        record
        for record in layer.records
        if record.state in {STATE_ADDED, STATE_CHANGED}
        or (include_unchanged and record.state == STATE_UNCHANGED)
    ]

    def verify_one(record: FileRecord) -> List[VerificationIssue]:
        issues: List[VerificationIssue] = []
        path = resolve_beneath(layer.data_root, record.path)
        if not path.exists():
            return [VerificationIssue(record.path, "missing", "file does not exist")]
        if not path.is_file() or path.is_symlink():
            return [VerificationIssue(record.path, "type", "entry is not a regular file")]
        current = path.stat()
        if current.st_size != record.size:
            issues.append(
                VerificationIssue(record.path, "size", f"expected {record.size}, found {current.st_size}")
            )
            return issues
        digest = file_sha256(path)
        if digest != record.sha256:
            issues.append(VerificationIssue(record.path, "sha256", "content digest does not match"))
        return issues

    if jobs < 1:
        raise BackupError("jobs must be at least 1")
    if jobs == 1:
        nested = [verify_one(record) for record in records]
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=jobs) as executor:
            nested = list(executor.map(verify_one, records))
    return [issue for group in nested for issue in group]


def verify_backup(
    manifest_path: Path,
    key: Optional[bytes] = None,
    jobs: int = 1,
    chain: bool = True,
    strict: bool = False,
) -> List[VerificationIssue]:
    """Verify a manifest, its payload, and optionally its complete chain."""

    layers = load_chain(manifest_path, key) if chain else [load_chain(manifest_path, key)[-1]]
    issues: List[VerificationIssue] = []
    for layer in layers:
        issues.extend(verify_layer(layer, jobs, include_unchanged=len(layers) == 1))
    if strict:
        for layer in layers:
            expected = {
                record.path
                for record in layer.records
                if record.state in {STATE_ADDED, STATE_CHANGED}
                or (len(layers) == 1 and record.state == STATE_UNCHANGED)
            }
            actual = {relative for relative, _ in iter_regular_files(layer.data_root, (), False)}
            for relative in sorted(actual - expected):
                issues.append(VerificationIssue(relative, "extra", "file is not listed in manifest layer"))
    return sorted(issues, key=lambda issue: (issue.path, issue.code))


def restore_backup(
    manifest_path: Path,
    destination: Path,
    key: Optional[bytes] = None,
    jobs: int = 1,
    overwrite: bool = False,
    verify_first: bool = True,
) -> Dict[str, int]:
    """Restore the effective chain into a destination directory."""

    layers = load_chain(manifest_path, key)
    if verify_first:
        issues = verify_backup(manifest_path, key, jobs, chain=True, strict=False)
        if issues:
            raise VerificationError(f"backup verification failed with {len(issues)} issue(s)")
    plan = effective_files(layers)
    destination = destination.resolve()
    destination.mkdir(parents=True, exist_ok=True)

    def restore_one(item: Tuple[str, Tuple[Layer, FileRecord]]) -> int:
        relative, (layer, record) = item
        source = resolve_beneath(layer.data_root, relative)
        target = resolve_beneath(destination, relative)
        copy_file_atomic(source, target, record.mode, overwrite)
        if file_sha256(target) != record.sha256:
            raise VerificationError(f"restored file digest mismatch: {relative}")
        return record.size

    ordered = sorted(plan.items())
    if jobs == 1:
        sizes = [restore_one(item) for item in ordered]
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=jobs) as executor:
            sizes = list(executor.map(restore_one, ordered))
    return {"files": len(ordered), "bytes": sum(sizes), "layers": len(layers)}


def inspection(manifest_path: Path, key: Optional[bytes] = None) -> Dict[str, Any]:
    """Return a concise, machine-readable chain description."""

    layers = load_chain(manifest_path, key)
    effective = effective_files(layers)
    return {
        "manifest": str(manifest_path.resolve()),
        "layers": [
            {
                "manifest": str(layer.manifest_path),
                "data_root": str(layer.data_root),
                "created_at": layer.manifest["created_at"],
                "summary": layer.manifest["summary"],
            }
            for layer in layers
        ],
        "effective_files": len(effective),
        "effective_bytes": sum(record.size for _, record in effective.values()),
    }


def emit_json(value: Any, stream: Any = None) -> None:
    """Write stable human-readable JSON."""

    if stream is None:
        stream = sys.stdout
    json.dump(value, stream, sort_keys=True, indent=2, ensure_ascii=False)
    stream.write("\n")


def positive_integer(value: str) -> int:
    """Argparse converter for positive integers."""

    try:
        result = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if result < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return result


def add_common_key(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--hmac-key-file", type=Path, help="authenticate manifests with this key file")


def build_parser() -> argparse.ArgumentParser:
    """Create the command-line parser."""

    parser = argparse.ArgumentParser(
        prog="opentenbase_backup",
        description="Create, verify, and restore OpenTenBase backup manifests.",
    )
    parser.add_argument("--version", action="version", version="%(prog)s 1.0")
    subparsers = parser.add_subparsers(dest="command", required=True)

    manifest_parser = subparsers.add_parser("manifest", help="create a full or incremental manifest")
    manifest_parser.add_argument("root", type=Path, help="quiesced backup directory")
    manifest_parser.add_argument("--output", type=Path, required=True, help="manifest output path")
    manifest_parser.add_argument("--parent", type=Path, help="previous manifest")
    manifest_parser.add_argument("--exclude", action="append", default=[], help="relative glob to omit")
    manifest_parser.add_argument("--jobs", type=positive_integer, default=1)
    manifest_parser.add_argument("--follow-symlinks", action="store_true")
    add_common_key(manifest_parser)

    delta_parser = subparsers.add_parser("delta", help="materialize changed files since a parent")
    delta_parser.add_argument("root", type=Path, help="new complete backup directory")
    delta_parser.add_argument("--parent", type=Path, required=True)
    delta_parser.add_argument("--output-directory", type=Path, required=True)
    delta_parser.add_argument("--exclude", action="append", default=[])
    delta_parser.add_argument("--jobs", type=positive_integer, default=1)
    delta_parser.add_argument("--follow-symlinks", action="store_true")
    add_common_key(delta_parser)

    verify_parser = subparsers.add_parser("verify", help="verify backup content and chain")
    verify_parser.add_argument("manifest", type=Path)
    verify_parser.add_argument("--jobs", type=positive_integer, default=1)
    verify_parser.add_argument("--no-chain", action="store_true")
    verify_parser.add_argument("--strict", action="store_true")
    add_common_key(verify_parser)

    restore_parser = subparsers.add_parser("restore", help="restore a verified manifest chain")
    restore_parser.add_argument("manifest", type=Path)
    restore_parser.add_argument("destination", type=Path)
    restore_parser.add_argument("--jobs", type=positive_integer, default=1)
    restore_parser.add_argument("--overwrite", action="store_true")
    restore_parser.add_argument("--skip-verify", action="store_true")
    add_common_key(restore_parser)

    inspect_parser = subparsers.add_parser("inspect", help="print manifest chain metadata")
    inspect_parser.add_argument("manifest", type=Path)
    add_common_key(inspect_parser)
    return parser


def run(args: argparse.Namespace) -> int:
    """Execute one parsed command."""

    key = read_key(getattr(args, "hmac_key_file", None))
    if args.command == "manifest":
        result = create_manifest(
            args.root,
            args.output,
            args.parent,
            args.exclude,
            args.jobs,
            args.follow_symlinks,
            key,
        )
        emit_json(result["summary"])
        return 0
    if args.command == "delta":
        result = materialize_delta(
            args.root,
            args.parent,
            args.output_directory,
            args.exclude,
            args.jobs,
            args.follow_symlinks,
            key,
        )
        emit_json(result["summary"])
        return 0
    if args.command == "verify":
        issues = verify_backup(
            args.manifest,
            key,
            args.jobs,
            chain=not args.no_chain,
            strict=args.strict,
        )
        emit_json({"ok": not issues, "issues": [issue.to_dict() for issue in issues]})
        return 0 if not issues else 2
    if args.command == "restore":
        result = restore_backup(
            args.manifest,
            args.destination,
            key,
            args.jobs,
            args.overwrite,
            verify_first=not args.skip_verify,
        )
        emit_json(result)
        return 0
    if args.command == "inspect":
        emit_json(inspection(args.manifest, key))
        return 0
    raise BackupError(f"unsupported command: {args.command}")


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI entry point with stable error handling."""

    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return run(args)
    except (BackupError, OSError) as exc:
        print(f"opentenbase_backup: error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
