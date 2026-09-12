"""Crash-tolerant artifact storage for IVFFlat diagnosis runs."""

from __future__ import annotations

import csv
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
import json
import math
import os
from pathlib import Path
import tempfile
import threading
from typing import Any, Iterable, Mapping

from .schema import utc_now


def _json_default(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, (Path, Enum)):
        return str(value.value if isinstance(value, Enum) else value)
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    raise TypeError(f"{type(value).__name__} is not JSON serializable")


def _reject_non_finite(value: Any) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("artifacts cannot contain NaN or Infinity")
    if isinstance(value, Mapping):
        for item in value.values():
            _reject_non_finite(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _reject_non_finite(item)


def _target(root: Path, relative_path: str | Path) -> Path:
    root = root.resolve()
    target = (root / relative_path).resolve()
    if not target.is_relative_to(root):
        raise ValueError(f"artifact path escapes run directory: {relative_path}")
    target.parent.mkdir(parents=True, exist_ok=True)
    return target


def _atomic_text(target: Path, text: str) -> None:
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=target.parent, prefix=f".{target.name}.",
        delete=False
    ) as stream:
        temporary = Path(stream.name)
        try:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        except Exception:
            temporary.unlink(missing_ok=True)
            raise
    os.replace(temporary, target)


def write_artifact(root: Path, relative_path: str | Path, data: Any, *,
                   kind: str = "json", fieldnames: Iterable[str] | None = None) -> Path:
    """Write one artifact without updating an artifact index."""
    target = _target(root, relative_path)
    if kind == "json":
        _reject_non_finite(data)
        text = json.dumps(data, indent=2, ensure_ascii=False, allow_nan=False,
                          default=_json_default) + "\n"
        _atomic_text(target, text)
    elif kind == "text":
        _atomic_text(target, str(data))
    elif kind == "jsonl":
        _reject_non_finite(data)
        line = json.dumps(data, ensure_ascii=False, allow_nan=False,
                          default=_json_default) + "\n"
        with target.open("a", encoding="utf-8") as stream:
            stream.write(line)
            stream.flush()
    elif kind == "csv":
        rows = list(data)
        names = list(fieldnames or (rows[0].keys() if rows else ()))
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", newline="", dir=target.parent,
            prefix=f".{target.name}.", delete=False
        ) as stream:
            temporary = Path(stream.name)
            writer = csv.DictWriter(stream, names, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                cooked = {
                    key: json.dumps(value, ensure_ascii=False, default=_json_default)
                    if isinstance(value, (dict, list, tuple)) else value
                    for key, value in row.items()
                }
                writer.writerow(cooked)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
    else:
        raise ValueError(f"unsupported artifact kind: {kind}")
    return target


class RunStore:
    """Thread-safe writer that maintains an artifact index."""

    def __init__(self, root: Path, *, index_path: str | Path = "raw/artifact-index.json"):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.index_path = Path(index_path)
        self._lock = threading.RLock()
        self._artifacts: dict[str, dict[str, Any]] = {}

    def _record(self, relative_path: str | Path, kind: str,
                status: str = "complete", reason: str | None = None) -> None:
        key = Path(relative_path).as_posix()
        self._artifacts[key] = {
            "kind": kind,
            "status": status,
            "reason": reason,
            "updated_at": utc_now(),
        }
        write_artifact(self.root, self.index_path, {
            "updated_at": utc_now(),
            "artifacts": self._artifacts,
        })

    def write_json(self, relative_path: str | Path, data: Any) -> Path:
        with self._lock:
            target = write_artifact(self.root, relative_path, data, kind="json")
            self._record(relative_path, "json")
            return target

    def write_text(self, relative_path: str | Path, text: str) -> Path:
        with self._lock:
            target = write_artifact(self.root, relative_path, text, kind="text")
            self._record(relative_path, "text")
            return target

    def write_csv(self, relative_path: str | Path, rows: Iterable[Mapping[str, Any]],
                  fieldnames: Iterable[str] | None = None) -> Path:
        with self._lock:
            target = write_artifact(
                self.root, relative_path, rows, kind="csv", fieldnames=fieldnames
            )
            self._record(relative_path, "csv")
            return target

    def append_jsonl(self, relative_path: str | Path, record: Mapping[str, Any]) -> Path:
        with self._lock:
            target = write_artifact(self.root, relative_path, record, kind="jsonl")
            key = Path(relative_path).as_posix()
            if key not in self._artifacts:
                self._record(relative_path, "jsonl", status="streaming")
            return target

    def mark_complete(self, relative_path: str | Path) -> None:
        with self._lock:
            key = Path(relative_path).as_posix()
            if key in self._artifacts:
                self._artifacts[key]["status"] = "complete"
                self._artifacts[key]["updated_at"] = utc_now()
                self._record(key, self._artifacts[key]["kind"])

    def mark_incomplete(self, relative_path: str | Path, reason: str) -> None:
        with self._lock:
            kind = self._artifacts.get(Path(relative_path).as_posix(), {}).get("kind", "unknown")
            self._record(relative_path, kind, status="incomplete", reason=reason)
